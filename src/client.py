import zmq
import json
import os
import hashlib
import threading
import time
from manager import ShoppingListManager

def main():

    client_id = input("Enter your client ID (or create a new one): ").strip()

    if not client_id:
        print("Client ID cannot be empty. Please enter a valid ID.")
        return
    
    # path to local client database
    db_path = f"{client_id}_client_local.db"

    # Check if the database already exists
    if os.path.exists(db_path):
        print(f"Welcome back, {client_id}! Loading your shopping list database...")
    else:
        print(f"Creating a new shopping list database for {client_id}...")

    # Initialize local database
    manager = ShoppingListManager(db_path)

    # Initialize ZeroMQ client for server communication
    context = zmq.Context()

    client_identity = generate_client_identity(client_id)
    client = context.socket(zmq.DEALER)
    client.identity = client_identity.encode()
    print(f"Client identity: {client.identity.decode()}")
    client.connect("tcp://localhost:5555")

    # Start polling in a separate thread
    threading.Thread(target=polling_and_sync, args=(client, manager, client_id), daemon=True).start()

    while True:
        print("\n--- Shopping List Client ---")
        print("1. View local shopping lists")
        print("2. View shopping lists synchronized in server")
        print("3. Create a new shopping list (local and then synchronized in server-side)")
        print("4. Delete locally a shopping list")
        print("5. Add item to a shopping list (local and then synchronized in server-side)")
        print("6. View items in a local shopping list")
        print("7. View items in shopping lists synchronized in server")
        print("8. Create a new local shopping list")
        print("9. Add item to a local shopping list")
        print("10. Exit")

        choice = input("Choose an option: ")
        if choice == "1":
            try:
                lists = manager.view_all_lists_local(client_id)
                request = {}
                if lists:
                    print(f"\n{client_id}'s shopping lists:")
                    for lst in lists:
                        print(f"URL: {lst['url']}, Name: {lst['name']}, Creator: {lst['creator']}")
                else:
                    print("\nNo active lists found.")
            except Exception as e:
                print(f"Error viewing lists: {e}")

        elif choice == "2":  
            print("\Retrieving shopping lists from the server...")

            try:
                request = {"action": "view_all_lists"}

                print("\nClient sending request:", request)
                client.send_multipart([client.identity, json.dumps(request).encode()])
                
                client.setsockopt(zmq.RCVTIMEO, 5000)

                print("\nClient waiting for response...")
                response_parts = client.recv_multipart() 

                if response_parts:
                    response_raw = response_parts[0] 
                    response = json.loads(response_raw.decode())  

                    print("\nClient received response:", response)

                    if response.get("status") == "success":
                        if response["lists"]:
                            print("\nShopping lists synchronized with the server:")
                            for lst in response["lists"]:
                                print(f"- URL: {lst['url']}\n  Name: {lst['name']}\n  Creator: {lst['creator']}\n")
                        else:
                            print("\nNo active shopping lists found on the server.")
                    else:
                        print(f"\nError retrieving lists from server: {response.get('message')}")

                else:
                    print("\nEmpty response from server.")

            except zmq.Again:
                print("\nFailed to retrieve lists: Server timeout.")
            except Exception as e:
                print(f"Error retrieving lists from server: {e}")


        elif choice == "3":
            name = input("Enter the name of the new list: ")
            creator = input("Enter the creator's name of the new list: ")
            try:
                new_list = manager.create_list(name, creator, client_id)
                print(f"\nNew list created locally: {new_list}")

                # Sync to server
                request = {"action": "sync_list", "list": new_list, "client_id": client_id}
                synchronization_response(client, request)

                manager.list_is_sync(new_list["url"])
            except Exception as e:
                print(f"Error creating list: {e}")

        elif choice == "4":
            list_url = input("Enter the URL of the shopping list to delete: ")
            try:
                manager.delete_list(list_url, client_id)
                print(f"Your list with URL '{list_url}' deleted locally.")

            except Exception as e:
                print(f"Error deleting list: {e}")

        elif choice == "5":
            list_url = input("Enter the shopping list URL: ")
            item_name = input("Enter item name: ")
            quantity = int(input("Enter quantity: "))
            #tag = f"{time.time()}_{client_id}"

            try:
                manager.add_item(list_url, item_name, quantity, client_id)
                print(f"Item '{item_name}' added locally to your list '{list_url}'.")

                # Send the item to the server for synchronization
                request = {
                    "action": "sync_item",
                    "item": {
                        "list_url": list_url,
                        "name": item_name,
                        "quantity": quantity,
                        #"tag": tag,
                    },
                }
                synchronization_response(client, request)

                manager.item_is_sync(item_name, list_url)
            except Exception as e:
                print(f"Error adding item: {e}")

        elif choice == "6":  
            list_url = input("Enter the shopping list URL: ")
            try:
                print(f"\nItems in your list with URL {list_url}:")
                manager.view_items_in_list_local(list_url, client_id)
            except Exception as e:
                print(f"Error viewing items: {e}")

        elif choice == "7":
            list_url = input("Enter the shopping list URL: ")
            print(f"\Retrieving items for shopping list {list_url} from the server...")

            try:
                # Send request to server to get items
                request = {"action": "view_items", "list_url": list_url}
                print("\nClient sending request:", request)
                client.send_multipart([client.identity, json.dumps(request).encode()])
                client.setsockopt(zmq.RCVTIMEO, 5000)  # Timeout after 10 seconds

                print("\nClient waiting for response...")
                response_parts = client.recv_multipart()
                
                if response_parts:
                    response_raw = response_parts[0]
                    response = json.loads(response_raw.decode())

                    if response.get("status") == "success":
                        if response["items"]:
                            print(f"\nItems in the list '{list_url}':")
                            for item in response["items"]:
                                status = "Bought" if item["bought"] else "Not Bought"
                                print(f"- Name: {item['name']}, Quantity: {item['quantity']}, Status: {status}")
                        else:
                            print(f"\nNo items found in the list '{list_url}'.")
                    else:
                        print(f"\nError retrieving items from server: {response.get('message')}")
                else:
                    print("\nEmpty response from server.")
            except zmq.Again:
                print("\nFailed to retrieve items: Server timeout.")
            except Exception as e:
                print(f"Error retrieving items from server: {e}")

        elif choice == "8":
            name = input("Enter the name of the new list: ")
            creator = input("Enter the creator's name of the new list: ")
            try:
                new_list = manager.create_list(name, creator, client_id)
                print(f"\nNew list created locally: {new_list}")
            except Exception as e:
                print(f"Error creating list: {e}")

        elif choice == "9":
            list_url = input("Enter the shopping list URL: ")
            item_name = input("Enter item name: ")
            quantity = int(input("Enter quantity: "))
            try:
                manager.add_item(list_url, item_name, quantity, client_id)
                print(f"Item '{item_name}' added locally to your list '{list_url}'.")
            except Exception as e:
                print(f"Error adding item: {e}")

        elif choice == "10":
            print("Exiting...")
            break
        else:
            print("Invalid choice. Try again.")
            continue


def synchronization_response(client, request):
    print("\nClient sending request:", request)
    client.send_multipart([client.identity, json.dumps(request).encode()])
    client.setsockopt(zmq.RCVTIMEO, 5000)  # Timeout after 5 seconds

    try:
        print("\nWaiting for server response...")
        response_parts = client.recv_multipart()

        if response_parts:
            response_raw = response_parts[0]
            response = json.loads(response_raw.decode())

            print("\nClient received response:", response)

            if response.get("status") == "success":
                print("\nSynchronization successful!")
            else:
                print(f"\nError from server: {response.get('message')}")
        else:
            print("\nEmpty response from server.")
    except zmq.Again:
        print("\nSync failed: Server timeout.")
    except Exception as e:
        print(f"Sync failed: {e}")


def polling_and_sync(client, manager, client_id):

    print("Starting polling and synchronization every 10 seconds...")
    while True:
        try:
            unsynced_lists = manager.get_unsynced_lists()
            unsynced_items = manager.get_unsynced_items()

            for list in unsynced_lists:
                request = {"action": "polling_list", "list": list, "client_id": client_id}
                synchronize_server(client, request, manager)

            for item in unsynced_items:
                request = {"action": "polling_item", "item": item, "client_id": client_id}
                synchronize_server(client, request, manager)

            #if not unsynced_lists and not unsynced_items:
                #print("\nNo changed data founded to be synchronized.")
        except Exception as e:
            print(f"Error during synchronization: {e}")

        # Poll every 10 seconds
        #print("Polling and synchronization cycle complete. Waiting for 20 seconds...")
        time.sleep(10)
    

def synchronize_server(client, request, manager):
    print("\nClient sending request:", request)
    client.send_multipart([client.identity, json.dumps(request).encode()])
    client.setsockopt(zmq.RCVTIMEO, 5000)

    try:
        response_parts = client.recv_multipart()
        response = json.loads(response_parts[0].decode())

        if response.get("status") == "success":
            action = response.get("action")
            if action == "sync_list":
                manager.list_is_sync(response.get("list_url"))
            elif action == "sync_item":
                manager.item_is_sync(response.get("name"), response.get("list_url"))
            print(f"Successfully synchronized: {action}")
        else:
            print(f"Error from server: {response.get('message')}")

    except zmq.Again:
        print("Sync failed: Server timeout.")
    except Exception as e:
        print(f"Sync failed: {e}")



def generate_client_identity(client_id):
    return hashlib.sha256(client_id.encode('utf-8')).hexdigest()

if __name__ == "__main__":
    main()
