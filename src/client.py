import zmq
import json
import random
import os
import hashlib
from manager_local import ShoppingListManagerLocal

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

    # Initialize local database and manager
    manager = ShoppingListManagerLocal(db_path)

    # Initialize ZeroMQ client for server communication
    context = zmq.Context()

    client_identity = generate_client_identity(client_id)
    client = context.socket(zmq.DEALER)
    client.identity = client_identity.encode()
    print(f"Client identity: {client.identity.decode()}")
    client.connect("tcp://localhost:5555")

    while True:
        print("\n--- Shopping List Client ---")
        print("1. View local shopping lists")
        print("2. Create a new shopping list (local and then synchronized in server-side)")
        print("3. Add item to a list (local and then synchronized in server-side)")
        print("4. View items in a local list")
        print("5. Delete a list (local and then synchronized in server-side)")
        print("6. View shopping lists synchronized in server")
        print("7. View items in shopping lists synchronized in server")
        print("9. Exit")

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
            name = input("Enter the name of the new list: ")
            creator = input("Enter the creator's name: ")
            try:
                new_list = manager.create_list(name, creator, client_id)
                print(f"\nNew list created locally: {new_list}")

                # Sync to server
                request = {"action": "sync_list", "list": new_list}
                sync_with_server(client, request)

                # Mark the list as synced locally after successful sync
                manager.mark_as_synced(list_url=new_list["url"])
            except Exception as e:
                print(f"Error creating list: {e}")

        elif choice == "3":
            list_url = input("Enter the list URL: ")
            item_name = input("Enter item name: ")
            quantity = int(input("Enter quantity: "))
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
                    },
                }
                sync_with_server(client, request)

                # Mark the item as synced locally after successful sync
                manager.mark_as_synced(list_url, item_name)
            except Exception as e:
                print(f"Error adding item: {e}")

        elif choice == "4":  
            list_url = input("Enter the list URL: ")
            try:
                print(f"\nItems in your list with URL {list_url}:")
                manager.view_items_in_list(list_url, client_id)
            except Exception as e:
                print(f"Error viewing items: {e}")

        elif choice == "5":
            list_url = input("Enter the URL of the list to delete: ")
            request = {"action": "delete_list", "list_url": list_url}
            try:
                manager.delete_list(list_url)
                print(f"Your list with URL '{list_url}' deleted locally.")

                # Sync with server
                request = {"action": "delete_list", "list_url": list_url}
                sync_with_server(client, request, manager, list_url)
            except Exception as e:
                print(f"Error deleting list: {e}")

        elif choice == "6":  
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


        elif choice == "7":
            list_url = input("Enter the list URL: ")
            print(f"\Retrieving items for list {list_url} from the server...")

            try:
                # Send request to server to get items
                request = {"action": "view_items", "list_url": list_url}
                print("\nClient sending request:", request)
                client.send_multipart([client.identity, json.dumps(request).encode()])
                client.setsockopt(zmq.RCVTIMEO, 5000)  # Timeout after 10 seconds

                # Wait for response
                print("\nClient waiting for response...")
                response_parts = client.recv_multipart()
                
                if response_parts:
                    response_raw = response_parts[0]
                    response = json.loads(response_raw.decode())

                    if response.get("status") == "success":
                        items = response.get("items", [])
                        if items:
                            print(f"\nItems in the list '{list_url}':")
                            for item in items:
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
    
        elif choice == "9":
            print("Exiting...")
            break
        else:
            print("Invalid choice. Try again.")
            continue


def sync_with_server(client, request):
    print("\nClient sending request:", request)
    """Send a request to the server and handle the response."""
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
                print("\nSync successful!")
            else:
                print(f"\nError from server: {response.get('message')}")
        else:
            print("\nEmpty response from server.")
    except zmq.Again:
        print("\nSync failed: Server timeout.")
    except Exception as e:
        print(f"Sync failed: {e}")

def generate_client_identity(client_id):
    return hashlib.sha256(client_id.encode('utf-8')).hexdigest()

if __name__ == "__main__":
    main()
