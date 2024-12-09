import zmq
import json
import random

def main():
    context = zmq.Context()

    # Set a unique identity for each client
    client = context.socket(zmq.DEALER)
    client.identity = f"client_{random.randint(1000, 9999)}".encode()
    print(f"Client identity: {client.identity.decode()}")
    client.connect("tcp://localhost:5555")

    while True:
        print("\n--- Shopping List Client ---")
        print("1. View all shopping lists")
        print("2. Create a new shopping list")
        print("8. Delete a list")
        print("9. Exit")

        choice = input("Choose an option: ")
        if choice == "1":
            request = {"action": "view_all_lists"}

        elif choice == "2":
            name = input("Enter the name of the new list: ")
            creator = input("Enter the creator's name: ")
            request = {"action": "create_list", "name": name, "creator": creator}

        elif choice == "8":
            list_url = input("Enter the URL of the list to delete: ")
            request = {"action": "delete_list", "list_url": list_url}

        elif choice == "9":
            print("Exiting...")
            break
        else:
            print("Invalid choice. Try again.")
            continue

        # Send the request to the broker
        print("request is", request)
        print("request encoded is", json.dumps(request).encode())
        print("\nClient sending request:", request)
        client.send_multipart([client.identity, json.dumps(request).encode()])
        print("Client sent request:", json.dumps(request).encode())
        print(client.identity)

        client.setsockopt(zmq.RCVTIMEO, 10000)  # Timeout after 5 seconds
        
        try:
            print("\nClient waiting for response...")
            response_parts = client.recv_multipart()  # Receive multipart message

            # Assuming the response is a list where the first part contains the JSON response
            if response_parts:
                response_raw = response_parts[0]  # Extract the first part
                response = json.loads(response_raw.decode())  # Decode and parse JSON

                print("\nClient received response:", response)
            else:
                print("\nClient received an empty response.")
        except zmq.Again:
            print("\nClient timed out waiting for response.")
            continue  # Skip further processing if no response
        except Exception as e:
            print(response)
            #print(json.loads(client_msg.decode()))
            print(f"Error receiving response: {e}")
            continue  # Skip further processing if there's an error

        status = response.get("status")
        print("status is", status)
        if status == "success":
            if choice == "1" and "lists" in response:
                if response["lists"]:
                    print("\nActive shopping lists:")
                    for lst in response["lists"]:
                        print(f"- URL: {lst['url']}\n  Name: {lst['name']}\n  Creator: {lst['creator']}\n")
                else:
                    print("\nThere are no lists to show.\n")
            elif choice == "2" and "list" in response:
                new_list = response["list"]
                print(f"\nNew list created successfully!")
                print(f"URL: {new_list['url']}, Name: {new_list['name']}, Creator: {new_list['creator']}\n")
            elif choice == "8":
                deleted_list = response["list"]
                print(f"\nList deleted successfully!")
                print(f"URL: {deleted_list['url']}\n")
        else:
            print(f"\nError: {response.get('message')}\n")

if __name__ == "__main__":
    main()
