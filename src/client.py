import zmq
import json

def main():
    context = zmq.Context()

    # connection between client and broker
    client = context.socket(zmq.REQ)
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
        print("\nClient sending request:", request)
        client.send_json(request)

        # Wait for the response
        response = client.recv_json()
        
        if response.get("status") == "success":
            if choice == "1" and "lists" in response:
                print("\nActive shopping lists:")
                for lst in response["lists"]:
                    print(f"- URL: {lst['url']}\n  Name: {lst['name']}\n  Creator: {lst['creator']}\n")
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
