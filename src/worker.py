import zmq
import time
import json
from manager import ShoppingListManager

def main():
    context = zmq.Context()

    # Connect to the DEALER socket on the broker
    worker = context.socket(zmq.REP)
    worker.connect("tcp://localhost:5556")

    # Initialize the shopping list manager with a local database
    manager = ShoppingListManager("local_1.db")

    print("Worker is ready and waiting for tasks...")

    while True:
        # Receive request from the broker
        request = worker.recv_json()
        print(f"Worker received request: {request}")

        time.sleep(1)

        # Handle the request
        action = request.get("action")
        if action == "view_all_lists":
            try:
                lists = manager.view_all_lists()
                response = {
                    "status": "success",
                    "lists": lists
                }
            except Exception as e:
                response = {
                    "status": "error",
                    "message": f"Failed to retrieve lists: {str(e)}"
                }
        elif action == "create_list":
            try:
                name = request.get("name")
                creator = request.get("creator")
                if not name or not creator:
                    raise ValueError("Name and creator are required.")

                new_list = manager.create_list(name, creator)
                response = {
                    "status": "success",
                    "message": f"List '{name}' created successfully.",
                    "list": new_list
                }
            except Exception as e:
                response = {
                    "status": "error",
                    "message": f"Failed to create list: {str(e)}"
                }
        elif action == "delete_list":
            try:
                list_url = request.get("list_url")
                if not list_url:
                    raise ValueError("List URL is required.")

                deleted_list = manager.delete_list(list_url)
                response = {
                    "status": "success",
                    "message": f"List '{deleted_list['url']}' deleted successfully.",
                    "list": deleted_list
                }
            except Exception as e:
                response = {
                    "status": "error",
                    "message": f"Failed to delete list: {str(e)}"
                }

        else:
            response = {"status": "error", "message": "Unknown action."}

        # Send response back to the broker
        worker.send_json(response)
        print(f"Worker sent response: {response}")

if __name__ == "__main__":
    main()
