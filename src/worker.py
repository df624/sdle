import zmq
import time
import os
from manager import ShoppingListManager

def main(port):
    context = zmq.Context()

    # Dynamically create the database path based on the worker's port
    database_path = f"worker_{port}.db"

    # Initialize the database if it does not exist
    if not os.path.exists(database_path):
        print(f"Initializing database for worker on port {port}...")
        manager = ShoppingListManager(database_path)
        del manager  

    # Create the shopping list manager for handling requests
    manager = ShoppingListManager(database_path)

    # REP socket for the worker
    worker = context.socket(zmq.REP)
    worker.connect("tcp://localhost:5556")  

    print(f"Worker running on port {port} with database {database_path}...")

    while True:
        try:
            # Receive request from the proxy
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
                        "message": "Lists retrieved successfully.",
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

            # Send response back to the proxy
            worker.send_json(response)
            print(f"Worker sent response: {response}")
        except KeyboardInterrupt:
            print(f"Worker on port {port} shutting down...")
            break

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python worker.py <port>")
        sys.exit(1)

    port = sys.argv[1]
    main(port)
