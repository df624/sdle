import zmq
import json
from manager import ShoppingListManager

def main(worker_id):
    context = zmq.Context()

    # Connect worker on the proxy
    worker = context.socket(zmq.DEALER)
    worker.identity = f"worker{worker_id}".encode()
    worker.connect("tcp://localhost:5556")

    # Initialize the shopping list manager with a local database
    db_path = f"local_{worker_id}.db"
    print(f"Worker {worker_id} is using database: {db_path}")
    manager = ShoppingListManager(db_path)

    print(f"Worker {worker_id} is ready and waiting for tasks...")

    while True:
        # Receive message from the proxy
        message = worker.recv_multipart()
        #print(f"Worker {worker_id} raw message: {message}")

        try:
            # Ensure the message contains exactly 3 parts
            if len(message) != 2:
                raise ValueError(f"Expected 2 parts in message, got {len(message)}")

            client_id, request_raw = message
            request = json.loads(request_raw.decode())  # Decode the JSON payload
            print(f"Worker {worker_id} received request: {request}")

        except ValueError as e:
            print(f"Worker {worker_id} failed to decode message: {e}")
            # Respond with an error to the client
            if len(message) > 1:
                client_id = message[1]
            else:
                client_id = b"unknown_client"
            response = {"status": "error", "message": str(e)}
            worker.send_multipart([client_id, json.dumps(response).encode()])
            continue


        # Process the request
        #print("request is", request)
        action = request.get("action")
        #print("action is", action)

        response = {}
        if action == "view_all_lists":
            try:
                lists = manager.view_all_lists()
                response = {"status": "success", "lists": lists}
            except Exception as e:
                response = {"status": "error", "message": str(e)}
        elif action == "create_list":
            name = request.get("name")
            creator = request.get("creator")
            try:
                new_list = manager.create_list(name, creator)
                response = {"status": "success", "list": new_list}
            except Exception as e:
                response = {"status": "error", "message": str(e)}
        else:
            response = {"status": "error", "message": "Unknown action."}

        # Send response back to the client via proxy
        worker.send_multipart([client_id, json.dumps(response).encode()])
        print(f"Worker {worker_id} sent response: {response} to client {client_id}")
        #print([client_id, json.dumps(response).encode()]) 


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python worker.py <worker_id>")
        sys.exit(1)
    main(worker_id=sys.argv[1])
