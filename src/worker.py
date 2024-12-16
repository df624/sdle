import zmq
import json
from manager import ShoppingListManager
import threading

def main(worker_id):
    context = zmq.Context()

    # Connect worker to the proxy
    worker = context.socket(zmq.DEALER)
    worker.identity = f"worker{worker_id}".encode()
    worker.connect("tcp://localhost:5556")

    # Control socket for shutdown command
    control = context.socket(zmq.PULL)
    control_port = 6000 + int(worker_id)  # Unique port for each worker
    control.bind(f"tcp://*:{control_port}")
    print(f"Worker {worker_id} control socket bound to port {control_port}")

    db_path = f"server_{worker_id}.db"
    print(f"Worker {worker_id} is using database: {db_path}")
    manager = ShoppingListManager(db_path)

    print(f"Worker {worker_id} is ready and waiting for tasks...")

    poller = zmq.Poller()
    poller.register(worker, zmq.POLLIN)
    poller.register(control, zmq.POLLIN)

    running = True

    while running:
        sockets = dict(poller.poll())  # Wait for activity on either socket

        if control in sockets:
            # Control socket received a shutdown signal
            shutdown_signal = control.recv()
            print(f"Worker {worker_id} received shutdown signal: {shutdown_signal.decode()}")
            running = False

        if worker in sockets:
            # Main socket handles messages from the proxy
            message = worker.recv_multipart()

            try:
                if len(message) != 2:
                    raise ValueError(f"Expected 2 parts in message, got {len(message)}")

                client_id, request_raw = message
                request = json.loads(request_raw.decode())
                print(f"Worker {worker_id} received request: {request}")

            except ValueError as e:
                print(f"Worker {worker_id} failed to decode message: {e}")
                if len(message) > 1:
                    client_id = message[1]
                else:
                    client_id = b"unknown_client"
                response = {"status": "error", "message": str(e)}
                worker.send_multipart([client_id, json.dumps(response).encode()])
                continue

            # Process the request
            action = request.get("action")
            response = {}
            if action == "view_all_lists":
                try:
                    lists = manager.view_all_lists()
                    response = {"status": "success", "lists": lists}
                except Exception as e:
                    response = {"status": "error", "message": str(e)}
            elif action == "sync_list":
                try:
                    list_details = request.get("list", {})
                    url = list_details.get("url")
                    name = list_details.get("name")
                    creator = list_details.get("creator")
                    client_id_list = request.get("client_id")
                    manager.save_list(url, name, creator, client_id_list)
                    response = {"status": "success", "message": "List synchronized successfully."}
                except Exception as e:
                    response = {"status": "error", "message": str(e)}
            elif action == "shutdown":
                print(f"Worker {worker_id} shutting down from proxy command...")
                running = False
                response = {"status": "success", "message": "Worker shutting down."}
            else:
                response = {"status": "error", "message": "Unknown action."}

            worker.send_multipart([client_id, json.dumps(response).encode()])
            print(f"Worker {worker_id} sent response: {response}")

    print(f"Worker {worker_id} has shut down.")

if __name__ == "__main__":
    import sys
    if len(sys.argv) == 3 and sys.argv[2] == "--shutdown":
        # Send shutdown signal via the control socket
        worker_id = sys.argv[1]
        control_port = 6000 + int(worker_id)
        context = zmq.Context()
        socket = context.socket(zmq.PUSH)
        socket.connect(f"tcp://localhost:{control_port}")
        socket.send(b"shutdown")
        print(f"Sent shutdown signal to worker {worker_id}")
        sys.exit(0)
    elif len(sys.argv) < 2:
        print("Usage: python worker.py <worker_id> [--shutdown]")
        sys.exit(1)

    main(worker_id=sys.argv[1])
