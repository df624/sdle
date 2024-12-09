import zmq
from hashring import HashRing
import json
import random

def main():
    context = zmq.Context()

    # Create ROUTER socket to accept client connections
    frontend = context.socket(zmq.ROUTER)
    frontend.bind("tcp://*:5555")

    # Create ROUTER socket for workers
    backend = context.socket(zmq.ROUTER)
    backend.bind("tcp://*:5556")

    # Worker identities for hash ring
    worker_addresses = [f"worker{i}".encode() for i in range(1, 6)]
    ring = HashRing(nodes=worker_addresses, replicas=10)

    


    print("Proxy is running...")

    poller = zmq.Poller()
    poller.register(frontend, zmq.POLLIN)
    poller.register(backend, zmq.POLLIN)

    while True:
        sockets = dict(poller.poll())

        # Handle messages from clients
        if frontend in sockets:
            client_msg = frontend.recv_multipart()

            print(f"Proxy received message from client: {client_msg}")

            client_id = client_msg[0]  # Client identity
            request = client_msg[2]  # The JSON request payload

            # Debugging: Log the raw request
            print(f"Raw request from client: {request}")

            try:
                # Decode the request JSON
                request_data = json.loads(request.decode())
                print(f"Decoded request from client {client_id.decode()}: {request_data}")
            except json.JSONDecodeError as e:
                print(f"Error decoding request: {e}")
                continue

            # Determine target worker using the hash ring
            key = request_data.get("key", f"{client_id.decode()}_{random.randint(1, 10000)}")
            target_worker = ring.get_node(key)

            

            # Forward the request to the worker
            backend.send_multipart([target_worker, client_id, request])
            print(f"Proxy sent message to worker: {[target_worker, client_id, request]}")



        # Handle messages from workers
        if backend in sockets:
            worker_msg = backend.recv_multipart()
            print(f"worker_msg: {worker_msg}")
            worker_id, client_id, response = worker_msg
            print(f"Proxy received message from worker: {response} to client {client_id.decode()}")

            frontend.send_multipart([client_id, response])
            print([client_id, response])
            print(f"Proxy sent raw response to client {client_id.decode()}: {response}")
            print(f"Proxy sent response to client {client_id.decode()}: {response.decode()}")

if __name__ == "__main__":
    main()
