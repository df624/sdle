import zmq
from hashring import HashRing
import json

def main():
    context = zmq.Context()

    # Create ROUTER socket for client connections
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

            client_id = client_msg[0] 
            request = client_msg[2]  

            print("client id:", client_id)


            key = client_id.decode()
            target_worker = ring.get_node(key)


            # send request to the worker
            backend.send_multipart([target_worker, client_id, request])
            print(f"Proxy sent message to worker: {[target_worker, client_id, request]}")

        # Handle messages from workers
        if backend in sockets:
            worker_msg = backend.recv_multipart()
            #print(f"worker_msg: {worker_msg}")
            worker_id, client_id, response = worker_msg
            print(f"Proxy received message from worker: {response} to client {client_id.decode()}")

            frontend.send_multipart([client_id, response])
            #print([client_id, response])
            #print(f"Proxy sent raw response to client {client_id.decode()}: {response}")
            print(f"Proxy sent response to client {client_id.decode()}: {response.decode()}")

if __name__ == "__main__":
    main()
