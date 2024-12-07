import zmq
from dynamic_hash_ring import DynamicHashRing

def main():
    context = zmq.Context()

    try:
        # ROUTER socket to accept client connections
        frontend = context.socket(zmq.ROUTER)
        frontend.bind("tcp://*:5555")  # Proxy listens for client requests
        print("Frontend (ROUTER) bound to tcp://*:5555")

        # DEALER socket to communicate with workers
        backend = context.socket(zmq.DEALER)
        backend.bind("tcp://*:5556")  # Proxy communicates with workers
        print("Backend (DEALER) bound to tcp://*:5556")

        # Initialize a dynamic hash ring for worker management
        hash_ring = DynamicHashRing()

        # Define workers and add them to the hash ring
        workers = [
            "tcp://localhost:5561",
            "tcp://localhost:5562",
            "tcp://localhost:5563",
            "tcp://localhost:5564",
            "tcp://localhost:5565",
        ]
        for worker in workers:
            hash_ring.add_worker(worker)

        print("Proxy running with dynamic consistent hashing and replication...")

        while True:
            # Receive a message from the client
            client_id, _, request = frontend.recv_multipart()
            request_data = zmq.utils.jsonapi.loads(request)

            # Determine the primary worker and replicas based on the hash ring
            data_key = str(request_data.get("data_id", client_id))  
            assigned_workers = hash_ring.get_workers(data_key, num_replicas=3)  

            primary_worker = assigned_workers[0]
            replica_workers = assigned_workers[1:]

            print(f"Routing request with key {data_key} to primary: {primary_worker} and replicas: {replica_workers}")

            # Send request to the primary worker
            backend.send_multipart([primary_worker.encode(), b"", request])

            # Forward the request to replicas 
            for replica in replica_workers:
                backend.send_multipart([replica.encode(), b"", request])

            # Receive the response from the primary worker
            _, _, response = backend.recv_multipart()

            # Forward the response back to the client
            frontend.send_multipart([client_id, b"", response])

    except Exception as e:
        print(f"Error occurred in proxy: {e}")
    finally:
        frontend.close()
        backend.close()
        context.term()

if __name__ == "__main__":
    main()
