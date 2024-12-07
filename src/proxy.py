import zmq

def main():
    context = zmq.Context()

    # Create ROUTER socket to accept client connections
    frontend = context.socket(zmq.ROUTER)
    frontend.bind("tcp://*:5555")

    # Create DEALER socket to communicate with workers
    backend = context.socket(zmq.DEALER)
    backend.bind("tcp://*:5556")

    # Start the proxy to forward messages between frontend and backend
    print("Broker is running...")
    zmq.proxy(frontend, backend)

if __name__ == "__main__":
    main()
