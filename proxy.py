import zmq
import json
import time
import threading
import logging
from datetime import datetime
from replication_manager import ReplicationManager
import sys

# Constants
FRONTEND_PORT = 5555
BACKEND_PORT = 5556
HEARTBEAT_PORT = 5557
SOCKET_TIMEOUT = 5000  # 5 seconds
HEARTBEAT_INTERVAL = 5
WORKER_TIMEOUT = 10

class WorkerRegistry:
    def __init__(self):
        self.workers = {}  # addr -> last_heartbeat
        self.lock = threading.Lock()
        self.logger = logging.getLogger('worker_registry')

    def register_worker(self, addr):
        with self.lock:
            is_new = addr not in self.workers
            self.workers[addr] = time.time()
            if is_new:
                self.logger.info(f"Registered new worker: {addr}")
            return is_new

    def remove_worker(self, addr):
        with self.lock:
            if addr in self.workers:
                self.logger.info(f"Removing worker: {addr}")
                del self.workers[addr]

    def get_active_workers(self):
        with self.lock:
            current_time = time.time()
            return {
                addr: last_seen
                for addr, last_seen in self.workers.items()
                if current_time - last_seen <= WORKER_TIMEOUT
            }

class ProxyServer:
    def __init__(self):
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger('proxy')

        # Initialize ZMQ context and sockets
        self.context = zmq.Context()
        self.frontend = self.context.socket(zmq.ROUTER)
        self.backend = self.context.socket(zmq.ROUTER)
        self.heartbeat = self.context.socket(zmq.REP)

        # Set socket timeouts
        self.backend.setsockopt(zmq.RCVTIMEO, SOCKET_TIMEOUT)
        self.heartbeat.setsockopt(zmq.RCVTIMEO, SOCKET_TIMEOUT)

        # Bind sockets
        self.frontend.bind(f"tcp://*:{FRONTEND_PORT}")
        self.backend.bind(f"tcp://*:{BACKEND_PORT}")
        self.heartbeat.bind(f"tcp://*:{HEARTBEAT_PORT}")

        # Initialize components
        self.replication_mgr = ReplicationManager()
        self.registry = WorkerRegistry()
        self.running = True

        self.logger.info("Proxy server initialized and ready")

    def handle_heartbeat(self):
        """Handle heartbeat messages from workers."""
        try:
            msg = self.heartbeat.recv_json(flags=zmq.NOBLOCK)
            worker_addr = msg["worker_address"]
            
            if self.registry.register_worker(worker_addr):
                self.logger.info(f"New worker joined: {worker_addr}")
                # Handle replication for new worker
                self.handle_worker_join(worker_addr)
                
            self.heartbeat.send_json({"status": "ack"})
            
        except zmq.Again:
            pass
        except Exception as e:
            self.logger.error(f"Error handling heartbeat: {e}")

    def handle_worker_join(self, worker_addr):
        """Handle new worker joining."""
        try:
            
            replication_needed = self.replication_mgr.handle_new_worker(worker_addr)
            
            # Initiate replication
            for data_key, (source, target) in replication_needed.items():
                self.request_replication(data_key, source, target)
                
        except Exception as e:
            self.logger.error(f"Error handling worker join: {e}")

    def handle_worker_leave(self, worker_addr):
        """Handle worker leaving."""
        try:
            self.registry.remove_worker(worker_addr)
            affected_data = self.replication_mgr.handle_worker_removal(worker_addr)
            
            for data_key, remaining_locations in affected_data.items():
                if remaining_locations:  
                    primary, replicas = self.replication_mgr.get_data_placement(data_key)
                    
                    for replica in replicas:
                        if replica not in remaining_locations:
                            source = next(iter(remaining_locations))
                            self.request_replication(data_key, source, replica)
                
        except Exception as e:
            self.logger.error(f"Error handling worker leave: {e}")

    def forward_request_to_worker(self, worker_addr, client_id, request_data):
        """Forward a request to a specific worker."""
        worker_port = worker_addr.split(":")[-1]
        worker_id = f"worker-{worker_port}".encode()
        
        try:
            self.logger.info(f"Forwarding to worker {worker_id}: {request_data}")
            
            # Send to worker
            frames = [worker_id, b""]
            if client_id:
                frames.extend([client_id, b""])
            frames.append(json.dumps(request_data).encode())
            
            self.backend.send_multipart(frames)
            
            
            try:
                response_frames = self.backend.recv_multipart()
                response = response_frames[-1]
                
                # Forward to client if this was a client request
                if client_id:
                    self.frontend.send_multipart([
                        client_id,
                        b"",
                        response
                    ])
                
                return True, response
                
            except zmq.Again:
                self.logger.error("Timeout waiting for worker response")
                return False, None
                
        except Exception as e:
            self.logger.error(f"Error forwarding request: {e}")
            return False, None

    def handle_request(self, client_id, request_data):
        """Handle client request with replication for write operations."""
        try:
            action = request_data.get('action')
            
            
            if action == 'delete_list':
                list_id = request_data.get('list_url')
            else:
                list_id = request_data.get('url')
                
            if not list_id:
                raise ValueError("No list identifier in request")

            # Handle delete operations
            if action == 'delete_list':
                
                known_locations = self.replication_mgr.get_list_locations(list_id)
                primary, replicas = self.replication_mgr.get_data_placement(list_id)
                
                success = False
                delete_response = None
                
                
                if primary in known_locations:
                    primary_success, response = self.forward_request_to_worker(
                        primary, 
                        client_id,
                        request_data
                    )
                    if primary_success:
                        response_data = json.loads(response)
                        if response_data.get("had_list"):
                            success = True
                            delete_response = response_data
                            self.replication_mgr.remove_list_location(list_id, primary)
                
                # Try replicas
                for replica in replicas:
                    if replica in known_locations:
                        replica_success, response = self.forward_request_to_worker(
                            replica,
                            None,  
                            request_data
                        )
                        if replica_success:
                            response_data = json.loads(response)
                            if response_data.get("had_list"):
                                self.replication_mgr.remove_list_location(list_id, replica)
                
                if success:
                    
                    self.replication_mgr.record_deletion(list_id)
                    
                    
                    self.frontend.send_multipart([
                        client_id,
                        b"",
                        json.dumps(delete_response).encode()
                    ])
                else:
                    error_response = {
                        "status": "error",
                        "message": "List not found"
                    }
                    self.frontend.send_multipart([
                        client_id,
                        b"",
                        json.dumps(error_response).encode()
                    ])
                
            # Handle create_list and other write operations
            elif action in ['create_list', 'update_list']:
                
                primary, replicas = self.replication_mgr.get_data_placement(list_id)
                
    
                success, response = self.forward_request_to_worker(
                    primary, 
                    client_id, 
                    request_data
                )
                
                if success:
                    response_data = json.loads(response)
                    
                    
                    if action == 'create_list':
                        self.replication_mgr.record_list_location(list_id, primary)
                    
                    
                    if response_data.get("status") == "success":
                        for replica in replicas:
                            replica_success, _ = self.forward_request_to_worker(
                                replica,
                                None,  
                                {
                                    "action": "replicate_write",
                                    "original_data": request_data
                                }
                            )
                            if replica_success and action == 'create_list':
                                self.replication_mgr.record_list_location(list_id, replica)
                
            # Handle read operations
            elif action in ['get_list', 'view_lists']:
                
                primary, replicas = self.replication_mgr.get_data_placement(list_id)
                success, response = self.forward_request_to_worker(
                    primary,
                    client_id,
                    request_data
                )
                
                
                if not success and replicas:
                    for replica in replicas:
                        success, response = self.forward_request_to_worker(
                            replica,
                            client_id,
                            request_data
                        )
                        if success:
                            break
                
                
                if not success:
                    error_response = {
                        "status": "error",
                        "message": "Unable to process read request"
                    }
                    self.frontend.send_multipart([
                        client_id,
                        b"",
                        json.dumps(error_response).encode()
                    ])
                    
            else:
                raise ValueError(f"Unknown action: {action}")
                    
        except Exception as e:
            self.logger.error(f"Error handling request: {e}")
            error_response = {
                "status": "error",
                "message": str(e)
            }
            self.frontend.send_multipart([
                client_id,
                b"",
                json.dumps(error_response).encode()
            ])

    def request_replication(self, data_key, source_worker, target_worker):
        """Request data replication between workers."""
        try:
            request = {
                "action": "replicate_data",
                "data_key": data_key,
                "target_worker": target_worker
            }
            
            success, response = self.forward_request_to_worker(
                source_worker,
                None, 
                request
            )
            
            if success:
                self.replication_mgr.record_list_location(data_key, target_worker)
                self.logger.info(f"Replication completed for {data_key} to {target_worker}")
            else:
                self.logger.error(f"Replication failed for {data_key} to {target_worker}")
                
        except Exception as e:
            self.logger.error(f"Error requesting replication: {e}")

    def cleanup_inactive_workers(self):
        """Remove inactive workers and handle their data."""
        active_workers = self.registry.get_active_workers()
        current_workers = set(self.registry.workers.keys())
        inactive_workers = current_workers - set(active_workers.keys())
        
        for worker in inactive_workers:
            self.logger.info(f"Removing inactive worker: {worker}")
            self.handle_worker_leave(worker)

    def run(self):
        """Main proxy server loop."""
        self.logger.info("Starting proxy server main loop")
        
        while self.running:
            try:
            
                self.handle_heartbeat()
                
                
                try:
                    frames = self.frontend.recv_multipart(flags=zmq.NOBLOCK)
                    if len(frames) == 3:
                        client_id, _, request = frames
                        request_data = json.loads(request)
                        self.logger.info(f"Request from {client_id}: {request_data}")
                        self.handle_request(client_id, request_data)
                except zmq.Again:
                    pass
                
                
                self.cleanup_inactive_workers()
                
            except Exception as e:
                self.logger.error(f"Error in main loop: {e}")
            
            time.sleep(0.001)

    def shutdown(self):
        """Clean shutdown of the proxy server."""
        self.logger.info("Shutting down proxy server...")
        self.running = False
        self.frontend.close()
        self.backend.close()
        self.heartbeat.close()
        self.context.term()
        self.logger.info("Proxy server shutdown complete")

if __name__ == "__main__":
    proxy = ProxyServer()
    
    # Handle shutdown signals
    import signal
    def signal_handler(signum, frame):
        print("\nReceived shutdown signal. Cleaning up...")
        proxy.shutdown()
        sys.exit(0)
        
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        proxy.run()
    except KeyboardInterrupt:
        proxy.shutdown()
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        proxy.shutdown()
        sys.exit(1)