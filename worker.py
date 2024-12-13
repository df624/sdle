import zmq
import json
import time
import threading
import sqlite3
import sys
import logging
from datetime import datetime

# Constants
HEARTBEAT_INTERVAL = 5
WORKER_TIMEOUT = 10
SOCKET_TIMEOUT = 5000

class DatabaseConnection:
    def __init__(self, db_path):
        self.db_path = db_path
        self.lock = threading.Lock()
        self.init_database()

    def init_database(self):
        with self.get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS list (
                    url TEXT NOT NULL PRIMARY KEY,
                    name TEXT,
                    creator TEXT,
                    active BOOLEAN DEFAULT TRUE,
                    is_replica BOOLEAN DEFAULT FALSE,
                    source_worker TEXT,
                    last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()

    def get_connection(self):
        return sqlite3.connect(self.db_path)

    def execute(self, query, params=None):
        with self.lock:
            with self.get_connection() as conn:
                try:
                    if params is None:
                        result = conn.execute(query)
                    else:
                        result = conn.execute(query, params)
                    conn.commit()
                    return result
                except sqlite3.Error as e:
                    logging.error(f"Database error: {e}")
                    conn.rollback()
                    raise

class ShoppingListManager:
    def __init__(self, db_path):
        self.db = DatabaseConnection(db_path)
        self.logger = logging.getLogger(f'manager_{db_path}')

    def store_list(self, url, name, creator, is_replica=False, source_worker=None):
        try:
            self.db.execute("""
                INSERT OR REPLACE INTO list 
                (url, name, creator, active, is_replica, source_worker, last_modified)
                VALUES (?, ?, ?, TRUE, ?, ?, CURRENT_TIMESTAMP)
            """, (url, name, creator, is_replica, source_worker))
            return {"url": url, "name": name, "creator": creator}
        except Exception as e:
            self.logger.error(f"Error storing list: {e}")
            raise

    def delete_list(self, url):
        try:
            self.db.execute("""
                UPDATE list 
                SET active = FALSE, 
                    last_modified = CURRENT_TIMESTAMP 
                WHERE url = ?
            """, (url,))
            return {"url": url}
        except Exception as e:
            self.logger.error(f"Error deleting list: {e}")
            raise

    def get_list_status(self, url):
        """Check if a list exists and is active."""
        try:
            cursor = self.db.execute("""
                SELECT active 
                FROM list 
                WHERE url = ?
            """, (url,))
            row = cursor.fetchone()
            if row is not None:
                return {"exists": True, "active": bool(row[0])}
            return {"exists": False, "active": False}
        except Exception as e:
            self.logger.error(f"Error checking list status: {e}")
            raise

    def get_list_data(self, url):
        try:
            cursor = self.db.execute("""
                SELECT name, creator, is_replica, source_worker
                FROM list
                WHERE url = ? AND active = TRUE
            """, (url,))
            row = cursor.fetchone()
            if row:
                return {
                    "url": url,
                    "name": row[0],
                    "creator": row[1],
                    "is_replica": bool(row[2]),
                    "source_worker": row[3]
                }
            return None
        except Exception as e:
            self.logger.error(f"Error getting list data: {e}")
            raise

class Worker:
    def __init__(self, port):
        self.port = int(port)
        self.address = f"tcp://localhost:{self.port}"
        self.identity = f"worker-{self.port}"
        self.context = zmq.Context()
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(f'worker_{self.port}')
        
        self.manager = ShoppingListManager(f"worker_{self.port}.db")
        self.running = True
        
        self.setup_sockets()

    def setup_sockets(self):
        """Setup ZMQ sockets."""
        # Main worker socket
        self.worker_socket = self.context.socket(zmq.DEALER)
        self.worker_socket.setsockopt_string(zmq.IDENTITY, self.identity)
        self.worker_socket.setsockopt(zmq.RCVTIMEO, 1000)
        self.worker_socket.connect("tcp://localhost:5556")
        
        # Heartbeat socket
        self.heartbeat_socket = self.context.socket(zmq.REQ)
        self.heartbeat_socket.connect("tcp://localhost:5557")

        # Replication receiver socket
        self.replication_socket = self.context.socket(zmq.REP)
        replication_port = self.port + 1000
        self.replication_socket.bind(f"tcp://*:{replication_port}")

    def handle_request(self, request_data):
        """Process a client or replication request."""
        try:
            action = request_data.get("action")
            
            if action == "create_list":
                url = request_data.get("url")
                name = request_data.get("name")
                creator = request_data.get("creator")
                is_replica = request_data.get("is_replica", False)
                
                if not is_replica:
                    self.logger.info(f"Creating list as PRIMARY: {name} (id: {url})")
                else:
                    self.logger.info(f"Creating list as REPLICA: {name} (id: {url})")
                    
                stored_list = self.manager.store_list(url, name, creator)
                
                return {
                    "status": "success",
                    "message": "List created successfully",
                    "list": stored_list,
                    "requires_replication": not is_replica
                }
            
            elif action == "check_list":
                url = request_data.get("list_url")
                list_status = self.manager.get_list_status(url)
                return {
                    "status": "success",
                    "exists": list_status["exists"],
                    "active": list_status["active"]
                }
                
            elif action == "delete_list":
                url = request_data.get("list_url")
                list_status = self.manager.get_list_status(url)
                
                if list_status["exists"]:
                    if list_status["active"]:
                        self.manager.delete_list(url)
                        return {
                            "status": "success",
                            "message": "List deleted successfully",
                            "had_list": True
                        }
                    else:
                        return {
                            "status": "success",
                            "message": "List was already deleted",
                            "had_list": True
                        }
                else:
                    return {
                        "status": "error",
                        "message": "List not found",
                        "had_list": False
                    }

            elif action == "replicate_write":
                original_action = request_data.get("original_action")
                original_data = request_data.get("original_data")
                original_data["is_replica"] = True
                
                self.logger.info(f"Replicating {original_action} operation")
                
                return self.handle_request(original_data)

            elif action == "replicate_data":
                data_key = request_data.get("data_key")
                target_worker = request_data.get("target_worker")
                
                list_data = self.manager.get_list_data(data_key)
                if not list_data:
                    raise Exception(f"Data not found for replication: {data_key}")

                self.logger.info(f"Sending list {data_key} to replica {target_worker}")
                
                repl_socket = self.context.socket(zmq.REQ)
                target_port = int(target_worker.split(":")[-1]) + 1000
                repl_socket.connect(f"tcp://localhost:{target_port}")
                
                try:
                    repl_socket.send_json({
                        "action": "receive_replication",
                        "data": list_data,
                        "source_worker": self.address
                    })
                    
                    response = repl_socket.recv_json()
                    if response.get("status") != "success":
                        raise Exception(f"Replication failed: {response.get('message')}")
                    
                    return {
                        "status": "success",
                        "message": "Replication completed successfully"
                    }
                finally:
                    repl_socket.close()
                    
            elif action == "receive_replication":
                data = request_data.get("data")
                source_worker = request_data.get("source_worker")
                
                if not data:
                    raise Exception("No data received for replication")
                
                self.logger.info(f"Storing replicated list: {data['name']} (id: {data['url']})")
                
                self.manager.store_list(
                    url=data["url"],
                    name=data["name"],
                    creator=data["creator"],
                    is_replica=True,
                    source_worker=source_worker
                )
                
                return {
                    "status": "success",
                    "message": "Replication data received and stored"
                }
                
            else:
                raise ValueError(f"Unknown action: {action}")
                
        except Exception as e:
            self.logger.error(f"Error processing request: {e}")
            return {
                "status": "error",
                "message": str(e)
            }

    def send_heartbeat(self):
        """Send periodic heartbeats to proxy."""
        while self.running:
            try:
                self.heartbeat_socket.send_json({
                    "worker_address": self.address
                })
                response = self.heartbeat_socket.recv_json()
                if response.get("status") != "ack":
                    self.logger.warning("Invalid heartbeat response")
            except Exception as e:
                self.logger.error(f"Heartbeat error: {e}")
            time.sleep(HEARTBEAT_INTERVAL)

    def process_message(self, frames):
        """Process incoming message frames."""
        try:
            if len(frames) == 4:  
                empty1, client_id, empty2, request = frames
                self.logger.info(f"Processing client request: {request}")
            elif len(frames) == 2: 
                empty, request = frames
                client_id = None
                self.logger.info(f"Processing internal request: {request}")
            else:
                raise ValueError(f"Invalid message format: {frames}")

            request_data = json.loads(request)
            response = self.handle_request(request_data)
            response_encoded = json.dumps(response).encode()
            
            # Send response with appropriate framing
            if client_id:
                self.worker_socket.send_multipart([
                    b"",
                    client_id,
                    b"",
                    response_encoded
                ])
            else:
                self.worker_socket.send_multipart([
                    b"",
                    response_encoded
                ])

        except Exception as e:
            self.logger.error(f"Error processing message: {e}")
            error_response = {
                "status": "error",
                "message": f"Internal error: {str(e)}"
            }
            response_encoded = json.dumps(error_response).encode()
            
            if len(frames) == 4:
                self.worker_socket.send_multipart([
                    b"",
                    frames[1],
                    b"",
                    response_encoded
                ])
            else:
                self.worker_socket.send_multipart([
                    b"",
                    response_encoded
                ])

    def run(self):
        """Main worker loop."""
        self.logger.info(f"Worker started on port {self.port}")

        
        heartbeat_thread = threading.Thread(
            target=self.send_heartbeat,
            daemon=True
        )
        heartbeat_thread.start()

        # Setup polling
        poller = zmq.Poller()
        poller.register(self.worker_socket, zmq.POLLIN)
        poller.register(self.replication_socket, zmq.POLLIN)

        while self.running:
            try:
                socks = dict(poller.poll(1000))
                
                if self.worker_socket in socks:
                    frames = self.worker_socket.recv_multipart()
                    self.process_message(frames)
                    
                if self.replication_socket in socks:
                    request = self.replication_socket.recv_json()
                    response = self.handle_request(request)
                    self.replication_socket.send_json(response)

            except zmq.Again:
                continue
            except Exception as e:
                self.logger.error(f"Error in main loop: {e}")
                time.sleep(1)

    def shutdown(self):
        """Clean shutdown of the worker."""
        self.logger.info("Shutting down worker...")
        self.running = False
        self.worker_socket.close()
        self.heartbeat_socket.close()
        self.replication_socket.close()
        self.context.term()
        self.logger.info("Worker shutdown complete")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python worker.py <port>")
        sys.exit(1)

    port = sys.argv[1]
    worker = Worker(port)
    
    try:
        worker.run()
    except KeyboardInterrupt:
        worker.shutdown()
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        worker.shutdown()
        sys.exit(1)