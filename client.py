import zmq
import json
import random
import sqlite3
import uuid
import time
import threading
import logging
from datetime import datetime

# Constants
SYNC_INTERVAL = 5
MAX_RETRIES = 3
SOCKET_TIMEOUT = 5000  # 5 seconds

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
                    synced BOOLEAN DEFAULT FALSE,
                    deleted BOOLEAN DEFAULT FALSE,
                    retry_count INTEGER DEFAULT 0,
                    last_sync_attempt TIMESTAMP,
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

class LocalStorage:
    def __init__(self, client_id):
        self.db_path = f"client_{client_id}.db"
        self.client_id = client_id
        self.db = DatabaseConnection(self.db_path)
        self.logger = logging.getLogger(f'client_{client_id}')

    def create_list(self, name, creator):
        url = str(uuid.uuid4())
        try:
            self.db.execute("""
                INSERT INTO list (url, name, creator, synced, deleted, retry_count)
                VALUES (?, ?, ?, FALSE, FALSE, 0)
            """, (url, name, creator))
            return {"url": url, "name": name, "creator": creator}
        except Exception as e:
            self.logger.error(f"Error creating list: {e}")
            raise

    def reset_failed_retries(self):
        """Reset retry count for all lists that failed to sync"""
        try:
            self.db.execute("""
                UPDATE list 
                SET retry_count = 0
                WHERE synced = FALSE 
                AND retry_count >= 3
            """)
        except Exception as e:
            self.logger.error(f"Error resetting failed retries: {e}")
            raise

    def delete_list(self, url):
        try:
            cursor = self.db.execute("""
                SELECT synced, deleted FROM list 
                WHERE url = ?
            """, (url,))
            row = cursor.fetchone()
            
            if not row:
                raise ValueError(f"List {url} not found")
                
            was_synced, already_deleted = row
            
            if already_deleted:
                return  # Already deleted, nothing to do
                
            if was_synced:
                # Only mark for sync if it was previously synced
                self.db.execute("""
                    UPDATE list 
                    SET deleted = TRUE, 
                        synced = FALSE,
                        retry_count = 0,
                        last_modified = CURRENT_TIMESTAMP 
                    WHERE url = ?
                """, (url,))
            else:
                # If never synced, just mark as deleted locally
                self.db.execute("""
                    UPDATE list 
                    SET deleted = TRUE 
                    WHERE url = ?
                """, (url,))
        except Exception as e:
            self.logger.error(f"Error deleting list: {e}")
            raise

    def mark_as_synced(self, url):
        try:
            self.db.execute("""
                UPDATE list 
                SET synced = TRUE,
                    retry_count = 0,
                    last_sync_attempt = CURRENT_TIMESTAMP,
                    last_modified = CURRENT_TIMESTAMP 
                WHERE url = ?
            """, (url,))
        except Exception as e:
            self.logger.error(f"Error marking list as synced: {e}")
            raise

    def increment_retry_count(self, url):
        try:
            self.db.execute("""
                UPDATE list 
                SET retry_count = retry_count + 1,
                    last_sync_attempt = CURRENT_TIMESTAMP 
                WHERE url = ?
            """, (url,))
        except Exception as e:
            self.logger.error(f"Error incrementing retry count: {e}")
            raise

    def get_unsynced_lists(self):
        try:
            cursor = self.db.execute("""
                SELECT url, name, creator, deleted, retry_count, synced,
                    CAST((julianday('now') - julianday(last_sync_attempt)) * 24 * 60 AS INTEGER) as minutes_since_attempt
                FROM list 
                WHERE synced = FALSE 
                ORDER BY last_modified ASC
            """)  # Removed AND deleted = FALSE to include deleted items that need syncing
            
            return [{
                "url": row[0],
                "name": row[1],
                "creator": row[2],
                "deleted": bool(row[3]),
                "retry_count": row[4],
                "synced": bool(row[5]),
                "minutes_since_attempt": row[6]
            } for row in cursor.fetchall()]
        except Exception as e:
            self.logger.error(f"Error getting unsynced lists: {e}")
            return []

    def get_my_lists(self):
        try:
            cursor = self.db.execute("""
                SELECT url, name, creator, synced, retry_count 
                FROM list 
                WHERE deleted = FALSE
                ORDER BY last_modified DESC
            """)
            return [{
                "url": row[0],
                "name": row[1],
                "creator": row[2],
                "synced": bool(row[3]),
                "retry_count": row[4]
            } for row in cursor.fetchall()]
        except Exception as e:
            self.logger.error(f"Error getting lists: {e}")
            return []
    def mark_sync_failed(self, url):
        """Mark a list as permanently failed to sync after max retries"""
        try:
            self.db.execute("""
                UPDATE list 
                SET synced = TRUE,  -- Mark as synced to prevent further attempts
                    last_sync_attempt = CURRENT_TIMESTAMP 
                WHERE url = ? 
                AND retry_count >= ?
            """, (url, MAX_RETRIES))
        except Exception as e:
            self.logger.error(f"Error marking sync failed: {e}")
            raise


def setup_sync_socket(context, client_id):
    sync_socket = context.socket(zmq.REQ)
    sync_socket.identity = f"client_{client_id}".encode()
    sync_socket.connect("tcp://localhost:5555")
    sync_socket.setsockopt(zmq.RCVTIMEO, SOCKET_TIMEOUT)
    sync_socket.setsockopt(zmq.LINGER, 0)
    return sync_socket

def auto_sync_thread(context, local_storage, client_id, stop_event):
    sync_socket = setup_sync_socket(context, client_id)
    logger = logging.getLogger(f'sync_thread_{client_id}')
    delays = [1, 30, 60]  # Retry delays in seconds
    
    logger.info("Sync thread started")
    
    while not stop_event.is_set():
        try:
            unsynced_changes = local_storage.get_unsynced_lists()
            
            if unsynced_changes:
                had_success = False
                
                for lst in unsynced_changes:
                    try:
                        # Skip if max retries reached
                        if lst["retry_count"] >= MAX_RETRIES:
                            local_storage.mark_sync_failed(lst["url"])
                            logger.warning(f"Max retries reached for list {lst['url']}, marking as failed")
                            continue
                            
                        if lst["deleted"]:
                            request = {
                                "action": "delete_list",
                                "list_url": lst["url"]
                            }
                        else:
                            request = {
                                "action": "create_list",
                                "name": lst["name"],
                                "creator": lst["creator"],
                                "url": lst["url"]
                            }
                        
                        try:
                            sync_socket.send_json(request)
                            response = sync_socket.recv_json()
                            
                            if response.get("status") == "success":
                                local_storage.mark_as_synced(lst["url"])
                                had_success = True
                            else:
                                if lst["retry_count"] < len(delays):
                                    local_storage.increment_retry_count(lst["url"])
                                    delay = delays[lst["retry_count"]]
                                    time.sleep(delay)
                                
                        except zmq.Again:
                            if lst["retry_count"] < MAX_RETRIES:
                                local_storage.increment_retry_count(lst["url"])
                                delay = delays[min(lst["retry_count"], len(delays)-1)]
                                time.sleep(delay)
                            
                            sync_socket.close()
                            sync_socket = setup_sync_socket(context, client_id)

                    except Exception as e:
                        if lst["retry_count"] < MAX_RETRIES:
                            local_storage.increment_retry_count(lst["url"])
                            delay = delays[min(lst["retry_count"], len(delays)-1)]
                            time.sleep(delay)
                        
                        sync_socket.close()
                        sync_socket = setup_sync_socket(context, client_id)

                if had_success:
                    local_storage.reset_failed_retries()

            time.sleep(SYNC_INTERVAL)
                
        except Exception as e:
            logger.error(f"Error in sync thread: {e}")
            time.sleep(SYNC_INTERVAL)
            
            sync_socket.close() 
            sync_socket = setup_sync_socket(context, client_id)

    sync_socket.close()
    logger.info("Sync thread stopped")

def setup_logging(client_id):
    logger = logging.getLogger(f'client_{client_id}')
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

def main():
    client_id = random.randint(1000, 9999)
    logger = setup_logging(client_id)
    logger.info(f"Client started with ID: client_{client_id}")

    context = zmq.Context()
    local_storage = LocalStorage(client_id)

    stop_event = threading.Event()
    sync_thread = threading.Thread(
        target=auto_sync_thread,
        args=(context, local_storage, client_id, stop_event),
        daemon=True
    )
    sync_thread.start()

    while True:
        try:
            print("\n=== Shopping List Client ===")
            print("1. View all shopping lists")
            print("2. Create a new shopping list")
            print("3. Delete a list")
            print("4. View sync status")
            print("5. Exit")

            choice = input("\nChoose an option: ")

            if choice == "1":
                lists = local_storage.get_my_lists()
                if lists:
                    print("\nMy Shopping Lists:")
                    for lst in lists:
                        sync_status = "synced" if lst["synced"] else f"not synced (retries: {lst['retry_count']})"
                        print(
                            f"Name: {lst['name']}, Creator: {lst['creator']}, "
                            f"URL: {lst['url']} ({sync_status})"
                        )
                else:
                    print("\nNo lists found.")

            elif choice == "2":
                name = input("Enter the name of the new list: ")
                creator = input("Enter the creator's name: ")
                new_list = local_storage.create_list(name, creator)
                print(
                    f"\nNew list created! Name: {new_list['name']}, "
                    f"Creator: {new_list['creator']}, URL: {new_list['url']}"
                )
                print("List will be synced with server...")

            elif choice == "3":
                lists = local_storage.get_my_lists()
                if lists:
                    print("\nAvailable lists:")
                    for lst in lists:
                        print(f"Name: {lst['name']}, URL: {lst['url']}")
                    url = input("\nEnter the URL of the list to delete: ")
                    local_storage.delete_list(url)
                    print("List deleted.")
                else:
                    print("\nNo lists available to delete.")

            elif choice == "4":
                unsynced = local_storage.get_unsynced_lists()
                if unsynced:
                    print("\nUnsynced Changes:")
                    for lst in unsynced:
                        print(f"URL: {lst['url']} (not synced, retries: {lst['retry_count']})")
                else:
                    print("\nAll lists are synced.")

            elif choice == "5":
                print("Shutting down...")
                stop_event.set()
                sync_thread.join(timeout=2)
                break
                
            else:
                print("Invalid choice. Please try again.")

        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            print(f"\nAn error occurred: {e}")
            print("Please try again.")

    context.term()
    logger.info("Client shutdown complete")

if __name__ == "__main__":
    main()