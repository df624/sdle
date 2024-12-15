import sqlite3
import uuid

class ShoppingListManager:
    def __init__(self, db_path):
        self.db = sqlite3.connect(db_path)
        self.initialize_database()

    def initialize_database(self):
        #Initialize the database with the required tables.
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS lists (
                url TEXT NOT NULL PRIMARY KEY,
                name TEXT,
                creator TEXT,
                client_id TEXT NOT NULL,
                active BOOLEAN DEFAULT TRUE,
                sync_status TEXT DEFAULT 'unsynced'
            )
        """)
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS items (
                name TEXT NOT NULL,
                list_url TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                bought BOOLEAN DEFAULT FALSE,
                deleted BOOLEAN DEFAULT FALSE,
                sync_status TEXT DEFAULT 'unsynced',
                PRIMARY KEY (name, list_url),
                FOREIGN KEY (list_url) REFERENCES list (url)
            )
        """)
        self.db.commit()

    def create_list(self, name, creator, client_id):
        #Create a new shopping list.
        url = str(uuid.uuid4()) 
        self.db.execute("INSERT INTO lists (url, name, creator, client_id) VALUES (?, ?, ?, ?)", (url, name, creator, client_id))
        self.db.commit()
        return {"url": url, "name": name, "creator": creator}

    def view_all_lists(self):
        #View all synchronized shopping lists.
        cursor = self.db.execute("SELECT url, name, creator FROM lists WHERE active = 1")
        lists = cursor.fetchall()
        
        result = [{"url": lst[0], "name": lst[1], "creator": lst[2]} for lst in lists]
        
        return result
    
    def view_all_lists_local(self, client_id):
        #View all local shopping lists 
        cursor = self.db.execute("SELECT url, name, creator FROM lists WHERE client_id = ? AND active = 1", (client_id,))
        lists = cursor.fetchall()
        
        result = [{"url": lst[0], "name": lst[1], "creator": lst[2]} for lst in lists]
        
        return result


    def add_item(self, list_url, name, quantity, client_id):
        #Add a new item to a shopping list
        cursor = self.db.execute("SELECT COUNT(*) FROM lists WHERE url = ? AND client_id = ?", (list_url,client_id,))
        if cursor.fetchone()[0] == 0:
            raise ValueError(f"No list found with URL: {list_url}")

        self.db.execute("""
            INSERT INTO items (name, list_url, quantity) 
            VALUES (?, ?, ?)
        """, (name, list_url, quantity))
        self.db.commit()
        print(f"Item '{name}' added successfully to list '{list_url}'!")


    def view_items_in_list(self, list_url):
        #View all items in a synchronized shopping list
        cursor = self.db.execute("""
            SELECT i.name, i.quantity, i.bought 
            FROM items AS i
            JOIN lists AS l ON i.list_url = l.url
            WHERE i.list_url = ? AND i.deleted = 0
        """, (list_url,))
        items = cursor.fetchall()
        if not items:
            print(f"No items found in the list with URL '{list_url}'.")
        else:
            items = [{"name": item[0], "quantity": item[1], "bought": item[2]} for item in items]
        
        return items

    def view_items_in_list_local(self, list_url, client_id):
        #View all items in a local shopping list
        cursor = self.db.execute("""
            SELECT i.name, i.quantity, i.bought 
            FROM items AS i
            JOIN lists AS l ON i.list_url = l.url
            WHERE i.list_url = ? AND l.client_id = ? AND i.deleted = 0
        """, (list_url, client_id))
        items = cursor.fetchall()
        if not items:
            print(f"No items found in the list with URL '{list_url}' for client '{client_id}'.")
        else:
            for item in items:
                status = "Bought" if item[2] else "Not Bought"
                print(f"Item: {item[0]}, Quantity: {item[1]}, Status: {status}")


    def delete_list(self, list_url, client_id):
        #Delete a shopping list
        cursor = self.db.execute("SELECT COUNT(*) FROM lists WHERE url = ? AND client_id = ?", (list_url,client_id,))
        if cursor.fetchone()[0] == 0:
            raise ValueError(f"No list found with URL: {list_url}")

        self.db.execute("UPDATE lists SET active = 0 WHERE url = ?", (list_url,))

        self.db.execute("UPDATE items SET deleted = 1 WHERE list_url = ?", (list_url,))
        self.db.commit()
        return {"url": list_url}

    def save_list(self, url, name, creator, client_id):
        #Save an already created list into the server database
        cursor = self.db.execute("SELECT COUNT(*) FROM lists WHERE url = ?", (url,))
        if cursor.fetchone()[0] > 0:
            raise ValueError(f"List with URL '{url}' already exists in the server database.")

        # Save the list in the server database
        self.db.execute("""
            INSERT INTO lists (url, name, creator, client_id, active) 
            VALUES (?, ?, ?, ?, 1)
        """, (url, name, creator, client_id))
        self.db.commit()
        print(f"List '{name}' with URL '{url}' saved in the server database.")

    def save_item(self, list_url, name, quantity):
        #Save an already created item into the server database
        cursor = self.db.execute("SELECT COUNT(*) FROM lists WHERE url = ?", (list_url,))
        if cursor.fetchone()[0] == 0:
            raise ValueError(f"No list found with URL '{list_url}'.")
        
        print("\n", quantity,"\n")

        # Save the item in the server database
        self.db.execute("""
            INSERT INTO items (name, list_url, quantity) 
            VALUES (?, ?, ?)
        """, (name, list_url, quantity))
        self.db.commit()
        print(f"Item '{name}' added to list '{list_url}' in the server database.")

        

