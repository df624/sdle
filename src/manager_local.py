import sqlite3
import uuid

class ShoppingListManagerLocal:
    def __init__(self, db_path):
        self.db = sqlite3.connect(db_path)
        self.initialize_database()

    def initialize_database(self):
        """Initialize the database with the required tables."""
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS list (
                url TEXT NOT NULL PRIMARY KEY,
                name TEXT,
                creator TEXT,
                client_id TEXT NOT NULL,
                active BOOLEAN DEFAULT TRUE,
                sync_status TEXT DEFAULT 'unsynced'
            )
        """)
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS item (
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
        """Create a new shopping list."""
        url = str(uuid.uuid4()) 
        self.db.execute("INSERT INTO list (url, name, creator, client_id) VALUES (?, ?, ?, ?)", (url, name, creator, client_id))
        self.db.commit()
        return {"url": url, "name": name, "creator": creator}

    def view_all_lists(self):
        """View all active shopping lists."""
        cursor = self.db.execute("SELECT url, name, creator FROM list WHERE active = 1")
        lists = cursor.fetchall()
        
        result = [{"url": lst[0], "name": lst[1], "creator": lst[2]} for lst in lists]
        
        return result
    
    def view_all_lists_local(self, client_id):
        """View all active shopping lists."""
        cursor = self.db.execute("SELECT url, name, creator FROM list WHERE client_id = ? AND active = 1", (client_id,))
        lists = cursor.fetchall()
        
        result = [{"url": lst[0], "name": lst[1], "creator": lst[2]} for lst in lists]
        
        return result


    def add_item(self, list_url, name, quantity, client_id):
        """Add an item to an existing shopping list."""
        cursor = self.db.execute("SELECT COUNT(*) FROM list WHERE url = ? AND client_id = ?", (list_url,client_id,))
        if cursor.fetchone()[0] == 0:
            raise ValueError(f"No list found with URL: {list_url}")

        self.db.execute("""
            INSERT INTO item (name, list_url, quantity) 
            VALUES (?, ?, ?)
        """, (name, list_url, quantity))
        self.db.commit()
        print(f"Item '{name}' added successfully to list '{list_url}'!")


    def view_items_in_list(self, list_url, client_id):
        """View items in a specific list for a specific client."""
        cursor = self.db.execute("""
            SELECT i.name, i.quantity, i.bought 
            FROM item AS i
            JOIN list AS l ON i.list_url = l.url
            WHERE i.list_url = ? AND l.client_id = ? AND i.deleted = 0
        """, (list_url, client_id))
        items = cursor.fetchall()
        if not items:
            print(f"No items found in the list with URL '{list_url}' for client '{client_id}'.")
        else:
            for item in items:
                status = "Bought" if item[2] else "Not Bought"
                print(f"Item: {item[0]}, Quantity: {item[1]}, Status: {status}")


    def update_item(self, list_url, name, quantity=None, bought=None):
        """Update an existing item in a shopping list."""
        cursor = self.db.execute("""
            SELECT COUNT(*) FROM item WHERE list_url = ? AND name = ? AND deleted = 0
        """, (list_url, name))
        if cursor.fetchone()[0] == 0:
            raise ValueError(f"No active item '{name}' found in list '{list_url}'")

        updates = []
        params = []
        if quantity is not None:
            updates.append("quantity = ?")
            params.append(quantity)
        if bought is not None:
            updates.append("bought = ?")
            params.append(bought)

        if updates:
            params.append(list_url)
            params.append(name)
            update_query = f"UPDATE item SET {', '.join(updates)} WHERE list_url = ? AND name = ?"
            self.db.execute(update_query, params)
            self.db.commit()
            print(f"Item '{name}' updated successfully in list '{list_url}'!")
        else:
            print("No updates were provided.")

    def delete_list(self, list_url, client_id):
        """Delete a shopping list and its items."""
        # Checks if the list exists
        cursor = self.db.execute("SELECT COUNT(*) FROM list WHERE url = ? AND client_id = ?", (list_url,client_id,))
        if cursor.fetchone()[0] == 0:
            raise ValueError(f"No list found with URL: {list_url}")

        # Marks the list as inactive
        self.db.execute("UPDATE list SET active = 0 WHERE url = ?", (list_url,))

        # Marks all items in the list as deleted
        self.db.execute("UPDATE item SET deleted = 1 WHERE list_url = ?", (list_url,))
        self.db.commit()
        return {"url": list_url}
    
    def mark_as_synced(self, list_url=None, item_name=None):
        """Mark lists or items as synced after successful server sync."""
        if list_url and item_name:
            self.db.execute("""
                UPDATE item SET sync_status = 'synced' 
                WHERE list_url = ? AND name = ?
            """, (list_url, item_name))
        elif list_url:
            self.db.execute("""
                UPDATE list SET sync_status = 'synced' 
                WHERE url = ?
            """, (list_url,))
        self.db.commit()

    def save_existing_list(self, url, name, creator):
        """Save an existing list into the server database."""
        # Check if the list already exists in the server database
        cursor = self.db.execute("SELECT COUNT(*) FROM list WHERE url = ?", (url,))
        if cursor.fetchone()[0] > 0:
            raise ValueError(f"List with URL '{url}' already exists in the server database.")

        # Save the list in the server database
        self.db.execute("""
            INSERT INTO list (url, name, creator, active) 
            VALUES (?, ?, ?, 1)
        """, (url, name, creator))
        self.db.commit()
        print(f"List '{name}' with URL '{url}' saved in the server database.")

    def save_existing_item(self, list_url, name, quantity):
        """Save an existing item into the server database."""
        # Check if the list exists in the server database
        cursor = self.db.execute("SELECT COUNT(*) FROM list WHERE url = ?", (list_url,))
        if cursor.fetchone()[0] == 0:
            raise ValueError(f"No list found with URL '{list_url}'.")
        
        print("\n", quantity,"\n")

        # Save the item in the server database
        self.db.execute("""
            INSERT INTO item (name, list_url, quantity) 
            VALUES (?, ?, ?)
        """, (name, list_url, quantity))
        self.db.commit()
        print(f"Item '{name}' added to list '{list_url}' in the server database.")

        

