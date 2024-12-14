import sqlite3
import uuid

class ShoppingListManager:
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
                active BOOLEAN DEFAULT TRUE
            )
        """)
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS item (
                name TEXT NOT NULL,
                list_url TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                bought BOOLEAN DEFAULT FALSE,
                deleted BOOLEAN DEFAULT FALSE,
                PRIMARY KEY (name, list_url),
                FOREIGN KEY (list_url) REFERENCES list (url)
            )
        """)
        self.db.commit()

    def create_list(self, name, creator):
        """Create a new shopping list."""
        url = str(uuid.uuid4()) 
        self.db.execute("INSERT INTO list (url, name, creator) VALUES (?, ?, ?)", (url, name, creator))
        self.db.commit()
        return {"url": url, "name": name, "creator": creator}

    def view_all_lists(self):
        """View all active shopping lists."""
        cursor = self.db.execute("SELECT url, name, creator FROM list WHERE active = 1")
        lists = cursor.fetchall()
        
        result = [{"url": lst[0], "name": lst[1], "creator": lst[2]} for lst in lists]
        
        return result


    def add_item(self, list_url, name, quantity):
        """Add an item to an existing shopping list."""
        cursor = self.db.execute("SELECT COUNT(*) FROM list WHERE url = ?", (list_url,))
        if cursor.fetchone()[0] == 0:
            raise ValueError(f"No list found with URL: {list_url}")

        self.db.execute("""
            INSERT INTO item (name, list_url, quantity) 
            VALUES (?, ?, ?)
        """, (name, list_url, quantity))
        self.db.commit()
        print(f"Item '{name}' added successfully to list '{list_url}'!")


    def view_items_in_list(self, list_url):
        """View items in a specific list for a specific client."""
        cursor = self.db.execute("""
            SELECT i.name, i.quantity, i.bought 
            FROM item AS i
            JOIN list AS l ON i.list_url = l.url
            WHERE i.list_url = ? AND i.deleted = 0
        """, (list_url))
        items = cursor.fetchall()
        if not items:
            print(f"No items found in the list with URL '{list_url}'.")
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

    def delete_list(self, list_url):
        """Delete a shopping list and its items."""
        # Checks if the list exists
        cursor = self.db.execute("SELECT COUNT(*) FROM list WHERE url = ?", (list_url,))
        if cursor.fetchone()[0] == 0:
            raise ValueError(f"No list found with URL: {list_url}")

        # Marks the list as inactive
        self.db.execute("UPDATE list SET active = 0 WHERE url = ?", (list_url,))

        # Marks all items in the list as deleted
        self.db.execute("UPDATE item SET deleted = 1 WHERE list_url = ?", (list_url,))
        self.db.commit()
        return {"url": list_url}

