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
                active BOOLEAN DEFAULT TRUE
            )
        """)#version INTEGER DEFAULT 0, last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP, is_synced BOOLEAN DEFAULT 0   
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS item (
                name TEXT NOT NULL,
                list_url TEXT NOT NULL,
                current_quantity INTEGER DEFAULT 0,
                total_quantity INTEGER NOT NULL,
                deleted BOOLEAN DEFAULT FALSE,
                PRIMARY KEY (name, list_url),
                FOREIGN KEY (list_url) REFERENCES list (url)
            )
        """) #version INTEGER DEFAULT 0, last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP, is_synced BOOLEAN DEFAULT 0
        self.db.commit()

    def create_list(self, name, creator):
        """Create a new shopping list."""
        url = str(uuid.uuid4())
        self.db.execute("INSERT INTO list (url, name, creator) VALUES (?, ?, ?)", (url, name, creator))
        self.db.commit()
        print(f"List '{name}' created successfully! URL: {url}")

    def view_all_lists(self):
        """View all active shopping lists."""
        cursor = self.db.execute("SELECT url, name, creator FROM list WHERE active = 1")
        lists = cursor.fetchall()
        if not lists:
            print("No active lists found.")
        for lst in lists:
            print(f"URL: {lst[0]}, Name: {lst[1]}, Creator: {lst[2]}")

    def add_item(self, list_url, name, current_quantity, total_quantity):
        """Add an item to an existing shopping list."""
        

        cursor = self.db.execute("SELECT COUNT(*) FROM list WHERE url = ?", (list_url,))
        if cursor.fetchone()[0] == 0:
            raise ValueError(f"No list found with URL: {list_url}")


        self.db.execute("""
            INSERT INTO item (name, list_url, current_quantity, total_quantity) 
            VALUES (?, ?, ?, ?)
        """, (name, list_url, current_quantity, total_quantity))
        self.db.commit()
        print(f"Item '{name}' added successfully to list '{list_url}'!")

    def view_items_in_list(self, list_url):
        """View items in a specific list."""
        cursor = self.db.execute("""
            SELECT name, current_quantity, total_quantity 
            FROM item WHERE list_url = ? AND deleted = 0
        """, (list_url,))
        items = cursor.fetchall()
        if not items:
            print("No items found in this list.")
        for item in items:
            print(f"Item: {item[0]}, Current Quantity: {item[1]}, Total Quantity: {item[2]}")

    def update_item(self, list_url, name, current_quantity=None, total_quantity=None):
            """Update an existing item in a shopping list."""
            # Validates that the item exists
            cursor = self.db.execute("""
                SELECT COUNT(*) FROM item WHERE list_url = ? AND name = ? AND deleted = 0
            """, (list_url, name))
            if cursor.fetchone()[0] == 0:
                raise ValueError(f"No active item '{name}' found in list '{list_url}'")

            # Updates the item
            updates = []
            params = []
            if current_quantity is not None:
                updates.append("current_quantity = ?")
                params.append(current_quantity)
            if total_quantity is not None:
                updates.append("total_quantity = ?")
                params.append(total_quantity)

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
        print(f"List '{list_url}' and its items have been deleted.")