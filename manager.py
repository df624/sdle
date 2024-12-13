import sqlite3
import uuid
import time

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
                active BOOLEAN DEFAULT TRUE,
                last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS item (
                name TEXT NOT NULL,
                list_url TEXT NOT NULL,
                current_quantity INTEGER DEFAULT 0,
                total_quantity INTEGER NOT NULL,
                deleted BOOLEAN DEFAULT FALSE,
                last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (name, list_url),
                FOREIGN KEY (list_url) REFERENCES list (url)
            )
        """)
        self.db.commit()

    def create_list(self, name, creator):
        """Create a new shopping list."""
        url = str(uuid.uuid4())
        self.db.execute("""
            INSERT INTO list (url, name, creator, last_modified) 
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        """, (url, name, creator))
        self.db.commit()
        return {"url": url, "name": name, "creator": creator}

    def view_all_lists(self):
        """View all active shopping lists."""
        cursor = self.db.execute("SELECT url, name, creator FROM list WHERE active = 1")
        lists = cursor.fetchall()
        return [{"url": lst[0], "name": lst[1], "creator": lst[2]} for lst in lists]

    def add_item(self, list_url, name, current_quantity, total_quantity):
        """Add an item to an existing shopping list."""
        cursor = self.db.execute("SELECT COUNT(*) FROM list WHERE url = ?", (list_url,))
        if cursor.fetchone()[0] == 0:
            raise ValueError(f"No list found with URL: {list_url}")

        self.db.execute("""
            INSERT INTO item (name, list_url, current_quantity, total_quantity, last_modified) 
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, (name, list_url, current_quantity, total_quantity))
        
        # Update list's last_modified
        self.db.execute("""
            UPDATE list 
            SET last_modified = CURRENT_TIMESTAMP 
            WHERE url = ?
        """, (list_url,))
        
        self.db.commit()

    def view_items_in_list(self, list_url):
        """View items in a specific list."""
        cursor = self.db.execute("""
            SELECT name, current_quantity, total_quantity 
            FROM item 
            WHERE list_url = ? AND deleted = 0
        """, (list_url,))
        items = cursor.fetchall()
        return [{"name": item[0], "current_quantity": item[1], "total_quantity": item[2]} 
                for item in items]

    def update_item(self, list_url, name, current_quantity=None, total_quantity=None):
        """Update an existing item in a shopping list."""
        cursor = self.db.execute("""
            SELECT COUNT(*) FROM item 
            WHERE list_url = ? AND name = ? AND deleted = 0
        """, (list_url, name))
        
        if cursor.fetchone()[0] == 0:
            raise ValueError(f"No active item '{name}' found in list '{list_url}'")

        updates = []
        params = []
        if current_quantity is not None:
            updates.append("current_quantity = ?")
            params.append(current_quantity)
        if total_quantity is not None:
            updates.append("total_quantity = ?")
            params.append(total_quantity)

        if updates:
            updates.append("last_modified = CURRENT_TIMESTAMP")
            params.extend([list_url, name])
            update_query = f"""
                UPDATE item 
                SET {', '.join(updates)} 
                WHERE list_url = ? AND name = ?
            """
            self.db.execute(update_query, params)
            
            # Update list's last_modified
            self.db.execute("""
                UPDATE list 
                SET last_modified = CURRENT_TIMESTAMP 
                WHERE url = ?
            """, (list_url,))
            
            self.db.commit()

    def delete_list(self, list_url):
        """Delete a shopping list and its items."""
        cursor = self.db.execute("SELECT COUNT(*) FROM list WHERE url = ?", (list_url,))
        if cursor.fetchone()[0] == 0:
            raise ValueError(f"No list found with URL: {list_url}")

        self.db.execute("""
            UPDATE list 
            SET active = 0, last_modified = CURRENT_TIMESTAMP 
            WHERE url = ?
        """, (list_url,))

        self.db.execute("""
            UPDATE item 
            SET deleted = 1, last_modified = CURRENT_TIMESTAMP 
            WHERE list_url = ?
        """, (list_url,))
        
        self.db.commit()
        return {"url": list_url}

    def export_data(self, since_timestamp=None, keys=None):
        """Export lists and their items, optionally filtered by timestamp and specific keys."""
        cursor = self.db.cursor()
        
        query_params = []
        if keys and since_timestamp:
            query_params.extend(list(keys))
            query_params.append(since_timestamp)
            cursor.execute(f"""
                SELECT url, name, creator FROM list 
                WHERE active = 1 AND url IN ({','.join('?' * len(keys))}) AND last_modified > ?
            """, query_params)
        elif keys:
            cursor.execute(f"""
                SELECT url, name, creator FROM list 
                WHERE active = 1 AND url IN ({','.join('?' * len(keys))})
            """, list(keys))
        elif since_timestamp:
            cursor.execute("""
                SELECT url, name, creator FROM list 
                WHERE active = 1 AND last_modified > ?
            """, (since_timestamp,))
        else:
            cursor.execute("SELECT url, name, creator FROM list WHERE active = 1")
            
        lists = cursor.fetchall()
        
        export_data = {
            'lists': [],
            'items': []
        }
        
        for lst in lists:
            export_data['lists'].append({
                'url': lst[0],
                'name': lst[1],
                'creator': lst[2]
            })
            
            if since_timestamp:
                cursor.execute("""
                    SELECT name, current_quantity, total_quantity 
                    FROM item 
                    WHERE list_url = ? AND deleted = 0 AND last_modified > ?
                """, (lst[0], since_timestamp))
            else:
                cursor.execute("""
                    SELECT name, current_quantity, total_quantity 
                    FROM item 
                    WHERE list_url = ? AND deleted = 0
                """, (lst[0],))
                
            items = cursor.fetchall()
            for item in items:
                export_data['items'].append({
                    'list_url': lst[0],
                    'name': item[0],
                    'current_quantity': item[1],
                    'total_quantity': item[2]
                })
        
        return export_data

    def import_data(self, data):
        """Import lists and items from another worker."""
        try:
            for lst in data['lists']:
                try:
                    self.db.execute("""
                        INSERT OR REPLACE INTO list (url, name, creator, active, last_modified)
                        VALUES (?, ?, ?, 1, CURRENT_TIMESTAMP)
                    """, (lst['url'], lst['name'], lst['creator']))
                except sqlite3.Error as e:
                    print(f"Error importing list {lst['url']}: {e}")
            
            for item in data['items']:
                try:
                    self.db.execute("""
                        INSERT OR REPLACE INTO item 
                        (name, list_url, current_quantity, total_quantity, deleted, last_modified)
                        VALUES (?, ?, ?, ?, 0, CURRENT_TIMESTAMP)
                    """, (
                        item['name'],
                        item['list_url'],
                        item['current_quantity'],
                        item['total_quantity']
                    ))
                except sqlite3.Error as e:
                    print(f"Error importing item {item['name']}: {e}")
            
            self.db.commit()
            return True
        except Exception as e:
            print(f"Error during import: {e}")
            self.db.rollback()
            return False

    def list_all_keys(self):
        """Return all list URLs (keys) in the database."""
        cursor = self.db.cursor()
        cursor.execute("SELECT url FROM list WHERE active = 1")
        return [row[0] for row in cursor.fetchall()]