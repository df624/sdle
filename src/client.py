from manager import ShoppingListManager

def main():
    client_id = input("Enter your client ID: ")
    db_path = f"local_{client_id}.db"
    
    manager = ShoppingListManager(db_path)
    
    while True:
        print("\n--- Shopping List Manager ---")
        print("1. Create a new shopping list")
        print("2. View all shopping lists")
        print("3. Add an item to a list")
        print("4. View items in a list")
        print("5. Update an item in a list")
        print("6. Delete a shopping list")
        print("7. Exit")

        choice = input("Choose an option: ")
        try:
            if choice == "1":
                name = input("Enter the list name: ")
                creator = input("Enter your name: ")
                manager.create_list(name, creator)
            elif choice == "2":
                manager.view_all_lists()
            elif choice == "3":
                list_url = input("Enter list URL: ")
                item_name = input("Enter item name: ")
                total_quantity = int(input("Enter total quantity: "))
                manager.add_item(list_url, item_name, 0, total_quantity)
            elif choice == "4":
                list_url = input("Enter list URL: ")
                manager.view_items_in_list(list_url)
            elif choice == "5":
                list_url = input("Enter list URL: ")
                item_name = input("Enter item name: ")
                current_quantity = input("Enter current quantity (leave blank to skip): ")
                total_quantity = input("Enter total quantity (leave blank to skip): ")

                # Converts inputs if provided
                current_quantity = int(current_quantity) if current_quantity else None
                total_quantity = int(total_quantity) if total_quantity else None

                manager.update_item(list_url, item_name, current_quantity, total_quantity)
            elif choice == "6":
                list_url = input("Enter list URL: ")
                manager.delete_list(list_url)
            elif choice == "7":
                print("Exiting...")
                break
            else:
                print("Invalid choice. Please try again.")
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    main()
