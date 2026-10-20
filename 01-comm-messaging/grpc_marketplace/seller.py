import grpc
import os
import market_service_pb2
import market_service_pb2_grpc
import notify_service_pb2_grpc
import uuid
import threading
import time
import socket
from concurrent import futures

SERVER_IP = '10.128.0.5'
SERVER_PORT = os.getenv('SELLER_PORT', '8001')

class SellerClient:
    def __init__(self, server_address):
        self.channel = grpc.insecure_channel(server_address)
        self.market_stub = market_service_pb2_grpc.MarketServiceStub(self.channel)
        self.notify_stub = notify_service_pb2_grpc.NotifyServiceStub(self.channel)
        self.uuid = None
        self.address = None
        self.notification_address = None
        self.notification_stop_event = threading.Event()

    def register_seller(self):
        ip_address = input("Enter your IP address: ")
        port = input("Enter your port: ")
        unique_id = str(uuid.uuid1())
        response = self.market_stub.RegisterSeller(
            market_service_pb2.RegisterSellerRequest(
                ip_address=ip_address,
                port=port,
                uuid=unique_id
            )
        )
        print(f"address={ip_address}:{port}, uuid={unique_id}")
        self.uuid = unique_id
        self.address = f'{ip_address}:{port}'
        if response.success:
            print("SUCCESS")
        else:
            print("FAIL")
        print(response.message)
        return response.success

    def login_seller(self):
        self.uuid = input("Enter your UUID to login: ")
        response = self.market_stub.LoginSeller(
            market_service_pb2.LoginSellerRequest(uuid=self.uuid)
        )
        self.address = response.message
        if response.success:
            print("SUCCESS")
        else:
            print("FAIL")
        print(response.message)
        return response.success

    def sell_item(self):
        # Implementation for selling an item
        if not self.uuid:
            print("Please login first")
            return False
        product_name = input("Enter product name: ")
        print("category must belong to 'ELECTRONICS', 'FASHION', 'OTHERS'")
        category = input("Enter category: ")
        while (category not in ['ELECTRONICS', 'FASHION', 'OTHERS']):
            print("category must belong to 'ELECTRONICS', 'FASHION', 'OTHERS'")
            category = input("Enter category: ")
        quantity = 0
        while True:
            quantity = input("Enter quantity: ")
            try:
                quantity = int(quantity)
                if quantity <= 0:
                    print("Quantity must be a positive integer")
                else:
                    break
            except:
                print("Invalid quantity input")
        description = input("Enter description: ")
        price_per_unit = 0
        while True:
            price_per_unit = input("Enter price per unit: ")
            try:
                price = float(price_per_unit)
                if price <= 0:
                    print("price must be a positive number")
                else:
                    break
            except:
                print("Invalid price input")
        request = market_service_pb2.SellItemRequest(
            product_name=product_name,
            category=category,
            quantity=quantity,
            description=description,
            seller_address=self.address,
            price_per_unit=price,
            uuid=self.uuid
        )

        response = self.market_stub.SellItem(request)
        if response.success:
            print("SUCCESS")
        else:
            print("FAIL")
        print(response.message)
        return response.success

    def update_item(self):
        # Implementation for updating an item
        if not self.uuid:
            print("Please login first")
            return False
        item_id = 0
        while True:
            id = input("Enter item ID to update: ")
            try:
                item_id = int(id)
                break
            except:
                print("Invalid price input")

        new_price = 0
        while True:
            price = input("Enter new price: ")
            try:
                new_price = float(price)
                if new_price <= 0:
                    print("Price must be a positive integer")
                else:
                    break
            except:
                print("Invalid price input")

        new_quantity = 0
        while True:
            quantity = input("Enter new quantity: ")
            try:
                new_quantity = int(quantity)
                if new_quantity <= 0:
                    print("quantity must be a positive integer")
                else:
                    break
            except:
                print("Invalid quantity input")

        request = market_service_pb2.UpdateItemRequest(
            item_id=item_id,
            new_price=new_price,
            new_quantity=new_quantity,
            seller_address=self.address,
            uuid=self.uuid
        )

        response = self.market_stub.UpdateItem(request)
        if response.success:
            print("SUCCESS")
        else:
            print("FAIL")
        print(response.message)
        return response.success


    def delete_item(self):
        # Implementation for deleting an item
        if not self.uuid:
            print("Please login first")
            return False
        
        item_id = 0
        while True:
            id = input("Enter item ID to update: ")
            try:
                item_id = int(id)
                break
            except:
                print("Invalid price input")
        request = market_service_pb2.DeleteItemRequest(
            item_id=item_id,
            seller_address=self.address,
            uuid=self.uuid
        )

        response = self.market_stub.DeleteItem(request)
        if response.success:
            print("SUCCESS")
        else:
            print("FAIL")
        print(response.message)
        return response.success

    def display_items(self):
        # Implementation for displaying seller's items
        if not self.uuid:
            print("Please login first")
            return False
        
        request = market_service_pb2.DisplaySellerItemsRequest(
            seller_address=self.address,
            uuid=self.uuid
        )

        response=self.market_stub.DisplaySellerItems(request)
        for item in response.items:
            print("\nItem ID:", item.id)
            print("Price:", item.price)
            print("Name:", item.name)
            print("Category:", market_service_pb2.Category.Name(item.category))
            print("Description:", item.description)
            print("Quantity Remaining:", item.quantity)
            print("Seller:", item.seller_address)
            print("Rating:", item.rating, "/ 5")
        return True
    

    def set_notification_address(self):
        # Implementation for listening to notifications
        if not self.uuid:
            print("Please login first")
            return False
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(('localhost', 0))
                _, port = s.getsockname()

            ip_address = SERVER_IP

            if not ip_address:
                print("Failed to find suitable IP address")
                return False
        except Exception as e:
            print("Error finding available port or IP address", e)
            return False
        self.notification_address = f"{ip_address}:{port}"

        request = market_service_pb2.NotificationSellerAddressRequest(
            uuid = self.uuid,
            address = self.notification_address
        )

        response = self.market_stub.NotificationSellerAddress(request)
        if response.success:
            print("Notification address set successfully")
            print("Address:{address}")
            self.start_receiving_notifications()
        else:
            print("Failed to set notification address")
        
        return response.success
    
    def start_receiving_notifications(self):
        if not self.notification_address:
            print("Notification address is not set")
            return False
        def notification_listener():
            server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
            notify_service_pb2_grpc.add_NotifyServiceServicer_to_server(NotifyService(), server)
            server.add_insecure_port(f'{self.notification_address}')
            server.start()
            print("Notification listener started")
            try:
                while not self.notification_stop_event.is_set():
                    time.sleep(1)
            except KeyboardInterrupt:
                print("Notification listener stopped")
                server.stop(0)
        self.notification_thread = threading.Thread(target=notification_listener)
        self.notification_thread.start()
    
    def stop_receiving_notifications(self):
        self.notification_stop_event.set()
        if self.notification_thread:
            self.notification_thread.join()

class NotifyService(notify_service_pb2_grpc.NotifyServiceServicer):
    def NotifyClient(self, request, context):
        print("Received notification:", request.message)
        return notify_service_pb2_grpc.NotificationResponse(success=True)
    


def main():
    server_address = "10.128.0.4:50051"  # Change this to your server's address
    client = SellerClient(server_address)

    while True:
        print("\nAvailable operations:")
        print("1. Register Seller")
        print("2. Login Seller")
        print("3. Set Notification Address")
        print("4. Stop Receiving Notifications")
        print("5. Sell Item")
        print("6. Update Item")
        print("7. Delete Item")
        print("8. Display Items")
        print("9. Exit")
        choice = input("Select an operation: ")

        if choice == '1':
            client.register_seller()
        elif choice == '2':
            client.login_seller()
        elif choice == '3':
            client.set_notification_address()
        elif choice == '4':
            client.stop_receiving_notifications() 
        elif choice == '5':
            client.sell_item()
        elif choice == '6':
            client.update_item()
        elif choice == '7':
            client.delete_item()
        elif choice == '8':
            client.display_items()
        elif choice == '9':
            print("Exiting...")
            client.stop_receiving_notifications
            break
        else:
            print("Invalid choice.")

if __name__ == "__main__":
    main()
