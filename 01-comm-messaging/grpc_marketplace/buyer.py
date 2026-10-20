import grpc
import os
import threading
import uuid
import market_service_pb2
import market_service_pb2_grpc
import notify_service_pb2_grpc
from concurrent import futures
import socket
import time

SERVER_IP = '10.128.0.5'
SERVER_PORT = os.getenv('BUYER_PORT', '8003')

class BuyerClient:
    def __init__(self, server_address):
        self.channel = grpc.insecure_channel(server_address)
        self.market_stub = market_service_pb2_grpc.MarketServiceStub(self.channel)
        self.notify_stub = notify_service_pb2_grpc.NotifyServiceStub(self.channel)
        self.uuid = None
        self.address = None
        self.notification_address = None
        self.notification_stop_event = threading.Event()
        

    def register_buyer(self):
        ip_address = input("Enter your IP address: ")
        port = input("Enter your port: ")
        unique_id = str(uuid.uuid1())
        response = self.market_stub.RegisterBuyer(
            market_service_pb2.RegisterBuyerRequest(
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

    def login_buyer(self):
        self.uuid = input("Enter your UUID to login: ")
        response = self.market_stub.LoginBuyer(
            market_service_pb2.LoginBuyerRequest(uuid=self.uuid)
        )
        self.address = response.message
        if response.success:
            print("SUCCESS")
        else:
            print("FAIL")
        print(response.message)
        return response.success
    
    def search_item(self):
        print("Search for an item. Leave the name empty to display all items.")
        item_name = input("Enter the item name: ")
        print("Choose a category:")
        print("1. Electronics")
        print("2. Fashion")
        print("3. Others")
        print("4. Any")
        category_choice = input("Enter your choice (1-4): ")
        while True:
            if (category_choice == '1'):
                category = 'ELECTRONICS'
                break
            elif (category_choice == '2'):
                category = 'FASHION'
                break
            elif (category_choice == '3'):
                category = 'OTHERS'
                break
            elif (category_choice == '4'):
                category = 'ANY'
                break
            else:
                print("Invalid input, try again ")
                category_choice = input("Enter your choice (1-4): ")

        # Make the gRPC call
        response = self.market_stub.SearchItem(
            market_service_pb2.SearchItemRequest(item_name=item_name, category=category)
        )

        # Print the search results
        if response.items:
            for item in response.items:
                print(f"\n–\n\nItem ID: {item.id}, Price: ${item.price}, Name: {item.name}, "
                    f"Category: {market_service_pb2.Category.Name(item.category)},\nDescription: {item.description}, "
                    f"Quantity Remaining: {item.quantity},\nRating: {item.rating} / 5  |  "
                    f"Seller: {item.seller_address}\n–")
        else:
            print("No items found.")

    def buy_item(self):
        if not self.uuid:
            print("Please login first")
            return False
        
        item_id = 0
        while True:
            item = input("Enter Item ID: ")
            try:
                item_id = int(item)
                break
            except:
                print("Invalid ID input")

        quantity = 0
        while True:
            q = input("Enter quantity: ")
            try:
                quantity = int(q)
                if quantity <= 0:
                    print("Quantity must be a positive integer")
                else:
                    break
            except:
                print("Invalid quantity input")

        response = self.market_stub.BuyItem(
            market_service_pb2.BuyItemRequest(
                item_id=item_id,
                quantity=quantity,
                buyer_address=self.address,
                buyer_uuid=self.uuid
            )
        )
        if response.success:
            print("SUCCESS")
        else:
            print("FAIL")
        print(response.message)
        return response.success

    def add_to_wishlist(self):
        if not self.uuid:
            print("Please login first")
            return False
        
        item_id = 0
        while True:
            item = input("Enter ID: ")
            try:
                item_id = int(item)
                break
            except:
                print("Invalid ID input")

        response = self.market_stub.AddToWishList(
            market_service_pb2.AddToWishListRequest(
                item_id=item_id,
                buyer_address=self.address,
                buyer_uuid=self.uuid
            )
        )
        if response.success:
            print("SUCCESS")
        else:
            print("FAIL")
        print(response.message)
        return response.success

    def rate_item(self):
        if not self.uuid:
            print("Please login first")
            return False
        
        item_id = 0
        while True:
            item = input("Enter Item ID: ")
            try:
                item_id = int(item)
                break
            except:
                print("Invalid ID input")

        rating = 0
        while True:
            rated = input("Enter rating (1-5): ")
            try:
                rating = int(rated)
                if (rating >= 1 and rating <= 5):
                    break
                else:
                    print("Invalid rating")
            except:
                print("Invalid rating input")

        response = self.market_stub.RateItem(
            market_service_pb2.RateItemRequest(
                item_id=item_id,
                rating=rating,
                buyer_address=self.address,
                buyer_uuid=self.uuid
            )
        )
        print(response.message)
    
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

        request = market_service_pb2.NotificationBuyerAddressRequest(
            uuid = self.uuid,
            address = self.notification_address
        )

        response = self.market_stub.NotificationBuyerAddress(request)
        if response.success:
            print("Notification address set successfully")
            print(f"Address:{self.notification_address}")
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
    server_address = "10.128.0.4:50051"  # Adjust to your server's IP and port
    client = BuyerClient(server_address)

    while True:
        print("\nAvailable operations for Buyer:")
        print("1. Register Buyer")
        print("2. Login Buyer")
        print("3. Search Item")
        print("4. Set Notification Address")
        print("5. Stop Receiving Notifications")
        print("6. Buy Item")
        print("7. Add to Wishlist")
        print("8. Rate Item")
        print("9. Exit")
        choice = input("Choose an operation: ")

        if choice == '1':
            client.register_buyer()
        elif choice == '2':
            client.login_buyer()
        elif choice == '3':
            client.search_item()
        elif choice == '4':
            client.set_notification_address()
        elif choice == '5':
            client.stop_receiving_notifications() 
        elif choice == '6':
            client.buy_item()
        elif choice == '7':
            client.add_to_wishlist()
        elif choice == '8':
            client.rate_item()
        elif choice == '9':
            print("Exiting...")
            client.stop_receiving_notifications
            break
        else:
            print("Invalid choice, please try again.")

if __name__ == "__main__":
    main()
