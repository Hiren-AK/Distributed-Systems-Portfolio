import grpc
from concurrent import futures
import time
import os
import market_service_pb2
import notify_service_pb2
import market_service_pb2_grpc
import notify_service_pb2_grpc
from market_service_pb2 import Response, ItemList, Item, Category
from market_service_pb2_grpc import MarketServiceServicer, add_MarketServiceServicer_to_server
from notify_service_pb2_grpc import NotifyServiceServicer, add_NotifyServiceServicer_to_server
from notify_service_pb2 import Notification

SERVER_IP = os.getenv('SERVER_IP', '[::]')  # Defaults to listening on all interfaces
SERVER_PORT = os.getenv('SERVER_PORT', '50051')
NOTIFY_SERVER_PORT = os.getenv('NOTIFY_SERVER_PORT', '50052')

class MarketServiceServicer(market_service_pb2_grpc.MarketServiceServicer):
    def __init__(self):
        self.sellers = {}
        self.items = {}
        self.subscriptions = {}
        self.buyers = {}
        self.wishlists = {}
        self.ratings = {}
        self.invoices = {}
        self.itemCount = 0
        self.notificationSellAddress = {}
        self.notificationBuyAddress = {}
    
    def LoginSeller(self, request, context):
        uuid = request.uuid

        # Check if the UUID exists in sellers or buyers
        if uuid in self.sellers:
            print(f'seller with {self.sellers[uuid]} trying to login')
            return market_service_pb2.Response(success=True, message=self.sellers[uuid])
        else:
            return market_service_pb2.Response(success=False, message="UUID not found")
    
    def NotificationSellerAddress(self, request, context):
        uuid = request.uuid
        address = request.address

        if uuid in self.sellers:
            print(f'seller {self.sellers[uuid]} setting notification service')
            self.notificationSellAddress[uuid] = address
            return market_service_pb2.Response(success=True, message=f'notification address updated')
        else:
            return market_service_pb2.Response(success=False, message="UUID not found")
        
    def NotificationBuyerAddress(self, request, context):
        uuid = request.uuid
        address = request.address

        if uuid in self.buyers:
            print(f'buyer {self.buyers[uuid]} setting notification service')
            self.notificationBuyAddress[uuid] = address
            return market_service_pb2.Response(success=True, message=f'notification address updated')
        else:
            return market_service_pb2.Response(success=False, message="UUID not found")
        
    def LoginBuyer(self, request, context):
        uuid = request.uuid

        # Check if the UUID exists in sellers or buyers
        if uuid in self.buyers:
            print(f'buyer with {self.buyers[uuid]} trying to login')
            return market_service_pb2.Response(success=True, message=self.buyers[uuid])
        else:
            return market_service_pb2.Response(success=False, message="UUID not found")
        
    def RegisterSeller(self, request, context):

        seller_address = f'{request.ip_address}:{request.port}'
        seller_uuid = request.uuid

        print(f"Seller join request from {seller_address}, uuid = {seller_uuid}")

        if seller_uuid in self.sellers:
            return Response(success=False, message="Seller already registered")

        self.sellers[seller_uuid] = request.ip_address + ':' + request.port
        return Response(success=True, message="Seller registered successfully")
    
    def RegisterBuyer(self, request, context):
        buyer_address = f'{request.ip_address}:{request.port}'
        buyer_uuid = request.uuid

        print(f"buyer join request from {buyer_address}, uuid = {buyer_uuid}")

        if buyer_uuid in self.buyers:
            return Response(success=False, message="buyer already registered")

        self.buyers[buyer_uuid] = request.ip_address + ':' + request.port
        return Response(success=True, message="buyer registered successfully")
    
    def SellItem(self, request, context):
        seller_address = request.seller_address
        seller_uuid = request.uuid

        print(f"Sell Item request from {seller_address}")

        if seller_uuid not in self.sellers or seller_address != self.sellers[seller_uuid]:
            return Response(success=False, message="Invalid seller credentials")

        # Implementation for selling an item
        item_id = self.itemCount + 1
        self.itemCount += 1
        item_details = Item(
            id=item_id,
            name=request.product_name,
            category=request.category,
            description=request.description,
            quantity=request.quantity,
            price=request.price_per_unit,
            seller_address=request.seller_address,
            rating=0.0
        )
        self.items[item_id] = item_details

        print(f"Sell Item request from {seller_address}, item_id={item_id}")
        return Response(success=True, message=f"Item with ID:{item_id} added successfully")

    def UpdateItem(self, request, context):
        # Implementation for updating an item
        seller_address = request.seller_address
        seller_uuid = request.uuid

        print(f"Update Item {request.item_id} request from {seller_address}")

        if seller_uuid not in self.sellers or self.sellers[seller_uuid] != seller_address:
            return Response(success=False, message="Invalid seller credentials")

        item_id = request.item_id
        if item_id not in self.items:
            return Response(success=False, message=f"Item with ID:{item_id} not found")
        
        # Check if the seller updating the item is the original seller
        original_seller_address = self.items[item_id].seller_address
        if seller_address != original_seller_address:
            return Response(success=False, message="You are not the original seller of this item as your address doesn't match with that of the seller")

        self.items[item_id].price = request.new_price
        self.items[item_id].quantity = request.new_quantity

        print(f"Update Item with ID:{item_id} request from {seller_address}")
        # NotifyClients (wishlisted buyers and seller) now
        self.NotifyClient(item_id)

        return Response(success=True, message=f"Item with ID:{item_id} updated successfully")

    def DeleteItem(self, request, context):
        # Implementation for deleting an item
        seller_address = request.seller_address
        seller_uuid = request.uuid
        print(f"Delete Item {request.item_id} request from {seller_address}")

        if seller_uuid not in self.sellers or self.sellers[seller_uuid] != seller_address:
            return Response(success=False, message="Invalid seller credentials")
        
        item_id = request.item_id
        if item_id not in self.items:
            return Response(success=False, message=f"Item with ID:{item_id} not found")
        
        original_seller_address = self.items[item_id].seller_address
        if seller_address != original_seller_address:
            return Response(success=False, message="You are not the original seller of this item as your address doesn't match with that of the seller")
        
        del self.items[item_id]

        print(f"Delete Item {item_id} request from {seller_address}")
        return Response(success=True, message=f"Item with ID:{item_id} deleted successfully")

    def DisplaySellerItems(self, request, context):
        # Implementation for displaying seller's items
        seller_address = request.seller_address
        seller_uuid = request.uuid

        print(f"Display Items request from {seller_address}")

        if seller_uuid not in self.sellers or self.sellers[seller_uuid] != seller_address:
            return ItemList(items=[])

        seller_items = [item for item in self.items.values() if item.seller_address == seller_address]
        print(f"Display Items request from {seller_address}")

        return ItemList(items=seller_items)
    
    def SearchItem(self, request, context):
        # Implementation for searching items
        item_name = request.item_name
        category = request.category

        print(f"Search request for item name: {item_name}, category: {category}")

        filtered_items = []

        if item_name:
            if category == market_service_pb2.Category.ANY:
                # If name is provided and category is ANY, include all items with the provided name
                filtered_items = [item for item in self.items.values() if
                                  item.name.lower() == item_name.lower()]
            else:
                # If both name and category are provided, include items with the provided name and category
                filtered_items = [item for item in self.items.values() if
                                  item.name.lower() == item_name.lower() and item.category == category]
        else:
            if category == market_service_pb2.Category.ANY:
                # If no name is provided and category is ANY, include all items
                filtered_items = list(self.items.values())
            else:
                # If no name is provided but category is specified, include items with the specified category
                filtered_items = [item for item in self.items.values() if item.category == category]

        return market_service_pb2.ItemList(items=filtered_items)

    def BuyItem(self, request, context):
        # Implementation for buying an item
        item_id = request.item_id
        quantity = request.quantity
        buyer_address = request.buyer_address
        buyer_uuid = request.buyer_uuid

        print(f"Buy Request Quantity:{quantity} of Item:{item_id}, from {buyer_address}") 
        if buyer_uuid not in self.buyers or self.buyers[buyer_uuid] != buyer_address:
            return Response(success=False, message=f"Invalid buyer credentials")

        if item_id not in self.items:
            return Response(success=False, message=f"Item with ID:{item_id} not found")

        if self.items[item_id].quantity < quantity:
            return Response(success=False, message=f"Not enough stock available for Item with ID:{item_id}")

        # Update item quantity
        self.items[item_id].quantity -= quantity

        # Trigger notification to seller
        seller_address = self.items[item_id].seller_address
        self.notify_seller(item_id, quantity, buyer_address, seller_address)

        self.invoices[buyer_uuid] = {}
        self.invoices[buyer_uuid][item_id] = quantity

        return Response(success=True, message=f"Successfully bought {quantity} quantity of Item with ID:{item_id}")

    def AddToWishList(self, request, context):
        # Implementation for adding to wish list
        item_id = request.item_id
        buyer_address = request.buyer_address
        buyer_uuid = request.buyer_uuid

        print(f"Wishlist request of item:{item_id}, from {buyer_address}")

        if buyer_uuid not in self.buyers or self.buyers[buyer_uuid] != buyer_address:
            return Response(success=False, message=f"Invalid buyer credentials")
        
        if item_id not in self.items:
            return Response(success=False, message=f"Item with ID:{item_id} not found")

        # Update wishlist for the buyer
        if buyer_uuid not in self.wishlists:
            self.wishlists[buyer_uuid] = set()
        self.wishlists[buyer_uuid].add(item_id)

        return Response(success=True, message=f"Item with ID:{item_id} added to wishlist successfully")

    def RateItem(self, request, context):
        # Implementation for rating an item
        item_id = request.item_id
        rating = request.rating
        buyer_address = request.buyer_address

        print(f"{buyer_address} rated item {item_id} with a {rating}")

        if item_id not in self.items:
            return Response(success=False, message=f"Item with ID:{item_id} not found")

        if rating < 1 or rating > 5:
            return Response(success=False, message="Rating should be between 1 and 5")

        # Update item rating
        self.items[item_id].rating = (self.items[item_id].rating + rating) / 2

        # Update rating in the ratings database
        if item_id not in self.ratings:
            self.ratings[item_id] = []
        self.ratings[item_id].append({
            "buyer_address": buyer_address,
            "rating": rating
        })

        return Response(success=True, message=f"Item with ID:{item_id} rated successfully")

    def NotifyClient(self, item_id):
        addresses = []
        updated_item = self.items[item_id]
        seller_address = updated_item.seller_address
        seller_uuid = None
        for uuid in self.sellers:
            if(self.sellers[uuid] == seller_address):
                seller_uuid = uuid
        if not seller_uuid:
            print("Couldn't fetch seller address")
            return
        addresses.append(self.notificationSellAddress[seller_uuid])

        for uuid in self.wishlists:
            if item_id in self.wishlists[uuid]:
                addresses.append(self.notificationBuyAddress[uuid])

        if len(addresses)==0:
            print("Item is not in any buyers' wishlist")
        
        for client in addresses:
            try:
                message = f"The Following Item has been updated:\n\n" \
                f"Item ID: {updated_item.id}, " \
                f"Price: ${updated_item.price}, " \
                f"Name: {updated_item.name}, " \
                f"Category: {Category.Name(updated_item.category)}, " \
                f"Description: {updated_item.description}, " \
                f"Quantity Remaining: {updated_item.quantity}, " \
                f"Rating: {updated_item.rating} / 5 | Seller: {updated_item.seller_address}"

                # Create a gRPC channel to the client's address
                with grpc.insecure_channel(client) as channel:
                    # Create a stub for the notification service
                    stub = notify_service_pb2_grpc.NotifyServiceStub(channel)
                    # Call the NotifyClient method of the stub to send the message
                    stub.NotifyClient(notify_service_pb2.Notification(message=message, updated_item=updated_item))


            except grpc.RpcError as e:
                print(f"Error during notifying buyers about update of item with ID:{item_id}")
            
            print(f"notified all clients about the item with ID:{item_id}")
        return
    
    def notify_seller(self, item_id, quantity, buyer_address, seller_address):
        updated_item = self.items[item_id]
        seller_uuid = None
        for uuid in self.sellers:
            if(self.sellers[uuid] == seller_address):
                seller_uuid = uuid
        if not seller_uuid:
            print("Couldn't fetch seller address")
            return
        try:
                message = f"Invoice:\n\n" \
                f"Item ID: {updated_item.id}, " \
                f"Price: ${updated_item.price}, " \
                f"Name: {updated_item.name}, " \
                f"Category: {Category.Name(updated_item.category)}, " \
                f"Description: {updated_item.description}, " \
                f"Quantity Sold: {quantity}, " \
                f"Seller: {seller_address} | Buyer: {buyer_address}"

                # Create a gRPC channel to the client's address
                with grpc.insecure_channel(self.notificationSellAddress[seller_uuid]) as channel:
                    # Create a stub for the notification service
                    stub = notify_service_pb2_grpc.NotifyServiceStub(channel)
                    # Call the NotifyClient method of the stub to send the message
                    stub.NotifyClient(notify_service_pb2.Notification(message=message))

        except grpc.RpcError as e:
            print(f"Error during notifying buyers about update of item with ID:{item_id}")

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=7))
    add_MarketServiceServicer_to_server(MarketServiceServicer(), server)
    add_NotifyServiceServicer_to_server(MarketServiceServicer(), server)
    server.add_insecure_port(f'{SERVER_IP}:{SERVER_PORT}')
    server.start()
    print("Market server started.")
    print(f'server_ip:server_port - {SERVER_IP}:{SERVER_PORT}')
    try:
        while True:
            time.sleep(86400)  # One day in seconds
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()
