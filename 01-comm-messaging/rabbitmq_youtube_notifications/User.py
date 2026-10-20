import pika
import sys
import json
import requests

class User:
    def __init__(self, username):
        self.username = username
        
        # Establish a connection to the remote RabbitMQ server on Google Cloud
        credentials = pika.PlainCredentials('user1', 'user1')
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host='10.128.0.2',
                credentials=credentials,
            )
        )
        self.channel = self.connection.channel()

        # Declare a queue for this user to receive notifications
        self.channel.queue_declare(queue=self.username)

    def update_subscription(self, action, youtuber):
        # Send a subscription or unsubscription message to the server
        subscribe_status = True if action == 'subscribe' else False
        message = json.dumps({
            'user': self.username,
            'youtuber': youtuber,
            'subscribe': subscribe_status  # The server expects 'subscribe' key
        })
        self.channel.basic_publish(
            exchange='',
            routing_key='user_requests',  # Make sure this matches the server queue name for user requests
            body=message
        )
        action_text = 'subscribed to' if action == 'subscribe' else 'unsubscribed from'
        print(f"SUCCESS: {self.username} has {action_text} {youtuber}.")



    def receive_notifications(self):
        # Define the callback for processing notifications
        def callback(ch, method, properties, body):
            notification = json.loads(body)
            print(f"New Notification: {notification['youtuber']} uploaded {notification['videoName']}")

        # Start consuming notifications from this user's queue
        self.channel.basic_consume(
            queue=self.username,
            on_message_callback=callback,
            auto_ack=True
        )
        print(f"Listening for notifications for {self.username}...")
        self.channel.start_consuming()

    def close_connection(self):
        # Close the connection to the RabbitMQ server
        self.connection.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: User.py [Username] [Action: s/u] [YoutuberName]")
        sys.exit(1)

    username = sys.argv[1]
    user = User(username)

    # Check for action and execute accordingly
    if len(sys.argv) == 4:
        if sys.argv[2]== 's':
            action = 'subscribe'
        elif sys.argv[2] == 'u':
            action = 'unsubscribe'
        else:
            print("Invalid action. Use 's' for subscribe or 'u' for unsubscribe.")
            sys.exit(1)

        youtuber = sys.argv[3]
        user.update_subscription(action, youtuber)

    # Always listen for notifications
    try:
        user.receive_notifications()
    except KeyboardInterrupt:
        user.close_connection()
