import pika
import threading
import json

class YouTubeServer:
    def __init__(self):
        # Establish a connection to RabbitMQ server
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()

        # Declare queues for user requests and YouTuber video uploads
        self.channel.queue_declare(queue='user_requests')
        self.channel.queue_declare(queue='youtuber_uploads')

        # Data structures to hold users and YouTubers information
        self.users = {}  # Example: {'user1': ['YouTuber1', 'YouTuber2'], ...}
        self.youtubers = {}  # Example: {'YouTuber1': ['Video1', 'Video2'], ...}
        self.notifications = {}  # Example {'user1': ['YouTuber1 uploaded Video1', ...]}

    def consume_user_requests(self):
            # New connection and channel for this thread
            connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
            channel = connection.channel()
            channel.queue_declare(queue='user_requests')

            def callback(ch, method, properties, body):
                request = json.loads(body)
                username = request['user']

                # Handle user login
                if 'action' in request and request['action'] == 'login':
                    print(f"{username} logged in")
                    # Send queued notifications here

                # Handle subscribe/unsubscribe
                elif 'subscribe' in request:
                    youtuber_name = request['youtuber']
                    if request['subscribe']:
                        # Add subscription
                        self.users.setdefault(username, []).append(youtuber_name)
                        print(f"{username} subscribed to {youtuber_name}")
                    else:
                        # Remove subscription
                        if youtuber_name in self.users.get(username, []):
                            self.users[username].remove(youtuber_name)
                            print(f"{username} unsubscribed from {youtuber_name}")
                        else:
                            print(f"{username} is not subscribed to {youtuber_name}")

            channel.basic_consume(queue='user_requests', on_message_callback=callback, auto_ack=True)
            print('Server is waiting for user requests')
            channel.start_consuming()

        

    def consume_youtuber_requests(self):
        # New connection and channel for this thread
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()
        channel.queue_declare(queue='youtuber_requests')

        def callback(ch, method, properties, body):
            request = json.loads(body)
            youtuber_name = request['youtuber']
            video_name = request['videoName'] 

            # Add YouTuber or video to the youtubers dictionary
            self.youtubers.setdefault(youtuber_name, []).append(video_name)

            print(f"{youtuber_name} uploaded {video_name}")
            # Notify subscribed users
            self.notify_users(youtuber_name, video_name)

        channel.basic_consume(queue='youtuber_requests', on_message_callback=callback, auto_ack=True)
        print('Server is waiting for YouTuber uploads')
        channel.start_consuming()

    def notify_users(self, youtuber_name, video_name):
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()
        channel.queue_declare(queue='user_requests')
        # Find all users subscribed to the youtuber
        for user, subscriptions in self.users.items():
            if youtuber_name in subscriptions:
                notification = f"{youtuber_name} uploaded {video_name}"
                # Add notification to the user's queue
                self.notifications.setdefault(user, []).append(notification)
                print(f"Notification sent to {user}: {notification}")
                # Send the actual notification message to the user's queue
                self.channel.basic_publish(
                    exchange='',
                    routing_key=user,
                    body=json.dumps({'youtuber': youtuber_name, 'videoName': video_name})
                )


if __name__ == "__main__":
    server = YouTubeServer()
    # Create separate threads for consuming user and youtuber requests
    user_thread = threading.Thread(target=server.consume_user_requests)
    youtuber_thread = threading.Thread(target=server.consume_youtuber_requests)

    # Start the threads
    user_thread.start()
    youtuber_thread.start()

    # Wait for the threads to finish
    user_thread.join()
    youtuber_thread.join()
