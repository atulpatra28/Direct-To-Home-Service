import sys
import random
import json
import mysql.connector
from kafka import KafkaConsumer

# Get viewer name from command line
if len(sys.argv) < 2:
    print("[❌ ERROR] No viewer name provided. Exiting...")
    sys.exit(1)

viewer_name = sys.argv[1]  # Get viewer name from script argument

# Fetch channels from MySQL
def fetch_channel_ids():
    try:
        connection = mysql.connector.connect(
            host="localhost",
            database="dth",
            user="root",
            password="iamatulletmein"
        )
        cursor = connection.cursor()
        cursor.execute("SELECT channel_id FROM channels;")
        channel_ids = [str(row[0]) for row in cursor.fetchall()]
        
        cursor.close()
        connection.close()
        return channel_ids
    except mysql.connector.Error as e:
        print(f"[❌ ERROR] MySQL Connection Failed: {e}")
        return []

# Fetch all channels
all_channels = fetch_channel_ids()

# Randomly assign 3 channels to this viewer
subscribed_channels = random.sample(all_channels, 3) if len(all_channels) >= 3 else all_channels

# Create Kafka Consumer
consumer = KafkaConsumer(
    *subscribed_channels,
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(f"[👤 {viewer_name}] Subscribed to channels: {subscribed_channels}")
print(f"[📡 LISTENING] {viewer_name} is waiting for messages...\n")

# Listening for messages
for message in consumer:
    print(f"[📩 RECEIVED] Viewer: {viewer_name} | Channel ID: {message.topic} | Message: {message.value}")

