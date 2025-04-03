import sys
import random
import json
import time
import mysql.connector
from datetime import datetime
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

print(f"[👤 {viewer_name}] Subscribed to channels: {subscribed_channels}")

# Viewer starts watching a random channel from their subscription
current_channel = random.choice(subscribed_channels)
switch_time = time.time() + random.randint(30, 120)  # Random switch time

switch_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # First switch timestamp
print(f"[📡 {viewer_name}] {switch_timestamp} | Now watching channel {current_channel}...\n")

# Kafka Consumer for dynamic topic selection
consumer = KafkaConsumer(
    current_channel,
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

while True:
    # Check if it's time to switch channels
    if time.time() >= switch_time:
        new_channel = random.choice(subscribed_channels)
        while new_channel == current_channel:  # Ensure a different channel
            new_channel = random.choice(subscribed_channels)

        switch_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Get current timestamp

        print(f"\n[🔄 SWITCH] {switch_timestamp} | {viewer_name} switched from Channel {current_channel} to {new_channel}!\n")

        # Update the consumer subscription
        consumer.unsubscribe()
        consumer.subscribe([new_channel])
        current_channel = new_channel
        switch_time = time.time() + random.randint(30, 120)  # Set next switch time

    # Listen for messages only from the current channel
    message_batch = consumer.poll(timeout_ms=1000)  # Poll for messages

    for topic_partition, messages in message_batch.items():
        for message in messages:
            print(f"[📩 RECEIVED] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Viewer: {viewer_name} | Channel {message.topic} | Message: {message.value}")

