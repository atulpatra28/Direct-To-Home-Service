import sys
import random
import json
import time
import mysql.connector
from datetime import datetime, timedelta
from kafka import KafkaConsumer

# Get viewer name from command line
if len(sys.argv) < 3:
    print("[❌ ERROR] No viewer name provided. Exiting...")
    sys.exit(1)

viewer_name = sys.argv[1] + " " + sys.argv[2]
print(viewer_name)

# DB Connection Params
DB_CONFIG = {
    "host": "localhost",
    "database": "dth",
    "user": "root",
    "password": "iamatulletmein"
}

# Get user_id from name
def get_user_id(name):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT user_id FROM viewers WHERE name = %s", (name,))
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        return result[0] if result else None
    except mysql.connector.Error as e:
        print(f"[❌ ERROR] Failed to fetch user_id: {e}")
        return None

# Fetch available channel IDs
def fetch_channel_ids():
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor()
        cursor.execute("SELECT channel_id FROM channels;")
        channel_ids = [str(row[0]) for row in cursor.fetchall()]
        cursor.close()
        connection.close()
        return channel_ids
    except mysql.connector.Error as e:
        print(f"[❌ ERROR] MySQL Connection Failed: {e}")
        return []

# Insert a watch log into database
def insert_watch_log(user_id, channel_id, start_time, stop_time):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO watch_logs (user_id, channel_id, start_time, stop_time)
            VALUES (%s, %s, %s, %s)
        """, (user_id, int(channel_id), start_time.strftime('%Y-%m-%d %H:%M:%S'),
              stop_time.strftime('%Y-%m-%d %H:%M:%S')))
        conn.commit()
        cursor.close()
        conn.close()

        duration = int((stop_time - start_time).total_seconds())
        print(f"[✅ INFO] Watch log added | Duration: {duration} seconds")
        
    except mysql.connector.Error as e:
        print(f"[❌ ERROR] Failed to insert watch log: {e}")

# Insert billing record for a user
def insert_billing_record(user_id, subscribed_channel_ids):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Calculate total amount for subscribed channels
        format_strings = ','.join(['%s'] * len(subscribed_channel_ids))
        query = f"SELECT SUM(price_per_month) FROM channels WHERE channel_id IN ({format_strings})"
        cursor.execute(query, tuple(subscribed_channel_ids))
        total_amount = cursor.fetchone()[0]

        # Billing and due dates
        billing_date = datetime.now()
        due_date = billing_date + timedelta(days=10)

        # Insert into billing table
        cursor.execute("""
            INSERT INTO billing (user_id, amount, billing_date, due_date)
            VALUES (%s, %s, %s, %s)
        """, (user_id, total_amount, billing_date.strftime('%Y-%m-%d %H:%M:%S'),
              due_date.strftime('%Y-%m-%d %H:%M:%S')))
        conn.commit()
        print(f"[💰 BILLING] Billing record inserted for User {user_id} | Amount: {total_amount}")
        cursor.close()
        conn.close()

    except mysql.connector.Error as e:
        print(f"[❌ ERROR] Failed to insert billing record: {e}")

# ---- MAIN LOGIC ----
user_id = get_user_id(viewer_name)
if user_id is None:
    print(f"[❌ ERROR] Viewer '{viewer_name}' not found in database.")
    sys.exit(1)

all_channels = fetch_channel_ids()
subscribed_channels = random.sample(all_channels, 3) if len(all_channels) >= 3 else all_channels
print(f"[👤 {viewer_name}] Subscribed to channels: {subscribed_channels}")

# Insert billing record for this user for one month
insert_billing_record(user_id, subscribed_channels)

current_channel = random.choice(subscribed_channels)
switch_time = time.time() + random.randint(30, 120)
current_time = datetime.now()
print(f"[📡 {viewer_name}] {current_time.strftime('%Y-%m-%d %H:%M:%S')} | Now watching channel {current_channel}...\n")

consumer = KafkaConsumer(
    current_channel,
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Variables for tracking logs
watch_start_time = None
watch_stop_time = None
first_message_received = False

while True:
    if time.time() >= switch_time:
        watch_stop_time = datetime.now()

        if watch_start_time and first_message_received:
            insert_watch_log(user_id, current_channel, watch_start_time, watch_stop_time)

        new_channel = random.choice(subscribed_channels)
        while new_channel == current_channel:
            new_channel = random.choice(subscribed_channels)

        switch_timestamp = watch_stop_time.strftime('%Y-%m-%d %H:%M:%S')
        print(f"\n[🔄 SWITCH] {switch_timestamp} | {viewer_name} switched from Channel {current_channel} to {new_channel}!\n")

        consumer.unsubscribe()
        consumer.subscribe([new_channel])
        current_channel = new_channel
        switch_time = time.time() + random.randint(30, 120)

        watch_start_time = None
        watch_stop_time = None
        first_message_received = False

    message_batch = consumer.poll(timeout_ms=1000)

    for topic_partition, messages in message_batch.items():
        for message in messages:
            now = datetime.now()
            print(f"[📩 RECEIVED] {now.strftime('%Y-%m-%d %H:%M:%S')} | Viewer: {viewer_name} | Channel {message.topic} | Message: {message.value}")

            if not first_message_received:
                watch_start_time = now
                watch_stop_time = datetime.fromtimestamp(switch_time)
                first_message_received = True

