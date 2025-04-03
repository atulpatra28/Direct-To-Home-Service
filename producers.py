"""
from kafka import KafkaProducer
import json
import mysql.connector
import time
import signal
import sys

# Fetch channel IDs and names from MySQL database
def fetch_channels():
    try:
        connection = mysql.connector.connect(
            host="localhost",
            database="dth",
            user="root",
            password="iamatulletmein"
        )
        
        # print("[✅ DEBUG] Database connection successful!")  # Debug success message
        
        cursor = connection.cursor()
        cursor.execute("SELECT channel_id, channel_name FROM channels;")  
        channels = {row[0]: row[1] for row in cursor.fetchall()}  
        
        cursor.close()
        connection.close()
        return channels
    
    except mysql.connector.Error as e:
        print(f"[❌ ERROR] MySQL Connection Failed: {e}")
        return {}

# Create Kafka producers dynamically
def create_producers():
    channels = fetch_channels()
    producers = {}

    for channel_id, channel_name in channels.items():
        try:
            producers[channel_id] = {
                "producer": KafkaProducer(
                    bootstrap_servers='localhost:9092',
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                ),
                "channel_name": channel_name,  
                "count": 0  
            }
            # print(f"[✅ SUCCESS] Kafka Producer initialized for '{channel_name}' (Topic ID: {channel_id})")
        except Exception as e:
            print(f"[❌ ERROR] Kafka Producer Failed for '{channel_name}' (Topic ID: {channel_id}): {e}")
    
    return producers

# Create all producers
producers = create_producers()

# Graceful shutdown handler
def shutdown_handler(signal_received, frame):
    print("\n[⚠️ SHUTDOWN] Termination signal received. Flushing and closing producers...")

    for channel_id, producer_data in producers.items():
        producer = producer_data["producer"]
        channel_name = producer_data["channel_name"]
        try:
            # print(f"[🔄 FLUSH] Flushing messages for '{channel_name}' (Topic ID: {channel_id})...")
            producer.flush()  
            producer.close()  
            # print(f"[✅ CLOSED] Producer for '{channel_name}' closed.")
        except Exception as e:
            print(f"[❌ ERROR] Failed to close producer for '{channel_name}': {e}")

    print("[🚀 EXIT] All producers closed. Exiting program.")
    sys.exit(0)

# Register the shutdown handler
signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)

# Sending messages infinitely with increasing count
try:
    while True:
        for channel_id, producer_data in producers.items():
            producer = producer_data["producer"]
            channel_name = producer_data["channel_name"]

            # Increment count every 30 seconds
            producer_data["count"] += 1
            count = producer_data["count"]

            message = {
                "event": "program_update", 
                "data": f"You are now watching program number {count} on {channel_name}."
            }
            try:
                # print(f"[DEBUG] Sending message to topic '{channel_id}'...")
                future = producer.send(str(channel_id), value=message)  
                result = future.get(timeout=10)  
                # print(f"[📩 SUCCESS] Message sent from {channel_name} to topic '{channel_id}', partition: {result.partition}, offset: {result.offset}")
            except Exception as e:
                print(f"[❌ ERROR] Failed to send message to '{channel_id}': {e}")
        
        time.sleep(30)  

except KeyboardInterrupt:
    shutdown_handler(signal.SIGINT, None)

"""


from kafka import KafkaProducer
import json
import mysql.connector
import time
import signal
import sys
import random
from datetime import datetime  # Import datetime module

# Fetch channel IDs and names from MySQL database
def fetch_channels():
    try:
        connection = mysql.connector.connect(
            host="localhost",
            database="dth",
            user="root",
            password="iamatulletmein"
        )
        
        cursor = connection.cursor()
        cursor.execute("SELECT channel_id, channel_name FROM channels;")  
        channels = {row[0]: row[1] for row in cursor.fetchall()} 
        
        cursor.close()
        connection.close()
        return channels
    
    except mysql.connector.Error as e:
        print(f"[❌ ERROR] MySQL Connection Failed: {e}")
        return {}

# Create Kafka producers dynamically
def create_producers():
    channels = fetch_channels()
    producers = {}

    for channel_id, channel_name in channels.items():
        try:
            producers[channel_id] = {
                "producer": KafkaProducer(
                    bootstrap_servers='localhost:9092',
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                ),
                "channel_name": channel_name,  
                "program_number": 1,  # First program number
                "remaining_time": random.randint(30, 120)  # Random duration for the first program
            }
        except Exception as e:
            print(f"[❌ ERROR] Kafka Producer Failed for '{channel_name}' (Topic ID: {channel_id}): {e}")
    
    return producers

# Create all producers
producers = create_producers()

# Graceful shutdown handler
def shutdown_handler(signal_received, frame):
    print("\n[⚠️ SHUTDOWN] Termination signal received. Flushing and closing producers...")

    for producer_data in producers.values():
        producer = producer_data["producer"]
        try:
            producer.flush()  
            producer.close()  
        except Exception:
            pass

    print("[🚀 EXIT] All producers closed. Exiting program.")
    sys.exit(0)

# Register the shutdown handler
signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)

# Sending messages infinitely with dynamically changing program numbers
try:
    while True:
        for channel_id, producer_data in producers.items():
            producer = producer_data["producer"]
            channel_name = producer_data["channel_name"]
            program_number = producer_data["program_number"]
            remaining_time = producer_data["remaining_time"]

            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Capture current timestamp

            message = {
                "event": "program_update", 
                "data": f"You are now watching program number {program_number} on {channel_name}.",
                "timestamp": timestamp
            }

            try:
                producer.send(str(channel_id), value=message)
                print(f"[📩 SENT] {timestamp} | {channel_name} | Program #{program_number} | Remaining: {remaining_time} sec")
            except Exception as e:
                print(f"[⚠️ ERROR] Failed to send message for {channel_name}: {e}")

            # Reduce the remaining time
            producer_data["remaining_time"] -= 10  

            # If time is up, switch to the next program
            if producer_data["remaining_time"] <= 0:
                producer_data["program_number"] += 1
                producer_data["remaining_time"] = random.randint(30, 120)  # Assign new duration

        time.sleep(10)  # Log updates every 10 seconds

except KeyboardInterrupt:
    shutdown_handler(signal.SIGINT, None)

