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
        
        if connection.is_connected():
            print("[✅ DEBUG] Database connection successful!")  # Debug success message
        
        cursor = connection.cursor()
        cursor.execute("SELECT channel_id, channel_name FROM channels;")  # Fetch channel_id
        channels = {row[0]: row[1] for row in cursor.fetchall()}  # Store as {channel_id: channel_name}
        
        cursor.close()
        connection.close()
        return channels
    
    except mysql.connector.Error as e:
        print(f"[❌ ERROR] MySQL Connection Failed: {e}")
        return {}  # Return empty dict if DB fails

# Create Kafka producers dynamically
def create_producers():
    channels = fetch_channels()
    producers = {}

    for channel_id, channel_name in channels.items():
        try:
            producers[channel_id] = KafkaProducer(
                bootstrap_servers='localhost:9092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print(f"[✅ SUCCESS] Kafka Producer initialized for '{channel_name}' (Topic ID: {channel_id})")
        except Exception as e:
            print(f"[❌ ERROR] Kafka Producer Failed for '{channel_name}' (Topic ID: {channel_id}): {e}")
    
    return producers

# Create all producers
producers = create_producers()

# Graceful shutdown handler
def shutdown_handler(signal_received, frame):
    print("\n[⚠️ SHUTDOWN] Termination signal received. Flushing and closing producers...")

    for channel_id, producer in producers.items():
        try:
            print(f"[🔄 FLUSH] Flushing messages for topic '{channel_id}'...")
            producer.flush()  # Ensure all buffered messages are sent
            producer.close()  # Close the producer cleanly
            print(f"[✅ CLOSED] Producer for topic '{channel_id}' closed.")
        except Exception as e:
            print(f"[❌ ERROR] Failed to close producer for '{channel_id}': {e}")

    print("[🚀 EXIT] All producers closed. Exiting program.")
    sys.exit(0)

# Register the shutdown handler for SIGINT (Ctrl+C) and SIGTERM
signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)

# Sending messages continuously
try:
    while True:
        for channel_id, producer in producers.items():
            message = {"event": "welcome_message", "data": f"Welcome to Channel {channel_id}!"}
            try:
                print(f"[DEBUG] Sending message to topic '{channel_id}'...")
                future = producer.send(str(channel_id), value=message)  # Topic is channel_id (string)
                result = future.get(timeout=10)  # Ensures message is sent before moving forward
                print(f"[📩 SUCCESS] Message sent to '{channel_id}', partition: {result.partition}, offset: {result.offset}")
            except Exception as e:
                print(f"[❌ ERROR] Failed to send message to '{channel_id}': {e}")
        
        time.sleep(5)  # Wait before sending again

except KeyboardInterrupt:
    shutdown_handler(signal.SIGINT, None)

