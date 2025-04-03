from kafka import KafkaConsumer
import json
import mysql.connector

# Fetch channel IDs from MySQL database
def fetch_channel_ids():
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
        cursor.execute("SELECT channel_id FROM channels;")  # Fetch only channel IDs
        channel_ids = [str(row[0]) for row in cursor.fetchall()]  # Convert to string for Kafka topics
        
        cursor.close()
        connection.close()
        return channel_ids
    
    except mysql.connector.Error as e:
        print(f"[❌ ERROR] MySQL Connection Failed: {e}")
        return []  # Return empty list if DB fails

# Fetch channel IDs (topics) from MySQL
topics = fetch_channel_ids()

# Ensure we have topics to subscribe to
if not topics:
    print("[⚠️ WARNING] No channel IDs found in database. Exiting...")
    exit()

# Create Kafka Consumer and subscribe to all retrieved topics (channel IDs)
consumer = KafkaConsumer(
    *topics,  # Subscribe to all channel IDs as topics
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',  # Start from the earliest message
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Deserialize JSON messages
)

print(f"[✅ SUCCESS] Subscribed to {len(topics)} channels: {topics}")

# Listening for messages
print("[📡 LISTENING] Waiting for messages...\n")
for message in consumer:
    print(f"[📩 RECEIVED] Channel ID: {message.topic} | Message: {message.value}")

