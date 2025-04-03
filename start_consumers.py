import subprocess
import time
import mysql.connector

# Fetch viewer names from the database
def fetch_viewers():
    try:
        connection = mysql.connector.connect(
            host="localhost",
            database="dth",
            user="root",
            password="iamatulletmein"
        )
        cursor = connection.cursor()
        cursor.execute("SELECT name FROM viewers;")  # Fetch viewer names
        viewers = [row[0] for row in cursor.fetchall()]
        
        cursor.close()
        connection.close()
        return viewers

    except mysql.connector.Error as e:
        print(f"[❌ ERROR] MySQL Connection Failed: {e}")
        return []

# Get the list of viewers
viewers = fetch_viewers()

# Run a separate consumer for each viewer
for viewer in viewers:
    cmd = f"gnome-terminal -- bash -c 'python3 consumers.py {viewer}; exec bash'" 
    subprocess.Popen(cmd, shell=True)  # Open each consumer in a new terminal
    time.sleep(1)  # Small delay to prevent overload

