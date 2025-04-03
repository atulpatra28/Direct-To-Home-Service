import mysql.connector

try:
    print("[DEBUG] Attempting to connect to MySQL...")
    connection = mysql.connector.connect(
        host="localhost",
        database="dth",
        user="root",
        password="iamatulletmein"
    )
    
    if connection.is_connected():
        print("[✅ SUCCESS] Database connection established!")
        connection.close()
    else:
        print("[❌ ERROR] Connection failed.")

except mysql.connector.Error as err:
    print(f"[❌ ERROR] {err}")

