import pandas as pd
import mysql.connector
import matplotlib.pyplot as plt
from datetime import datetime

def run_analytics(df):
    # Convert start_time and stop_time to datetime
    df['start_time'] = pd.to_datetime(df['start_time'])
    df['stop_time'] = pd.to_datetime(df['stop_time'])

    # Calculate watch duration in minutes
    df['watch_duration'] = (df['stop_time'] - df['start_time']).dt.total_seconds() / 60

    # 📊 Most Viewed Channels (by number of logs)
    view_counts = df['channel_id'].value_counts().reset_index()
    view_counts.columns = ['channel_id', 'view_count']
    view_counts['rank'] = view_counts['view_count'].rank(method='dense', ascending=False).astype(int)
    view_counts = view_counts.sort_values(by='view_count', ascending=False)

    print("\n🔢 Most Viewed Channels (by number of logs):")
    print(view_counts.to_string(index=False))

    # 📺 TRP: Normalized as % of total watch time
    total_watch_time = df['watch_duration'].sum()
    trp_df = df.groupby('channel_id')['watch_duration'].sum().reset_index()
    trp_df['TRP (%)'] = (trp_df['watch_duration'] / total_watch_time) * 100
    trp_df = trp_df.sort_values(by='TRP (%)', ascending=False)

    print("\n📺 TRP (Normalized - Percentage of Total Watch Time):")
    print(trp_df[['channel_id', 'TRP (%)']].round(2).to_string(index=False))

    # 📈 Plot: Total view time per channel
    plt.figure(figsize=(10, 6))
    plt.bar(trp_df['channel_id'].astype(str), trp_df['watch_duration'], color='skyblue')
    plt.title('Total View Time per Channel (in Minutes)')
    plt.xlabel('Channel ID')
    plt.ylabel('Total Watch Time (Minutes)')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.show()

def main():
    try:
        # DB connection
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="iamatulletmein",
            database="dth"
        )
        print("[✅] Connected to database.")

        query = "SELECT * FROM watch_logs"
        df = pd.read_sql(query, connection)

        run_analytics(df)

    except mysql.connector.Error as err:
        print(f"[❌] Database Error: {err}")
    except Exception as e:
        print(f"[❌] Unexpected Error: {e}")
    finally:
        if 'connection' in locals() and connection.is_connected():
            connection.close()

if __name__ == "__main__":
    main()

