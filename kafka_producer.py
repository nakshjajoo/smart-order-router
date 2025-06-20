import pandas as pd
from kafka import KafkaProducer
import json
import time
from datetime import datetime

# Creates a Kafka producer
def create_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            linger_ms=10, # Batch messages for 10ms
            batch_size=16384 # 16KB batch size
        )
        print("Kafka Producer created successfully.")
        return producer
    except Exception as e:
        print(f"Error creating Kafka producer: {e}")
        return None

# Reads market data, filters it for the specified time window, and streams it to a Kafka topic
def stream_data(producer, topic):
    if not producer:
        print("Producer is not available. Aborting stream.")
        return

    try:
        df = pd.read_csv('l1_day.csv')
        # Convert ts_event to datetime objects for filtering
        df['ts_event'] = pd.to_datetime(df['ts_event'])
    except FileNotFoundError:
        print("Error: l1_day.csv not found.")
        return
    except Exception as e:
        print(f"An error occurred while reading the CSV: {e}")
        return

    # Defining the simulation time window
    start_time = pd.to_datetime('2024-08-01 13:36:32.000000000Z')
    end_time = pd.to_datetime('2024-08-01 13:45:14.000000000Z')

    # filter for the relevant time window and rtype=10 (market snapshots)
    # also filter for 'ask_px_00' > 0 to ensure valid ask prices
    mask = (df['ts_event'] >= start_time) & (df['ts_event'] <= end_time) & (df['rtype'] == 10) & (df['ask_px_00'] > 0)
    
    sim_df = df[mask].copy()

    if sim_df.empty:
        print("No data available for the specified time window.")
        return
        
    # To ensure we have the most recent update for each publisher at each timestamp, we sort by ts_event and sequence and drop duplicates.
    sim_df = sim_df.sort_values(by=['ts_event', 'sequence']).drop_duplicates(
        subset=['ts_event', 'publisher_id'],
        keep='last'
    )
    
    print(f"Streaming {len(sim_df)} snapshots from {sim_df['ts_event'].min()} to {sim_df['ts_event'].max()}")

    # Grouping data by timestamp to send snapshots together
    grouped = sim_df.groupby('ts_event')
    last_ts = None

    for ts, group in grouped:
        if last_ts:
            # simulating real-time pacing
            delta = (ts - last_ts).total_seconds()
            if delta > 0:
                time.sleep(delta)

        snapshot = []
        for _, row in group.iterrows():
            message = {
                'ts_event': row['ts_event'].isoformat(),
                'publisher_id': row['publisher_id'],
                'ask_px_00': row['ask_px_00'],
                'ask_sz_00': row['ask_sz_00']
            }
            snapshot.append(message)
        
        # Send the complete snapshot for the timestamp
        producer.send(topic, value={'snapshot': snapshot})
        print(f"Sent snapshot for timestamp: {ts}")
        last_ts = ts
    
    print("Finished streaming data.")
    producer.flush()
    producer.close()

if __name__ == "__main__":
    kafka_topic = 'mock_l1_stream'
    kafka_producer = create_producer()
    if kafka_producer:
        stream_data(kafka_producer, kafka_topic)