from confluent_kafka import Consumer
import json
import time
import csv

topic_name = 'CryptoStreamTracker'
group_id = 'crypto_group'
auto_offset_reset = 'earliest'
csv_file_path = 'crypto_data.csv'
fieldnames = ['timestamp','id','symbol','name','priceUsd','changePercent24Hr','firstValue','vwap24Hr']

consumer = Consumer({'bootstrap.servers': 'localhost:29092', 'group.id': group_id, 'auto.offset.reset': auto_offset_reset})
consumer.subscribe([topic_name])

with open(csv_file_path, 'a', newline='') as file:
    writer = csv.DictWriter(file, fieldnames=fieldnames)
    
    if not file.tell():
        writer.writeheader()
        
    try:
        while True:
            message = consumer.poll(timeout=1.0)
            if message is None:
                continue
            if message.error():
                print(f"Consumer error: {message.error()}")
                continue
            data = json.loads(message.value())
            writer.writerow(data)
            print(f"time: {data['timestamp']}, Currency: {data['id']}, {data['priceUsd']}, First value: {data['firstValue']}")
            
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        print("Close consumer")
        time.sleep(5)





