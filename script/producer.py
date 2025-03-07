from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import requests
import json
import time
import datetime
import os
import csv



def create_topic(topic_name, num_partitions, replication_factor):
    admin_client = AdminClient({'bootstrap.servers': 'localhost:29092'})
    topic_name = 'CryptoStreamTracker'

    # Define the topic configuration
    topic = NewTopic(topic_name, num_partitions, replication_factor)

    # Check if the topic already exists
    topics = admin_client.list_topics().topics
    if 'CryptoStreamTracker' not in topics:
        print(f'{topic_name} does not exist')
        # Create the topic
        admin_client.create_topics([topic])
        print(f"Topic '{topic_name}' created successfully.")
    else:
        print(f'{topic_name} already exists')


def append_dict_to_csv(new_value):
    first_values_csv = 'first_value.csv'
    field_names = new_value.keys()
    print(new_value)
    with open(first_values_csv,mode='a',newline='') as file:
        writer = csv.DictWriter(file, fieldnames=field_names)
        if not file.tell():
            writer.writeheader()
            
        writer.writerow(new_value)
        
def load_csv_to_dict(first_values):
    first_values_csv = 'first_value.csv'
    
    try:
        with open(first_values_csv,mode='r',newline='') as file:
            # Create a CSV DictReader Object
            csv_reader = csv.DictReader(file)
            
            for row in csv_reader:
                key =  row['id']
                first_values[key] = {
                    'date': row['date'],
                    'value': row['value']
                }
    except:
        print(f"file {first_values_csv} doesn't exist")
        pass
    
    
            
    return first_values

def update_first_value(first_values,crypto_id, value, timestamp): 
    
    current_date = timestamp.date().strftime('%Y-%m-%d') 
    
    data_dict = {
            'id': crypto_id,
            'date': current_date,
            'value': value
        }
    
       
    if crypto_id not in first_values or (crypto_id in first_values and first_values[crypto_id]['date'] != current_date):
        print(f'cryto_id is {crypto_id}')
        print(f'current date is: {current_date}')
        first_values[crypto_id] = {'value': value,'date':current_date}
        append_dict_to_csv(data_dict)
        
    return first_values


def create_producer(first_values,topic_name,crypto_list,api_url,sleep_time):
    producer = Producer({'bootstrap.servers': 'localhost:29092'})
    
    while True:
        response = requests.get(api_url)
        data = response.json()
        if response.status_code == 200:
            for crypto in data['data']:
                if crypto['id'] in crypto_list:
                    # Save and/or find first value
                    first_value_crypto = update_first_value(first_values,crypto['id'],crypto['priceUsd'],datetime.datetime.now())
                    
                    # Dictionary with the crypto data
                    crypto = {
                        'timestamp': time.time(),
                        'id': crypto['id'],
                        'symbol': crypto['symbol'],
                        'name': crypto['name'],
                        'priceUsd': crypto['priceUsd'],
                        'changePercent24Hr': crypto['changePercent24Hr'],
                        'firstValue': first_value_crypto[crypto['id']]['value'],
                        'vwap24Hr': crypto['vwap24Hr']
                    }
                    # Send the data to the topic
                    producer.produce(topic_name, json.dumps(crypto))
                    print(f"time: {crypto['timestamp']}, Currency: {crypto['id']}, {crypto['priceUsd']}, {crypto['firstValue']}")
        else:
            print(f'Error in the request {response.status_code}')
                        
        time.sleep(sleep_time)


topic_name = 'CryptoStreamTracker'
crypto_list = ['bitcoin', 'ethereum', 'xrp', 'tether', 'binance-coin', 'solana','dogecoin']
sleep_time = 20
api_url = 'https://api.coincap.io/v2/assets'
first_values = {}

first_values = load_csv_to_dict(first_values)

print(f'results {first_values}')     

create_topic(topic_name, 3, 1)

create_producer(first_values,topic_name, crypto_list, api_url, sleep_time)



 

