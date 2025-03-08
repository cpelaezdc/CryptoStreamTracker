from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic,KafkaException
import requests
import json
import time
import datetime
import os
import csv



def create_topic(topic_name, num_partitions, replication_factor):
    """
    Creates a Kafka topic if it does not already exist.

    Args:
        topic_name (str): The name of the Kafka topic to create.
        num_partitions (int): The number of partitions for the topic.
        replication_factor (int): The replication factor for the topic.

    Side Effects:
        - Creates a Kafka topic on the specified Kafka broker if it doesn't exist.
        - Prints messages to the console indicating the topic's existence or creation status.

    Raises:
        KafkaException: If there is an error communicating with the Kafka broker or creating the topic.
        Exception: If any other unexpected error occurs.
    """
    try:
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
    except KafkaException as e:
        print(f"Kafka error encountered while creating topic: {e}")
        raise #re-raise the kafka exception.

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise #re-raise other exceptions.

def append_dict_to_csv(new_value):
    """
    Appends a dictionary (new_value) as a new row to a CSV file ('first_value.csv').

    Args:
        new_value (dict): A dictionary representing the data to be appended to the CSV file.
                         The keys of the dictionary will be used as column headers.

    Side Effects:
        - Creates or appends to the 'first_value.csv' file in the current directory.
        - Prints the 'new_value' dictionary to the console.

    Raises:
        IOError: If there's an issue opening or writing to the CSV file.
    """
    first_values_csv = 'first_value.csv'   # Define the CSV file name
    field_names = new_value.keys()   # Get the field names from the dictionary keys
   
    try:
        with open(first_values_csv,mode='a',newline='') as file:
            writer = csv.DictWriter(file, fieldnames=field_names) # Create a DictWriter object
        
            # Check if the file is empty (i.e., if it's a new file or the first row)
            if not file.tell():
                writer.writeheader() # Write the header row if the file is empty
            
            writer.writerow(new_value)   # Write the data row
    except IOError as e:
        print(f"Error writing to CSV file: {e}") #Print error to console.
        raise #re-raise the error so the calling function also knows an error occured.
        
def load_csv_to_dict(first_values, first_values_csv):
    """
    Loads data from a CSV file ('first_value.csv') into a dictionary.

    Args:
        first_values (dict): A dictionary to store the loaded data, keyed by 'id' from the CSV.
        first_values_csv (str): The name of the CSV file to read.

    Side Effects:
        - Populates the 'first_values' dictionary with data from the CSV file.
        - Prints a message to the console if the CSV file does not exist.

    Raises:
        IOError: If there's an issue reading the CSV file (other than file not found).
        csv.Error: If there's an error parsing the CSV data.
    """
    
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
    except FileNotFoundError:
        print(f"file {first_values_csv} doesn't exist")
        pass
    except IOError as e:
        print(f"Error reading CSV file: {e}")
        raise #re-raise the error.
    except csv.Error as e:
        print(f"CSV parsing error: {e}")
        raise #re-raise the error.
    
    
            
    return first_values

def update_first_value(first_values,crypto_id, value, timestamp_utc): 
    """
    Updates the 'first_values' dictionary with the first value encountered for a given crypto_id on a specific UTC date.

    Args:
        first_values (dict): A dictionary to store the first value for each crypto_id, keyed by crypto_id.
                             Each value is a dictionary with 'value' and 'date' keys.
        crypto_id (str): The identifier for the cryptocurrency.
        value (float): The value to be stored.
        timestamp_utc (datetime.datetime): A UTC datetime object representing the timestamp of the value.

    Returns:
        dict: The updated 'first_values' dictionary.

    Side Effects:
        - Prints debug information to the console.
        - Appends data to a CSV file using the 'append_dict_to_csv' function.
    """
    
    # Extract the UTC date and format it as 'YYYY-MM-DD'
    current_date_utc = timestamp_utc.date().strftime('%Y-%m-%d') 
    
    # Create a dictionary to store the data
    data_dict = {
            'id': crypto_id,
            'date': current_date_utc,
            'value': value
        }
    
    # Check if the crypto_id is new or if the date has changed for an existing crypto_id and date   
    if crypto_id not in first_values or (crypto_id in first_values and first_values[crypto_id]['date'] != current_date_utc):
        print(f'cryto_id is {crypto_id}')
        print(f'current date is: {current_date_utc}')
        
        # Update the first_values dictionary with the new value and date
        first_values[crypto_id] = {'value': value,'date':current_date_utc}
        
        # Append the data to a CSV file
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
                    
                    # The current time in UTC
                    utc_current_time = datetime.datetime.now(datetime.timezone.utc)
                    
                    first_value_crypto = update_first_value(first_values,crypto['id'],crypto['priceUsd'],utc_current_time)
                    
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


# Define the name of the Kafka topic to be used.
topic_name = 'CryptoStreamTracker'

# Define a list of cryptocurrency IDs to be tracked.
crypto_list = ['bitcoin', 'ethereum', 'xrp', 'tether', 'binance-coin', 'solana','dogecoin']

# Define the sleep time (in seconds) between API requests.
sleep_time = 20

# Define the URL of the CoinCap API to fetch cryptocurrency data.
api_url = 'https://api.coincap.io/v2/assets'

# Define the name of the CSV file to store the first values of each cryptocurrency.
first_values_csv = 'first_value.csv'

# Send and empty dictionary and the name of the CSV file to the load_csv_to_dict function.
first_values = load_csv_to_dict({},first_values_csv)

# Print the loaded first values for debugging or informational purposes.
print(f'results {first_values}')     

# Create the Kafka topic if it doesn't already exist.
# The topic will have 3 partitions and a replication factor of 1.
create_topic(topic_name, 3, 1)

# Create a Kafka producer to send cryptocurrency data to the specified topic.
# It will use the loaded 'first_values', the topic name, the cryptocurrency list,
# the API URL, and the sleep time to fetch and send data.
create_producer(first_values,topic_name, crypto_list, api_url, sleep_time)



 

