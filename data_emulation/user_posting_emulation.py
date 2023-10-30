"""
This script retrieves data from AWS RDS, formats it, and sends it to Kafka topics.
"""
import random
import requests
import user_posting_emulation_toolkit as emulation_toolkit
from time import sleep

new_connector = emulation_toolkit.AWSDBConnector() 

def run_infinite_post_data_loop(invoke_url):
    """
    Runs an infinite loop to retrieve data from AWS RDS and send it to Kafka topics.

    Parameters:
        invoke_url (str): The URL to invoke the Kafka topic.
    """
    while True:
        sleep(random.randrange(0, 2))
        pin_result, geo_result, user_result = emulation_toolkit.DataRetriever(
            new_connector
        ).retrieve_data_from_db()             
        print(pin_result)
        print(geo_result)
        print(user_result)
        send_data_to_kafka(pin_result, '0a48d8473ced.pin', invoke_url)
        send_data_to_kafka(geo_result, '0a48d8473ced.geo', invoke_url)
        send_data_to_kafka(user_result, '0a48d8473ced.user', invoke_url)

def send_data_to_kafka(data, topic_name, invoke_url):
    """
    Sends data to a specified Kafka topic.

    Parameters:
        data (dict): The data to be sent.
        topic_name (str): The name of the Kafka topic.
        invoke_url (str): The URL to invoke the Kafka topic.
    """
    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
    payload = emulation_toolkit.build_payload_batch(data)
    response = requests.post(
        f'{invoke_url}/{topic_name}', headers=headers, data=payload
    )
    if response.status_code == 200:
        print(f'Successfully sent data to Kafka topic {topic_name}')
    else:
        print(f'Failed to send data to Kafka topic {topic_name}')
        print(f'Response: {response.status_code}, {response.text}')


if __name__ == "__main__":
    invoke_url = 'https://29tbtqme3j.execute-api.us-east-1.amazonaws.com/test/topics'
    run_infinite_post_data_loop(invoke_url)
    print('Working')
