"""
This script retrieves data from AWS RDS, formats it, and sends it to Kinesis streams.
"""
import random
import requests
import user_posting_emulation_toolkit as emulation_toolkit
from time import sleep

new_connector = emulation_toolkit.AWSDBConnector()

def run_infinite_post_data_loop(invoke_url):
    """
    Runs an infinite loop to retrieve data from AWS RDS 
    and send it to Kinesis streams.

    Parameters
    ----------
    invoke_url (str): The URL for invoking the Kinesis stream.
    """
    while True:
        sleep(random.randrange(0, 2))
        pin_result, geo_result, user_result = emulation_toolkit.DataRetriever(
            new_connector
        ).retrieve_data_from_db()            
        send_data_to_kinesis_stream(pin_result, 'streaming-0a48d8473ced-pin', invoke_url)
        send_data_to_kinesis_stream(geo_result, 'streaming-0a48d8473ced-geo', invoke_url)
        send_data_to_kinesis_stream(user_result, 'streaming-0a48d8473ced-user', invoke_url)

def send_data_to_kinesis_stream(data, stream_name, invoke_url):
    """
    Sends data to a Kinesis stream.

    Parameters
    ----------
    data (dict): The data to be sent to the Kinesis stream.
    stream_name (str): The name of the Kinesis stream.
    invoke_url (str): The URL for invoking the Kinesis stream.
    """
    headers = {'Content-Type': 'application/json'}
    invoke_url = invoke_url.replace('<stream_name>', stream_name)
    payload = emulation_toolkit.build_payload_stream(data, stream_name)
    response = requests.request("PUT", invoke_url, headers=headers, data=payload)
    if response.status_code == 200:
        print(f'Successfully sent data to Kinesis stream {stream_name}')
    else:
        print(f'Failed to send data to Kinesis stream {stream_name}')
        print(f'Response: {response.status_code}, {response.text}')


if __name__ == "__main__":
    invoke_url = 'https://29tbtqme3j.execute-api.us-east-1.amazonaws.com/test/streams/<stream_name>/record'
    run_infinite_post_data_loop(invoke_url)
    print('Working')
