"""
This script retrieves data from AWS RDS, formats it, and sends it to Kinesis streams.
"""
import boto3
import json
import random
import requests
import sqlalchemy 
from multiprocessing import Process
from sqlalchemy import text
from time import sleep

random.seed(100)

class AWSDBConnector:
    """
    Connects to an AWS RDS instance.

    Attributes
    ----------
    HOST : str
        The host address for the AWS database.
    USER : str
        The username for accessing the AWS database.
    PASSWORD : str
        The password for accessing the AWS database.
    DATABASE : str
        The name of the database.
    PORT : int
        The port number for the AWS database.

    Methods
    -------
    create_db_connector()
        Creates an engine for connecting to the AWS database.
    """

    def __init__(self):
        self.HOST = "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        self.USER = 'project_user'
        self.PASSWORD = ':t%;yCY3Yjg'
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306

    def create_db_connector(self):
        """
        Creates and returns an SQLAlchemy engine for the AWS RDS instance.
        """
        engine = sqlalchemy.create_engine(
            f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}"
            f":{self.PORT}/{self.DATABASE}?charset=utf8mb4"
        )             
        return engine


new_connector = AWSDBConnector()


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
    #Data should be send as pairs of column_name:value, 
    #with different columns separated by commas
    payload = json.dumps({
        "StreamName": stream_name,
        "Data": {
            key: data[key] for key in data.keys()
        },
        "PartitionKey": data['ind'] if 'ind' in data.keys() else data['index']
    })
    response = requests.request("PUT", invoke_url, headers=headers, data=payload)
    if response.status_code == 200:
        print(f'Successfully sent data to Kinesis stream {stream_name}')
    else:
        print(f'Failed to send data to Kinesis stream {stream_name}')
        print(f'Response: {response.status_code}, {response.text}')


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
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()
        with engine.connect() as connection:
            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            for row in pin_selected_row:
                pin_result = dict(row._mapping)
            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            for row in geo_selected_row:
                geo_result = dict(row._mapping)
            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            for row in user_selected_row:
                user_result = dict(row._mapping)
            #datetime is not JSON serialisable    
            geo_result['timestamp'] = str(geo_result['timestamp'])
            user_result['date_joined'] = str(user_result['date_joined'])
            send_data_to_kinesis_stream(pin_result, 'streaming-0a48d8473ced-pin', invoke_url)
            send_data_to_kinesis_stream(geo_result, 'streaming-0a48d8473ced-geo', invoke_url)
            send_data_to_kinesis_stream(user_result, 'streaming-0a48d8473ced-user', invoke_url)


if __name__ == "__main__":
    invoke_url = 'https://29tbtqme3j.execute-api.us-east-1.amazonaws.com/test/streams/<stream_name>/record'
    run_infinite_post_data_loop(invoke_url)
    print('Working')
