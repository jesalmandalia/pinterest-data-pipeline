import boto3
import json
import random
import sqlalchemy
from multiprocessing import Process
from sqlalchemy import text

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

class DataRetriever:
    """
    A class to facilitate data retrieval from AWS RDS.
    
    Attributes
    ----------
    connector : AWSDBConnector
        The connector for the AWS RDS instance.
    
    Methods
    -------   
    retrieve_data_from_db()
        Retrieves data from AWS RDS, including pin, geolocation, and user data.
        Returns the retrieved data.
    _get_result(data_selected_row)
        Retrieves a single row of data from the AWS RDS and returns it as a dictionary.
    """

    def __init__(self, connector):
        """
        Initializes the DataRetriever with the given connector.
        """
        self.connector = connector

    def retrieve_data_from_db(self):
        """
        Retrieves data from AWS RDS, including pin, geolocation, and user data.
        Returns the retrieved data.
        """
        random_row = random.randint(0, 11000)
        engine = self.connector.create_db_connector()
        with engine.connect() as connection:
            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            geo_selected_row = connection.execute(geo_string)
            user_selected_row = connection.execute(user_string)
            pin_result = self._get_result(pin_selected_row)
            geo_result = self._get_result(geo_selected_row)
            user_result = self._get_result(user_selected_row)
            # datetime is not JSON serializable
            geo_result['timestamp'] = str(geo_result['timestamp'])
            user_result['date_joined'] = str(user_result['date_joined'])
            return pin_result, geo_result, user_result

    def _get_result(self, data_selected_row):
        """
        Retrieves a single row of data from the AWS RDS and returns it as a dictionary.
        """
        for row in data_selected_row:
            result = dict(row._mapping)
        return result

def build_payload_batch(data):
    """
    Builds a payload for sending data to a Kafka topic.    
    Data should be send as pairs of column_name:value, with different columns 
    separated by commas
    
    Parameters:
        data (dict): The data to be sent.
    """
    payload = json.dumps({
        "records": [{
                "value": {key: data[key] for key in data.keys()}
            }]
    })
    return payload

def build_payload_stream(data, stream_name):
    """
    Builds a payload for sending data to a Kinesis stream.
    Data should be send as pairs of column_name:value with different columns 
    separated by commas

    Parameters
    ----------
    data (dict): The data to be sent to the Kinesis stream.
    stream_name (str): The name of the Kinesis stream.
    """
    payload = json.dumps({
        "StreamName": stream_name,
        "Data": {
            key: data[key] for key in data.keys()
        },
        "PartitionKey": data['ind'] if 'ind' in data.keys() else data['index']
    })
    return payload