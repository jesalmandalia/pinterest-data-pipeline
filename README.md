# Pinterest Data Pipeline
- [Pinterest Data Pipeline](#pinterest-data-pipeline)
	- [Description](#description)
		- [Project Goals](#project-goals)
	- [Installation](#installation)
	- [File Structure](#file-structure)
	- [Usage](#usage)
		- [For batch processing](#for-batch-processing)
		- [For streaming data](#for-streaming-data)
	- [Project Steps](#project-steps)
		- [Batch Processing](#batch-processing)
			- [Configure the EC2 Kafka Client](#configure-the-ec2-kafka-client)
			- [Connect a MSK cluster to a S3 bucket](#connect-a-msk-cluster-to-a-s3-bucket)
			- [Configure an API in API Gateway](#configure-an-api-in-api-gateway)
			- [Databricks](#databricks)
			- [Spark on Databricks](#spark-on-databricks)
			- [AWS MWAA](#aws-mwaa)
		- [Stream Processing](#stream-processing)
			- [AWS Kinesis](#aws-kinesis)
	- [License](#license)


## Description

The "Pinterest Data Pipeline" project aims to create a data pipeline system similar to the one used by Pinterest on the AWS Cloud platform. Pinterest processes vast amounts of data daily to enhance user experience and deliver personalised content. This project serves as a learning exercise to replicate Pinterest's data handling infrastructure, incorporating various AWS services. The project's primary goal is to understand and implement a scalable data pipeline architecture using AWS tools and services. Through this project, I have gained insights into handling large-scale data, configuring cloud-based services, integrating various components of a data pipeline, data cleaning and data analysis. 


### Project Goals
- Create a scalable and efficient data processing pipeline.
- Ingest, process, and analyse data.
- Gain insights into AWS services.
- Setup data streaming pipeline


## Installation

To set up and run the "Pinterest Data Pipeline" project, follow these installation steps:

1. Clone the repository:

```bash
git clone https://github.com/jesalmandalia/pinterest-data-pipeline.git
```

2. Install any required dependencies and libraries. 

Please ensure that you have installed the necessary libraries (e.g., pandas, sqlalchemy, pyspark, apache-airflow) before running the scripts.

```bash
pip install -r requirements.txt
```

3. Configure your AWS credentials and IAM roles for access to AWS services.   
The AWS services which are used in this project are:
- Amazon EC2 (Elastic Compute Cloud): Used for creating and managing virtual servers.
- Amazon S3 (Simple Storage Service): Utilised for object storage and to store data in buckets.
- Amazon MSK (Managed Streaming for Apache Kafka): Employed for setting up and managing Apache Kafka clusters.
- AWS IAM (Identity and Access Management): Utilised for securely controlling access to AWS services and resources.
- AWS API Gateway: Used to create, publish, maintain, monitor and secure APIs at any scale.
- AWS Databricks: An Apache Spark-based analytics platform. Used for data cleaning and analysis.
- AWS MWAA (Managed Workflows for Apache Airflow): Utilised for orchestrating and automating complex data pipelines.
- AWS Kinesis Data Streams: Needed for collecting and processing large streams of data records in real time.


## File Structure

The project contains the following files:

- **user_posting_emulation.py**: Script containing login credentials for the RDS database, simulating user data posting to Pinterest and sending data to Kafka topics. This file retrieves, pinterest data which contains data about posts being updated to Pinterest, geolocation_data which contains data about the geolocation of each Pinterest post found in the pinterest data and user data which contains data about the user that has uploaded each post. The data is processed and sent to Kafka topics. Assuming that the built the plugin-connector pair is built, data going through all the three previously created Kafka topics will get saved to an S3 bucket.
- **mount_s3_bucket.ipynb**: Python notebook on how to mount an S3 bucket to the Databricks account. Data transformations and analysis are also performed using PySpark in the notebook.
- **0a48d8473ced_dag.py** is an Airflow DAG file triggering the Databricks notebook on a scheduled basis. This has been uploaded to the mwaa-dags-bucket. 
- **user_posting_emulation_streaming.py**: Python script facilitating the sending of data to Kinesis data streams.
- **kinesis_data_streams.ipynb**: Python notebook demonstrating the creation and management of data streams using AWS Kinesis.
- **README.md**: The project's README file (you're reading it!).
- **requirements.txt** Can be used to replicate the project environment.  
- **LICENSE**: Information about the project's license.

## Usage

Here are the instructions for using the Pinterest Data Pipeline:

### For batch processing
1. Run the user_posting_emulation.py script to send the user data after formatting to the designated Kafka topics:

```bash
python user_posting_emulation.py
```

1. Databricks is then used to run the `mount_s3_bucket.ipynb` notebook to clean and analyse the Pintrest data.

### For streaming data
1. Run the user_posting_emulation_streaming.py script:

```bash
python user_posting_emulation_streaming.py
```

This file is similar to the user_posting_emulation.py file in loading Pintrest data from the RDS database but uses AWS kinesis data streams to stream the data. 

2. The `kinesis_data_streams.ipynb` file is then used in Databricks to read the Kinesis streams data and transform the data. The streaming data in finally saved in a Delta table.
   
## Project Steps

The project's main structure involves several steps, each focusing on specific tasks and configurations. These are organised as follows:

1. Batch Processing: Configure the EC2 Kafka Client
2. Batch Processing: Connect a MSK cluster to a S3 bucket
3. Batch Processing: Configure an API in API Gateway
4. Batch Processing: Databricks
5. Batch Processing: Spark on Databricks
6. Batch Processing: AWS MWAA
7. Stream Processing: AWS Kinesis

### Batch Processing
#### Configure the EC2 Kafka Client
- Create a .pem file for EC2 instance access via Parameter Store, and save it with the key pair name.
- Install Kafka and IAM MSK authentication package on the EC2 machine. Ensure that necessary permissions are configured to authenticate the MSK cluster.
- Configure the Kafka client by modifying the client.properties file in the kafka_folder/bin directory to enable AWS IAM authentication to the cluster.
- Create specific topics the following Kafka topics using the Bootstrap servers and Apache Zookeeper strings obtained from the MSK Management Console.

The purpose of configuring the EC2 Kafka client was to enable secure access to the MSK cluster through IAM authentication.
  

#### Connect a MSK cluster to a S3 bucket
- Create/locate the designated S3 bucket in the S3 console.
- Download the Confluent.io Amazon S3 Connector on the EC2 client and transfer it to the identified S3 bucket.
- Create a custom plugin in the MSK Connect console.
- Set up a connector with the name in the MSK Connect console.
- Configure the connector with the correct bucket name and 'topics.regex' structure to ensure proper data flow.
- Assign the IAM role for MSK cluster authentication in the access permissions tab while building the connector.

This setup will enable automatic data storage from the IAM authenticated cluster to the designated S3 bucket.

#### Configure an API in API Gateway
- Setup or utilise the provided API, setting up a PROXY integration and HTTP ANY method with the correct EC2 PublicDNS, and deploying the API to obtain the Invoke URL.
- Install the Confluent package for Kafka REST Proxy on your EC2 client machine, configure IAM authentication for the MSK cluster in the kafka-rest.properties file and start the REST proxy to enable data transmission to the MSK Cluster using the existing plugin-connector pair.

The purpose of configuring the API in API Gateway and setting up the Kafka REST Proxy was for seamless data transmission from the API to the MSK Cluster, enabling efficient data processing and communication within the AWS environment.


#### Databricks
- Mount the designated S3 bucket to the Databricks account, creating or using the provided authentication credentials file. Create three distinct Data Frames (`df_pin`, `df_geo`, `df_user`) for processing Pinterest post data, geolocation data, and user data. 
- Modify the user_posting_emulation.py script to send data to corresponding Kafka topics via the API Invoke URL and verify the data flow to the cluster using Kafka consumers, ensuring proper data organisation within the designated S3 bucket.

These steps are essential to seamlessly process and analyse data within the Databricks environment.


#### Spark on Databricks
- Clean the `df_pin` Data Frame by replacing empty and irrelevant entries, ensuring numeric data types and reordering the columns.
- Clean the `df_geo` Data Frame by creating a coordinates array, converting data types and reordering the columns.
- Clean the `df_user` Data Frame by creating a concatenated 'user_name', converting data types and reordering the columns.
- Perform queries to find answers to meaningful questions using Spark's DataFrame operations in Databricks.

These data cleaning and querying tasks were conducted to ensure the integrity and organisation of the datasets, allowing for meaningful analysis of the Pinterest platform which can aid in informed decision-making and targeted strategy formulation.

#### AWS MWAA
- Utilise the provided MWAA environment Databricks-Airflow-env and mwaa-dags-bucket to create an Airflow DAG triggering a Databricks Notebook on a specified schedule and ensure successful manual triggering of the uploaded DAG for seamless batch processing within the AWS MWAA environment.

This process utilises the AWS MWAA environment for batch processing tasks, enabling the triggering of Databricks Notebooks on a scheduled basis therefore allowing efficient data processing and analysis.


### Stream Processing
#### AWS Kinesis
- Create 3 Kinesis Data Streams for the three Pinterest tables
- Configure the REST API to enable Kinesis actions using the access role.
- Create the user_posting_emulation_streaming.py script based on user_posting_emulation.py to send data to the corresponding Kinesis streams from the Pinterest tables.
- Read in the authentication credentials in a new Databricks Notebook, ingest data into the Kinesis data streams and verify it is received in the Kinesis console.
- Perform data cleaning for the streamed data following the same procedure used for batch data cleaning.
- Save each stream in a Delta Table for further analysis.

These tasks are essential to establish a seamless data streaming process utilising AWS Kinesis for the three designated Pinterest tables, ensuring data cleanliness and organisation through Delta Tables in Databricks.

## License

This project is licensed under the [MIT License](LICENSE). See the LICENSE file for details.

