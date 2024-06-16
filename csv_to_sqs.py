import json
import boto3
import logging
from dotenv import load_dotenv
import os
import csv

# Load environment variables from .env file
env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'env_config')
load_dotenv(dotenv_path=env_path)


# AWS Credentials and SQS Queue URL
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')
SQS_QUEUE_URL = os.getenv('SQS_QUEUE_URL')


# Logging Configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def send_csv_to_sqs(csv_file_path):
    
    

    
    try:
        sqs = boto3.client('sqs', region_name=AWS_REGION,
                           aws_access_key_id=AWS_ACCESS_KEY_ID,
                           aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
        with open(csv_file_path, 'r', newline='') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                # Prepare message body
                message_body = {
                    "user_id": row["user_id"],
                    "device_type": row["device_type"],
                    "ip": row["ip"],
                    "device_id": row["device_id"],
                    "locale": row["locale"],
                    "app_version": row["app_version"],
                    # Adjust this based on your CSV structure
                }

                # Send message to SQS
                response = sqs.send_message(
                    QueueUrl=SQS_QUEUE_URL,
                    MessageBody=json.dumps(message_body)
                )
                logger.info(f"Message sent to SQS: {response['MessageId']}")

    except Exception as e:
        logger.error(f"Error sending CSV data to SQS: {str(e)}")
        raise

csv_file_path = '/Users/muskankachhela/Downloads/MOCK_DATA.csv'
send_csv_to_sqs(csv_file_path)
