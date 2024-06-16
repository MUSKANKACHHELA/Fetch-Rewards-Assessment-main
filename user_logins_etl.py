from datetime import datetime
import json
import psycopg2
import hashlib
import boto3
import logging
from dotenv import load_dotenv
import os
import time


# Load environment variables from dotenv file
env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'env_config')
load_dotenv(dotenv_path=env_path)


# AWS Credentials and SQS Queue URL
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')
SQS_QUEUE_URL = os.getenv('SQS_QUEUE_URL')

# PostgreSQL Connection String
DB_CONNECTION_STRING = "dbname=postgres user=postgres password=postgres host=localhost port=5432"

# Logging Configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_db_connection():
    return psycopg2.connect(DB_CONNECTION_STRING)

def mask_field(value):
    return hashlib.sha256(value.encode()).hexdigest()

def insert_data(cursor, user_data):
    try:
        insert_query = """
        INSERT INTO user_logins (user_id, device_type, masked_ip, masked_device_id, locale, app_version, create_date)
        VALUES (%(user_id)s, %(device_type)s, %(masked_ip)s, %(masked_device_id)s, %(locale)s, %(app_version)s, %(create_date)s)
        """
        cursor.execute(insert_query, user_data)
        logger.info("Data inserted successfully.")
    except Exception as error:
        logger.error(f"Error inserting data into PostgreSQL: {str(error)}")
        raise

def strip_extra_chars(s, limit = 32):
    return s[:limit]

def process_message(cursor, message):
    try:
        message_body = json.loads(message["Body"])
        message_body["masked_ip"] = mask_field(message_body["ip"])
        message_body["masked_device_id"] = mask_field(message_body["device_id"])

        user_data = {
            "user_id": message_body["user_id"],
            "device_type": strip_extra_chars(message_body["device_type"]),
            "masked_ip": message_body["masked_ip"],
            "masked_device_id": message_body["masked_device_id"],
            "locale": message_body["locale"],
            "app_version": message_body["app_version"],
            "create_date": datetime.now()
        }

        print(user_data)
        insert_data(cursor, user_data)
    except Exception as error:
        logger.error(f"Error processing message: {str(error)}")
        log_error(cursor, error, message, message_body)
        raise

def log_error(cursor, error_message, message, message_body):
    try:
        create_error_table_query = """
        CREATE TABLE IF NOT EXISTS error_records (
            error_id serial PRIMARY KEY,
            error_message text,
            message jsonb,
            message_body jsonb
        );
        """
        cursor.execute(create_error_table_query)
        cursor.execute("INSERT INTO error_records (error_message, message, message_body) VALUES (%s, %s, %s)",
                       (str(error_message), json.dumps(message), json.dumps(message_body)))
        logger.info("Error message logged successfully.")
    except Exception as error:
        logger.error(f"Error logging the error message: {str(error)}")



def receive_sqs_messages():
    try:
        sqs = boto3.client('sqs', region_name=AWS_REGION,
                           aws_access_key_id=AWS_ACCESS_KEY_ID,
                           aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

        response = sqs.receive_message(
            QueueUrl=SQS_QUEUE_URL,
            AttributeNames=['All'],
            MaxNumberOfMessages=1
        )
        return response.get('Messages', [])
    except Exception as error:
        logger.error(f"Error receiving SQS message: {str(error)}")
        raise

def delete_sqs_message(receipt_handle):
    try:
        sqs = boto3.client('sqs', region_name=AWS_REGION,
                           aws_access_key_id=AWS_ACCESS_KEY_ID,
                           aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

        sqs.delete_message(
            QueueUrl=SQS_QUEUE_URL,
            ReceiptHandle=receipt_handle
        )
        logger.info("Message deleted successfully.")
    except Exception as error:
        logger.error(f"Error deleting SQS message: {str(error)}")

def get_sqs_message_count():
    try:
        sqs = boto3.client('sqs', region_name=AWS_REGION,
                           aws_access_key_id=AWS_ACCESS_KEY_ID,
                           aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

        response = sqs.get_queue_attributes(
            QueueUrl=SQS_QUEUE_URL,
            AttributeNames=['ApproximateNumberOfMessages']
        )
        return int(response['Attributes']['ApproximateNumberOfMessages'])
    except Exception as error:
        logger.error(f"Error getting SQS message count: {str(error)}")
        return 0



def poll_sqs_queue():
    connection = get_db_connection()
    cursor = connection.cursor()

    while True:
        messages = receive_sqs_messages()
        if not messages:
            logger.info("No messages in the queue. Waiting for new messages...")
            time.sleep(10)  # Wait for 10 seconds before checking again
            continue

        for message in messages:
            try:
                process_message(cursor, message)
                delete_sqs_message(message["ReceiptHandle"])
            except Exception as error:
                logger.error(f"Error processing message: {str(error)}")
                log_error(cursor, error, message, json.loads(message["Body"]))


def main():
    
    connection = get_db_connection()
    cursor = connection.cursor()
    # Initialize SQS client
    

    # Continuously poll SQS for messages
    while True:

        # Get the approximate number of messages in the queue
        
        max_no_of_messages = get_sqs_message_count()
      
        logger.info(f"Number of messages in the queue: {max_no_of_messages}")
        

        # Check if there are messages to process
        if  max_no_of_messages != 0:

            
            # Extract the message body
            

            messages = receive_sqs_messages()

            for message in messages:
                try:
                    process_message(cursor, message)
                    delete_sqs_message(message["ReceiptHandle"])
                    connection.commit()
                except Exception as error:
                    logger.error(f"Error processing message: {str(error)}")
                    log_error(cursor, error, message, json.loads(message["Body"]))
          

        else:
          
          logger.info("No messages to process. Exiting.")
          break

    cursor.close()
    connection.close()
         
          
    


    

if __name__ == "__main__":
    main()
