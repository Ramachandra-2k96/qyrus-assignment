"""
SQS utilities for connecting to and managing AWS SQS queues.

This module provides functions for establishing connections to SQS,
creating queues, and basic queue operations.
"""

import logging
from typing import Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def connect_SQS(
    endpoint_url: str = "http://localhost:4566",
    region_name: str = "us-east-1",
    aws_access_key_id: str = "test",
    aws_secret_access_key: str = "test"
) -> Tuple[Optional[str], Optional[boto3.client]]:
    """
    Establish connection to SQS and ensure the orders-queue exists.

    This function creates a boto3 SQS client and ensures that the 'orders-queue'
    exists, creating it if necessary. It uses LocalStack-compatible credentials
    for development environments.

    Args:
        endpoint_url: The SQS endpoint URL (default: LocalStack URL)
        region_name: AWS region name (default: us-east-1)
        aws_access_key_id: AWS access key ID (default: test credentials)
        aws_secret_access_key: AWS secret access key (default: test credentials)

    Returns:
        Tuple containing:
        - queue_url: The URL of the SQS queue, or None if connection failed
        - sqs_client: The boto3 SQS client instance, or None if connection failed

    Raises:
        ClientError: If there's an AWS service error
        Exception: For other connection-related errors
    """
    logger.info("Attempting to connect to SQS at %s", endpoint_url)

    try:
        # Create SQS client
        sqs = boto3.client(
            "sqs",
            endpoint_url=endpoint_url,
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )

        logger.debug("SQS client created successfully")

        # Try to get existing queue, create if it doesn't exist
        try:
            response = sqs.get_queue_url(QueueName="orders-queue")
            queue_url = response['QueueUrl']
            logger.info("Found existing orders-queue: %s", queue_url)

        except sqs.exceptions.QueueDoesNotExist:
            logger.info("orders-queue does not exist, creating new queue")
            response = sqs.create_queue(QueueName="orders-queue")
            queue_url = response['QueueUrl']
            logger.info("Created new orders-queue: %s", queue_url)

        except ClientError as e:
            logger.error("Error accessing queue: %s", e)
            return None, None

        logger.info("SQS connection established successfully")
        return queue_url, sqs

    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        logger.error("AWS SQS error [%s]: %s", error_code, e)
        return None, None

    except Exception as e:
        logger.error("Unexpected error connecting to SQS: %s", e)
        return None, None

if __name__ == "__main__":
    queue_url, sqs_client = connect_SQS()
    if queue_url:
        logger.info("Connected to SQS queue at URL: %s", queue_url)
    else:
        logger.error("Failed to connect to SQS")