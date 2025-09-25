import time
import json
import logging
from typing import Any, Dict
from utils.processing import process_order
from utils.redis_utils import connect_Redis
from utils.sqs_utils import connect_SQS

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MessageWorker:
    def __init__(self):
        """Initialize the worker with Redis connection"""
        self.redis_client = connect_Redis(host="redis", port=6379, db=0, password=None)
        self.queue_url, self.SQS_client = connect_SQS(endpoint_url="http://localstack:4566", region_name="us-east-1",aws_access_key_id="test", aws_secret_access_key="test")
        self.running = True
        
    def health_redis(self):
        """Test Redis connection"""
        try:
            self.redis_client.ping()
            logger.info("Connected to Redis successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            return False
        
    def health_sqs(self):
        """Test SQS connection"""
        try:
            self.SQS_client.get_queue_url(QueueName="orders-queue")
            logger.info("Connected to SQS successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to SQS: {e}")
            return False
    
    def run(self):
        """Main worker loop"""
        logger.info("Starting message worker...")
            
        if not self.health_redis():
            logger.error("Cannot start worker without Redis connection")
            return
        if not self.health_sqs():
            logger.error("Cannot start worker without SQS connection")
            return
        
        while self.running:
            try:
                # Poll SQS for messages
                response = self.SQS_client.receive_message(
                    QueueUrl=self.queue_url,
                    MaxNumberOfMessages=1,
                    WaitTimeSeconds=20  # Long polling
                )
                
                messages = response.get('Messages', [])
                if messages:
                    for message in messages:
                        try:
                            # Parse message body
                            message_body = json.loads(message['Body'])
                            
                            # Preprocess the order
                            processed = process_order(message_body)
                            
                            if processed['status'] == 'VALID':
                                # Update Redis aggregates
                                user_id = processed['user_id']
                                order_value = processed['order_value']
                                order_id = processed['order_id']
                                order_timestamp = processed['order_timestamp']
                                
                                # Extract date from timestamp (YYYY-MM-DD)
                                date = order_timestamp.split('T')[0]
                                
                                # Update daily user stats
                                user_key_daily = f"user:{user_id}:{date}"
                                self.redis_client.hincrby(user_key_daily, 'order_count', 1)
                                new_daily_spend = self.redis_client.hincrbyfloat(user_key_daily, 'total_spend', order_value)
                                
                                # Update overall user stats
                                user_key = f"user:{user_id}"
                                self.redis_client.hincrby(user_key, 'order_count', 1)
                                self.redis_client.hincrbyfloat(user_key, 'total_spend', order_value)
                                
                                # Update daily rankings sorted set
                                sorted_set_key_daily = f"daily:{date}"
                                self.redis_client.zadd(sorted_set_key_daily, {user_id: new_daily_spend})
                                
                                # Update global stats
                                self.redis_client.hincrby('global:stats', 'total_orders', 1)
                                self.redis_client.hincrbyfloat('global:stats', 'total_revenue', order_value)
                                
                                logger.info(f"Updated aggregates for valid order {order_id} (User: {user_id}, Value: {order_value}, Date: {date})")
                                
                                # Remove from queue
                                self.SQS_client.delete_message(
                                    QueueUrl=self.queue_url,
                                    ReceiptHandle=message['ReceiptHandle']
                                )
                                logger.info(f"Removed order {order_id} from SQS queue")
                            else:
                                logger.warning(f"Invalid order {processed.get('order_id', 'Unknown')}, not adding to Redis")
                                # Still remove from queue? Probably yes, since invalid, don't retry
                                self.SQS_client.delete_message(
                                    QueueUrl=self.queue_url,
                                    ReceiptHandle=message['ReceiptHandle']
                                )
                        except Exception as e:
                            logger.error(f"Error processing SQS message: {e}")
                else:
                    logger.info("No messages in queue, waiting...")
                
            except KeyboardInterrupt:
                logger.info("Worker stopped by user")
                self.running = False
            except Exception as e:
                logger.error(f"Worker error: {e}")
                time.sleep(10)  # Wait longer on error
if __name__ == "__main__":
    worker = MessageWorker()
    worker.run()
