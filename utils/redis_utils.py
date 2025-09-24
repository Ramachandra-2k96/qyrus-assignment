import redis
from typing import Optional

import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def connect_Redis(host: str = "localhost", port: int = 6379, db: int = 0, password: Optional[str] = None) -> Optional[redis.Redis]:
    """
    Connect to Redis server.

    Args:
        host: Redis server hostname (default: localhost)
        port: Redis server port (default: 6379)
        db: Redis database number (default: 0)
        password: Redis password if required (default: None)
    Returns:
        Redis client instance if connection is successful, None otherwise.
    """
    try:    
        logger.info("Attempting to connect to Redis at %s:%d", host, port)
        client = redis.Redis(host=host, port=port, db=db, password=password, decode_responses=True)
        # Test connection
        client.ping()
        logger.info("Connected to Redis successfully")
        return client
    except redis.ConnectionError as e:
        logger.error("Failed to connect to Redis: %s", e)
        return None

if __name__ == "__main__":
    redis_client = connect_Redis()
    if redis_client:
        logger.info("Redis client is ready to use")
    else:
        logger.error("Redis connection failed")