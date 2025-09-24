import json
import random
from datetime import datetime
import os
import sys
import uuid
from typing import Dict, Any

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.sqs_utils import connect_SQS

# Hardcoded users
USERS = ["U5678", "U1234", "U9999", "U1111", "U2222"]

# Hardcoded products with prices
PRODUCTS = [
    {"product_id": "P001", "price_per_unit": 20.00},
    {"product_id": "P002", "price_per_unit": 59.99},
    {"product_id": "P003", "price_per_unit": 15.50},
    {"product_id": "P004", "price_per_unit": 100.00},
    {"product_id": "P005", "price_per_unit": 5.99},
]

def generate_random_orders(num_orders: int) -> Dict[str, Any]:
    """
    Generate and send random orders to SQS
    
    Args:
        num_orders (int): Number of random orders to generate.
    
    Returns:
        Dict[str, Any]: Summary of the operation.
    """
    
    queue_url, sqs_client = connect_SQS(
        endpoint_url="http://localhost:4566", region_name="us-east-1",
        aws_access_key_id="test", aws_secret_access_key="test"
    )
    if not queue_url or not sqs_client:
        print("Failed to connect to SQS")
        return

    for i in range(num_orders):
        user_id = random.choice(USERS)
        
        now = datetime.now()
        order_id = f"ORD{now.strftime('%Y%m%d%H%M%S')}{random.randint(100,999)}"
        
        order_timestamp = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        num_items = random.randint(1, 5)
        items = []
        total_value = 0.0
        for _ in range(num_items):
            product = random.choice(PRODUCTS)
            quantity = random.randint(1, 10)
            item_total = quantity * product["price_per_unit"]
            total_value += item_total
            items.append({
                "product_id": product["product_id"],
                "quantity": quantity,
                "price_per_unit": product["price_per_unit"]
            })
        
        addresses = [
            "123 MG Road, Indiranagar, Bengaluru, Karnataka 560038",
            "45 Sector 18, Noida, Uttar Pradesh 201301",
            "78 Park Street, Kolkata, West Bengal 700016",
            "12 Anna Salai, T. Nagar, Chennai, Tamil Nadu 600017",
            "89 FC Road, Shivaji Nagar, Pune, Maharashtra 411005"
        ]
        shipping_address = random.choice(addresses)

        payment_methods = ["CreditCard", "GPay", "BankTransfer", "Cash on Delivery", "Debit Card"]
        payment_method = random.choice(payment_methods)
        
        # Create order
        order = {
            "order_id": order_id,
            "user_id": user_id,
            "order_timestamp": order_timestamp,
            "order_value": round(total_value, 2),
            "items": items,
            "shipping_address": shipping_address,
            "payment_method": payment_method
        }
        
        # Send to SQS
        try:
            sqs_client.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps(order)
            )
            print(f"Sent order {order_id} for user {user_id} with value {total_value:.2f}")
        except Exception as e:
            print(f"Failed to send order {order_id}: {e}")

if __name__ == "__main__":
    generate_random_orders(100)
