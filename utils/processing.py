from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def process_order(order_data: Dict[str, Any], correct_value: bool = True) -> Dict[str, Any]:
    """
    Validate and preprocess an order dictionary.

    Steps:
    1. Validate mandatory fields (user_id, order_id, order_value).
    2. Verify that order_value matches computed total from items.
    3. Extract essential fields for downstream aggregation.

    Args:
        order_data (Dict[str, Any]): Raw order dictionary.
        correct_value (bool): If True, automatically correct mismatched order_value.

    Returns:
        Dict[str, Any]: Preprocessed order summary or error report.
    
    Example:
    >>> sample_order = {
        "order_id": "ORD1234",
        "user_id": "U5678",
        "order_timestamp": "2024-12-13T10:00:00Z",
        "order_value": 91.99,
        "items": [
            {"product_id": "P001", "quantity": 2, "price_per_unit": 20.00},
            {"product_id": "P002", "quantity": 1, "price_per_unit": 59.99}
        ],
        "shipping_address": "123 Main St, Springfield",
        "payment_method": "CreditCard"
    }
    """

    # ---------- 1. Validate Mandatory Fields ----------
    required_fields = {"user_id": str, "order_id": str, "order_value": (int, float)}
    errors = []

    for field, expected_type in required_fields.items():
        if field not in order_data:
            errors.append(f"Missing required field: '{field}'")
        elif order_data[field] is None or (isinstance(order_data[field], str) and order_data[field].strip() == ""):
            errors.append(f"Empty required field: '{field}'")
        elif not isinstance(order_data[field], expected_type):
            errors.append(f"Invalid type for '{field}': expected {expected_type}, got {type(order_data[field])}")

    if errors:
        logger.error("Order validation failed: %s", errors)
        return {
            "status": "INVALID",
            "order_id": order_data.get("order_id", "Unknown"),
            "errors": errors
        }

    items = order_data.get("items", [])
    computed_total = sum(
        item.get("quantity", 0) * item.get("price_per_unit", 0.0)
        for item in items
    )

    reported_value = float(order_data["order_value"])

    if abs(computed_total - reported_value) > 1e-2: 
        msg = (f"Order value mismatch: reported ${reported_value:.2f}, "
               f"computed ${computed_total:.2f}")

        if correct_value:
            logger.warning("%s. Correcting value.", msg)
            reported_value = computed_total
        else:
            logger.error("%s. Marking as invalid.", msg)
            return {
                "status": "INVALID",
                "order_id": order_data["order_id"],
                "errors": [msg]
            }

    summary = {
        "status": "VALID",
        "order_id": order_data["order_id"],
        "user_id": order_data["user_id"],
        "order_value": reported_value,
        "order_timestamp": order_data.get("order_timestamp", None),
        "computed_total": computed_total
    }

    logger.info(
        "Processed order %s (User: %s) | Total: $%.2f | Timestamp: %s",
        summary["order_id"],
        summary["user_id"],
        summary["order_value"],
        summary["order_timestamp"]
    )

    return summary


if __name__ == "__main__":
    sample_order = {
        "order_id": "ORD1234",
        "user_id": "U5678",
        "order_timestamp": "2024-12-13T10:00:00Z",
        "order_value": 99.99,
        "items": [
            {"product_id": "P001", "quantity": 2, "price_per_unit": 20.00},
            {"product_id": "P002", "quantity": 1, "price_per_unit": 59.99}
        ],
        "shipping_address": "123 Main St, Springfield",
        "payment_method": "CreditCard"
    }

    processed = process_order(sample_order, correct_value=True)
    print(processed)

