#!/usr/bin/env python3
"""
Orders Data Generator for E-commerce Data Pipeline

This script generates mock orders data in CSV format to simulate
batch data from an e-commerce platform.

The data includes:
- Order ID
- Customer ID
- Order Date
- Order Status
- Payment Method
- Shipping Method
- Subtotal
- Tax
- Shipping Cost
- Total
- Discount
- Coupon Code
- Items (product_id, quantity, price)
- Billing Address
- Shipping Address
"""

import csv
import random
import uuid
import datetime
import os
from faker import Faker

# Initialize Faker
fake = Faker()

# Constants
ORDER_STATUSES = [
    "completed", 
    "processing", 
    "shipped", 
    "cancelled", 
    "refunded", 
    "on_hold"
]

ORDER_STATUS_WEIGHTS = [70, 15, 10, 3, 1, 1]  # Relative frequency of statuses

PAYMENT_METHODS = [
    "credit_card", 
    "paypal", 
    "apple_pay", 
    "google_pay", 
    "bank_transfer", 
    "cash_on_delivery"
]

PAYMENT_METHOD_WEIGHTS = [60, 20, 10, 5, 3, 2]  # Relative frequency of payment methods

SHIPPING_METHODS = [
    "standard", 
    "express", 
    "next_day", 
    "pickup"
]

SHIPPING_METHOD_WEIGHTS = [70, 20, 5, 5]  # Relative frequency of shipping methods

COUPON_CODES = [
    "WELCOME10", 
    "SUMMER20", 
    "FLASH30", 
    "FREESHIP", 
    "LOYALTY15"
]

# Categories and product ranges from user_activity_generator
CATEGORIES = [
    "electronics", 
    "clothing", 
    "home_goods", 
    "beauty", 
    "sports", 
    "books", 
    "toys", 
    "food", 
    "jewelry", 
    "automotive"
]

# Product ID ranges for each category
PRODUCT_RANGES = {
    "electronics": (1000, 1999),
    "clothing": (2000, 2999),
    "home_goods": (3000, 3999),
    "beauty": (4000, 4999),
    "sports": (5000, 5999),
    "books": (6000, 6999),
    "toys": (7000, 7999),
    "food": (8000, 8999),
    "jewelry": (9000, 9999),
    "automotive": (10000, 10999)
}

# Price ranges for each category
PRICE_RANGES = {
    "electronics": (50, 2000),
    "clothing": (10, 200),
    "home_goods": (20, 500),
    "beauty": (5, 100),
    "sports": (15, 300),
    "books": (5, 50),
    "toys": (10, 100),
    "food": (5, 50),
    "jewelry": (50, 5000),
    "automotive": (20, 500)
}

def generate_product_data(category=None):
    """Generate random product data."""
    if not category:
        category = random.choice(CATEGORIES)
    
    product_range = PRODUCT_RANGES[category]
    price_range = PRICE_RANGES[category]
    
    product_id = random.randint(*product_range)
    price = round(random.uniform(*price_range), 2)
    
    return {
        "product_id": product_id,
        "category": category,
        "price": price
    }

def generate_order_items(min_items=1, max_items=10):
    """Generate a list of order items."""
    num_items = random.randint(min_items, max_items)
    items = []
    
    for _ in range(num_items):
        product_data = generate_product_data()
        quantity = random.randint(1, 5)
        
        items.append({
            "product_id": product_data["product_id"],
            "category": product_data["category"],
            "price": product_data["price"],
            "quantity": quantity
        })
    
    return items

def generate_address():
    """Generate a random address."""
    return {
        "street": fake.street_address(),
        "city": fake.city(),
        "state": fake.state_abbr(),
        "zip": fake.zipcode(),
        "country": "US"
    }

def generate_orders(num_orders=1000, start_date=None, end_date=None):
    """
    Generate order data.
    
    Args:
        num_orders: Number of orders to generate
        start_date: Start date for orders (datetime object)
        end_date: End date for orders (datetime object)
    
    Returns:
        List of orders as dictionaries
    """
    if not start_date:
        start_date = datetime.datetime.now() - datetime.timedelta(days=30)
    if not end_date:
        end_date = datetime.datetime.now()
    
    orders = []
    
    # Generate customer IDs (fewer than orders as customers can have multiple orders)
    num_customers = int(num_orders * 0.7)  # Assume 70% unique customers
    customer_ids = [str(uuid.uuid4()) for _ in range(num_customers)]
    
    for _ in range(num_orders):
        # Select a random customer
        customer_id = random.choice(customer_ids)
        
        # Generate order date
        order_date = start_date + datetime.timedelta(
            seconds=random.randint(0, int((end_date - start_date).total_seconds()))
        )
        
        # Generate order items
        items = generate_order_items(min_items=1, max_items=5)
        
        # Calculate order totals
        subtotal = sum(item["price"] * item["quantity"] for item in items)
        
        # Apply discount?
        has_discount = random.random() < 0.3  # 30% chance of discount
        discount = 0
        coupon_code = ""
        
        if has_discount:
            discount_percent = random.choice([10, 15, 20, 25, 30])
            discount = round(subtotal * (discount_percent / 100), 2)
            coupon_code = random.choice(COUPON_CODES)
        
        # Calculate tax and shipping
        tax_rate = random.uniform(0.05, 0.1)  # 5-10% tax rate
        tax = round((subtotal - discount) * tax_rate, 2)
        
        shipping_method = random.choices(
            SHIPPING_METHODS, 
            weights=SHIPPING_METHOD_WEIGHTS, 
            k=1
        )[0]
        
        # Shipping cost based on method
        if shipping_method == "standard":
            shipping_cost = round(random.uniform(5, 10), 2)
        elif shipping_method == "express":
            shipping_cost = round(random.uniform(10, 20), 2)
        elif shipping_method == "next_day":
            shipping_cost = round(random.uniform(20, 30), 2)
        else:  # pickup
            shipping_cost = 0
        
        # Calculate total
        total = subtotal - discount + tax + shipping_cost
        
        # Generate addresses
        billing_address = generate_address()
        
        # Same shipping address as billing 80% of the time
        if random.random() < 0.8:
            shipping_address = billing_address
        else:
            shipping_address = generate_address()
        
        # Create order
        order = {
            "order_id": str(uuid.uuid4()),
            "customer_id": customer_id,
            "order_date": order_date.isoformat(),
            "order_status": random.choices(
                ORDER_STATUSES, 
                weights=ORDER_STATUS_WEIGHTS, 
                k=1
            )[0],
            "payment_method": random.choices(
                PAYMENT_METHODS, 
                weights=PAYMENT_METHOD_WEIGHTS, 
                k=1
            )[0],
            "shipping_method": shipping_method,
            "subtotal": subtotal,
            "tax": tax,
            "shipping_cost": shipping_cost,
            "discount": discount,
            "coupon_code": coupon_code,
            "total": total,
            "items": items,
            "billing_address": billing_address,
            "shipping_address": shipping_address
        }
        
        orders.append(order)
    
    # Sort orders by date
    orders.sort(key=lambda x: x["order_date"])
    
    return orders

def flatten_order_for_csv(order):
    """
    Flatten an order object for CSV output.
    Converts nested structures to strings.
    """
    flat_order = order.copy()
    
    # Convert items to string
    flat_order["items"] = str(order["items"])
    
    # Convert addresses to strings
    flat_order["billing_address"] = str(order["billing_address"])
    flat_order["shipping_address"] = str(order["shipping_address"])
    
    return flat_order

def save_to_csv(orders, filename):
    """Save orders to a CSV file."""
    if not orders:
        return
    
    # Flatten orders for CSV
    flat_orders = [flatten_order_for_csv(order) for order in orders]
    
    # Get fieldnames from the first order
    fieldnames = flat_orders[0].keys()
    
    with open(filename, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(flat_orders)

def main():
    """Main function to generate and save orders data."""
    # Create data directory if it doesn't exist
    os.makedirs("../data", exist_ok=True)
    
    # Generate orders data
    print("Generating orders data...")
    orders = generate_orders(
        num_orders=1000,
        start_date=datetime.datetime.now() - datetime.timedelta(days=30),
        end_date=datetime.datetime.now()
    )
    
    # Save to CSV file
    output_file = "../data/orders.csv"
    save_to_csv(orders, output_file)
    print(f"Generated {len(orders)} orders")
    print(f"Data saved to {os.path.abspath(output_file)}")

if __name__ == "__main__":
    main()
