#!/usr/bin/env python3
"""
User Activity Data Generator for E-commerce Data Pipeline

This script generates mock user activity data in JSON format to simulate
streaming data from an e-commerce platform.

The data includes:
- User ID
- Session ID
- Timestamp
- Event type (page_view, add_to_cart, remove_from_cart, checkout, purchase)
- Product ID (if applicable)
- Category (if applicable)
- Price (if applicable)
- Quantity (if applicable)
- Device type
- IP address
- User agent
"""

import json
import random
import uuid
import datetime
import ipaddress
import os
from faker import Faker

# Initialize Faker
fake = Faker()

# Constants
EVENT_TYPES = [
    "page_view", 
    "product_view", 
    "add_to_cart", 
    "remove_from_cart", 
    "checkout_start", 
    "checkout_complete", 
    "purchase"
]

EVENT_WEIGHTS = [70, 40, 20, 5, 3, 2, 1]  # Relative frequency of events

DEVICE_TYPES = ["desktop", "mobile", "tablet"]
DEVICE_WEIGHTS = [50, 40, 10]  # Relative frequency of devices

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

# User agents
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPad; CPU OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 11; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.120 Mobile Safari/537.36"
]

def generate_ip():
    """Generate a random IP address."""
    return str(ipaddress.IPv4Address(random.randint(0, 2**32 - 1)))

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

def generate_user_activity(num_users=1000, events_per_user_range=(1, 20), start_date=None, end_date=None):
    """
    Generate user activity data.
    
    Args:
        num_users: Number of unique users to simulate
        events_per_user_range: Range of events per user (min, max)
        start_date: Start date for events (datetime object)
        end_date: End date for events (datetime object)
    
    Returns:
        List of user activity events as dictionaries
    """
    if not start_date:
        start_date = datetime.datetime.now() - datetime.timedelta(days=30)
    if not end_date:
        end_date = datetime.datetime.now()
    
    user_activities = []
    
    # Generate user IDs
    user_ids = [str(uuid.uuid4()) for _ in range(num_users)]
    
    for user_id in user_ids:
        # Each user has a random number of sessions
        num_sessions = random.randint(1, 5)
        
        for _ in range(num_sessions):
            session_id = str(uuid.uuid4())
            device_type = random.choices(DEVICE_TYPES, weights=DEVICE_WEIGHTS, k=1)[0]
            ip_address = generate_ip()
            user_agent = random.choice(USER_AGENTS)
            
            # Random number of events in this session
            num_events = random.randint(*events_per_user_range)
            
            # Session start time
            session_start = start_date + datetime.timedelta(
                seconds=random.randint(0, int((end_date - start_date).total_seconds()))
            )
            
            # Keep track of cart items for this session
            cart_items = {}
            
            # Generate events for this session
            for event_idx in range(num_events):
                # Events happen in sequence with some time passing between them
                event_time = session_start + datetime.timedelta(minutes=random.randint(1, 10) * event_idx)
                
                if event_time > end_date:
                    break
                
                # Select event type with weighted probability
                event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]
                
                # Base event data
                event = {
                    "user_id": user_id,
                    "session_id": session_id,
                    "timestamp": event_time.isoformat(),
                    "event_type": event_type,
                    "device_type": device_type,
                    "ip_address": ip_address,
                    "user_agent": user_agent
                }
                
                # Add event-specific data
                if event_type == "page_view":
                    # Page views might be to category pages or other site pages
                    if random.random() < 0.3:
                        event["page"] = f"/category/{random.choice(CATEGORIES)}"
                    else:
                        event["page"] = random.choice(["/", "/about", "/contact", "/deals", "/account"])
                
                elif event_type == "product_view":
                    # Product views always have product data
                    product_data = generate_product_data()
                    event.update(product_data)
                    event["page"] = f"/product/{product_data['product_id']}"
                
                elif event_type == "add_to_cart":
                    # Add to cart events have product data and quantity
                    product_data = generate_product_data()
                    quantity = random.randint(1, 5)
                    
                    # Store in cart for potential removal later
                    cart_items[product_data["product_id"]] = {
                        "quantity": quantity,
                        "product_data": product_data
                    }
                    
                    event.update(product_data)
                    event["quantity"] = quantity
                
                elif event_type == "remove_from_cart":
                    # Can only remove items that are in the cart
                    if cart_items:
                        product_id = random.choice(list(cart_items.keys()))
                        item = cart_items[product_id]
                        
                        # Sometimes remove all, sometimes just reduce quantity
                        if random.random() < 0.7 or item["quantity"] == 1:
                            # Remove completely
                            event.update(item["product_data"])
                            event["quantity"] = item["quantity"]
                            del cart_items[product_id]
                        else:
                            # Reduce quantity
                            remove_qty = random.randint(1, item["quantity"] - 1)
                            event.update(item["product_data"])
                            event["quantity"] = remove_qty
                            cart_items[product_id]["quantity"] -= remove_qty
                    else:
                        # If cart is empty, change to a page view event
                        event["event_type"] = "page_view"
                        event["page"] = random.choice(["/", "/cart"])
                
                elif event_type == "checkout_start":
                    # Checkout events include cart total
                    if cart_items:
                        cart_total = sum(
                            item["product_data"]["price"] * item["quantity"] 
                            for item in cart_items.values()
                        )
                        event["cart_total"] = round(cart_total, 2)
                        event["items_count"] = sum(item["quantity"] for item in cart_items.values())
                    else:
                        # If cart is empty, change to a page view event
                        event["event_type"] = "page_view"
                        event["page"] = random.choice(["/", "/cart"])
                
                elif event_type == "checkout_complete" or event_type == "purchase":
                    # Purchase events include cart items and total
                    if cart_items:
                        cart_total = sum(
                            item["product_data"]["price"] * item["quantity"] 
                            for item in cart_items.values()
                        )
                        
                        event["cart_total"] = round(cart_total, 2)
                        event["items_count"] = sum(item["quantity"] for item in cart_items.values())
                        
                        # Add order ID for purchases
                        if event_type == "purchase":
                            event["order_id"] = str(uuid.uuid4())
                            
                            # Add purchased items detail
                            event["items"] = [
                                {
                                    "product_id": pid,
                                    "category": item["product_data"]["category"],
                                    "price": item["product_data"]["price"],
                                    "quantity": item["quantity"]
                                }
                                for pid, item in cart_items.items()
                            ]
                            
                            # Clear cart after purchase
                            cart_items = {}
                    else:
                        # If cart is empty, change to a page view event
                        event["event_type"] = "page_view"
                        event["page"] = random.choice(["/", "/cart"])
                
                user_activities.append(event)
    
    # Sort all events by timestamp
    user_activities.sort(key=lambda x: x["timestamp"])
    
    return user_activities

def save_to_json(data, filename):
    """Save data to a JSON file."""
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2)

def main():
    """Main function to generate and save user activity data."""
    # Create data directory if it doesn't exist
    os.makedirs("../data", exist_ok=True)
    
    # Generate user activity data
    print("Generating user activity data...")
    user_activities = generate_user_activity(
        num_users=1000,
        events_per_user_range=(5, 30),
        start_date=datetime.datetime.now() - datetime.timedelta(days=30),
        end_date=datetime.datetime.now()
    )
    
    # Save to JSON file
    output_file = "../data/user_activity.json"
    save_to_json(user_activities, output_file)
    print(f"Generated {len(user_activities)} user activity events")
    print(f"Data saved to {os.path.abspath(output_file)}")

if __name__ == "__main__":
    main()
