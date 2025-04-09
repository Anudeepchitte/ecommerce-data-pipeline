#!/usr/bin/env python3
"""
Inventory Data Generator for E-commerce Data Pipeline

This script generates mock inventory data in XLSX format to simulate
weekly batch data from an e-commerce platform.

The data includes:
- Product ID
- Product Name
- Category
- Brand
- Supplier
- Cost Price
- Retail Price
- Current Stock
- Reorder Level
- Reorder Quantity
- Warehouse Location
- Last Restock Date
- Is Active
"""

import random
import datetime
import os
import pandas as pd
from faker import Faker

# Initialize Faker
fake = Faker()

# Constants
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

# Brands for each category
BRANDS = {
    "electronics": ["Apple", "Samsung", "Sony", "LG", "Dell", "HP", "Lenovo", "Asus", "Acer", "Bose"],
    "clothing": ["Nike", "Adidas", "Zara", "H&M", "Levi's", "Gap", "Calvin Klein", "Ralph Lauren", "Gucci", "Prada"],
    "home_goods": ["IKEA", "Crate & Barrel", "Pottery Barn", "West Elm", "Williams-Sonoma", "Bed Bath & Beyond", "HomeGoods", "Wayfair", "Target", "Walmart"],
    "beauty": ["L'Oreal", "Maybelline", "MAC", "Estee Lauder", "Clinique", "Revlon", "CoverGirl", "Neutrogena", "Olay", "Dove"],
    "sports": ["Nike", "Adidas", "Under Armour", "Puma", "Reebok", "The North Face", "Columbia", "Patagonia", "Wilson", "Callaway"],
    "books": ["Penguin Random House", "HarperCollins", "Simon & Schuster", "Hachette", "Macmillan", "Scholastic", "Wiley", "Oxford", "Cambridge", "Dover"],
    "toys": ["Lego", "Hasbro", "Mattel", "Fisher-Price", "Playmobil", "Melissa & Doug", "Disney", "Nintendo", "Bandai", "Ravensburger"],
    "food": ["Kraft", "Nestle", "General Mills", "Kellogg's", "Unilever", "PepsiCo", "Coca-Cola", "Mars", "Mondelez", "Danone"],
    "jewelry": ["Tiffany & Co.", "Cartier", "Pandora", "Swarovski", "David Yurman", "Bulgari", "Harry Winston", "Chopard", "Van Cleef & Arpels", "Mikimoto"],
    "automotive": ["Bosch", "3M", "Michelin", "Goodyear", "Castrol", "Mobil", "Armor All", "STP", "Meguiar's", "WD-40"]
}

# Suppliers for each category
SUPPLIERS = {
    "electronics": ["Tech Distributors Inc.", "Global Electronics Supply", "Circuit City Wholesale", "Digital Parts Co.", "ElectroTech Solutions"],
    "clothing": ["Fashion Forward Supply", "Textile Traders Ltd.", "Apparel Distributors Inc.", "Fabric Wholesalers", "Garment Supply Co."],
    "home_goods": ["Home Essentials Supply", "Domestic Goods Inc.", "Interior Wholesale Ltd.", "Household Distributors", "Living Space Supply Co."],
    "beauty": ["Beauty Supply Network", "Cosmetic Distributors Inc.", "Glamour Wholesale", "Personal Care Products", "Beauty Essentials Co."],
    "sports": ["Athletic Supply Co.", "Sports Equipment Inc.", "Fitness Wholesale", "Active Life Distributors", "Game Day Supply"],
    "books": ["Literary Distributors Inc.", "Book Wholesalers Ltd.", "Page Turner Supply", "Publishing Partners", "Reader's Wholesale"],
    "toys": ["Toy Box Distributors", "Play Time Wholesale", "Kid's Corner Supply", "Fun Factory Inc.", "Toy Chest Suppliers"],
    "food": ["Food Service Supply", "Grocery Distributors Inc.", "Culinary Wholesale", "Pantry Suppliers Ltd.", "Edible Goods Co."],
    "jewelry": ["Gem Distributors Inc.", "Precious Metals Supply", "Jewelry Wholesale Ltd.", "Accessory Partners", "Luxury Items Co."],
    "automotive": ["Auto Parts Distributors", "Vehicle Supply Inc.", "Car Component Wholesale", "Automotive Essentials", "Motor Supply Co."]
}

# Warehouse locations
WAREHOUSES = ["A1", "A2", "A3", "B1", "B2", "B3", "C1", "C2", "C3", "D1", "D2", "D3"]

def generate_product_name(category, brand):
    """Generate a realistic product name based on category and brand."""
    if category == "electronics":
        product_type = random.choice(["Laptop", "Smartphone", "Tablet", "Headphones", "TV", "Camera", "Speaker", "Monitor", "Keyboard", "Mouse"])
        feature = random.choice(["Pro", "Ultra", "Max", "Elite", "Premium", "Wireless", "Bluetooth", "4K", "HD", "Smart"])
        model = random.choice(["X", "Y", "Z", "1", "2", "3", "5", "7", "9", "10"]) + str(random.randint(100, 999))
        return f"{brand} {product_type} {feature} {model}"
    
    elif category == "clothing":
        product_type = random.choice(["T-Shirt", "Jeans", "Dress", "Jacket", "Sweater", "Hoodie", "Shorts", "Skirt", "Pants", "Shirt"])
        style = random.choice(["Slim Fit", "Regular", "Relaxed", "Athletic", "Vintage", "Modern", "Classic", "Casual", "Formal", "Designer"])
        color = random.choice(["Black", "Blue", "Red", "White", "Gray", "Green", "Yellow", "Purple", "Brown", "Pink"])
        return f"{brand} {style} {color} {product_type}"
    
    elif category == "home_goods":
        product_type = random.choice(["Sofa", "Chair", "Table", "Bed", "Lamp", "Rug", "Curtains", "Pillow", "Blanket", "Vase"])
        style = random.choice(["Modern", "Traditional", "Contemporary", "Rustic", "Minimalist", "Vintage", "Industrial", "Bohemian", "Scandinavian", "Coastal"])
        material = random.choice(["Wood", "Metal", "Glass", "Fabric", "Leather", "Ceramic", "Plastic", "Marble", "Cotton", "Wool"])
        return f"{brand} {style} {material} {product_type}"
    
    elif category == "beauty":
        product_type = random.choice(["Foundation", "Lipstick", "Mascara", "Eyeshadow", "Blush", "Moisturizer", "Serum", "Cleanser", "Shampoo", "Conditioner"])
        feature = random.choice(["Hydrating", "Volumizing", "Long-lasting", "Matte", "Glossy", "Natural", "Organic", "Anti-aging", "Brightening", "Nourishing"])
        shade = random.choice(["Rose", "Nude", "Berry", "Coral", "Red", "Pink", "Beige", "Brown", "Peach", "Plum"])
        return f"{brand} {feature} {product_type} {shade}"
    
    elif category == "sports":
        product_type = random.choice(["Running Shoes", "Basketball", "Tennis Racket", "Golf Clubs", "Yoga Mat", "Weights", "Bicycle", "Helmet", "Gloves", "Jersey"])
        feature = random.choice(["Pro", "Elite", "Performance", "Training", "Competition", "Lightweight", "Durable", "Adjustable", "Ergonomic", "Breathable"])
        model = random.choice(["X", "Y", "Z", "1", "2", "3", "5", "7", "9", "10"]) + str(random.randint(100, 999))
        return f"{brand} {feature} {product_type} {model}"
    
    elif category == "books":
        genres = ["Fiction", "Mystery", "Romance", "Sci-Fi", "Biography", "History", "Self-Help", "Business", "Cooking", "Travel"]
        genre = random.choice(genres)
        title = fake.catch_phrase()
        return f"{title}: A {genre} Book by {brand}"
    
    elif category == "toys":
        product_type = random.choice(["Action Figure", "Doll", "Building Set", "Board Game", "Puzzle", "Plush Toy", "Remote Control Car", "Educational Toy", "Outdoor Toy", "Arts and Crafts"])
        age_group = random.choice(["Toddler", "Kids", "Teen", "Family", "Adult", "All Ages"])
        feature = random.choice(["Interactive", "Educational", "Creative", "Classic", "Electronic", "Collectible", "Limited Edition", "Deluxe", "Starter", "Advanced"])
        return f"{brand} {feature} {product_type} for {age_group}"
    
    elif category == "food":
        product_type = random.choice(["Cereal", "Snack", "Chocolate", "Coffee", "Tea", "Pasta", "Sauce", "Chips", "Cookies", "Candy"])
        flavor = random.choice(["Original", "Chocolate", "Vanilla", "Strawberry", "Mint", "Caramel", "Spicy", "Savory", "Sweet", "Sour"])
        feature = random.choice(["Organic", "Gluten-Free", "Low-Fat", "Sugar-Free", "All-Natural", "Vegan", "Premium", "Family Size", "Snack Size", "Limited Edition"])
        return f"{brand} {feature} {flavor} {product_type}"
    
    elif category == "jewelry":
        product_type = random.choice(["Necklace", "Bracelet", "Earrings", "Ring", "Watch", "Pendant", "Brooch", "Anklet", "Cufflinks", "Tiara"])
        material = random.choice(["Gold", "Silver", "Platinum", "Diamond", "Pearl", "Ruby", "Sapphire", "Emerald", "Crystal", "Gemstone"])
        style = random.choice(["Classic", "Modern", "Vintage", "Statement", "Minimalist", "Elegant", "Bohemian", "Art Deco", "Victorian", "Contemporary"])
        return f"{brand} {style} {material} {product_type}"
    
    elif category == "automotive":
        product_type = random.choice(["Oil Filter", "Air Filter", "Brake Pads", "Wiper Blades", "Car Battery", "Spark Plugs", "Tire", "Car Wax", "Car Cleaner", "Engine Oil"])
        vehicle_type = random.choice(["Car", "Truck", "SUV", "Motorcycle", "ATV", "RV", "Boat", "Scooter", "Van", "Commercial"])
        feature = random.choice(["Premium", "Heavy Duty", "Long-lasting", "Performance", "Economy", "Professional", "Advanced", "Synthetic", "Organic", "Eco-friendly"])
        return f"{brand} {feature} {product_type} for {vehicle_type}"
    
    # Default fallback
    return f"{brand} {category.title()} Product {random.randint(100, 999)}"

def generate_inventory_data(num_products=1000, as_of_date=None):
    """
    Generate inventory data.
    
    Args:
        num_products: Number of products to generate
        as_of_date: Date for inventory data (datetime object)
    
    Returns:
        List of inventory items as dictionaries
    """
    if not as_of_date:
        as_of_date = datetime.datetime.now()
    
    inventory_items = []
    
    # Distribute products across categories
    products_per_category = num_products // len(CATEGORIES)
    remaining_products = num_products % len(CATEGORIES)
    
    for category in CATEGORIES:
        # Calculate number of products for this category
        category_products = products_per_category
        if remaining_products > 0:
            category_products += 1
            remaining_products -= 1
        
        # Product ID range for this category
        id_range = PRODUCT_RANGES[category]
        price_range = PRICE_RANGES[category]
        
        # Get brands and suppliers for this category
        category_brands = BRANDS[category]
        category_suppliers = SUPPLIERS[category]
        
        # Generate products for this category
        for i in range(category_products):
            # Generate product ID within range
            product_id = id_range[0] + i
            if product_id > id_range[1]:
                # If we exceed the range, wrap around
                product_id = id_range[0] + (product_id - id_range[1] - 1)
            
            # Select brand and supplier
            brand = random.choice(category_brands)
            supplier = random.choice(category_suppliers)
            
            # Generate prices
            retail_price = round(random.uniform(*price_range), 2)
            # Cost price is typically 40-70% of retail
            cost_price = round(retail_price * random.uniform(0.4, 0.7), 2)
            
            # Generate stock levels
            current_stock = random.randint(0, 500)
            reorder_level = random.randint(10, 50)
            reorder_quantity = random.randint(50, 200)
            
            # Generate warehouse location
            warehouse_location = random.choice(WAREHOUSES)
            
            # Generate last restock date (between 1-60 days ago)
            days_ago = random.randint(1, 60)
            last_restock_date = as_of_date - datetime.timedelta(days=days_ago)
            
            # Is the product active? (90% chance)
            is_active = random.random() < 0.9
            
            # Generate product name
            product_name = generate_product_name(category, brand)
            
            # Create inventory item
            inventory_item = {
                "product_id": product_id,
                "product_name": product_name,
                "category": category,
                "brand": brand,
                "supplier": supplier,
                "cost_price": cost_price,
                "retail_price": retail_price,
                "current_stock": current_stock,
                "reorder_level": reorder_level,
                "reorder_quantity": reorder_quantity,
                "warehouse_location": warehouse_location,
                "last_restock_date": last_restock_date.strftime("%Y-%m-%d"),
                "is_active": is_active
            }
            
            inventory_items.append(inventory_item)
    
    return inventory_items

def save_to_excel(inventory_data, filename):
    """Save inventory data to an Excel file."""
    # Convert to DataFrame
    df = pd.DataFrame(inventory_data)
    
    # Create Excel writer
    writer = pd.ExcelWriter(filename, engine='openpyxl')
    
    # Write data to Excel
    df.to_excel(writer, sheet_name='Inventory', index=False)
    
    # Save the Excel file
    writer.close()

def main():
    """Main function to generate and save inventory data."""
    # Create data directory if it doesn't exist
    os.makedirs("../data", exist_ok=True)
    
    # Generate inventory data
    print("Generating inventory data...")
    inventory_data = generate_inventory_data(
        num_products=1000,
        as_of_date=datetime.datetime.now()
    )
    
    # Save to Excel file
    output_file = "../data/inventory.xlsx"
    save_to_excel(inventory_data, output_file)
    print(f"Generated {len(inventory_data)} inventory items")
    print(f"Data saved to {os.path.abspath(output_file)}")

if __name__ == "__main__":
    main()
