import pandas as pd
import numpy as np
import random
import datetime as dt
from faker import Faker
import csv

# Initialize Faker for realistic data
fake = Faker()
Faker.seed(42)  # For reproducible results
random.seed(42)
np.random.seed(42)

def generate_large_dataset(num_records=100000, filename="large_demo_dataset.csv"):
    """
    Generate a large, realistic e-commerce dataset for demo purposes
    
    Args:
        num_records: Number of records to generate (default 100,000)
        filename: Output CSV filename
    """
    
    print(f"🚀 Generating {num_records:,} records...")
    print("📊 This may take 2-3 minutes for 100K records...")
    
    # Sample data categories - expanded for variety
    categories = [
        'Electronics', 'Clothing', 'Home & Garden', 'Sports & Outdoors', 
        'Books & Media', 'Automotive', 'Health & Beauty', 'Toys & Games',
        'Food & Beverages', 'Office Supplies', 'Jewelry', 'Pet Supplies'
    ]
    
    regions = [
        'Lagos', 'Abuja', 'Port Harcourt', 'Kano', 'Ibadan', 'Benin City',
        'Jos', 'Kaduna', 'Warri', 'Aba', 'Onitsha', 'Enugu'
    ]
    
    statuses = ['Completed', 'Pending', 'Cancelled', 'Returned', 'Processing', 'Shipped']
    
    payment_methods = ['Card', 'Bank Transfer', 'Mobile Money', 'Cash on Delivery']
    
    # Date range for the last 12 months
    start_date = dt.datetime.now() - dt.timedelta(days=365)
    
    # Product names for variety
    product_prefixes = ['Premium', 'Standard', 'Deluxe', 'Pro', 'Basic', 'Ultra', 'Smart', 'Classic']
    product_types = ['Phone', 'Laptop', 'Shirt', 'Shoes', 'Book', 'Watch', 'Bag', 'Headset']
    
    # Generate data in batches to manage memory
    batch_size = 10000
    total_batches = (num_records + batch_size - 1) // batch_size
    
    # Open CSV file for writing
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        # Define fieldnames
        fieldnames = [
            'order_id', 'customer_id', 'customer_name', 'customer_email', 
            'product_name', 'category', 'price', 'quantity', 'discount_percent',
            'total_amount', 'region', 'order_status', 'payment_method',
            'order_date', 'shipping_address', 'phone_number'
        ]
        
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        # Generate data in batches
        for batch_num in range(total_batches):
            print(f"⚡ Processing batch {batch_num + 1}/{total_batches}")
            
            batch_records = min(batch_size, num_records - (batch_num * batch_size))
            batch_data = []
            
            for i in range(batch_records):
                record_id = (batch_num * batch_size) + i
                
                # Generate realistic customer data
                customer_id = f'CUST-{random.randint(1000, 99999)}'
                customer_name = fake.name()
                
                # 5% invalid emails for demo purposes
                if random.random() < 0.05:
                    customer_email = f"invalid-email-{record_id}"
                else:
                    customer_email = fake.email()
                
                # Generate product information
                product_prefix = random.choice(product_prefixes)
                product_type = random.choice(product_types)
                product_name = f"{product_prefix} {product_type} {random.randint(100, 999)}"
                
                category = random.choice(categories)
                
                # Realistic pricing based on category
                price_ranges = {
                    'Electronics': (50, 1500),
                    'Clothing': (20, 300),
                    'Home & Garden': (30, 800),
                    'Sports & Outdoors': (25, 500),
                    'Books & Media': (5, 100),
                    'Automotive': (100, 2000),
                    'Health & Beauty': (15, 200),
                    'Toys & Games': (10, 150),
                    'Food & Beverages': (5, 50),
                    'Office Supplies': (10, 300),
                    'Jewelry': (50, 1000),
                    'Pet Supplies': (15, 200)
                }
                
                price_min, price_max = price_ranges.get(category, (10, 100))
                price = round(random.uniform(price_min, price_max), 2)
                
                # Some invalid prices for demo (2% of records)
                if random.random() < 0.02:
                    price = -abs(price)  # Negative price for validation demo
                
                quantity = random.randint(1, 8)
                
                # Discount logic - more discounts on higher quantities
                if quantity > 5:
                    discount_percent = round(random.uniform(10, 25), 1)
                elif quantity > 3:
                    discount_percent = round(random.uniform(5, 15), 1)
                else:
                    discount_percent = round(random.uniform(0, 10), 1) if random.random() < 0.3 else 0
                
                # Calculate total amount
                subtotal = abs(price) * quantity  # Use abs for calculation even with invalid prices
                discount_amount = subtotal * (discount_percent / 100)
                total_amount = round(subtotal - discount_amount, 2)
                
                # Random date within the last year
                random_days = random.randint(0, 365)
                order_date = start_date + dt.timedelta(days=random_days)
                
                # Other fields
                region = random.choice(regions)
                order_status = random.choice(statuses)
                payment_method = random.choice(payment_methods)
                shipping_address = fake.address()
                phone_number = fake.phone_number()
                
                record = {
                    'order_id': f'ORD-{10000 + record_id}',
                    'customer_id': customer_id,
                    'customer_name': customer_name,
                    'customer_email': customer_email,
                    'product_name': product_name,
                    'category': category,
                    'price': price,
                    'quantity': quantity,
                    'discount_percent': discount_percent,
                    'total_amount': total_amount,
                    'region': region,
                    'order_status': order_status,
                    'payment_method': payment_method,
                    'order_date': order_date.strftime('%Y-%m-%d'),
                    'shipping_address': shipping_address.replace('\n', ', '),
                    'phone_number': phone_number
                }
                
                batch_data.append(record)
            
            # Write batch to CSV
            writer.writerows(batch_data)
    
    print(f"✅ Successfully generated {num_records:,} records")
    print(f"📁 Saved to: {filename}")
    print(f"📊 File size: ~{num_records * 0.0003:.1f} MB")
    
    # Generate summary statistics
    df_sample = pd.DataFrame(batch_data[-1000:])  # Last 1000 records for quick stats
    print("\n📈 Sample Data Summary:")
    print(f"• Categories: {df_sample['category'].nunique()}")
    print(f"• Regions: {df_sample['region'].nunique()}")
    print(f"• Date range: {df_sample['order_date'].min()} to {df_sample['order_date'].max()}")
    print(f"• Price range: ₦{df_sample['price'].min():.2f} to ₦{df_sample['price'].max():.2f}")
    
    return filename

def generate_smaller_datasets():
    """Generate multiple dataset sizes for different demo scenarios"""
    
    datasets = [
        (1000, "demo_1k_records.csv"),
        (10000, "demo_10k_records.csv"),
        (50000, "demo_50k_records.csv"),
        (100000, "demo_100k_records.csv")
    ]
    
    for size, filename in datasets:
        print(f"\n{'='*50}")
        generate_large_dataset(size, filename)
        print(f"{'='*50}")

if __name__ == "__main__":
    # Install required package first
    try:
        from faker import Faker
    except ImportError:
        print("❌ Faker package required. Install with: pip install faker")
        exit(1)
    
    print("🚀 Large Dataset Generator for Data Engineering Demo")
    print("="*60)
    
    choice = input("""
Choose dataset size to generate:
1. 1K records (quick demo) - ~0.3MB
2. 10K records (impressive demo) - ~3MB  
3. 50K records (large demo) - ~15MB
4. 100K records (full scale) - ~30MB
5. All sizes (recommended)

Enter choice (1-5): """)
    
    if choice == '1':
        generate_large_dataset(1000, "demo_1k_records.csv")
    elif choice == '2':
        generate_large_dataset(10000, "demo_10k_records.csv")
    elif choice == '3':
        generate_large_dataset(50000, "demo_50k_records.csv")
    elif choice == '4':
        generate_large_dataset(100000, "demo_100k_records.csv")
    elif choice == '5':
        generate_smaller_datasets()
    else:
        print("Invalid choice. Generating 10K records as default...")
        generate_large_dataset(10000, "demo_10k_records.csv")
    
    print("\n🎯 Demo Tips:")
    print("• Use 1K-10K records for smooth presentation")
    print("• Mention scaling to millions in production")
    print("• Upload the CSV file in your Streamlit demo")
    print("• Highlight data quality issues automatically detected")
    print("\n✅ Ready for your presentation!")