"""
Creates a demo SQLite database with realistic sales data for testing
"""

import sqlite3
import random
from datetime import datetime, timedelta
from faker import Faker

# Initialize Faker for generating realistic data
fake = Faker()

def create_demo_database(db_name='demo_sales.db'):
    """Create a comprehensive demo database with 50+ records in each table"""
    
    print("="*70)
    print("CREATING DEMO DATABASE")
    print("="*70)
    
    # Connect to database
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    # Drop existing tables if they exist
    print("\n[1/5] Cleaning existing tables...")
    cursor.execute('DROP TABLE IF EXISTS sales')
    cursor.execute('DROP TABLE IF EXISTS products')
    cursor.execute('DROP TABLE IF EXISTS users')
    cursor.execute('DROP TABLE IF EXISTS categories')
    cursor.execute('DROP TABLE IF EXISTS inventory_log')
    print("  ✓ Cleaned existing tables")
    
    # ========================================================================
    # CREATE TABLES
    # ========================================================================
    
    print("\n[2/5] Creating table structures...")
    
    # Categories table
    cursor.execute('''
        CREATE TABLE categories (
            category_id INTEGER PRIMARY KEY AUTOINCREMENT,
            category_name TEXT NOT NULL UNIQUE,
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Products table
    cursor.execute('''
        CREATE TABLE products (
            product_id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_name TEXT NOT NULL,
            category_id INTEGER NOT NULL,
            price REAL NOT NULL,
            cost_price REAL NOT NULL,
            stock_quantity INTEGER DEFAULT 0,
            reorder_level INTEGER DEFAULT 10,
            supplier TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (category_id) REFERENCES categories(category_id)
        )
    ''')
    
    # Users table
    cursor.execute('''
        CREATE TABLE users (
            user_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            phone TEXT,
            address TEXT,
            city TEXT,
            country TEXT,
            registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_active INTEGER DEFAULT 1
        )
    ''')
    
    # Sales table
    cursor.execute('''
        CREATE TABLE sales (
            sale_id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            sale_date DATE NOT NULL,
            sale_time TIME,
            unit_price REAL NOT NULL,
            discount_percent REAL DEFAULT 0,
            total_amount REAL NOT NULL,
            payment_method TEXT,
            status TEXT DEFAULT 'completed',
            FOREIGN KEY (product_id) REFERENCES products(product_id),
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        )
    ''')
    
    # Inventory log table (optional - for tracking stock changes)
    cursor.execute('''
        CREATE TABLE inventory_log (
            log_id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL,
            change_quantity INTEGER NOT NULL,
            change_type TEXT NOT NULL,
            change_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            notes TEXT,
            FOREIGN KEY (product_id) REFERENCES products(product_id)
        )
    ''')
    
    print("  ✓ Created 5 tables")
    
    # ========================================================================
    # INSERT CATEGORIES (10 categories)
    # ========================================================================
    
    print("\n[3/5] Inserting categories...")
    
    categories_data = [
        ('Clothing', 'Apparel, fashion items, and accessories'),
        ('Electronics', 'Electronic devices, gadgets, and accessories'),
        ('Books', 'Physical and digital books, magazines'),
        ('Sports', 'Sports equipment, fitness gear, and accessories'),
        ('Home & Garden', 'Home decor, furniture, and garden supplies'),
        ('Beauty', 'Cosmetics, skincare, and personal care products'),
        ('Toys', 'Toys, games, and children activities'),
        ('Food & Beverages', 'Packaged food, snacks, and drinks'),
        ('Automotive', 'Car accessories and maintenance products'),
        ('Office Supplies', 'Stationery, office equipment, and supplies')
    ]
    
    cursor.executemany(
        'INSERT INTO categories (category_name, description) VALUES (?, ?)',
        categories_data
    )
    
    print(f"  ✓ Inserted {len(categories_data)} categories")
    
    # ========================================================================
    # INSERT PRODUCTS (60 products - 6 per category)
    # ========================================================================
    
    print("\n[4/5] Inserting products...")
    
    products_data = [
        # Clothing (category_id: 1)
        ('T-Shirt Blue Cotton', 1, 19.99, 8.00, 150, 20, 'Fashion Corp'),
        ('T-Shirt Red V-Neck', 1, 19.99, 8.00, 180, 20, 'Fashion Corp'),
        ('T-Shirt Green Polo', 1, 24.99, 10.00, 120, 15, 'Fashion Corp'),
        ('Jeans Classic Blue', 1, 49.99, 22.00, 100, 15, 'Denim Inc'),
        ('Hoodie Black Zipper', 1, 39.99, 18.00, 80, 10, 'Fashion Corp'),
        ('Dress Summer Floral', 1, 59.99, 25.00, 60, 10, 'Style Co'),
        
        # Electronics (category_id: 2)
        ('Laptop Pro 15-inch', 2, 1299.99, 950.00, 25, 5, 'Tech Solutions'),
        ('Laptop Air 13-inch', 2, 999.99, 750.00, 30, 5, 'Tech Solutions'),
        ('Smartphone X Pro', 2, 899.99, 650.00, 50, 10, 'Mobile World'),
        ('Tablet 10-inch HD', 2, 399.99, 280.00, 40, 8, 'Mobile World'),
        ('Headphones Wireless', 2, 149.99, 75.00, 120, 20, 'Audio Tech'),
        ('Smartwatch Fitness', 2, 249.99, 150.00, 70, 15, 'Wearable Co'),
        
        # Books (category_id: 3)
        ('Python Programming Guide', 3, 45.00, 20.00, 80, 10, 'Book Distributors'),
        ('Data Science Handbook', 3, 55.00, 25.00, 60, 10, 'Book Distributors'),
        ('Machine Learning Basics', 3, 50.00, 22.00, 70, 10, 'Book Distributors'),
        ('Web Development Complete', 3, 42.00, 19.00, 65, 10, 'Book Distributors'),
        ('Business Strategy 2024', 3, 38.00, 17.00, 55, 8, 'Book Distributors'),
        ('Cooking Masterclass', 3, 35.00, 16.00, 75, 12, 'Book Distributors'),
        
        # Sports (category_id: 4)
        ('Running Shoes Pro', 4, 89.99, 45.00, 90, 15, 'Athletic Gear'),
        ('Yoga Mat Premium', 4, 29.99, 12.00, 110, 20, 'Fitness Plus'),
        ('Dumbbells Set 20kg', 4, 79.99, 40.00, 50, 10, 'Fitness Plus'),
        ('Tennis Racket Carbon', 4, 120.00, 65.00, 35, 8, 'Sports Direct'),
        ('Basketball Official', 4, 34.99, 18.00, 85, 15, 'Sports Direct'),
        ('Bicycle Mountain 26inch', 4, 450.00, 280.00, 20, 5, 'Cycle World'),
        
        # Home & Garden (category_id: 5)
        ('Sofa 3-Seater Grey', 5, 599.99, 350.00, 15, 3, 'Home Furnish'),
        ('Coffee Table Oak', 5, 199.99, 100.00, 25, 5, 'Home Furnish'),
        ('Table Lamp Modern', 5, 45.00, 20.00, 70, 12, 'Lighting Co'),
        ('Garden Tools Set', 5, 89.99, 45.00, 40, 8, 'Garden Supply'),
        ('Plant Pot Ceramic Large', 5, 29.99, 12.00, 95, 15, 'Garden Supply'),
        ('Curtains Blackout Pair', 5, 55.00, 25.00, 60, 10, 'Home Textiles'),
        
        # Beauty (category_id: 6)
        ('Face Cream Anti-Aging', 6, 35.00, 15.00, 120, 20, 'Beauty Brands'),
        ('Shampoo Organic 500ml', 6, 18.99, 8.00, 150, 25, 'Beauty Brands'),
        ('Lipstick Matte Red', 6, 22.00, 10.00, 140, 20, 'Cosmetics Inc'),
        ('Perfume Floral 100ml', 6, 75.00, 35.00, 60, 10, 'Fragrance Co'),
        ('Nail Polish Set 12pcs', 6, 28.00, 12.00, 95, 15, 'Cosmetics Inc'),
        ('Hair Dryer Professional', 6, 89.99, 45.00, 45, 8, 'Beauty Tech'),
        
        # Toys (category_id: 7)
        ('LEGO City Set Large', 7, 99.99, 55.00, 70, 12, 'Toy Kingdom'),
        ('Board Game Family', 7, 35.00, 18.00, 85, 15, 'Game World'),
        ('Action Figure Superhero', 7, 24.99, 12.00, 110, 20, 'Toy Kingdom'),
        ('Puzzle 1000 Pieces', 7, 19.99, 9.00, 95, 15, 'Game World'),
        ('Remote Control Car', 7, 65.00, 32.00, 50, 10, 'RC Toys'),
        ('Stuffed Bear Large', 7, 29.99, 14.00, 80, 12, 'Toy Kingdom'),
        
        # Food & Beverages (category_id: 8)
        ('Organic Coffee Beans 1kg', 8, 25.00, 12.00, 200, 30, 'Food Suppliers'),
        ('Green Tea Premium 100bags', 8, 15.00, 7.00, 180, 25, 'Food Suppliers'),
        ('Protein Bar Box 12pcs', 8, 22.00, 10.00, 150, 20, 'Health Foods'),
        ('Dark Chocolate 85% 200g', 8, 8.99, 4.00, 250, 35, 'Sweet Treats'),
        ('Olive Oil Extra Virgin 1L', 8, 18.00, 9.00, 120, 20, 'Food Suppliers'),
        ('Almond Nuts Raw 500g', 8, 12.00, 6.00, 160, 25, 'Health Foods'),
        
        # Automotive (category_id: 9)
        ('Car Phone Holder', 9, 15.99, 7.00, 140, 25, 'Auto Parts'),
        ('Dashboard Camera HD', 9, 89.99, 45.00, 55, 10, 'Auto Electronics'),
        ('Car Vacuum Cleaner', 9, 45.00, 22.00, 70, 12, 'Auto Accessories'),
        ('Tire Pressure Gauge', 9, 12.99, 6.00, 100, 18, 'Auto Parts'),
        ('Car Air Freshener 3pack', 9, 8.99, 4.00, 180, 30, 'Auto Accessories'),
        ('Jump Starter Portable', 9, 79.99, 40.00, 40, 8, 'Auto Electronics'),
        
        # Office Supplies (category_id: 10)
        ('Notebook A4 Pack 5', 10, 12.99, 6.00, 200, 30, 'Office Depot'),
        ('Pen Set Blue 10pcs', 10, 6.99, 3.00, 250, 40, 'Office Depot'),
        ('Stapler Heavy Duty', 10, 18.99, 9.00, 90, 15, 'Office Equipment'),
        ('File Organizer Desktop', 10, 24.99, 12.00, 75, 12, 'Office Equipment'),
        ('Whiteboard Magnetic 90x60', 10, 55.00, 28.00, 45, 8, 'Office Equipment'),
        ('Calculator Scientific', 10, 19.99, 10.00, 85, 15, 'Office Electronics')
    ]
    
    cursor.executemany(
        'INSERT INTO products (product_name, category_id, price, cost_price, stock_quantity, reorder_level, supplier) VALUES (?, ?, ?, ?, ?, ?, ?)',
        products_data
    )
    
    print(f"  ✓ Inserted {len(products_data)} products")
    
    # ========================================================================
    # INSERT USERS (50 users)
    # ========================================================================
    
    print("\n[5/5] Inserting users and sales...")
    
    users_data = []
    for i in range(50):
        name = fake.name()
        email = fake.email()
        phone = fake.phone_number()
        address = fake.street_address()
        city = fake.city()
        country = fake.country()
        reg_date = fake.date_time_between(start_date='-2y', end_date='now')
        is_active = random.choice([1, 1, 1, 0])  # 75% active
        
        users_data.append((name, email, phone, address, city, country, reg_date, is_active))
    
    cursor.executemany(
        'INSERT INTO users (name, email, phone, address, city, country, registration_date, is_active) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
        users_data
    )
    
    print(f"  ✓ Inserted {len(users_data)} users")
    
    # ========================================================================
    # INSERT SALES (300 sales transactions over last 3 months)
    # ========================================================================
    
    print("  → Generating sales transactions...")
    
    sales_data = []
    base_date = datetime.now() - timedelta(days=90)
    
    payment_methods = ['Credit Card', 'Debit Card', 'PayPal', 'Cash', 'Bank Transfer']
    statuses = ['completed', 'completed', 'completed', 'completed', 'pending', 'cancelled']
    
    for i in range(300):
        product_id = random.randint(1, 60)
        user_id = random.randint(1, 50)
        quantity = random.randint(1, 5)
        days_ago = random.randint(0, 90)
        sale_date = (base_date + timedelta(days=days_ago)).date()
        sale_time = fake.time()
        
        # Get product price
        cursor.execute('SELECT price FROM products WHERE product_id = ?', (product_id,))
        unit_price = cursor.fetchone()[0]
        
        # Random discount (20% chance of discount)
        discount = random.choice([0, 0, 0, 0, 5, 10, 15, 20])
        
        # Calculate total
        subtotal = unit_price * quantity
        discount_amount = subtotal * (discount / 100)
        total = subtotal - discount_amount
        
        payment_method = random.choice(payment_methods)
        status = random.choice(statuses)
        
        sales_data.append((
            product_id, user_id, quantity, sale_date, sale_time,
            unit_price, discount, total, payment_method, status
        ))
    
    cursor.executemany(
        'INSERT INTO sales (product_id, user_id, quantity, sale_date, sale_time, unit_price, discount_percent, total_amount, payment_method, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
        sales_data
    )
    
    print(f"  ✓ Inserted {len(sales_data)} sales transactions")
    
    # ========================================================================
    # INSERT INVENTORY LOGS (100 entries)
    # ========================================================================
    
    print("  → Generating inventory logs...")
    
    inventory_data = []
    change_types = ['restock', 'adjustment', 'return', 'damage']
    
    for i in range(100):
        product_id = random.randint(1, 60)
        change_type = random.choice(change_types)
        
        if change_type == 'restock':
            change_qty = random.randint(20, 100)
            notes = f"Restocked from supplier"
        elif change_type == 'return':
            change_qty = random.randint(1, 5)
            notes = f"Customer return"
        elif change_type == 'damage':
            change_qty = -random.randint(1, 10)
            notes = f"Damaged goods removed"
        else:  # adjustment
            change_qty = random.randint(-10, 10)
            notes = f"Inventory adjustment"
        
        change_date = fake.date_time_between(start_date='-90d', end_date='now')
        
        inventory_data.append((product_id, change_qty, change_type, change_date, notes))
    
    cursor.executemany(
        'INSERT INTO inventory_log (product_id, change_quantity, change_type, change_date, notes) VALUES (?, ?, ?, ?, ?)',
        inventory_data
    )
    
    print(f"  ✓ Inserted {len(inventory_data)} inventory logs")
    
    # Commit all changes
    conn.commit()
    
    # ========================================================================
    # DISPLAY STATISTICS
    # ========================================================================
    
    print("\n" + "="*70)
    print("DATABASE CREATION COMPLETED")
    print("="*70)
    print(f"\n📁 Database: {db_name}")
    print(f"📊 Statistics:")
    
    cursor.execute('SELECT COUNT(*) FROM categories')
    print(f"   • Categories: {cursor.fetchone()[0]}")
    
    cursor.execute('SELECT COUNT(*) FROM products')
    print(f"   • Products: {cursor.fetchone()[0]}")
    
    cursor.execute('SELECT COUNT(*) FROM users')
    print(f"   • Users: {cursor.fetchone()[0]}")
    
    cursor.execute('SELECT COUNT(*) FROM sales')
    print(f"   • Sales: {cursor.fetchone()[0]}")
    
    cursor.execute('SELECT COUNT(*) FROM inventory_log')
    print(f"   • Inventory Logs: {cursor.fetchone()[0]}")
    
    cursor.execute('SELECT SUM(total_amount) FROM sales WHERE status = "completed"')
    total_revenue = cursor.fetchone()[0]
    print(f"   • Total Revenue: ${total_revenue:,.2f}")
    
    cursor.execute('SELECT COUNT(DISTINCT user_id) FROM sales')
    print(f"   • Active Customers: {cursor.fetchone()[0]}")
    
    print("\n✅ Database ready for NL-to-SQL system testing!")
    print("="*70 + "\n")
    
    # Close connection
    conn.close()
    
    return db_name


def display_sample_data(db_name='demo_sales.db'):
    """Display sample data from each table"""
    
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    print("\n" + "="*70)
    print("SAMPLE DATA")
    print("="*70)
    
    # Categories
    print("\n📁 CATEGORIES (First 5):")
    cursor.execute('SELECT * FROM categories LIMIT 5')
    for row in cursor.fetchall():
        print(f"   {row[0]}. {row[1]} - {row[2][:50]}...")
    
    # Products
    print("\n📦 PRODUCTS (First 5):")
    cursor.execute('SELECT product_id, product_name, price, stock_quantity FROM products LIMIT 5')
    for row in cursor.fetchall():
        print(f"   [{row[0]}] {row[1]} - ${row[2]:.2f} (Stock: {row[3]})")
    
    # Users
    print("\n👥 USERS (First 5):")
    cursor.execute('SELECT user_id, name, email, city FROM users LIMIT 5')
    for row in cursor.fetchall():
        print(f"   [{row[0]}] {row[1]} - {row[2]} ({row[3]})")
    
    # Sales
    print("\n💰 SALES (First 5):")
    cursor.execute('''
        SELECT s.sale_id, p.product_name, u.name, s.quantity, s.total_amount, s.sale_date 
        FROM sales s
        JOIN products p ON s.product_id = p.product_id
        JOIN users u ON s.user_id = u.user_id
        LIMIT 5
    ''')
    for row in cursor.fetchall():
        print(f"   [{row[0]}] {row[1]} → {row[2]} | Qty: {row[3]} | ${row[4]:.2f} | {row[5]}")
    
    print("\n" + "="*70 + "\n")
    
    conn.close()


if __name__ == "__main__":
    # Create the database
    db_name = create_demo_database('demo_sales.db')
    
    # Display sample data
    display_sample_data(db_name)
    
    print("🎉 You can now run: python nl_to_sql_langgraph.py")