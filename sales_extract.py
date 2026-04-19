import os
import random
from datetime import datetime, timedelta, date

from dotenv import load_dotenv
from faker import Faker
import snowflake.connector

load_dotenv()

fake = Faker()
random.seed(42)
Faker.seed(42)

SNOWFLAKE_ACCOUNT = os.environ["SNOWFLAKE_ACCOUNT"]
SNOWFLAKE_USER = os.environ["SNOWFLAKE_USER"]
SNOWFLAKE_PASSWORD = os.environ["SNOWFLAKE_PASSWORD"]
SNOWFLAKE_DATABASE = os.environ.get("SNOWFLAKE_DATABASE", "ALL_DATA")
SNOWFLAKE_SCHEMA = os.environ.get("SNOWFLAKE_SCHEMA", "ECOMMERCE_RAW")
SNOWFLAKE_WAREHOUSE = os.environ["SNOWFLAKE_WAREHOUSE"]

CATEGORIES = {
    "Electronics": (50, 1500),
    "Clothing": (10, 200),
    "Home & Kitchen": (15, 500),
    "Sports & Outdoors": (20, 400),
    "Books": (5, 60),
    "Beauty": (8, 120),
    "Toys": (10, 150),
    "Automotive": (15, 600),
}

ORDER_STATUSES = ["completed", "completed", "completed", "shipped", "processing", "cancelled", "refunded"]

DDL_CUSTOMERS = """
CREATE TABLE IF NOT EXISTS customers (
    customer_id     INTEGER PRIMARY KEY,
    first_name      VARCHAR(100),
    last_name       VARCHAR(100),
    email           VARCHAR(255),
    city            VARCHAR(100),
    state           VARCHAR(100),
    country         VARCHAR(100),
    signup_date     DATE
)
"""

DDL_PRODUCTS = """
CREATE TABLE IF NOT EXISTS products (
    product_id      INTEGER PRIMARY KEY,
    product_name    VARCHAR(255),
    category        VARCHAR(100),
    price           NUMBER(10,2),
    cost            NUMBER(10,2)
)
"""

DDL_ORDERS = """
CREATE TABLE IF NOT EXISTS orders (
    order_id        INTEGER PRIMARY KEY,
    customer_id     INTEGER,
    order_date      DATE,
    status          VARCHAR(50),
    total_amount    NUMBER(12,2)
)
"""

DDL_ORDER_ITEMS = """
CREATE TABLE IF NOT EXISTS order_items (
    order_item_id   INTEGER PRIMARY KEY,
    order_id        INTEGER,
    product_id      INTEGER,
    quantity        INTEGER,
    unit_price      NUMBER(10,2),
    subtotal        NUMBER(12,2)
)
"""


def get_connection():
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        warehouse=SNOWFLAKE_WAREHOUSE,
    )


def tables_exist(cur):
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = 'CUSTOMERS'",
        (SNOWFLAKE_SCHEMA.upper(),),
    )
    return cur.fetchone()[0] > 0


def create_tables(cur):
    for ddl in [DDL_CUSTOMERS, DDL_PRODUCTS, DDL_ORDERS, DDL_ORDER_ITEMS]:
        cur.execute(ddl)
    print("Tables created.")


def generate_customers(n, start_id=1, date_range=None):
    rows = []
    if date_range is None:
        start = date.today() - timedelta(days=365 * 4)
        end = date.today()
    else:
        start, end = date_range

    for i in range(n):
        signup = fake.date_between(start_date=start, end_date=end)
        rows.append((
            start_id + i,
            fake.first_name(),
            fake.last_name(),
            fake.unique.email(),
            fake.city(),
            fake.state(),
            "United States",
            signup,
        ))
    return rows


PRODUCT_NAMES = {
    "Electronics": [
        "Apple iPhone 15 Pro", "Samsung Galaxy S24", "Sony WH-1000XM5 Headphones",
        "Apple MacBook Air M2", "Dell XPS 15 Laptop", "iPad Pro 12.9",
        "Samsung 65\" QLED TV", "Sony PlayStation 5", "Xbox Series X",
        "Apple Watch Series 9", "Bose QuietComfort 45", "GoPro Hero 12",
        "Canon EOS R50 Camera", "LG 27\" 4K Monitor", "Kindle Paperwhite",
    ],
    "Clothing": [
        "Nike Air Force 1 Sneakers", "Levi's 501 Original Jeans", "Adidas Hoodie",
        "The North Face Fleece Jacket", "Ralph Lauren Polo Shirt", "Calvin Klein T-Shirt",
        "Patagonia Down Jacket", "Under Armour Running Shorts", "Vans Old Skool Shoes",
        "Champion Sweatpants", "Columbia Rain Jacket", "New Balance 990 Sneakers",
        "Carhartt Work Pants", "Tommy Hilfiger Chino Pants", "Hanes Crew Socks 6-Pack",
    ],
    "Home & Kitchen": [
        "Instant Pot Duo 7-in-1", "Ninja Air Fryer XL", "KitchenAid Stand Mixer",
        "Dyson V15 Vacuum", "Nespresso Vertuo Coffee Machine", "Cuisinart 12-Piece Cookware Set",
        "Roomba i7 Robot Vacuum", "Vitamix 5200 Blender", "Weber Spirit II Gas Grill",
        "Philips Hue Smart Bulbs 4-Pack", "iRobot Braava Mop", "Calphalon Non-Stick Pan Set",
        "Keurig K-Elite Coffee Maker", "OXO Good Grips Knife Set", "Shark Navigator Vacuum",
    ],
    "Sports & Outdoors": [
        "Peloton Bike+", "Bowflex SelectTech Dumbbells", "Trek Marlin 5 Mountain Bike",
        "Coleman 6-Person Tent", "Yeti Tundra 45 Cooler", "Nike Metcon 9 Training Shoes",
        "TRX Suspension Trainer", "Garmin Forerunner 255 Watch", "Wilson Pro Staff Tennis Racket",
        "Callaway Strata Golf Set", "Hydro Flask 32oz Water Bottle", "Black Diamond Trekking Poles",
        "Fitbit Charge 6", "Schwinn IC4 Indoor Bike", "Osprey Atmos 65 Backpack",
    ],
    "Books": [
        "Atomic Habits by James Clear", "The Psychology of Money", "Sapiens by Yuval Noah Harari",
        "The Lean Startup by Eric Ries", "Thinking Fast and Slow", "The Alchemist by Paulo Coelho",
        "Deep Work by Cal Newport", "Zero to One by Peter Thiel", "The 4-Hour Work Week",
        "Rich Dad Poor Dad", "Dune by Frank Herbert", "1984 by George Orwell",
        "The Great Gatsby", "Harry Potter and the Sorcerer's Stone", "The Midnight Library",
    ],
    "Beauty": [
        "Dyson Airwrap Styler", "La Mer Moisturizing Cream", "Charlotte Tilbury Flawless Filter",
        "Tatcha The Water Cream", "Fenty Beauty Foundation", "Olaplex No.3 Hair Perfector",
        "Drunk Elephant C-Firma Serum", "Mario Badescu Facial Spray", "The Ordinary Niacinamide Serum",
        "Maybelline Sky High Mascara", "NYX Lip Liner Set", "Neutrogena Sunscreen SPF 50",
        "CeraVe Moisturizing Cream", "Bioderma Micellar Water", "Laneige Lip Sleeping Mask",
    ],
    "Toys": [
        "LEGO Technic Lamborghini", "Barbie Dreamhouse", "Hot Wheels Ultimate Garage",
        "Nintendo Switch Lite", "Nerf Elite 2.0 Blaster", "Melissa & Doug Wooden Blocks",
        "Monopoly Board Game", "Play-Doh Ultimate Color Collection", "Razor A Kick Scooter",
        "Fisher-Price Laugh & Learn", "Uno Card Game", "Jenga Giant Game",
        "Pokémon Trading Card Set", "Star Wars Millennium Falcon LEGO", "Crayola Ultimate Art Kit",
    ],
    "Automotive": [
        "Garmin DriveSmart 65 GPS", "Michelin CrossClimate2 Tire", "Thule Roof Rack System",
        "NOCO Boost Plus Jump Starter", "Chemical Guys Car Wash Kit", "WeatherTech Floor Mats",
        "Dashcam BlackVue DR900X", "Armor All Cleaning Kit", "OBD2 Bluetooth Diagnostic Scanner",
        "Covercraft Custom Car Cover", "K&N Air Filter", "Optima RedTop Battery",
        "Meguiar's Ultimate Compound", "Black Magic Tire Wet Spray", "Husky Liners Floor Liners",
    ],
}


def generate_products():
    rows = []
    product_id = 1
    for category, names in PRODUCT_NAMES.items():
        price_min, price_max = CATEGORIES[category]
        for name in names:
            price = round(random.uniform(price_min, price_max), 2)
            cost = round(price * random.uniform(0.3, 0.6), 2)
            rows.append((product_id, name, category, price, cost))
            product_id += 1
    return rows


def generate_orders_and_items(order_count, customer_ids, products, start_order_id, start_item_id, date_range):
    start_dt, end_dt = date_range
    orders = []
    items = []
    order_id = start_order_id
    item_id = start_item_id

    for _ in range(order_count):
        customer_id = random.choice(customer_ids)
        order_date = fake.date_between(start_date=start_dt, end_date=end_dt)
        status = random.choice(ORDER_STATUSES)

        num_items = random.randint(1, 5)
        order_products = random.sample(products, min(num_items, len(products)))
        total = 0.0
        order_item_rows = []

        for prod in order_products:
            product_id, _, _, price, _ = prod
            qty = random.randint(1, 4)
            unit_price = price
            subtotal = round(unit_price * qty, 2)
            total += subtotal
            order_item_rows.append((item_id, order_id, product_id, qty, unit_price, subtotal))
            item_id += 1

        total = round(total, 2)
        orders.append((order_id, customer_id, order_date, status, total))
        items.extend(order_item_rows)
        order_id += 1

    return orders, items, order_id, item_id


def bulk_insert(cur, table, columns, rows, batch_size=1000):
    if not rows:
        return
    placeholders = ", ".join(["%s"] * len(columns))
    col_str = ", ".join(columns)
    sql = f"INSERT INTO {table} ({col_str}) VALUES ({placeholders})"
    for i in range(0, len(rows), batch_size):
        cur.executemany(sql, rows[i : i + batch_size])
    print(f"  Inserted {len(rows)} rows into {table}.")


def get_max_ids(cur):
    cur.execute("SELECT COALESCE(MAX(customer_id), 0) FROM customers")
    max_customer = cur.fetchone()[0]
    cur.execute("SELECT COALESCE(MAX(order_id), 0) FROM orders")
    max_order = cur.fetchone()[0]
    cur.execute("SELECT COALESCE(MAX(order_item_id), 0) FROM order_items")
    max_item = cur.fetchone()[0]
    cur.execute("SELECT customer_id FROM customers")
    customer_ids = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT product_id, product_name, category, price, cost FROM products")
    products = cur.fetchall()
    return max_customer, max_order, max_item, customer_ids, products


def run_initial_load(cur):
    print("=== Initial load: generating 4 years of historical data ===")

    customers = generate_customers(1000)
    print(f"  Generated {len(customers)} customers.")

    products = generate_products()
    print(f"  Generated {len(products)} products.")

    bulk_insert(cur, "customers", ["customer_id","first_name","last_name","email","city","state","country","signup_date"], customers)
    bulk_insert(cur, "products", ["product_id","product_name","category","price","cost"], products)

    customer_ids = [r[0] for r in customers]
    end_date = date.today()
    start_date = end_date - timedelta(days=365 * 4)
    months = 48

    all_orders = []
    all_items = []
    order_id = 1
    item_id = 1

    for m in range(months):
        month_start = start_date + timedelta(days=30 * m)
        month_end = month_start + timedelta(days=29)
        if month_end > end_date:
            month_end = end_date

        count = random.randint(450, 550)
        orders, items, order_id, item_id = generate_orders_and_items(
            count, customer_ids, products, order_id, item_id, (month_start, month_end)
        )
        all_orders.extend(orders)
        all_items.extend(items)

        if (m + 1) % 12 == 0:
            print(f"  Generated data through month {m+1}...")

    bulk_insert(cur, "orders", ["order_id","customer_id","order_date","status","total_amount"], all_orders)
    bulk_insert(cur, "order_items", ["order_item_id","order_id","product_id","quantity","unit_price","subtotal"], all_items)
    print(f"Initial load complete: {len(all_orders)} orders, {len(all_items)} order items.")


def run_incremental_load(cur):
    print("=== Incremental load: appending last week of data ===")

    max_customer, max_order, max_item, customer_ids, products = get_max_ids(cur)

    new_customer_count = random.randint(0, 5)
    new_customers = []
    if new_customer_count > 0:
        new_customers = generate_customers(
            new_customer_count,
            start_id=max_customer + 1,
            date_range=(date.today() - timedelta(days=7), date.today()),
        )
        bulk_insert(cur, "customers", ["customer_id","first_name","last_name","email","city","state","country","signup_date"], new_customers)
        customer_ids.extend([r[0] for r in new_customers])

    end_date = date.today()
    start_date = end_date - timedelta(days=7)
    order_count = random.randint(50, 100)

    orders, items, _, _ = generate_orders_and_items(
        order_count, customer_ids, products, max_order + 1, max_item + 1, (start_date, end_date)
    )

    bulk_insert(cur, "orders", ["order_id","customer_id","order_date","status","total_amount"], orders)
    bulk_insert(cur, "order_items", ["order_item_id","order_id","product_id","quantity","unit_price","subtotal"], items)
    print(f"Incremental load complete: {len(new_customers)} new customers, {len(orders)} orders, {len(items)} order items.")


def main():
    print(f"Connecting to Snowflake ({SNOWFLAKE_ACCOUNT} / {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA})...")
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
        cur.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")

        existing = tables_exist(cur)

        if not existing:
            create_tables(cur)
            run_initial_load(cur)
        else:
            run_incremental_load(cur)

        conn.commit()
        print("Done.")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
