import snowflake.connector
import os
from dotenv import load_dotenv

# Load secrets
load_dotenv()

def get_snowflake_conn():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )


products_to_track = [
    (
        "B09XS7JWHH", 
        "Sony WH-1000XM5 Wireless Headphones", 
        "Electronics", 
        "https://www.amazon.in/Sony-WH-1000XM5-Wireless-Cancelling-Headphones/dp/B09XS7JWHH"
    ),
    (
        "B0CHX1W1XY", 
        "Apple iPhone 15 (128 GB) - Black", 
        "Electronics", 
        "https://www.amazon.in/Apple-iPhone-15-128-GB/dp/B0CHX1W1XY"
    ),
    (
        "B0B11LJ69K", 
        "Logitech MX Master 3S", 
        "Electronics", 
        "https://www.amazon.in/Logitech-MX-Master-3S-Chrome-Graphite/dp/B0B11LJ69K"
    ),
    (
        "B0CY5HVDS2", 
        "Sony PlayStation5 Gaming Console", 
        "Electronics", 
        "https://www.amazon.in/Sony-CFI-2008A01X-PlayStation%C2%AE5-Console-slim/dp/B0CY5HVDS2"
    ),
    (
        "B0B3MNYGTW", 
        "OnePlus Bullets Z2 Bluetooth Wireless in Ear Earphones", 
        "Electronics", 
        "https://www.amazon.in/Oneplus-Bluetooth-Wireless-Earphones-Bombastic/dp/B0B3MNYGTW"
    )
    
     
]

def seed_database():
    conn = get_snowflake_conn()
    cursor = conn.cursor()
    
    print(f"🔌 Connected to Snowflake. Seeding {len(products_to_track)} products...")
    
    try:
        for pid, name, cat, url in products_to_track:
            # Using MERGE to avoid duplicates if you run this script twice
            query = """
            MERGE INTO RAW.PRODUCTS AS target
            USING (SELECT %s AS pid, %s AS name, %s AS cat, %s AS url) AS source
            ON target.product_id = source.pid
            WHEN NOT MATCHED THEN
                INSERT (product_id, product_name, category, product_url)
                VALUES (source.pid, source.name, source.cat, source.url);
            """
            cursor.execute(query, (pid, name, cat, url))
            print(f"✅ Inserted: {name}")
            
        conn.commit()
        print("🎉 Seeding Complete!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    seed_database()