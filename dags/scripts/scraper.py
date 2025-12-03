import asyncio
from playwright.async_api import async_playwright
import snowflake.connector
import os
from dotenv import load_dotenv
from datetime import datetime
import random

# Load environment variables
load_dotenv()

# --- CONFIGURATION ---
# If you are using Flipkart, change these selectors!
SELECTORS = {
    "amazon": {
        "price": "span.a-price > span.a-offscreen",
        "mrp": "span.a-text-price > span.a-offscreen",
        "availability": "#availability span, span.a-color-success"
    }
}

CURRENT_SITE = "amazon" # Switch to 'flipkart' if needed

def get_snowflake_conn():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )

def clean_price(price_str):
    """Converts '₹1,999.00' to float 1999.0"""
    if not price_str:
        return None
    # Remove currency symbol, commas, and whitespace
    clean = price_str.replace("₹", "").replace("$", "").replace(",", "").strip()
    try:
        return float(clean)
    except ValueError:
        return None

async def scrape_product(page, url):
    """Visits a page and extracts data"""
    print(f"🕵️  Scraping: {url[:60]}...")
    try:
        # random timeout to mimic human behavior
        await page.goto(url, timeout=60000) 
        await page.wait_for_selector('body')
        
        # Extract Text
        sel = SELECTORS[CURRENT_SITE]
        
        # Get Current Price
        price_el = await page.query_selector(sel["price"])
        current_price = await price_el.inner_text() if price_el else None
        
        # Get MRP (Original Price)
        mrp_el = await page.query_selector(sel["mrp"])
        mrp_price = await mrp_el.inner_text() if mrp_el else None
        
        return clean_price(current_price), clean_price(mrp_price)
        
    except Exception as e:
        print(f"❌ Error scraping {url}: {e}")
        return None, None

async def main():
    conn = get_snowflake_conn()
    cursor = conn.cursor()
    
    # 1. Fetch Products from Snowflake
    print("📥 Fetching products from Snowflake...")
    cursor.execute("SELECT product_id, product_url FROM RAW.PRODUCTS WHERE track_product = TRUE")
    products = cursor.fetchall()
    
    # 2. Launch Browser
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True) # Set False to see the browser work!
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )
        page = await context.new_page()

        for pid, url in products:
            current_price, original_price = await scrape_product(page, url)
            
            if current_price:
                # 3. Insert into Snowflake
                print(f"💰 Price Found: {current_price} (MRP: {original_price})")
                insert_query = """
                INSERT INTO RAW.PRICE_HISTORY 
                (record_id, product_id, current_price, original_price, scraped_at, is_deal_active)
                VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP(), %s)
                """
                # Generate a unique record ID (e.g., PID_TIMESTAMP)
                record_id = f"{pid}_{int(datetime.now().timestamp())}"
                is_deal = True if (original_price and current_price < original_price) else False
                
                cursor.execute(insert_query, (record_id, pid, current_price, original_price, is_deal))
                print("✅ Saved to DB")
            else:
                print("⚠️  No price found (Anti-bot or bad selector?)")
            
            # Be polite to the server
            await asyncio.sleep(random.uniform(2, 5))

        await browser.close()
    
    conn.commit()
    conn.close()
    print("🎉 Scraping Run Complete")

if __name__ == "__main__":
    asyncio.run(main())