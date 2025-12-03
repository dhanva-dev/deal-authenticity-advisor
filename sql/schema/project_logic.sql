-- 1. DATABASE SETUP
CREATE DATABASE IF NOT EXISTS DEAL_ADVISOR_DB;
USE DATABASE DEAL_ADVISOR_DB;
CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS ANALYTICS;

-- 2. RAW DATA TABLES
CREATE OR REPLACE TABLE RAW.PRODUCTS (
    product_id VARCHAR(50) PRIMARY KEY,
    product_name STRING,
    product_url STRING,
    category STRING,
    track_product BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE RAW.PRICE_HISTORY (
    record_id STRING,
    product_id VARCHAR(50),
    scraped_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    current_price FLOAT,
    original_price FLOAT,
    is_deal_active BOOLEAN,
    currency STRING DEFAULT 'INR',
    retailer STRING DEFAULT 'Amazon',
    FOREIGN KEY (product_id) REFERENCES RAW.PRODUCTS(product_id)
);

-- 3. ANALYTICS VIEW (The Brains)
CREATE OR REPLACE VIEW ANALYTICS.DEAL_ADVISOR_VIEW AS
WITH price_stats AS (
    SELECT 
        h.product_id,
        h.scraped_at,
        h.current_price,
        h.original_price,
        p.product_name,
        
        -- Moving Average (30 Days)
        AVG(h.current_price) OVER (
            PARTITION BY h.product_id 
            ORDER BY h.scraped_at 
            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ) as avg_price_30d,
        
        -- Standard Deviation (Volatility)
        STDDEV(h.current_price) OVER (
            PARTITION BY h.product_id 
            ORDER BY h.scraped_at 
            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ) as std_dev_30d,
        
        -- Previous MRP to detect fake spikes
        LAG(h.original_price, 1) OVER (
            PARTITION BY h.product_id ORDER BY h.scraped_at
        ) as prev_mrp
        
    FROM RAW.PRICE_HISTORY h
    JOIN RAW.PRODUCTS p ON h.product_id = p.product_id
)

SELECT 
    product_name,
    current_price,
    avg_price_30d,
    
    -- Real Discount Calculation
    CASE 
        WHEN avg_price_30d IS NOT NULL AND avg_price_30d > 0 
        THEN ROUND(((avg_price_30d - current_price) / avg_price_30d) * 100, 2)
        ELSE 0 
    END as real_discount_pct,

    -- Z-Score & Recommendation Logic
    CASE 
        WHEN (current_price - avg_price_30d) / NULLIF(std_dev_30d,0) < -1.5 THEN '🔥 BUY NOW (Great Deal)'
        WHEN original_price > (prev_mrp * 1.1) AND current_price >= avg_price_30d THEN '⚠️ FAKE DEAL (MRP Bumped)'
        WHEN current_price < avg_price_30d THEN '✅ Good Price'
        WHEN current_price > avg_price_30d THEN '❌ WAIT (Overpriced)'
        ELSE '😐 Neutral'
    END as recommendation,
    
    scraped_at

FROM price_stats
WHERE scraped_at >= DATEADD(day, -1, CURRENT_TIMESTAMP())
QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY scraped_at DESC) = 1;