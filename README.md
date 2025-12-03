# 🛍️ Deal Authenticity & Price Tracker (E-Commerce)

### **Overview**
A data pipeline that tracks product prices on Amazon/Flipkart, detects fake discounts using statistical analysis, and recommends the best time to buy.

**Problem:** E-commerce sites often inflate "Original Prices" to make discounts look bigger.  
**Solution:** I built a tool that tracks historical price volatility and uses a Z-Score algorithm to flag "Fake Deals" vs. "True Discounts."

---

### **Architecture**
* **Ingestion:** Python (Playwright) scraping daily prices.
* **Storage:** Snowflake Data Warehouse (Raw & Analytics layers).
* **Orchestration:** Apache Airflow (DAGs for daily scheduling).
* **Analytics:** SQL (Window Functions for Moving Averages & Z-Score calculation).
* **Visualization:** Power BI (Dashboard for Buy/Wait recommendations).

### **Key Features**
* **Fake Discount Detection:** Compares current price against a 30-day moving average (not just MRP).
* **Volatility Analysis:** Uses Standard Deviation to identify statistically significant price drops.
* **Automated Pipeline:** Runs daily to build a historical dataset.

### **Dashboard Preview**
*(Drag and drop your screenshot here after you push to GitHub!)*

---
