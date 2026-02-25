# 🧱 MergerMetrics Lakehouse Pipeline (Databricks + AWS S3)

### **Acquisition Integration: Summit Sporting Goods (Parent) + PowerBite Nutrition (Child)**

This project simulates a real-world acquisition scenario where a US enterprise (**Summit Sporting Goods**) acquires a fast-growing startup (**PowerBite Nutrition**) with messy operational data. The objective is to build a **reliable unified analytics layer** in **Databricks** using the **Lakehouse + Medallion Architecture (Bronze → Silver → Gold)**, and deliver a single “source of truth” for dashboards and ad-hoc analysis.

---

## 🎯 Business Problem

After the acquisition:

* **Parent (Summit Sporting Goods)** has mature ERP-driven data and reports at a **monthly** grain.
* **Child (PowerBite Nutrition)** has “data chaos”: CSVs, inconsistent formats, missing values, and **daily** transaction data.
* Leadership needs a **single consolidated dashboard** across both companies to support:

  * Sales performance reporting
  * Forecasting & inventory planning
  * Top products/customers/markets insights

✅ This project builds a unified pipeline that cleans, standardizes, aggregates, and merges PowerBite’s data into Summit’s analytics model.

---

## 🏗️ Architecture (High Level)

**Platform:** Databricks Free Edition
**Data Lake:** AWS S3 (Landing + Processed/Archive)
**Storage Format:** Delta Lake tables
**Design Pattern:** Medallion Architecture + Star Schema
**Consumption:** Databricks Dashboards + Genie + Tableau (extract)

### Medallion Flow (Child Company)

1. **S3 Landing → Bronze** (raw ingest “as-is” + metadata columns)
2. **Bronze → Silver** (data cleaning, standardization, validation)
3. **Silver → Gold** (business-ready tables aligned to enterprise model)
4. **Merge Gold (Child) → Gold (Parent)** using UPSERT (MERGE INTO)

### Serving Layer

* Denormalized **Gold View** (`vw_fact_orders_enriched`) for BI performance
* Databricks dashboards + Genie queries
* Tableau dashboard built from the enriched view

---

## 🧬 Data Model (Star Schema)

**Fact**

* `fact_orders`: monthly sales quantity per product & customer

**Dimensions**

* `dim_customers`
* `dim_products`
* `dim_gross_price`
* `dim_date`

**Key Concept:** Child data is transformed to match Parent schema so BI can query **one set of dimensions + one consolidated fact**.

---

## 📦 Repository Contents

### Databricks Workspace Notebook Structure

```
consolidated_pipeline/
  1_setup/
    setup_catalogs
    utilities
    dim_date_table_creation

  2_dimension_data_processing/
    1_customer_data_processing
    2_products_data_processing
    3_pricing_data_processing

  3_fact_data_processing/
    1_full_load_fact
    2_incremental_load_fact
```

### Key Gold Tables (Databricks Catalog: `fmcg`)

* `fmcg.gold.dim_customers` (consolidated parent + child)
* `fmcg.gold.dim_products` (consolidated parent + child)
* `fmcg.gold.dim_gross_price` (consolidated parent + child)
* `fmcg.gold.fact_orders` (consolidated parent + child, monthly grain)
* Child-only gold staging:

  * `fmcg.gold.sb_dim_customers`
  * `fmcg.gold.sb_dim_products`
  * `fmcg.gold.sb_dim_gross_price`
  * `fmcg.gold.sb_fact_orders`

### BI View

* `fmcg.gold.vw_fact_orders_enriched`

---

## 🧹 Data Engineering Logic (What’s Implemented)

### 1) Bronze Layer (Raw Ingestion)

* Reads CSVs from S3
* Writes to Delta tables using overwrite/append (depending on dataset)
* Adds lineage metadata:

  * `read_timestamp`
  * `file_name`
  * `file_size` (where applicable)

### 2) Silver Layer (Cleaning + Standardization)

Typical transformations across dimensions/fact:

* Deduplication by business keys
* Whitespace trimming & casing normalization
* Standardizing inconsistent values (example: city spelling variants)
* Null handling using business rules
* Parsing multiple date formats into one consistent type
* Fixing numeric anomalies:

  * negative prices → absolute values
  * unknown prices → handled by rule

### 3) Gold Layer (Business-Ready Tables)

* Produces tables aligned to parent schema
* Ensures stable joins by enforcing consistent key types
* Aligns granularity:

  * **Child fact data is daily**, but consolidated `fact_orders` is **monthly**
  * Child daily orders are aggregated to month-start to match parent

### 4) Key Engineering Challenge: Unreliable Product IDs

PowerBite product IDs may be inconsistent (numeric + alphanumeric).
Solution:

* Generate a surrogate `product_code` using SHA hash of product name
* Use `product_code` as primary join key across product/pricing/orders

---

## 🔁 Loading Strategy

### Full Load (Historical Backfill)

* Processes historical months in bulk
* Writes full dataset into Bronze → Silver → Gold
* Aggregates daily → monthly before merging into parent gold

### Incremental Load (Daily)

* New daily order files arrive in S3 Landing
* Pipeline reads only new files and loads into staging
* Transforms only the incremental data (does not reprocess history)
* Appends/merges to Silver/Gold
* Moves processed files from Landing → Processed (archive)

---

## ⚙️ Orchestration

Databricks Workflows (Jobs) can run notebooks in dependency order:

1. Customers dimension
2. Products dimension
3. Pricing dimension (depends on products)
4. Fact incremental processing (depends on dimensions)

This creates an automated daily pipeline that keeps the consolidated Gold layer up to date.

---

## 📊 Analytics & Dashboarding

### Denormalized View for BI

To keep dashboards fast, we create a single view that joins Fact + Dimensions:

**`vw_fact_orders_enriched` includes:**

* date attributes: year, quarter, month
* customer attributes: market, platform, channel
* product attributes: category, division, variant
* metrics: sold_quantity, price, total_amount

### Dashboards

* KPI cards (Revenue, Quantity, Unique customers, Avg Selling Price)
* Top products / customers
* Revenue by channel, market, division
* Monthly trend
* Product portfolio scatter (ASP vs Quantity)

### Genie (Databricks AI)

Natural language questions such as:

* “Top 5 products by revenue in 2025”
* “Revenue split by channel”
* “Quarterly revenue trend”

---

## ✅ How to Reproduce (Quick Start)

### Prerequisites

* Databricks Free Edition account
* AWS S3 bucket with:

  * full_load folders (child dims + historical orders)
  * incremental_load folder (daily orders)
* Databricks external location configured to access S3

### Steps

1. **Run Setup**

   * `1_setup/setup_catalogs`
   * `1_setup/utilities`
   * `1_setup/dim_date_table_creation`

2. **Run Dimensions**

   * `2_dimension_data_processing/1_customer_data_processing`
   * `2_dimension_data_processing/2_products_data_processing`
   * `2_dimension_data_processing/3_pricing_data_processing`

3. **Run Fact Full Load**

   * `3_fact_data_processing/1_full_load_fact`

4. **Run Fact Incremental**

   * Drop new daily order file(s) into S3 Landing
   * Run `3_fact_data_processing/2_incremental_load_fact`

5. **Create BI View**

   * Create/refresh `vw_fact_orders_enriched`

6. **Build Dashboards**

   * Databricks SQL dashboard using the enriched view
   * Tableau dashboard using extract from the same view

---

## 🧪 Data Quality Checks Included

* Duplicate detection (group + count checks)
* Schema alignment validation (key data types match)
* Join coverage checks:

  * fact → dim_customers missing keys
  * fact → dim_products missing keys
  * fact → dim_gross_price missing (product_code + year) combinations

---

## 📌 Key Takeaways (What This Demonstrates)

* End-to-end lakehouse pipeline design (Bronze/Silver/Gold)
* Handling messy startup data in a structured enterprise model
* Stable surrogate key generation for unreliable identifiers
* Grain alignment (daily → monthly) before consolidation
* Production-style incremental ingestion + file archival strategy
* BI-ready enriched view for performance + dashboard usability


---

## 📷 Screenshots / Demo Links

* Architecture slide (Medallion + merge strategy)
* Tableau dashboard screenshot
* Short demo video walkthrough (10 minutes)

---


* Databricks Lakehouse | SQL | PySpark | AWS S3 | BI Dashboards
* Project built for interview-ready demonstration of real-world data engineering practices.


