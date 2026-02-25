# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable


# COMMAND ----------

# MAGIC %run /Workspace/consolidated_pipeline/1_setup/utilities

# COMMAND ----------

print(bronze_schema, silver_schema, gold_schema)

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "customers", "Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

print(catalog, data_source)

# COMMAND ----------

base_path = f's3://powerbite-nutrition/{data_source}/*.csv'
print(base_path)

# COMMAND ----------

df = (
    spark.read.format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(base_path)
    .withColumn("read_timestamp", F.current_timestamp())
    .select("*", "_metadata.file_name", "_metadata.file_size")
)

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

bronze_table_path = f"{catalog}.{bronze_schema}.{data_source}"

df.write.format("delta") \
    .option("delta.enableChangeDataFeed", "true") \
    .mode("overwrite") \
    .saveAsTable(bronze_table_path)


# COMMAND ----------

df_bronze = spark.sql(f"SELECT * FROM {bronze_table_path}")
df_bronze.show(10)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

#check for duplicates

df_duplicates = df_bronze.groupBy("customer_id").count().filter("count > 1")
display(df_duplicates)


# COMMAND ----------

count_before = df_bronze.count()

df_silver = df_bronze.dropDuplicates(["customer_id"])
count_after = df_silver.count()

print(f"Count before dropping duplicates: {count_before}")
print(f"Count after dropping duplicates: {count_after}")


# COMMAND ----------

#check customer name has leading or trailing spaces

display(df_silver.filter(F.col("customer_name") != F.trim(F.col("customer_name"))))

# COMMAND ----------

#remove trailing white spaces
df_silver = df_silver.withColumn("customer_name", F.trim(F.col("customer_name")))

# COMMAND ----------

df_silver.select('city').distinct().show()

# COMMAND ----------

# DBTITLE 1,fixing city names
#fixing city names

city_mapping = {
    "New York": "New York",
    "Newyork": "New York",
    "New yok": "New York",
    "Chicago": "Chicago",
    "Chcago": "Chicago",
    "Chicgo": "Chicago",
    "Chicagoo": "Chicago",
    "Chciago": "Chicago",
    "Austin": "Austin",
    "Austn": "Austin",
    "Austiin": "Austin",
    "Austinn": "Austin"
}

allowed = ["New York", "Chicago", "Austin"]

# Fix: create the list of key-value pairs outside
city_map_pairs = []
for k, v in city_mapping.items():
    city_map_pairs.extend([F.lit(k), F.lit(v)])

city_map_expr = F.create_map(city_map_pairs)

df_silver = df_silver.withColumn(
    "city",
    F.when(F.col("city").isin(allowed), F.col("city"))
     .otherwise(
        F.coalesce(
            city_map_expr.getItem(F.col("city")),
            F.lit(None)
        )
     )
)

# COMMAND ----------

df_silver.select('city').distinct().show()

# COMMAND ----------

df_silver.select('customer_name').distinct().show()

# COMMAND ----------

#fix title case

df_silver = df_silver.withColumn(
    "customer_name",
    F.when(
        F.col("customer_name").isNull(), None)
        .otherwise(F.initcap(F.col("customer_name"))
    )
)

df_silver.select('customer_name').distinct().show()

# COMMAND ----------

display(df_silver.filter(F.col("city").isNull()))

# COMMAND ----------

null_customer_name = [
    "Sprintx Nutrition",
    "Zenathlete Foods",
    "Primefuel Nutrition",
    "Recovery Lane"
]

# COMMAND ----------

display(df_silver.filter(F.col("customer_name").isin(null_customer_name)))

# COMMAND ----------

# create dict for customer-city fix
customer_city_fix = {
    789403: "Chicago",
    789420: "Austin",
    789521: "New York",
    789603: "New York"
}

df_fix = spark.createDataFrame(
    [(k, v) for k, v in customer_city_fix.items()],
    ["customer_id", "fixed_city"]
)

display(df_fix)


# COMMAND ----------

display(df_silver)

# COMMAND ----------


df_silver = (
    df_silver.join(df_fix, "customer_id", "left")
    .withColumn(
        "city",
        F.coalesce(F.col("city"), F.col("fixed_city"))
    )
    .drop("fixed_city")
)
display(df_silver)

# COMMAND ----------

df_silver = df_silver.withColumn("customer_id", F.col("customer_id").cast("string"))
display(df_silver)

# COMMAND ----------


df_silver = df_silver.withColumn(
    "customer",
    F.concat_ws(
        "-",
        F.col("customer_name"),
        F.when(F.col("city").isNull(), F.lit("Unknown")).otherwise(F.col("city"))
    )
)
# static attributes aligned with parent data model

df_silver = df_silver.withColumn("platform", F.lit("PowerBite Nutrition"))
df_silver = df_silver.withColumn("channel", F.lit("Acquisition"))
df_silver = df_silver.withColumn(
    "market",
    F.when(F.col("city") == "New York", F.lit("Northeast"))
     .when(F.col("city") == "Chicago", F.lit("Midwest"))
     .when(F.col("city") == "Austin", F.lit("South"))
     .when(F.col("city") == "Los Angeles", F.lit("West Coast"))
     .otherwise(F.lit("Unknown"))
)
display(df_silver)


# COMMAND ----------

df_silver.printSchema()
display(df_silver)

# COMMAND ----------

silver_table_path = f"{catalog}.{silver_schema}.{data_source}"

df_silver.write.format("delta") \
    .option("delta.enableChangeDataFeed", "true") \
    .option("mergeSchema", "true") \
    .mode("overwrite") \
    .saveAsTable(silver_table_path)

# COMMAND ----------

df_silver = df_silver.withColumnRenamed("customer_code", "customer_id")
display(df_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC # **Gold Process**

# COMMAND ----------

# DBTITLE 1,Untitled
df_silver = spark.sql(f"SELECT * FROM {catalog}.{silver_schema}.{data_source};")

# take req cols only
# "customer_id, customer_name, city, read_timestamp, file_name, file_size, customer, market, platform, channel"
df_gold = df_silver.select("customer_id", "customer_name", "city", "customer", "market", "platform", "channel")

display(df_gold)

# COMMAND ----------

df_gold.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Merge Data Source

# COMMAND ----------


df_child_customers = (
    spark.table("fmcg.gold.sb_dim_customers")
      .select(
          F.col("customer_id").cast("string").alias("customer_code"),
          F.col("customer"),
          F.col("market"),
          F.col("platform"),
          F.col("channel")
      )
)

# COMMAND ----------

from delta.tables import DeltaTable

delta_table = DeltaTable.forName(spark, "fmcg.gold.dim_customers")

# COMMAND ----------

delta_table.alias("target").merge(
    source=df_child_customers.alias("source"),
    condition="target.customer_code = source.customer_code"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()