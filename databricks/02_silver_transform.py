# Databricks notebook source
storage_account = "ecommerceflowstorage2"
container_bronze = "bronze"
container_silver = "silver"

sas_token = "YOUR_SAS_TOKEN_HERE"

spark.conf.set(
    f"fs.azure.sas.{container_bronze}.{storage_account}.blob.core.windows.net",
    sas_token
)
spark.conf.set(
    f"fs.azure.sas.{container_silver}.{storage_account}.blob.core.windows.net",
    sas_token
)

print("✅ ADLS connection configured!")

# COMMAND ----------

from pyspark.sql.functions import col, when, lit, current_timestamp

base_path = f"wasbs://{container_bronze}@{storage_account}.blob.core.windows.net"

df_orders    = spark.read.option("header","true").option("inferSchema","true").csv(f"{base_path}/olist_orders_dataset.csv")
df_customers = spark.read.option("header","true").option("inferSchema","true").csv(f"{base_path}/olist_customers_dataset.csv")
df_products  = spark.read.option("header","true").option("inferSchema","true").csv(f"{base_path}/olist_products_dataset.csv")
df_sellers   = spark.read.option("header","true").option("inferSchema","true").csv(f"{base_path}/olist_sellers_dataset.csv")
df_items     = spark.read.option("header","true").option("inferSchema","true").csv(f"{base_path}/olist_order_items_dataset.csv")
df_payments  = spark.read.option("header","true").option("inferSchema","true").csv(f"{base_path}/olist_order_payments_dataset.csv")
df_reviews   = spark.read.option("header","true").option("inferSchema","true").csv(f"{base_path}/olist_order_reviews_dataset.csv")
df_geo       = spark.read.option("header","true").option("inferSchema","true").csv(f"{base_path}/olist_geolocation_dataset.csv")
df_category  = spark.read.option("header","true").option("inferSchema","true").csv(f"{base_path}/product_category_name_translation.csv")

print("✅ All 9 tables loaded from Bronze!")

# COMMAND ----------

from pyspark.sql.functions import col, when, lit, current_timestamp, coalesce

print("Starting data cleaning...")

# 1. ORDERS — handle null delivery dates
df_orders_clean = df_orders.withColumn(
    "order_delivered_customer_date",
    when(col("order_delivered_customer_date").isNull(), lit("Not Delivered"))
    .otherwise(col("order_delivered_customer_date"))
).withColumn(
    "order_approved_at",
    when(col("order_approved_at").isNull(), lit("Pending"))
    .otherwise(col("order_approved_at"))
)

print(f"✅ Orders cleaned   : {df_orders_clean.count():,} rows")

# 2. PRODUCTS — fill null categories
df_products_clean = df_products.withColumn(
    "product_category_name",
    when(col("product_category_name").isNull(), lit("Unknown"))
    .otherwise(col("product_category_name"))
).withColumn(
    "product_description_lenght",
    when(col("product_description_lenght").isNull(), lit(0))
    .otherwise(col("product_description_lenght"))
).withColumn(
    "product_photos_qty",
    when(col("product_photos_qty").isNull(), lit(0))
    .otherwise(col("product_photos_qty"))
)

print(f"✅ Products cleaned  : {df_products_clean.count():,} rows")

# 3. REVIEWS — remove duplicates, keep nulls
df_reviews_clean = df_reviews.dropDuplicates()
print(f"✅ Reviews cleaned   : {df_reviews_clean.count():,} rows")

# 4. GEO — deduplicate by zip code
df_geo_clean = df_geo.dropDuplicates(["geolocation_zip_code_prefix"])
print(f"✅ Geo cleaned       : {df_geo_clean.count():,} rows")

print("\n✅ Data cleaning complete!")

# COMMAND ----------

from pyspark.sql.functions import col, round as spark_round

print("Starting complex joins...")

# Join orders + customers
df_order_customers = df_orders_clean.join(
    df_customers,
    on="customer_id",
    how="left"
)
print(f"✅ Orders + Customers : {df_order_customers.count():,} rows")

# Join with order items
df_order_items_joined = df_order_customers.join(
    df_items,
    on="order_id",
    how="left"
)
print(f"✅ + Items            : {df_order_items_joined.count():,} rows")

# Join with products
df_with_products = df_order_items_joined.join(
    df_products_clean,
    on="product_id",
    how="left"
)
print(f"✅ + Products         : {df_with_products.count():,} rows")

# Join with sellers
df_with_sellers = df_with_products.join(
    df_sellers,
    on="seller_id",
    how="left"
)
print(f"✅ + Sellers          : {df_with_sellers.count():,} rows")

# Join with payments
df_with_payments = df_with_sellers.join(
    df_payments,
    on="order_id",
    how="left"
)
print(f"✅ + Payments         : {df_with_payments.count():,} rows")

# Join with reviews
df_enriched = df_with_payments.join(
    df_reviews_clean,
    on="order_id",
    how="left"
)
print(f"✅ + Reviews          : {df_enriched.count():,} rows")

print("\n✅ All joins complete!")
print(f"Final enriched table : {df_enriched.count():,} rows")
print(f"Total columns        : {len(df_enriched.columns)}")

# COMMAND ----------

from pyspark.sql.functions import concat, rand, explode, array

print("Applying salting on product_category...")

SALT_BUCKETS = 10

# Check skew first
print("\nTop 5 skewed categories:")
df_enriched.groupBy("product_category_name") \
    .count() \
    .orderBy("count", ascending=False) \
    .show(5)

# COMMAND ----------

from pyspark.sql.functions import col, round as spark_round
print("Checking data skew on product_category_name...")
print("=" * 50)

total_rows = df_enriched.count()

print(f"\nTotal rows: {total_rows:,}")
print(f"\nTop 10 categories by row count:")
print("-" * 50)

df_enriched.groupBy("product_category_name") \
    .count() \
    .withColumn("percentage", 
        spark_round((col("count")/total_rows)*100, 2)) \
    .orderBy("count", ascending=False) \
    .show(10, truncate=False)

# COMMAND ----------

from pyspark.sql.functions import concat, lit, floor, rand, explode, array

print("Applying salting to resolve skew...")
print("=" * 50)

SALT_BUCKETS = 10

# Step 1 — Add salt to large table (df_enriched)
df_salted = df_enriched.withColumn(
    "salted_key",
    concat(
        col("product_category_name"),
        lit("_"),
        floor(rand() * SALT_BUCKETS).cast("string")
    )
)

print(f"✅ Salt added to enriched table")
print(f"   Before salting : {df_enriched.count():,} rows")
print(f"   After salting  : {df_salted.count():,} rows")
print(f"   (Must be same!)")

# Step 2 — Verify salt distribution
print(f"\nSalt distribution for cama_mesa_banho:")
print("-" * 40)
df_salted.filter(
    col("product_category_name") == "cama_mesa_banho"
).groupBy("salted_key") \
 .count() \
 .orderBy("salted_key") \
 .show()

# Step 3 — Remove salt key after use
df_final_silver = df_salted.drop("salted_key")

print(f"✅ Salt key removed after use")
print(f"   Final rows    : {df_final_silver.count():,}")
print(f"   Final columns : {len(df_final_silver.columns)}")
print("\n✅ Salting complete!")

# COMMAND ----------

from pyspark.sql.functions import year, month

print("Writing to Silver layer...")
print("=" * 50)

silver_path = f"wasbs://{container_silver}@{storage_account}.blob.core.windows.net"

# Write enriched table as Delta
df_final_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("order_status") \
    .save(f"{silver_path}/fact_orders")

print(f"✅ fact_orders written to Silver!")
print(f"   Rows      : {df_final_silver.count():,}")
print(f"   Columns   : {len(df_final_silver.columns)}")
print(f"   Format    : Delta")
print(f"   Partition : order_status")

# Write clean dimension tables
df_customers.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{silver_path}/dim_customers")
print(f"✅ dim_customers written!")

df_products_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{silver_path}/dim_products")
print(f"✅ dim_products written!")

df_sellers.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{silver_path}/dim_sellers")
print(f"✅ dim_sellers written!")

print("\n✅ Silver layer complete!")