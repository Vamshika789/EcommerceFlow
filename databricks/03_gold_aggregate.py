# Databricks notebook source
# ================================================
# NOTEBOOK 3 — Gold Aggregate
# ================================================

storage_account  = "ecommerceflowstorage2"
container_silver = "silver"
container_gold   = "gold"

storage_key = "YOUR_STORAGE_KEY_HERE"

spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_key
)

silver_path = f"abfss://{container_silver}@{storage_account}.dfs.core.windows.net"
gold_path   = f"abfss://{container_gold}@{storage_account}.dfs.core.windows.net"

print("✅ ADLS Gen2 connected!")
print(f"Silver : {silver_path}")
print(f"Gold   : {gold_path}")

# COMMAND ----------

from pyspark.sql.functions import (
    col, sum, avg, count,
    round, year, month,
    max, min, countDistinct
)

# Read Silver fact table
df_silver = spark.read \
    .format("delta") \
    .load(f"{silver_path}/fact_orders")

# Read dimension tables
df_dim_customers = spark.read \
    .format("delta") \
    .load(f"{silver_path}/dim_customers")

df_dim_products = spark.read \
    .format("delta") \
    .load(f"{silver_path}/dim_products")

df_dim_sellers = spark.read \
    .format("delta") \
    .load(f"{silver_path}/dim_sellers")

print("=" * 50)
print("SILVER TABLES LOADED")
print("=" * 50)
print(f"fact_orders   : {df_silver.count():,} rows  {len(df_silver.columns)} cols")
print(f"dim_customers : {df_dim_customers.count():,} rows  {len(df_dim_customers.columns)} cols")
print(f"dim_products  : {df_dim_products.count():,} rows  {len(df_dim_products.columns)} cols")
print(f"dim_sellers   : {df_dim_sellers.count():,} rows  {len(df_dim_sellers.columns)} cols")
print("=" * 50)
print("✅ All Silver tables loaded!")

# COMMAND ----------

from pyspark.sql import functions as F

print("Creating Gold Fact Table...")
print("=" * 50)

# Select relevant columns for fact table
df_fact_sales = df_silver.select(
    "order_id",
    "customer_id",
    "product_id",
    "seller_id",
    "order_status",
    "order_purchase_timestamp",
    "order_delivered_customer_date",
    "payment_type",
    "payment_value",
    "payment_installments",
    "price",
    "freight_value",
    "review_score"
).withColumn(
    "order_year",
    F.year(F.col("order_purchase_timestamp"))
).withColumn(
    "order_month",
    F.month(F.col("order_purchase_timestamp"))
)

print(f"✅ fact_sales created!")
print(f"   Rows    : {df_fact_sales.count():,}")
print(f"   Columns : {len(df_fact_sales.columns)}")

# NULL CHECK
print("\nNULL CHECK — FACT TABLE")
print("=" * 50)

total = df_fact_sales.count()
null_columns = []

for c in df_fact_sales.columns:
    null_count = df_fact_sales.filter(
        F.col(c).isNull()
    ).count()
    if null_count > 0:
        pct = int((null_count/total)*10000)/100
        print(f"   ⚠️  {c:<35} → {null_count:>6,} nulls ({pct}%)")
        null_columns.append(c)
    else:
        print(f"   ✅ {c:<35} → clean")

# FIX NULLS
print("\nFixing nulls in fact table...")
print("=" * 50)

# Fix string nulls
df_fact_sales = df_fact_sales \
    .withColumn("payment_type",
        F.when(F.col("payment_type").isNull(),
               F.lit("unknown"))
        .otherwise(F.col("payment_type"))
    ) \
    .withColumn("order_delivered_customer_date",
        F.when(F.col("order_delivered_customer_date").isNull(),
               F.lit("Not Delivered"))
        .otherwise(F.col("order_delivered_customer_date"))
    ) \
    .withColumn("product_id",
        F.when(F.col("product_id").isNull(),
               F.lit("unknown"))
        .otherwise(F.col("product_id"))
    ) \
    .withColumn("seller_id",
        F.when(F.col("seller_id").isNull(),
               F.lit("unknown"))
        .otherwise(F.col("seller_id"))
    )

# Fix numeric nulls
df_fact_sales = df_fact_sales \
    .withColumn("payment_value",
        F.when(F.col("payment_value").isNull(),
               F.lit(0))
        .otherwise(F.col("payment_value"))
    ) \
    .withColumn("payment_installments",
        F.when(F.col("payment_installments").isNull(),
               F.lit(0))
        .otherwise(F.col("payment_installments"))
    ) \
    .withColumn("price",
        F.when(F.col("price").isNull(),
               F.lit(0))
        .otherwise(F.col("price"))
    ) \
    .withColumn("freight_value",
        F.when(F.col("freight_value").isNull(),
               F.lit(0))
        .otherwise(F.col("freight_value"))
    ) \
    .withColumn("review_score",
        F.when(F.col("review_score").isNull(),
               F.lit(0))
        .otherwise(F.col("review_score"))
    )

# VERIFY NULLS FIXED
print("\nNULL CHECK AFTER FIX:")
print("=" * 50)
total = df_fact_sales.count()
for c in df_fact_sales.columns:
    null_count = df_fact_sales.filter(
        F.col(c).isNull()
    ).count()
    if null_count > 0:
        pct = int((null_count/total)*10000)/100
        print(f"   ⚠️  {c:<35} → {null_count:>6,} nulls ({pct}%)")
    else:
        print(f"   ✅ {c:<35} → clean")

print("\n✅ Fact table nulls fixed!")
print(f"   Final rows    : {df_fact_sales.count():,}")
print(f"   Final columns : {len(df_fact_sales.columns)}")

# Display sample
print("\nSample Data:")
display(df_fact_sales.limit(10))

# COMMAND ----------

from pyspark.sql import functions as F

print("Creating Gold Dimension Tables...")
print("=" * 50)

# dim_customer
df_dim_customer_gold = df_dim_customers.select(
    "customer_id",
    "customer_unique_id",
    "customer_zip_code_prefix",
    "customer_city",
    "customer_state"
).dropDuplicates(["customer_id"])

print(f"✅ dim_customer : {df_dim_customer_gold.count():,} rows")

# dim_product
df_dim_product_gold = df_dim_products.select(
    "product_id",
    "product_category_name",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm"
).dropDuplicates(["product_id"])

print(f"✅ dim_product  : {df_dim_product_gold.count():,} rows")

# dim_seller
df_dim_seller_gold = df_dim_sellers.select(
    "seller_id",
    "seller_zip_code_prefix",
    "seller_city",
    "seller_state"
).dropDuplicates(["seller_id"])

print(f"✅ dim_seller   : {df_dim_seller_gold.count():,} rows")

print("\n✅ All dimension tables created!")
print("=" * 50)

# NULL CHECK
print("\nNULL CHECK — DIMENSION TABLES")
print("=" * 50)

for name, df in {
    'dim_customer': df_dim_customer_gold,
    'dim_product' : df_dim_product_gold,
    'dim_seller'  : df_dim_seller_gold
}.items():
    print(f"\n📋 {name.upper()}")
    total = df.count()
    for c in df.columns:
        null_count = df.filter(F.col(c).isNull()).count()
        if null_count > 0:
            pct = int((null_count/total)*10000)/100
            print(f"   ⚠️  {c:<30} → {null_count:>5,} nulls ({pct}%)")
        else:
            print(f"   ✅ {c:<30} → clean")

print("\n" + "=" * 50)
print("✅ Dimension null check complete!")

# Display samples
print("\nSample — dim_customer:")
display(df_dim_customer_gold.limit(5))

print("\nSample — dim_product:")
display(df_dim_product_gold.limit(5))

print("\nSample — dim_seller:")
display(df_dim_seller_gold.limit(5))

# COMMAND ----------

from pyspark.sql import functions as F

print("NULL CHECK — FACT TABLE AFTER FIX")
print("=" * 50)

total = df_fact_sales.count()
all_clean = True

for c in df_fact_sales.columns:
    null_count = df_fact_sales.filter(
        F.col(c).isNull()
    ).count()
    if null_count > 0:
        pct = int((null_count/total)*10000)/100
        print(f"⚠️  {c:<35} → {null_count:>6,} nulls ({pct}%)")
        all_clean = False
    else:
        print(f"✅ {c:<35} → clean")

print("=" * 50)
if all_clean:
    print("✅ ALL NULLS FIXED!")
else:
    print("⚠️  SOME NULLS REMAIN!")

# COMMAND ----------

from pyspark.sql import functions as F

print("NULL CHECK — ALL DIMENSION TABLES")
print("=" * 50)

for name, df in {
    'dim_customer': df_dim_customer_gold,
    'dim_product' : df_dim_product_gold,
    'dim_seller'  : df_dim_seller_gold
}.items():
    print(f"\n📋 {name.upper()}")
    total = df.count()
    all_clean = True
    for c in df.columns:
        null_count = df.filter(F.col(c).isNull()).count()
        if null_count > 0:
            pct = int((null_count/total)*10000)/100
            print(f"   ⚠️  {c:<30} → {null_count:>5,} nulls ({pct}%)")
            all_clean = False
        else:
            print(f"   ✅ {c:<30} → clean")
    if all_clean:
        print(f"   ✅ All clean!")

print("\n" + "=" * 50)

# COMMAND ----------

from pyspark.sql import functions as F

# Fix dim_product nulls
df_dim_product_gold = df_dim_product_gold \
    .withColumn("product_weight_g",
        F.when(F.col("product_weight_g").isNull(),
               F.lit(0))
        .otherwise(F.col("product_weight_g"))
    ) \
    .withColumn("product_length_cm",
        F.when(F.col("product_length_cm").isNull(),
               F.lit(0))
        .otherwise(F.col("product_length_cm"))
    ) \
    .withColumn("product_height_cm",
        F.when(F.col("product_height_cm").isNull(),
               F.lit(0))
        .otherwise(F.col("product_height_cm"))
    ) \
    .withColumn("product_width_cm",
        F.when(F.col("product_width_cm").isNull(),
               F.lit(0))
        .otherwise(F.col("product_width_cm"))
    )

print("✅ dim_product nulls fixed!")

# Verify
print("\nNULL CHECK AFTER FIX — DIM_PRODUCT")
print("=" * 50)
total = df_dim_product_gold.count()
for c in df_dim_product_gold.columns:
    null_count = df_dim_product_gold.filter(
        F.col(c).isNull()
    ).count()
    if null_count > 0:
        print(f"⚠️  {c:<30} → {null_count:>5,} nulls")
    else:
        print(f"✅ {c:<30} → clean")

print("=" * 50)
print(f"✅ dim_product : {df_dim_product_gold.count():,} rows")

# COMMAND ----------

from pyspark.sql import functions as F

print("Creating Gold Aggregations...")
print("=" * 50)

# 1. Monthly sales by category
df_monthly_sales = df_silver \
    .groupBy(
        F.year(F.col("order_purchase_timestamp")).alias("year"),
        F.month(F.col("order_purchase_timestamp")).alias("month"),
        "product_category_name"
    ).agg(
        F.count("order_id").alias("total_orders"),
        F.sum("payment_value").alias("total_revenue"),
        F.avg("review_score").alias("avg_rating")
    ).orderBy("year", "month")

print(f"✅ Monthly sales by category:")
print(f"   Rows : {df_monthly_sales.count():,}")
display(df_monthly_sales.limit(10))

# 2. Seller performance
df_seller_performance = df_silver \
    .groupBy(
        "seller_id",
        "seller_city",
        "seller_state"
    ).agg(
        F.count("order_id").alias("total_orders"),
        F.sum("payment_value").alias("total_revenue"),
        F.avg("review_score").alias("avg_rating")
    ).orderBy("total_revenue", ascending=False)

print(f"\n✅ Seller performance:")
print(f"   Rows : {df_seller_performance.count():,}")
display(df_seller_performance.limit(10))

# 3. Customer lifetime value
df_customer_ltv = df_silver \
    .groupBy(
        "customer_id",
        "customer_city",
        "customer_state"
    ).agg(
        F.count("order_id").alias("total_orders"),
        F.sum("payment_value").alias("lifetime_value"),
        F.avg("payment_value").alias("avg_order_value"),
        F.avg("review_score").alias("avg_rating")
    ).orderBy("lifetime_value", ascending=False)

print(f"\n✅ Customer LTV:")
print(f"   Rows : {df_customer_ltv.count():,}")
display(df_customer_ltv.limit(10))

# 4. Top products by revenue
df_top_products = df_silver \
    .groupBy(
        "product_id",
        "product_category_name"
    ).agg(
        F.count("order_id").alias("total_orders"),
        F.sum("payment_value").alias("total_revenue"),
        F.avg("review_score").alias("avg_rating")
    ).orderBy("total_revenue", ascending=False)

print(f"\n✅ Top products by revenue:")
print(f"   Rows : {df_top_products.count():,}")
display(df_top_products.limit(10))

print("\n" + "=" * 50)
print("✅ All Gold aggregations complete!")

# COMMAND ----------

from pyspark.sql import functions as F

print("Writing to Gold layer...")
print("=" * 50)

gold_path = f"abfss://{container_gold}@{storage_account}.dfs.core.windows.net"

# 1. Write fact_sales
df_fact_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("order_status") \
    .save(f"{gold_path}/fact_sales")

print(f"✅ fact_sales written!")
print(f"   Rows      : {df_fact_sales.count():,}")
print(f"   Format    : Delta")
print(f"   Partition : order_status")

# 2. Write dim_customer
df_dim_customer_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{gold_path}/dim_customer")

print(f"\n✅ dim_customer written!")
print(f"   Rows : {df_dim_customer_gold.count():,}")

# 3. Write dim_product
df_dim_product_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{gold_path}/dim_product")

print(f"\n✅ dim_product written!")
print(f"   Rows : {df_dim_product_gold.count():,}")

# 4. Write dim_seller
df_dim_seller_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{gold_path}/dim_seller")

print(f"\n✅ dim_seller written!")
print(f"   Rows : {df_dim_seller_gold.count():,}")

# 5. Write aggregations
df_monthly_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{gold_path}/monthly_sales")

print(f"\n✅ monthly_sales written!")
print(f"   Rows : {df_monthly_sales.count():,}")

df_seller_performance.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{gold_path}/seller_performance")

print(f"\n✅ seller_performance written!")
print(f"   Rows : {df_seller_performance.count():,}")

df_customer_ltv.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{gold_path}/customer_ltv")

print(f"\n✅ customer_ltv written!")
print(f"   Rows : {df_customer_ltv.count():,}")

df_top_products.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{gold_path}/top_products")

print(f"\n✅ top_products written!")
print(f"   Rows : {df_top_products.count():,}")

print("\n" + "=" * 50)
print("✅ Gold layer complete!")
print(f"   Tables written : 7")
print(f"   Format         : Delta")
print(f"   Location       : {gold_path}")

# COMMAND ----------

from datetime import datetime
from pyspark.sql import functions as F

print("=" * 55)
print("AUDIT LOG — GOLD LAYER")
print("=" * 55)
print(f"Run timestamp  : {datetime.now()}")
print(f"Storage account: {storage_account}")
print(f"Gold container : {container_gold}")
print(f"Pipeline       : Gold Aggregate")
print("=" * 55)

print(f"\n{'TABLE':<25} {'ROWS':>10} {'FORMAT':<10} {'STATUS'}")
print("-" * 55)

tables = {
    'fact_sales'          : df_fact_sales,
    'dim_customer'        : df_dim_customer_gold,
    'dim_product'         : df_dim_product_gold,
    'dim_seller'          : df_dim_seller_gold,
    'monthly_sales'       : df_monthly_sales,
    'seller_performance'  : df_seller_performance,
    'customer_ltv'        : df_customer_ltv,
    'top_products'        : df_top_products
}

total_rows = 0
for name, df in tables.items():
    rows = df.count()
    total_rows += rows
    print(f"{name:<25} {rows:>10,} {'Delta':<10} ✅")

print("-" * 55)
print(f"{'TOTAL':<25} {total_rows:>10,}")
print("=" * 55)
print(f"\n📋 SUMMARY:")
print(f"   Total tables written : {len(tables)}")
print(f"   Total rows written   : {total_rows:,}")
print(f"   Pipeline end time    : {datetime.now()}")
print(f"   Pipeline status      : ✅ GOLD COMPLETE!")
print("=" * 55)