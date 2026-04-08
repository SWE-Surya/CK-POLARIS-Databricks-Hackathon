import dlt
from pyspark.sql import functions as F

print("Initializing Enterprise Silver Layer: Sales Entity...")

# ==============================================================================
# 1. RULES FOR SALES
# ==============================================================================
sales_rules = {
    "valid_sales_id": "sales_id IS NOT NULL",
    "valid_car_id": "car_id IS NOT NULL",
    "valid_price": "original_selling_price >= 0 AND original_selling_price IS NOT NULL",
    "valid_region": "Region IN ('East', 'West', 'Central', 'South', 'North')" 
}

# Helper function to aggressively convert the text "null" into a true SQL Null
def force_true_null(col_name):
    trimmed = F.trim(F.col(col_name))
    return F.when(F.lower(trimmed).isin("null", "none", "", "nan"), F.lit(None)).otherwise(trimmed)

# ==============================================================================
# 2. THE TRANSFORMATION FUNCTION
# ==============================================================================
def get_clean_sales_data():
    raw_df = dlt.read_stream("primeinsurance.bronze.sales")
    
    

    clean_df = raw_df.select(
        # --- IDs ---
        force_true_null("sales_id").cast("int").alias("sales_id"),
        force_true_null("car_id").cast("string").alias("car_id"),
        
        # --- TIMESTAMPS ---
        F.to_timestamp(force_true_null("ad_placed_on"), "dd-MM-yyyy HH:mm").alias("ad_placed_on"),
        F.to_timestamp(force_true_null("sold_on"), "dd-MM-yyyy HH:mm").alias("sold_on"),
        
        # --- FINANCIAL METRICS ---
        force_true_null("original_selling_price").cast("int").alias("original_selling_price"),
        
        # --- CATEGORICAL STRINGS ---
        F.initcap(force_true_null("Region")).cast("string").alias("Region"),
        F.initcap(force_true_null("State")).cast("string").alias("State"),
        F.initcap(force_true_null("City")).cast("string").alias("City"),
        F.initcap(force_true_null("seller_type")).cast("string").alias("seller_type"),
        F.initcap(force_true_null("owner")).cast("string").alias("owner"),
        
        # --- LINEAGE ---
        F.col("_source_file")
    )
    
    # Apply the Region standardization map
    return clean_df.replace({"W": "West", "C": "Central", "E": "East", "S": "South", "N": "North"}, subset=["Region"])


# ==============================================================================
# 3. ROUTE GOOD DATA TO SILVER
# ==============================================================================
@dlt.table(
    name="primeinsurance.silver.sales_silver", 
    comment="Cleaned sales data with explicit timestamp parsing and region handling."
)
@dlt.expect_all_or_drop(sales_rules)
def sales_silver():
    # Apply dropDuplicates HERE so it doesn't mask DQ errors upstream
    return get_clean_sales_data().dropDuplicates(["sales_id"])

# ==============================================================================
# 4. APPEND BAD DATA TO CENTRAL DQ LOG
# ==============================================================================
@dlt.append_flow(target="primeinsurance.silver.dq_issues", name="append_sales_dq_flow")
def append_sales_dq():
    df = get_clean_sales_data()
    
    quarantine_condition = " OR ".join([f"NOT ({rule})" for rule in sales_rules.values()])
    
    # Explicit NULL trap to catch categorical columns that fail the IN clause
    null_trap = " OR Region IS NULL"
    
    bad_df = df.filter(quarantine_condition + null_trap)
    
    return bad_df.select(
        F.expr("uuid()").cast("string").alias("issue_id"),
        F.lit("sales").alias("table_name"),
        
        # 1. IDENTIFY THE EXACT COLUMN THAT BROKE
        F.when(F.col("sales_id").isNull(), F.lit("sales_id"))
         .when(F.col("car_id").isNull(), F.lit("car_id"))
         .when((F.col("original_selling_price") < 0) | F.col("original_selling_price").isNull(), F.lit("original_selling_price"))
         .otherwise(F.lit("Region")).alias("column_name"),
         
        # 2. CLEAR, HUMAN-READABLE ISSUE DESCRIPTION
        F.when(F.col("sales_id").isNull(), F.lit("Missing or empty Sales ID"))
         .when(F.col("car_id").isNull(), F.lit("Orphaned record: Missing Car ID"))
         .when((F.col("original_selling_price") < 0) | F.col("original_selling_price").isNull(), F.lit("Invalid financial metric: Price is negative or missing"))
         .otherwise(F.lit("Invalid Region value (Must be East, West, Central, North or South)")).alias("issue_description"),
         
        # 3. DYNAMIC SEVERITY RATING
        F.when(F.col("sales_id").isNull(), F.lit("4"))    # Primary Key missing
         .when(F.col("car_id").isNull(), F.lit("4"))      # Join Key missing
         .when((F.col("original_selling_price") < 0) | F.col("original_selling_price").isNull(), F.lit("4")) # Bad financials
         .otherwise(F.lit("2")).alias("severity"),        # Bad Region
         
        F.to_json(F.struct(F.col("*"))).alias("affected_record"),
        F.current_timestamp().alias("detected_at")
    )