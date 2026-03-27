import dlt
from pyspark.sql import functions as F

# ==============================================================================
# 1. CENTRAL DQ LOG INITIALIZATION
# ==============================================================================
# REMOVED hardcoded schema prefix to prevent internal routing errors
dlt.create_streaming_table(
    name="primeinsurance.silver.dq_issues",
    comment="Centralized Data Quality Error Log for all Silver Tables."
)

customer_rules = {
    "valid_customer_id": "CustomerID IS NOT NULL",
    "valid_region": "Region IN ('East', 'West', 'Central', 'South', 'North')",
    "valid_balance": "Balance >= 0",
    "valid_marital": "Marital IN ('single', 'married', 'divorced', 'na')",  
    "valid_education": "Education IN ('secondary', 'primary', 'na')"        
}

# ==============================================================================
# 2. THE STREAMING SOURCE (Cleans data and calculates the Survivorship Score)
# ==============================================================================
@dlt.view(name="customers_scored_source")
def customers_scored_source():
    raw_df = dlt.read_stream("primeinsurance.bronze.customers").replace({"null": None, "NULL": None, "": None})
    
    clean_df = raw_df.select(
        F.trim(F.coalesce(F.col("CustomerID"), F.col("Customer_ID"), F.col("cust_id"))).cast("string").alias("CustomerID"),
        F.initcap(F.trim(F.coalesce(F.col("Region"), F.col("Reg")))).cast("string").alias("Region"),
        F.initcap(F.trim(F.col("State"))).cast("string").alias("State"),
        F.initcap(F.trim(F.coalesce(F.col("City"), F.col("City_in_state")))).cast("string").alias("City"),
        F.lower(F.trim(F.col("Job"))).cast("string").alias("Job"),
        F.lower(F.trim(F.coalesce(F.col("Marital"), F.col("Marital_status")))).cast("string").alias("Marital"),
        F.lower(F.trim(F.coalesce(F.col("Education"), F.col("Edu")))).cast("string").alias("Education"),
        F.col("Default").cast("int").alias("Default"),
        F.col("Balance").cast("int").alias("Balance"),
        F.col("HHInsurance").cast("int").alias("HHInsurance"),
        F.col("CarLoan").cast("int").alias("CarLoan"),
        F.col("_source_file"),
        F.col("_loaded_at") 
    ).replace({"W": "West", "C": "Central", "E": "East", "S": "South", "N": "North"}, subset=["Region"])

    # 1. Calculate the completeness score
    scored_df = clean_df.withColumn(
        "completeness_score",
        F.when(F.col("Job").isNotNull() & (F.col("Job") != "na"), 1).otherwise(0) +
        F.when(F.col("Marital").isNotNull() & (F.col("Marital") != "na"), 1).otherwise(0) +
        F.when(F.col("Education").isNotNull() & (F.col("Education") != "na"), 1).otherwise(0)
    )
    
    # 2. THE TIE-BREAKER: Combine the score and the load timestamp into a single string
    return scored_df.withColumn(
        "cdc_sequence_key", 
        F.concat(F.col("completeness_score").cast("string"), F.lit("_"), F.col("_loaded_at").cast("string"))
    )

# ==============================================================================
# 3. ROUTE THE GOOD DATA (SMART DEDUPLICATION VIA CDC)
# ==============================================================================
dlt.create_streaming_table(
    name="primeinsurance.silver.customers_silver",
    comment="Clean data. Smart Survivorship deduplication applied with Tie-Breaker."
)

dlt.apply_changes(
    target="customers_silver",
    source="customers_scored_source",
    keys=["CustomerID"],
    sequence_by="cdc_sequence_key", 
    except_column_list=["completeness_score", "cdc_sequence_key"] # Hide both temp columns
)

# ==============================================================================
# 4. ROUTE THE BAD DATA TO DQ LOG
# ==============================================================================
@dlt.append_flow(target="dq_issues", name="append_customers_dq_flow")
def append_customers_dq():
    df = dlt.read_stream("customers_scored_source")
    
    dq_fail_condition = " OR ".join([f"NOT ({rule})" for rule in customer_rules.values()])
    null_trap = " OR Balance IS NULL OR Region IS NULL OR Marital IS NULL OR Education IS NULL"
    
    bad_df = df.filter(dq_fail_condition + null_trap)
    
    return bad_df.select(
        F.expr("uuid()").cast("string").alias("issue_id"),
        F.lit("customers").alias("table_name"),
        
        F.when(F.col("CustomerID").isNull(), F.lit("CustomerID"))
         .when((F.col("Balance") < 0) | F.col("Balance").isNull(), F.lit("Balance"))
         .when(~F.col("Marital").isin('single', 'married', 'divorced', 'na') | F.col("Marital").isNull(), F.lit("Marital"))
         .when(~F.col("Education").isin('secondary', 'primary', 'na') | F.col("Education").isNull(), F.lit("Education"))
         .otherwise(F.lit("Region")).alias("column_name"),
         
        F.when(F.col("CustomerID").isNull(), F.lit("Missing or empty Customer ID"))
         .when((F.col("Balance") < 0) | F.col("Balance").isNull(), F.lit("Negative or missing balance detected"))
         .when(~F.col("Marital").isin('single', 'married', 'divorced', 'na') | F.col("Marital").isNull(), F.lit("Invalid Marital status (Must be single, married, divorced, or na)"))
         .when(~F.col("Education").isin('secondary', 'primary', 'na') | F.col("Education").isNull(), F.lit("Invalid Education value (Must be secondary, primary, or na)"))
         .otherwise(F.lit("Invalid Region value (Must be East, West, Central, North or South)")).alias("issue_description"),
         
        F.when(F.col("CustomerID").isNull(), F.lit("4"))    
         .when((F.col("Balance") < 0) | F.col("Balance").isNull(), F.lit("3"))                
         .otherwise(F.lit("2")).alias("Severity"),            
         
        # Drop BOTH temp columns before converting to JSON so it matches the other tables
        F.to_json(F.struct(*[F.col(c) for c in df.columns if c not in ["completeness_score", "cdc_sequence_key"]])).alias("affected_record"),
        F.current_timestamp().alias("detected_at")
    )
