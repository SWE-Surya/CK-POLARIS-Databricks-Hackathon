import dlt
from pyspark.sql import functions as F

print("Initializing Enterprise Silver Layer: Policy Entity...")

# ==============================================================================
# 1. RULES FOR POLICY
# ==============================================================================
# A policy is invalid if it lacks its own ID, lacks join keys, or has negative financials
policy_rules = {
    "valid_policy_id": "policy_number IS NOT NULL",
    "valid_customer_id": "customer_id IS NOT NULL",
    "valid_car_id": "car_id IS NOT NULL",
    "valid_deductable": "policy_deductable >= 0 AND policy_deductable IS NOT NULL",
    "valid_premium": "policy_annual_premium >= 0 AND policy_annual_premium IS NOT NULL",
    "valid_umbrella": "umbrella_limit >= 0 AND umbrella_limit IS NOT NULL"
}

# Helper function to aggressively convert the text "null" into a true SQL Null
def force_true_null(col_name):
    trimmed = F.trim(F.col(col_name))
    return F.when(F.lower(trimmed).isin("null", "none", "", "nan"), F.lit(None)).otherwise(trimmed)

# ==============================================================================
# 2. THE TRANSFORMATION FUNCTION
# ==============================================================================
def transform_policy():
    raw_df = dlt.read_stream("primeinsurance.bronze.policy")
    
    clean_df = raw_df.select(
        # --- IDs (Strictly casting to string for clean joins later) ---
        force_true_null("policy_number").cast("string").alias("policy_number"),
        force_true_null("customer_id").cast("string").alias("customer_id"),
        force_true_null("car_id").cast("string").alias("car_id"),
        
        # --- DATES (String to physical Timestamp) ---
        F.to_timestamp(force_true_null("policy_bind_date")).alias("policy_bind_date"),
        
        # --- STRINGS (Trimming and upper standardizing) ---
        F.upper(force_true_null("policy_state")).cast("string").alias("policy_state"),
        force_true_null("policy_csl").cast("string").alias("policy_csl"),
        
        # --- FINANCIAL METRICS (Forcing to appropriate numerical types) ---
        force_true_null("policy_deductable").cast("int").alias("policy_deductable"),
        force_true_null("policy_annual_premium").cast("float").alias("policy_annual_premium"),
        force_true_null("umbrella_limit").cast("int").alias("umbrella_limit"),
        
        # --- LINEAGE ---
        F.col("_source_file")
    )
    
    return clean_df.dropDuplicates(["policy_number"])

# ==============================================================================
# 3. ROUTE GOOD DATA TO SILVER
# ==============================================================================
# 🏆 FIX: Removed the hardcoded 'primeinsurance.silver.' prefix
@dlt.table(
    name="primeinsurance.silver.policy_silver", 
    comment="Cleaned policy data aligned with strict data model and validated financials."
)
@dlt.expect_all_or_drop(policy_rules)
def policy_silver():
    return transform_policy()

# ==============================================================================
# 4. APPEND BAD DATA TO CENTRAL QUARANTINE LOG
# ==============================================================================
# 🏆 FIX: Changed target to "quarantine_table"
@dlt.append_flow(target="primeinsurance.silver.dq_issues", name="append_policy_dq_flow")
def append_policy_dq():
    df = transform_policy()
    
    quarantine_condition = " OR ".join([f"NOT ({rule})" for rule in policy_rules.values()])
    bad_df = df.filter(quarantine_condition)
    
    return bad_df.select(
        F.expr("uuid()").cast("string").alias("issue_id"),
        F.lit("policy").alias("table_name"),
        
        # 1. Identify the Column
        F.when(F.col("policy_number").isNull(), F.lit("policy_number"))
         .when(F.col("customer_id").isNull(), F.lit("customer_id"))
         .when(F.col("car_id").isNull(), F.lit("car_id"))
         .when((F.col("policy_deductable") < 0) | F.col("policy_deductable").isNull(), F.lit("policy_deductable"))
         .when((F.col("policy_annual_premium") < 0) | F.col("policy_annual_premium").isNull(), F.lit("policy_annual_premium"))
         .otherwise(F.lit("umbrella_limit")).alias("column_name"),
         
        # 2. Human-readable explanation
        F.when(F.col("policy_number").isNull(), F.lit("Missing or empty Policy ID"))
         .when(F.col("customer_id").isNull(), F.lit("Orphaned record: Missing customer_id link"))
         .when(F.col("car_id").isNull(), F.lit("Orphaned record: Missing car_id link"))
         .when((F.col("policy_deductable") < 0) | F.col("policy_deductable").isNull(), F.lit("Financial Error: Negative or missing policy deductible"))
         .when((F.col("policy_annual_premium") < 0) | F.col("policy_annual_premium").isNull(), F.lit("Financial Error: Negative or missing annual premium"))
         .otherwise(F.lit("Financial Error: Negative or missing umbrella limit")).alias("issue_description"),
         
        # 3. DYNAMIC SEVERITY RATING (Standardized to numeric levels)
        F.when(F.col("policy_number").isNull(), F.lit("4"))     # Primary Key missing = Critical (4)
         .when(F.col("customer_id").isNull(), F.lit("4"))       # Foreign Key missing = Critical (4)
         .when(F.col("car_id").isNull(), F.lit("4"))            # Foreign Key missing = Critical (4)
         .when((F.col("policy_deductable") < 0) | F.col("policy_deductable").isNull(), F.lit("3")) # Bad Finances = High (3)
         .when((F.col("policy_annual_premium") < 0) | F.col("policy_annual_premium").isNull(), F.lit("3"))
         .otherwise(F.lit("3")).alias("severity"),
         
        F.to_json(F.struct(F.col("*"))).alias("affected_record"),
        F.current_timestamp().alias("detected_at")
    )