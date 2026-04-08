import dlt
from pyspark.sql import functions as F

print("Initializing Enterprise Silver Layer: Claims Entity...")

# ==============================================================================
# 1. RULES FOR CLAIMS (Including Advanced Timeline Logic)
# ==============================================================================
claim_rules = {
    "valid_claim_id": "ClaimID IS NOT NULL",
    "valid_policy_id": "PolicyID IS NOT NULL",
    "valid_vehicle": "vehicle IS NOT NULL",
    "valid_incident_date": "incident_date > 0", 
    "injury can't be negative": "injury >= 0",
    "property can't be negative": "property >= 0",
    "vehicle can't be negative": "vehicle >= 0",
    "property_damage can't be null": "property_damage IS NOT NULL",
    "injury can't be null": "injury IS NOT NULL",
    "property can't be null": "property IS NOT NULL",
    "vehicle can't be null": "vehicle IS NOT NULL",
    # Timeline Logic: We define what makes the data GOOD by negating your BAD scenarios
    "valid_timeline_1": "NOT (incident_date > 0 AND Claim_Logged_On = 0 AND Claim_Processed_On > 0)",
    "valid_timeline_2": "NOT (incident_date > 0 AND Claim_Logged_On > 0 AND Claim_Logged_On > incident_date)",
    "valid_timeline_3": "NOT (incident_date > 0 AND Claim_Logged_On > 0 AND Claim_Logged_On < incident_date AND (Claim_Processed_On > Claim_Logged_On OR Claim_Processed_On > incident_date))"
}

# ==============================================================================
# 2. THE TRANSFORMATION FUNCTION
# ==============================================================================
def transform_claims():
    raw_df = dlt.read_stream("primeinsurance.bronze.claims").replace(
        {"null": None, "NULL": None, "": None, "?": None}
    )
    
    clean_df = raw_df.select(
        # --- IDs ---
        F.col("ClaimID").cast("string").alias("ClaimID"),
        F.col("PolicyID").cast("string").alias("PolicyID"),
        
        # --- DATES (Extracting the number before the colon, converting to INT, nulls = 0) ---
        F.when(F.col("Claim_Logged_On").isNull(), F.lit(0)) \
         .otherwise(F.split(F.col("Claim_Logged_On"), ":").getItem(0).cast("int")).alias("Claim_Logged_On"),
         
        F.when(F.col("Claim_Processed_On").isNull(), F.lit(0)) \
         .otherwise(F.split(F.col("Claim_Processed_On"), ":").getItem(0).cast("int")).alias("Claim_Processed_On"),
        
        # --- BOOLEAN 1 ---
        F.when(F.col("Claim_Rejected") == "Y", True).when(F.col("Claim_Rejected") == "N", False).alias("Claim_Rejected"),
        
        # --- MORE DATES (Extracting the number before the colon, converting to INT, nulls = 0) ---
        F.when(F.col("incident_date").isNull(), F.lit(0)) \
         .otherwise(F.split(F.col("incident_date"), ":").getItem(0).cast("int")).alias("incident_date"),
        
        # --- STRINGS ---
        F.upper(F.trim(F.col("incident_state"))).cast("string").alias("incident_state"),
        F.initcap(F.trim(F.col("incident_city"))).cast("string").alias("incident_city"),
        F.initcap(F.trim(F.col("incident_location"))).cast("string").alias("incident_location"),
        F.initcap(F.trim(F.col("incident_type"))).cast("string").alias("incident_type"),
        F.initcap(F.trim(F.col("collision_type"))).cast("string").alias("collision_type"),
        F.initcap(F.trim(F.col("incident_severity"))).cast("string").alias("incident_severity"),
        F.initcap(F.trim(F.col("authorities_contacted"))).cast("string").alias("authorities_contacted"),
        
        # --- BOOLEAN 2 ---
        F.when(F.col("police_report_available") == "YES", True).when(F.col("police_report_available") == "NO", False).alias("police_report_available"),
        
        # --- INTEGERS ---
        F.col("number_of_vehicles_involved").cast("int").alias("number_of_vehicles_involved"),
        F.col("bodily_injuries").cast("int").alias("bodily_injuries"),
        F.col("witnesses").cast("int").alias("witnesses"),
        F.col("injury").cast("float").alias("injury"),
        F.col("property").cast("float").alias("property"),
        F.col("vehicle").cast("float").alias("vehicle"),
        
        # --- BOOLEAN 3 ---
        F.when(F.col("property_damage") == "YES", True).when(F.col("property_damage") == "NO", False).alias("property_damage"),
        
        # --- LINEAGE ---
        F.col("_source_file")
    )
    
    # ==============================================================================
    # ENRICHMENT & BUCKET CALCULATIONS
    # ==============================================================================
    policy_df = dlt.read("primeinsurance.silver.policy_silver").select(
        F.col("policy_number").cast("string").alias("PolicyID"), 
        "policy_bind_date"
    )
    
    joined_df = clean_df.join(policy_df, on="PolicyID", how="left")

    final_df = joined_df.withColumn(
        "policy_bind_day", F.dayofmonth(F.col("policy_bind_date"))
    ).withColumn(
        "indicate_day_bucket", 
        F.col("policy_bind_day") + F.col("incident_date")
    ).withColumn(
        "Claim_Logged_On_day_bucket", 
        F.col("indicate_day_bucket") + F.col("Claim_Logged_On")
    ).withColumn(
        "Claim_Processed_On_day_bucket", 
        F.col("Claim_Logged_On_day_bucket") + F.col("Claim_Processed_On")
    ).drop("policy_bind_day")

    # Safe Deduplication via Primary Key
    return final_df.dropDuplicates(["ClaimID"])

# ==============================================================================
# 3. ROUTE GOOD DATA TO SILVER
# ==============================================================================
@dlt.table(
    name="primeinsurance.silver.claims_silver", 
    comment="Cleaned claims data with cross-column timeline validation."
)
@dlt.expect_all_or_drop(claim_rules)
def claims_silver():
    return transform_claims()


# ==============================================================================
# 4. APPEND BAD DATA TO CENTRAL DQ LOG
# ==============================================================================
@dlt.append_flow(target="primeinsurance.silver.dq_issues", name="append_claims_dq_flow")
def append_claims_dq():
    df = transform_claims()
    
    quarantine_condition = " OR ".join([f"NOT ({rule})" for rule in claim_rules.values()])
    bad_df = df.filter(quarantine_condition)
    
    # We define the complex logical variables here so the F.when block stays clean and readable
    cond_timeline_1 = (F.col("incident_date") > 0) & (F.col("Claim_Logged_On") == 0) & (F.col("Claim_Processed_On") > 0)
    cond_timeline_2 = (F.col("incident_date") > 0) & (F.col("Claim_Logged_On") > 0) & (F.col("Claim_Logged_On") > F.col("incident_date"))
    cond_timeline_3 = (F.col("incident_date") > 0) & (F.col("Claim_Logged_On") > 0) & (F.col("Claim_Logged_On") < F.col("incident_date")) & ((F.col("Claim_Processed_On") > F.col("Claim_Logged_On")) | (F.col("Claim_Processed_On") > F.col("incident_date")))

    return bad_df.select(
        F.expr("uuid()").cast("string").alias("issue_id"),
        F.lit("claims").alias("table_name"),
        
        # 1. IDENTIFY THE EXACT COLUMN THAT BROKE THE RULE
        F.when(F.col("ClaimID").isNull(), F.lit("ClaimID"))
         .when(F.col("PolicyID").isNull(), F.lit("PolicyID"))
         .when(F.col("incident_date") == 0, F.lit("incident_date"))
         .when(cond_timeline_1, F.lit("Claim_Logged_On"))
         .when(cond_timeline_2, F.lit("Claim_Logged_On"))
         .when(cond_timeline_3, F.lit("Claim_Processed_On"))
         # --- THE MISSING FINANCIAL/DAMAGE COLUMNS MAPPED ---
         .when(F.col("injury").isNull() | (F.col("injury") < 0), F.lit("injury"))
         .when(F.col("property").isNull() | (F.col("property") < 0), F.lit("property"))
         .when(F.col("vehicle").isNull() | (F.col("vehicle") < 0), F.lit("vehicle"))
         .when(F.col("property_damage").isNull(), F.lit("property_damage"))
         .otherwise(F.lit("Multiple/Complex")).alias("column_name"),
         
        # 2. CLEAR, HUMAN-READABLE ISSUE DESCRIPTIONS
        F.when(F.col("ClaimID").isNull(), F.lit("Missing or empty Claim ID"))
         .when(F.col("PolicyID").isNull(), F.lit("Orphaned record: Missing Policy ID"))
         .when(F.col("incident_date") == 0, F.lit("Invalid metric: Incident date is 00:00.0 or missing"))
         .when(cond_timeline_1, F.lit("Timeline Logic Error: Claim was processed but never logged"))
         .when(cond_timeline_2, F.lit("Timeline Logic Error: Claim was logged after the incident date"))
         .when(cond_timeline_3, F.lit("Timeline Logic Error: Processed date sequence is invalid against Logged/Incident date"))
         # --- THE MISSING DESCRIPTIONS ---
         .when(F.col("injury").isNull() | (F.col("injury") < 0), F.lit("Financial Error: Injury cost is missing or negative"))
         .when(F.col("property").isNull() | (F.col("property") < 0), F.lit("Financial Error: Property cost is missing or negative"))
         .when(F.col("vehicle").isNull() | (F.col("vehicle") < 0), F.lit("Financial Error: Vehicle cost is missing or negative"))
         .when(F.col("property_damage").isNull(), F.lit("Missing Data: Property damage flag is null"))
         .otherwise(F.lit("Data Quality Rule Violation")).alias("issue_description"),
         
        # 3. DYNAMIC SEVERITY RATING 
        F.when(F.col("ClaimID").isNull(), F.lit("4"))    
         .when(F.col("PolicyID").isNull(), F.lit("3"))
         # Financial errors are highly critical for insurance
         .when((F.col("injury") < 0) | (F.col("property") < 0) | (F.col("vehicle") < 0), F.lit("3"))
         .otherwise(F.lit("2")).alias("severity"),            
         
        F.to_json(F.struct(F.col("*"))).alias("affected_record"),
        F.current_timestamp().alias("detected_at")
    )