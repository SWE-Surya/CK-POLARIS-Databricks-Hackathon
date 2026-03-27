import dlt
from pyspark.sql import functions as F

print("Initializing Enterprise Silver Layer: Cars Entity...")

# ==============================================================================
# 1. RULES FOR CARS
# ==============================================================================
# A car record is only valid if it has its primary key.
car_rules = {
    "valid_car_id": "car_id IS NOT NULL",
    "valid_km": "km_driven >= 0 AND km_driven IS NOT NULL",       
    "valid_mileage": "mileage >= 0 AND mileage IS NOT NULL"    
}

# ==============================================================================
# 2. THE TRANSFORMATION FUNCTION
# ==============================================================================
def transform_cars():
    raw_df = dlt.read_stream("primeinsurance.bronze.cars").replace(
        {"null": None, "NULL": None, "": None}
    )
    
    clean_df = raw_df.select(
        # --- IDs ---
        F.col("car_id").cast("string").alias("car_id"),
        
        # --- STRINGS (Trimming & Standardizing Casing) ---
        F.trim(F.col("name")).cast("string").alias("name"),
        F.initcap(F.trim(F.col("fuel"))).cast("string").alias("fuel"),
        F.initcap(F.trim(F.col("transmission"))).cast("string").alias("transmission"),
        F.initcap(F.trim(F.col("model"))).cast("string").alias("model"),
        
        # Torque is incredibly messy and contains text, RPMs, and force. 
        # Safest approach is to leave it as a standardized string for analysts to parse.
        F.trim(F.col("torque")).cast("string").alias("torque"),
        
        # --- CLEAN NUMERICS ---
        F.col("km_driven").cast("int").alias("km_driven"),
        F.col("seats").cast("int").alias("seats"),
        
        # --- REGEX SURGERY (Extracting numbers from contaminated metrics) ---
        
        # Extracts float (e.g., "23.4 kmpl" -> "23.4" -> 23.4)
        F.regexp_extract(F.col("mileage"), r"([0-9.]+)", 1).cast("float").alias("mileage"),
        
        # Extracts integer (e.g., "1248 CC" -> "1248" -> 1248)
        F.regexp_extract(F.col("engine"), r"([0-9]+)", 1).cast("int").alias("engine"),
        
        # Extracts float (e.g., "103.52 bhp" -> "103.52" -> 103.52)
        F.regexp_extract(F.col("max_power"), r"([0-9.]+)", 1).cast("float").alias("max_power"),
        
        # --- LINEAGE ---
        F.col("_source_file")
    )
    
    return clean_df

# ==============================================================================
# 3. ROUTE GOOD DATA TO SILVER
# ==============================================================================
@dlt.table(
    name="primeinsurance.silver.cars_silver", 
    comment="Cleaned cars data with numerical metrics extracted from string units."
)
@dlt.expect_all_or_drop(car_rules)
def cars_silver():
    return transform_cars()

# ==============================================================================
# 4. APPEND BAD DATA TO CENTRAL DQ LOG
# ==============================================================================

@dlt.append_flow(target="dq_issues", name="append_cars_dq_flow")
def append_cars_dq():
    df = transform_cars()
    
    # Builds the trap based on our 3 new rules
    quarantine_condition = " OR ".join([f"NOT ({rule})" for rule in car_rules.values()])
    bad_df = df.filter(quarantine_condition)
    
    return bad_df.select(
        F.expr("uuid()").cast("string").alias("issue_id"),
        F.lit("cars").alias("table_name"),
        
        # 1. IDENTIFY THE EXACT COLUMN THAT FAILED
        F.when(F.col("car_id").isNull(), F.lit("car_id"))
         .when(F.col("km_driven") < 0, F.lit("km_driven"))
         .otherwise(F.lit("mileage")).alias("column_name"),
         
        # 2. CLEAR, HUMAN-READABLE ISSUE DESCRIPTION
        F.when(F.col("car_id").isNull(), F.lit("Missing or empty Car ID"))
         .when(F.col("km_driven") < 0, F.lit("Invalid metric: Negative KM Driven detected"))
         .otherwise(F.lit("Invalid metric: Negative Mileage detected")).alias("issue_description"),
         
        # 3. DYNAMIC SEVERITY RATING
        F.when(F.col("car_id").isNull(), F.lit("4"))         # Missing Primary Key is fatal
         .when(F.col("km_driven") < 0, F.lit("3"))           # Bad physical metrics ruin analytics
         .otherwise(F.lit("2")).alias("severity"),
         
        F.to_json(F.struct(F.col("*"))).alias("affected_record"),
        F.current_timestamp().alias("detected_at")
    )