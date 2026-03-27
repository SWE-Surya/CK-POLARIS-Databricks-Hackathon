# Databricks notebook source
# DBTITLE 1,Configuration Settings for Claim Anomaly Detection Mode
# Name of the source table containing insurance claims data
SOURCE_TABLE = "primeinsurance.silver.claims_silver"
# Name of the target table to store processed claim anomaly explanations
TARGET_TABLE = "primeinsurance.gold.claim_anomaly_explanations"
# Model name for OpenAI LLM used to generate investigation briefs
MODEL_NAME = "databricks-gpt-oss-20b"

# Maximum number of input rows to process (None means all rows)
MAX_INPUT_ROWS = None
# Minimum anomaly score required to flag a claim for review
FLAG_THRESHOLD = 45
# Threshold for assigning 'HIGH' priority tier based on anomaly score
HIGH_THRESHOLD = 70
# Threshold for assigning 'MEDIUM' priority tier based on anomaly score
MEDIUM_THRESHOLD = 45
# Threshold for assigning 'LOW' priority tier based on anomaly score
LOW_THRESHOLD = 25

# Temperature setting for LLM generation (controls randomness)
TEMPERATURE = 0.1
# Maximum number of tokens for LLM output
MAX_TOKENS = 800

# COMMAND ----------

# DBTITLE 1,Validate Source Table and Preview Initial Data Snapshot
# Check if the source table exists in the Spark catalog
if not spark.catalog.tableExists(SOURCE_TABLE):
    raise ValueError(f"Required source table not found: {SOURCE_TABLE}")

# Load the source table as a Spark DataFrame
claims_df = spark.table(SOURCE_TABLE)

# Print confirmation that the source table was found
print("Source table found:", SOURCE_TABLE)
# Print the number of rows in the DataFrame
print("Row count:", claims_df.count())
# Print the schema of the DataFrame
claims_df.printSchema()
# Display the first 10 rows of the DataFrame
display(claims_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Import PySpark Functions Window and JSON Modules
# Import Spark SQL functions for DataFrame transformations
from pyspark.sql import functions as F
# Import Window specification for windowed operations in Spark DataFrames
from pyspark.sql import Window
# Import data types for defining Spark DataFrame schemas
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType
# Import json module for handling JSON serialization and parsing
import json

# COMMAND ----------

# DBTITLE 1,Column Name Definitions for Insurance Claim Attributes
# Column name for unique claim identifier
claim_id_col = "ClaimID"
# Column name for unique policy identifier
policy_id_col = "PolicyID"
# Column name for raw claim logged date
claim_logged_raw_col = "Claim_Logged_On"
# Column name for raw claim processed date
claim_processed_raw_col = "Claim_Processed_On"
# Column name for claim rejection status
claim_rejected_col = "Claim_Rejected"
# Column name for raw incident date
incident_raw_col = "incident_date"

# Column name for incident state
incident_state_col = "incident_state"
# Column name for incident city
incident_city_col = "incident_city"
# Column name for incident location description
incident_location_col = "incident_location"
# Column name for incident type
incident_type_col = "incident_type"
# Column name for collision type
collision_type_col = "collision_type"
# Column name for incident severity
severity_col = "incident_severity"
# Column name for authorities contacted
authorities_col = "authorities_contacted"
# Column name for police report availability
police_report_col = "police_report_available"
# Column name for number of vehicles involved
vehicles_involved_col = "number_of_vehicles_involved"
# Column name for number of bodily injuries
bodily_injuries_col = "bodily_injuries"
# Column name for number of witnesses
witnesses_col = "witnesses"

# Column name for injury amount
injury_amount_col = "injury"
# Column name for property damage amount
property_amount_col = "property"
# Column name for vehicle damage amount
vehicle_amount_col = "vehicle"

# Column name for property damage flag
property_damage_col = "property_damage"
# Column name for source file identifier
source_file_col = "_source_file"
# Column name for policy bind date
policy_bind_date_col = "policy_bind_date"

# Column name for incident day bucket (normalized day)
incident_bucket_col = "indicate_day_bucket"
# Column name for claim logged day bucket (normalized day)
logged_bucket_col = "Claim_Logged_On_day_bucket"
# Column name for claim processed day bucket (normalized day)
processed_bucket_col = "Claim_Processed_On_day_bucket"

# COMMAND ----------

# DBTITLE 1,Transform and Normalize Insurance Claim Data Columns
# Start with the claims DataFrame loaded from the source table
work_df = claims_df
# If a maximum input row limit is set, apply it to restrict the number of rows processed
if MAX_INPUT_ROWS is not None:
    work_df = work_df.limit(MAX_INPUT_ROWS)

# Assign the working DataFrame to 'df' for further processing
df = work_df

# Define a helper function to normalize text columns (trim, uppercase, cast to string)
def norm_text(c):
    return F.upper(F.trim(F.col(c).cast("string")))

# Apply normalization and type casting to relevant columns for downstream rule logic
df = (
    df
    # Convert claim logged date bucket to integer
    .withColumn("claim_logged_on_day", F.col(logged_bucket_col).cast("int"))
    # Convert claim processed date bucket to integer
    .withColumn("claim_processed_on_day", F.col(processed_bucket_col).cast("int"))
    # Convert incident date bucket to integer
    .withColumn("incident_date_day", F.col(incident_bucket_col).cast("int"))
    # Normalize incident severity text
    .withColumn("severity_norm", norm_text(severity_col))
    # Normalize incident type text
    .withColumn("incident_type_norm", norm_text(incident_type_col))
    # Normalize collision type text
    .withColumn("collision_type_norm", norm_text(collision_type_col))
    # Normalize authorities contacted text
    .withColumn("authorities_contacted_norm", norm_text(authorities_col))
    # Normalize police report availability text
    .withColumn("police_report_available_norm", norm_text(police_report_col))
    # Normalize claim rejected status text
    .withColumn("claim_rejected_norm", norm_text(claim_rejected_col))
    # Cast number of vehicles involved to integer
    .withColumn("number_of_vehicles_involved_num", F.col(vehicles_involved_col).cast("int"))
    # Cast number of bodily injuries to integer
    .withColumn("bodily_injuries_num", F.col(bodily_injuries_col).cast("int"))
    # Cast number of witnesses to integer
    .withColumn("witnesses_num", F.col(witnesses_col).cast("int"))
    # Cast injury amount to double
    .withColumn("injury_amount", F.col(injury_amount_col).cast("double"))
    # Cast property damage amount to double
    .withColumn("property_amount", F.col(property_amount_col).cast("double"))
    # Cast vehicle damage amount to double
    .withColumn("vehicle_amount", F.col(vehicle_amount_col).cast("double"))
    # Normalize property damage flag text
    .withColumn("property_damage_norm", norm_text(property_damage_col))
)

# COMMAND ----------

# DBTITLE 1,Calculate Claim Metrics and Flag Report Discrepancies
df = (
    df
    # Calculate days between incident and claim logged date
    .withColumn("report_delay_days", F.col("claim_logged_on_day") - F.col("incident_date_day"))
    # Calculate days between claim logged and claim processed date
    .withColumn("processing_days", F.col("claim_processed_on_day") - F.col("claim_logged_on_day"))
    # Calculate days between incident and claim processed date (claim lifecycle)
    .withColumn("claim_lifecycle_days", F.col("claim_processed_on_day") - F.col("incident_date_day"))
    # Count number of claims per policy (windowed aggregation)
    .withColumn("policy_claim_count", F.count("*").over(Window.partitionBy(F.col(policy_id_col))))
    # Sum injury, property, and vehicle amounts to get total loss amount
    .withColumn(
        "total_loss_amount",
        F.coalesce(F.col("injury_amount"), F.lit(0.0)) +
        F.coalesce(F.col("property_amount"), F.lit(0.0)) +
        F.coalesce(F.col("vehicle_amount"), F.lit(0.0))
    )
    # Calculate complexity points based on vehicles, injuries, witnesses, and high amounts
    .withColumn(
        "complexity_points",
        F.coalesce(F.col("number_of_vehicles_involved_num"), F.lit(0)) +
        F.coalesce(F.col("bodily_injuries_num"), F.lit(0)) +
        F.when(F.coalesce(F.col("witnesses_num"), F.lit(0)) > 0, F.lit(1)).otherwise(F.lit(0)) +
        F.when(F.coalesce(F.col("injury_amount"), F.lit(0.0)) > 5000, F.lit(1)).otherwise(F.lit(0)) +
        F.when(F.coalesce(F.col("property_amount"), F.lit(0.0)) > 2500, F.lit(1)).otherwise(F.lit(0)) +
        F.when(F.col("property_damage_norm").isin("TRUE", "YES", "Y", "1"), F.lit(1)).otherwise(F.lit(0))
    )
    # Flag mismatch if authorities contacted but no police report available
    .withColumn(
        "authority_report_mismatch",
        F.when(
            (F.col("authorities_contacted_norm").isin("POLICE", "AMBULANCE", "FIRE", "OTHER")) &
            (~F.col("police_report_available_norm").isin("TRUE", "YES", "Y", "1")),
            1
        ).otherwise(0)
    )
    # Flag if high severity but no police report available
    .withColumn(
        "high_severity_no_police_report",
        F.when(
            F.col("severity_norm").isin("MAJOR DAMAGE", "SEVERE", "HIGH", "TOTAL LOSS") &
            (~F.col("police_report_available_norm").isin("TRUE", "YES", "Y", "1")),
            1
        ).otherwise(0)
    )
)

# COMMAND ----------

# DBTITLE 1,Map Severity Labels to Numeric Ranks in Dataframe
# Define a mapping from normalized severity text to integer rank values
severity_map = {
    "TRIVIAL DAMAGE": 1,
    "MINOR DAMAGE": 1,
    "LOW": 1,
    "MODERATE DAMAGE": 2,
    "MEDIUM": 2,
    "MAJOR DAMAGE": 3,
    "HIGH": 3,
    "SEVERE": 4,
    "TOTAL LOSS": 4,
    "CRITICAL": 4
}

# Create a Spark map expression for severity text to rank mapping
mapping_expr = F.create_map([F.lit(x) for kv in severity_map.items() for x in kv])

# Add a column 'severity_rank' to the DataFrame using the severity mapping
df = df.withColumn("severity_rank", mapping_expr.getItem(F.col("severity_norm")).cast("int"))

# COMMAND ----------

# DBTITLE 1,Derive Binary Flags for Claim Anomaly Rule Conditions
# Flag claims with delayed reporting (>30 days) or suspicious incident timing (incident day <= 3)
df = df.withColumn(
    "rule_timing_anomaly",
    F.when((F.col("report_delay_days") > 30) | (F.col("incident_date_day") <= 3), 1).otherwise(0)
)

# Flag claims with processing delays longer than 21 days
df = df.withColumn(
    "rule_processing_delay",
    F.when(F.col("processing_days") > 21, 1).otherwise(0)
)

# Flag claims with high complexity: many vehicles, injuries, or overall complexity points
df = df.withColumn(
    "rule_high_complexity",
    F.when(
        (F.col("complexity_points") >= 5) |
        (F.col("number_of_vehicles_involved_num") >= 3) |
        (F.col("bodily_injuries_num") >= 2),
        1
    ).otherwise(0)
)

# Flag claims with documentation mismatches: authorities contacted but no police report, or high severity with no police report
df = df.withColumn(
    "rule_documentation_mismatch",
    F.when(
        (F.col("authority_report_mismatch") == 1) |
        (F.col("high_severity_no_police_report") == 1),
        1
    ).otherwise(0)
)

# Flag claims that were rejected
df = df.withColumn(
    "rule_rejection_pattern",
    F.when(F.col("claim_rejected_norm").isin("TRUE", "YES", "Y", "1"), 1).otherwise(0)
)

# Flag claims where severity label mismatches the facts: low severity with high loss/complexity, or high severity with low loss/simple facts
df = df.withColumn(
    "rule_severity_mismatch",
    F.when(
        (
            F.col("severity_rank").isin(1, 2) &
            (
                (F.col("number_of_vehicles_involved_num") >= 3) |
                (F.col("bodily_injuries_num") >= 1) |
                (F.coalesce(F.col("injury_amount"), F.lit(0.0)) > 5000) |
                (F.coalesce(F.col("property_amount"), F.lit(0.0)) > 3000) |
                (F.coalesce(F.col("vehicle_amount"), F.lit(0.0)) > 10000)
            )
        ) |
        (
            F.col("severity_rank").isin(3, 4) &
            (F.coalesce(F.col("number_of_vehicles_involved_num"), F.lit(0)) <= 1) &
            (F.coalesce(F.col("bodily_injuries_num"), F.lit(0)) == 0) &
            (F.coalesce(F.col("injury_amount"), F.lit(0.0)) < 1000) &
            (F.coalesce(F.col("property_amount"), F.lit(0.0)) < 500) &
            (F.coalesce(F.col("vehicle_amount"), F.lit(0.0)) < 2500)
        ),
        1
    ).otherwise(0)
)

# COMMAND ----------

# DBTITLE 1,Compute Anomaly Scores and Assign Priority Tiers
# Define weights for each anomaly rule used in scoring
weights = {
    "rule_timing_anomaly": 20,
    "rule_processing_delay": 15,
    "rule_high_complexity": 20,
    "rule_documentation_mismatch": 15,
    "rule_rejection_pattern": 10,
    "rule_severity_mismatch": 20
}

# Calculate the maximum possible anomaly points (sum of all rule weights)
max_possible_weight = sum(weights.values())

# Initialize the score expression to zero
score_expr = F.lit(0)
# Add weighted points for each rule if triggered (rule column value is 1)
for rule_col, weight in weights.items():
    score_expr = score_expr + (F.col(rule_col) * F.lit(weight))

# Add a column for the raw anomaly points (sum of triggered rule weights)
df = df.withColumn("raw_anomaly_points", score_expr)
# Normalize the raw anomaly points to a percentage score (0-100)
df = df.withColumn("anomaly_score", F.round((F.col("raw_anomaly_points") / F.lit(max_possible_weight)) * F.lit(100), 2))

# Assign a priority tier based on the anomaly score thresholds
df = df.withColumn(
    "priority_tier",
    F.when(F.col("anomaly_score") >= HIGH_THRESHOLD, F.lit("HIGH"))
     .when(F.col("anomaly_score") >= MEDIUM_THRESHOLD, F.lit("MEDIUM"))
     .when(F.col("anomaly_score") >= LOW_THRESHOLD, F.lit("LOW"))
     .otherwise(F.lit("NOT_FLAGGED"))
)

# COMMAND ----------

# DBTITLE 1,Identify and Summarize Claims Triggering Anomaly Rules
# Create a column listing triggered anomaly rule descriptions for each claim
df = df.withColumn(
    "triggered_rules_raw",
    F.array(
        F.when(F.col("rule_timing_anomaly") == 1, F.lit("Delayed reporting or suspicious incident timing")),  # Add description if timing anomaly rule triggered
        F.when(F.col("rule_processing_delay") == 1, F.lit("Delayed claim processing")),                      # Add description if processing delay rule triggered
        F.when(F.col("rule_high_complexity") == 1, F.lit("High complexity accident pattern")),               # Add description if high complexity rule triggered
        F.when(F.col("rule_documentation_mismatch") == 1, F.lit("Police or authority documentation mismatch")), # Add description if documentation mismatch rule triggered
        F.when(F.col("rule_rejection_pattern") == 1, F.lit("Claim rejection pattern")),                     # Add description if rejection pattern rule triggered
        F.when(F.col("rule_severity_mismatch") == 1, F.lit("Severity mismatch versus incident facts and amount exposure")) # Add description if severity mismatch rule triggered
    )
)

# Filter out nulls from triggered_rules_raw to get only triggered rule descriptions
df = df.withColumn(
    "triggered_rules",
    F.expr("filter(triggered_rules_raw, x -> x is not null)")
).drop("triggered_rules_raw")  # Drop intermediate column

# Select claims flagged for review based on anomaly score threshold
flagged_df = df

# Count total number of claims processed
total_claims = df.count()
# Count number of flagged claims
flagged_claims = flagged_df.count()

# Print summary statistics for claim flagging
print("Total claims:", total_claims)
print("Flagged claims:", flagged_claims)
print("Flagged percentage:", round((flagged_claims / total_claims * 100) if total_claims else 0, 2))

# Display flagged claims with relevant columns, ordered by anomaly score descending
display(
    flagged_df.select(
        F.col(claim_id_col).alias("claim_id"),
        F.col(policy_id_col).alias("policy_id"),
        F.col(severity_col).alias("incident_severity"),
        F.col(incident_type_col).alias("incident_type"),
        "incident_date_day",
        "claim_logged_on_day",
        "claim_processed_on_day",
        "report_delay_days",
        "processing_days",
        "total_loss_amount",
        "anomaly_score",
        "priority_tier",
        "triggered_rules"
    ).orderBy(F.desc("anomaly_score"))
)

# COMMAND ----------

# DBTITLE 1,Initialize OpenAI Client and Define Databricks Response
# Import OpenAI client for interacting with LLM endpoints
from openai import OpenAI
# Import datetime and timezone for timestamp generation
from datetime import datetime, timezone

# Retrieve Databricks API token for authentication
DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
# Retrieve Databricks workspace URL from Spark configuration
WORKSPACE_URL = spark.conf.get("spark.databricks.workspaceUrl")

# Initialize OpenAI client with Databricks token and workspace endpoint URL
client = OpenAI(
    api_key=DATABRICKS_TOKEN,
    base_url=f"https://{WORKSPACE_URL}/serving-endpoints"
)

# Function to extract text content from Databricks LLM response
def extract_text_from_databricks_response(response):
    try:
        # Get the content from the first choice in the response
        content = response.choices[0].message.content
        # If content is a string, attempt to parse as JSON
        if isinstance(content, str):
            parsed = json.loads(content)
        else:
            parsed = content
        # If parsed content is a list, search for a dict block with type 'text'
        if isinstance(parsed, list):
            for block in parsed:
                if isinstance(block, dict) and block.get("type") == "text":
                    # Return the text value, stripped of whitespace
                    return (block.get("text") or "").strip()
        # Return empty string if no text block found
        return ""
    except Exception as e:
        # Return error string if parsing fails
        return f"PARSE_ERROR: {str(e)}"

# COMMAND ----------

# DBTITLE 1,Construct Investigation Brief Prompts for Flagged Claim
# List of columns to include in the investigation brief payload
brief_fields = [
    claim_id_col, policy_id_col, claim_logged_raw_col, claim_processed_raw_col, claim_rejected_col,
    incident_raw_col, incident_state_col, incident_city_col, incident_location_col, incident_type_col,
    collision_type_col, severity_col, authorities_col, police_report_col, vehicles_involved_col,
    bodily_injuries_col, witnesses_col, injury_amount_col, property_amount_col, vehicle_amount_col,
    property_damage_col, source_file_col, policy_bind_date_col, incident_bucket_col,
    logged_bucket_col, processed_bucket_col
]

# Helper function to normalize values for JSON serialization
def normalize_value(value):
    if value is None:
        return None  # Return None for missing values
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()  # Convert datetime to ISO string
        except Exception:
            return str(value)  # Fallback to string if conversion fails
    return value  # Return value as-is for other types

# Function to build system and user prompts for LLM investigation brief generation
def build_brief_messages(row_dict):
    # Extract base claim fields and normalize values for the brief
    base_payload = {c: normalize_value(row_dict.get(c)) for c in brief_fields}
    # Extract derived metrics and anomaly evidence for the brief
    derived_payload = {
        "anomaly_score": row_dict.get("anomaly_score"),
        "priority_tier": row_dict.get("priority_tier"),
        "triggered_rules": row_dict.get("triggered_rules"),
        "derived_incident_date_day": row_dict.get("incident_date_day"),
        "derived_claim_logged_on_day": row_dict.get("claim_logged_on_day"),
        "derived_claim_processed_on_day": row_dict.get("claim_processed_on_day"),
        "report_delay_days": row_dict.get("report_delay_days"),
        "processing_days": row_dict.get("processing_days"),
        "claim_lifecycle_days": row_dict.get("claim_lifecycle_days"),
        "policy_claim_count": row_dict.get("policy_claim_count"),
        "complexity_points": row_dict.get("complexity_points"),
        "total_loss_amount": row_dict.get("total_loss_amount"),
        "authority_report_mismatch": row_dict.get("authority_report_mismatch"),
        "high_severity_no_police_report": row_dict.get("high_severity_no_police_report")
    }

    # System prompt for LLM: instructs assistant on investigation brief requirements
    system_prompt = '''
You are an insurance claims investigation assistant.
The claim has already been flagged by structured anomaly rules.
You are not deciding whether fraud occurred.
You are explaining why the claim warrants review.

Rules:
- Use only the provided facts
- Be specific and reference actual data points
- Do not accuse the claimant of fraud
- Avoid generic statements
- Return exactly these labeled sections:

What makes this claim suspicious:
Which risk factors are present:
What the investigator should do next:
'''.strip()

    # User prompt for LLM: provides claim details and anomaly evidence for brief generation
    user_prompt = f'''
Generate an investigation brief for this flagged claim.

Claim details:
{json.dumps(base_payload)}


Anomaly evidence:
{json.dumps(base_payload)}

Use the triggered rules and metrics to explain why the claim should be reviewed.
'''.strip()

    # Return both prompts for LLM completion
    return system_prompt, user_prompt

# COMMAND ----------

# DBTITLE 1,Generate AI Investigation Brief with OpenAI Chat Completion
import time  # Import time module for sleep and delay handling

def generate_investigation_brief(row_dict):
    # Build system and user prompts for LLM based on claim row dictionary
    system_prompt, user_prompt = build_brief_messages(row_dict)

    retries = 3  # Number of retry attempts for LLM call
    delay = 2    # Initial delay in seconds for retry (exponential backoff)

    # Attempt LLM call up to 'retries' times
    for attempt in range(retries):
        try:
            # Call Databricks LLM endpoint to generate investigation brief
            response = client.chat.completions.create(
                model=MODEL_NAME,  # Specify LLM model name
                messages=[
                    {"role": "system", "content": system_prompt},  # System prompt for LLM
                    {"role": "user", "content": user_prompt}       # User prompt with claim details
                ],
                temperature=TEMPERATURE,  # LLM temperature setting
                max_tokens=MAX_TOKENS     # Maximum tokens for LLM output
            )

            # Extract text content from LLM response
            text = extract_text_from_databricks_response(response)

            # Return dictionary with brief, timestamp, model, and success status
            return {
                "ai_investigation_brief": text,  # AI-generated brief text
                "brief_generated_at": datetime.now(timezone.utc).isoformat(),  # UTC timestamp
                "model_name": MODEL_NAME,        # Model name used
                "generation_status": "SUCCESS"   # Status of generation
            }

        except Exception as e:
            # Handle rate limit errors (HTTP 429) with exponential backoff
            if "429" in str(e):
                print(f"Rate limit hit. Retrying in {delay}s...")  # Print retry message
                time.sleep(delay)  # Sleep for current delay
                delay *= 2        # Double delay for next retry
            else:
                # Return error details for other exceptions
                return {
                    "ai_investigation_brief": f"ERROR: {str(e)}",  # Error message
                    "brief_generated_at": datetime.now(timezone.utc).isoformat(),  # UTC timestamp
                    "model_name": MODEL_NAME,        # Model name used
                    "generation_status": "FAILED"    # Status of generation
                }

    # Return failure status after all retries exhausted
    return {
        "ai_investigation_brief": "FAILED AFTER RETRIES",  # Failure message
        "brief_generated_at": datetime.now(timezone.utc).isoformat(),  # UTC timestamp
        "model_name": MODEL_NAME,        # Model name used
        "generation_status": "FAILED"    # Status of generation
    }

# COMMAND ----------

# DBTITLE 1,Generate Investigation Briefs for Flagged Insurance Claim
import time  # Import time module for sleep functionality

# Convert flagged claims DataFrame rows to dictionaries for processing
flagged_rows = [r.asDict(recursive=True) for r in flagged_df.toLocalIterator()]
print("Flagged rows queued for brief generation:", len(flagged_rows))  # Print number of flagged rows

output_rows = []  # Initialize list to store output records

# Iterate over each flagged claim row dictionary
for idx, row_dict in enumerate(flagged_rows, start=1):
    # Generate AI investigation brief for the claim
    llm_result = generate_investigation_brief(row_dict)

    # Build output record dictionary with normalized and type-cast values
    output_record = {
        "claim_id": None if row_dict.get(claim_id_col) is None else str(row_dict.get(claim_id_col)),  # Unique claim ID
        "policy_id": None if row_dict.get(policy_id_col) is None else str(row_dict.get(policy_id_col)),  # Policy ID
        "claim_logged_on_raw": None if row_dict.get(claim_logged_raw_col) is None else str(row_dict.get(claim_logged_raw_col)),  # Raw claim logged date
        "claim_processed_on_raw": None if row_dict.get(claim_processed_raw_col) is None else str(row_dict.get(claim_processed_raw_col)),  # Raw claim processed date
        "incident_date_raw": None if row_dict.get(incident_raw_col) is None else str(row_dict.get(incident_raw_col)),  # Raw incident date
        "claim_logged_on_day_bucket": int(row_dict.get("claim_logged_on_day")) if row_dict.get("claim_logged_on_day") is not None else None,  # Normalized claim logged day
        "claim_processed_on_day_bucket": int(row_dict.get("claim_processed_on_day")) if row_dict.get("claim_processed_on_day") is not None else None,  # Normalized claim processed day
        "incident_date_day_bucket": int(row_dict.get("incident_date_day")) if row_dict.get("incident_date_day") is not None else None,  # Normalized incident day
        "claim_rejected": None if row_dict.get(claim_rejected_col) is None else str(row_dict.get(claim_rejected_col)),  # Claim rejection status
        "incident_state": None if row_dict.get(incident_state_col) is None else str(row_dict.get(incident_state_col)),  # Incident state
        "incident_city": None if row_dict.get(incident_city_col) is None else str(row_dict.get(incident_city_col)),  # Incident city
        "incident_location": None if row_dict.get(incident_location_col) is None else str(row_dict.get(incident_location_col)),  # Incident location description
        "incident_type": None if row_dict.get(incident_type_col) is None else str(row_dict.get(incident_type_col)),  # Incident type
        "collision_type": None if row_dict.get(collision_type_col) is None else str(row_dict.get(collision_type_col)),  # Collision type
        "incident_severity": None if row_dict.get(severity_col) is None else str(row_dict.get(severity_col)),  # Incident severity
        "authorities_contacted": None if row_dict.get(authorities_col) is None else str(row_dict.get(authorities_col)),  # Authorities contacted
        "police_report_available": None if row_dict.get(police_report_col) is None else str(row_dict.get(police_report_col)),  # Police report availability
        "number_of_vehicles_involved": int(row_dict.get("number_of_vehicles_involved_num")) if row_dict.get("number_of_vehicles_involved_num") is not None else None,  # Number of vehicles involved
        "bodily_injuries": int(row_dict.get("bodily_injuries_num")) if row_dict.get("bodily_injuries_num") is not None else None,  # Number of bodily injuries
        "witnesses": int(row_dict.get("witnesses_num")) if row_dict.get("witnesses_num") is not None else None,  # Number of witnesses
        "injury_amount": float(row_dict.get("injury_amount")) if row_dict.get("injury_amount") is not None else None,  # Injury amount
        "property_amount": float(row_dict.get("property_amount")) if row_dict.get("property_amount") is not None else None,  # Property damage amount
        "vehicle_amount": float(row_dict.get("vehicle_amount")) if row_dict.get("vehicle_amount") is not None else None,  # Vehicle damage amount
        "total_loss_amount": float(row_dict.get("total_loss_amount")) if row_dict.get("total_loss_amount") is not None else None,  # Total loss amount
        "property_damage": None if row_dict.get(property_damage_col) is None else str(row_dict.get(property_damage_col)),  # Property damage flag
        "policy_bind_date": None if row_dict.get(policy_bind_date_col) is None else str(row_dict.get(policy_bind_date_col)),  # Policy bind date
        "anomaly_score": float(row_dict.get("anomaly_score")) if row_dict.get("anomaly_score") is not None else None,  # Anomaly score
        "priority_tier": None if row_dict.get("priority_tier") is None else str(row_dict.get("priority_tier")),  # Priority tier
        "triggered_rules": row_dict.get("triggered_rules") or [],  # List of triggered anomaly rules
        "report_delay_days": int(row_dict.get("report_delay_days")) if row_dict.get("report_delay_days") is not None else None,  # Days between incident and claim logged
        "processing_days": int(row_dict.get("processing_days")) if row_dict.get("processing_days") is not None else None,  # Days between claim logged and processed
        "claim_lifecycle_days": int(row_dict.get("claim_lifecycle_days")) if row_dict.get("claim_lifecycle_days") is not None else None,  # Days between incident and claim processed
        "policy_claim_count": int(row_dict.get("policy_claim_count")) if row_dict.get("policy_claim_count") is not None else None,  # Number of claims for policy
        "complexity_points": int(row_dict.get("complexity_points")) if row_dict.get("complexity_points") is not None else None,  # Complexity points
        "authority_report_mismatch": int(row_dict.get("authority_report_mismatch")) if row_dict.get("authority_report_mismatch") is not None else None,  # Authority report mismatch flag
        "high_severity_no_police_report": int(row_dict.get("high_severity_no_police_report")) if row_dict.get("high_severity_no_police_report") is not None else None,  # High severity no police report flag
    }
    output_record.update(llm_result)  # Add AI brief and metadata to output record
    output_rows.append(output_record)  # Append output record to list

    time.sleep(0.7)  # Sleep to avoid rate limiting on LLM calls

    if idx % 25 == 0:
        print(f"Generated briefs for {idx} claims")  # Print progress every 25 claims

print("Output rows prepared:", len(output_rows))  # Print total output rows prepared

# COMMAND ----------

# DBTITLE 1,Define Schema and Save Processed Claims to Delta Table
# Define the schema for the output DataFrame, specifying column names, types, and nullability
output_schema = StructType([
    StructField("claim_id", StringType(), True),                      # Unique claim identifier
    StructField("policy_id", StringType(), True),                     # Policy identifier
    StructField("claim_logged_on_raw", StringType(), True),           # Raw claim logged date
    StructField("claim_processed_on_raw", StringType(), True),        # Raw claim processed date
    StructField("incident_date_raw", StringType(), True),             # Raw incident date
    StructField("claim_logged_on_day_bucket", IntegerType(), True),   # Normalized claim logged day bucket
    StructField("claim_processed_on_day_bucket", IntegerType(), True),# Normalized claim processed day bucket
    StructField("incident_date_day_bucket", IntegerType(), True),     # Normalized incident day bucket
    StructField("claim_rejected", StringType(), True),                # Claim rejection status
    StructField("incident_state", StringType(), True),                # Incident state
    StructField("incident_city", StringType(), True),                 # Incident city
    StructField("incident_location", StringType(), True),             # Incident location description
    StructField("incident_type", StringType(), True),                 # Incident type
    StructField("collision_type", StringType(), True),                # Collision type
    StructField("incident_severity", StringType(), True),             # Incident severity
    StructField("authorities_contacted", StringType(), True),         # Authorities contacted
    StructField("police_report_available", StringType(), True),       # Police report availability
    StructField("number_of_vehicles_involved", IntegerType(), True),  # Number of vehicles involved
    StructField("bodily_injuries", IntegerType(), True),              # Number of bodily injuries
    StructField("witnesses", IntegerType(), True),                    # Number of witnesses
    StructField("injury_amount", DoubleType(), True),                 # Injury amount
    StructField("property_amount", DoubleType(), True),               # Property damage amount
    StructField("vehicle_amount", DoubleType(), True),                # Vehicle damage amount
    StructField("total_loss_amount", DoubleType(), True),             # Total loss amount
    StructField("property_damage", StringType(), True),               # Property damage flag
    StructField("policy_bind_date", StringType(), True),              # Policy bind date
    StructField("anomaly_score", DoubleType(), True),                 # Anomaly score
    StructField("priority_tier", StringType(), True),                 # Priority tier
    StructField("triggered_rules", ArrayType(StringType()), True),    # List of triggered anomaly rules
    StructField("report_delay_days", IntegerType(), True),            # Days between incident and claim logged
    StructField("processing_days", IntegerType(), True),              # Days between claim logged and processed
    StructField("claim_lifecycle_days", IntegerType(), True),         # Days between incident and claim processed
    StructField("policy_claim_count", IntegerType(), True),           # Number of claims for policy
    StructField("complexity_points", IntegerType(), True),            # Complexity points
    StructField("authority_report_mismatch", IntegerType(), True),    # Authority report mismatch flag
    StructField("high_severity_no_police_report", IntegerType(), True),# High severity no police report flag
    StructField("ai_investigation_brief", StringType(), True),        # AI-generated investigation brief
    StructField("brief_generated_at", StringType(), True),            # Timestamp when brief was generated
    StructField("model_name", StringType(), True),                    # LLM model name used
    StructField("generation_status", StringType(), True),             # Status of brief generation
])

# Create a Spark DataFrame from the output rows using the defined schema
output_df = spark.createDataFrame(output_rows, schema=output_schema)

# Write the output DataFrame to a Delta table, overwriting existing data and updating schema if needed
(
    output_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

# Print confirmation of table write and display the row count in the target table
print("Wrote target table:", TARGET_TABLE)
print("Target row count:", spark.table(TARGET_TABLE).count())

# COMMAND ----------

# DBTITLE 1,Preview Top Claims by Anomaly Score with Investigation
# Load the processed claims Delta table into a Spark DataFrame
result_df = spark.table(TARGET_TABLE)

# Display the full DataFrame for review in Databricks notebook
display(result_df)

# Display the top 3 claims with highest anomaly scores and key columns for investigation
display(
    result_df.select(
        "claim_id",                # Unique claim identifier
        "policy_id",               # Policy identifier
        "incident_type",           # Type of incident reported
        "incident_severity",       # Severity label for the incident
        "total_loss_amount",       # Total calculated loss amount
        "anomaly_score",           # Anomaly score assigned to the claim
        "priority_tier",           # Priority tier based on anomaly score
        "triggered_rules",         # List of triggered anomaly rules
        "ai_investigation_brief",  # AI-generated investigation brief
        "generation_status"        # Status of AI brief generation
    ).orderBy(F.desc("anomaly_score")).limit(3)  # Order by anomaly score descending and limit to top 3
)