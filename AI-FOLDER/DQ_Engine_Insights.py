# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM primeinsurance.silver.dq_issues

# COMMAND ----------

# COMMAND ----------
# Imports

from pyspark.sql import functions as F, types as T
from pyspark.sql import Row
from openai import OpenAI
import json
from datetime import datetime, timezone

# COMMAND ----------

# COMMAND ----------
# Configuration

SOURCE_TABLE = "primeinsurance.silver.quarantine_table"
TARGET_TABLE = "primeinsurance.gold.dq_explanation_report"
MODEL_NAME = "databricks-gpt-oss-20b"
BATCH_LIMIT = None   # Set to an integer for testing, e.g. 50


# COMMAND ----------

# COMMAND ----------
# 1) Confirm the source table exists

table_check = spark.sql(f"SHOW TABLES IN primeinsurance.silver").filter(F.col("tableName") == "quarantine_table")
if table_check.count() == 0:
    raise ValueError(f"Required source table not found: {SOURCE_TABLE}")

print(f"Confirmed source table exists: {SOURCE_TABLE}")

# COMMAND ----------

# COMMAND ----------
# 2) Inspect schema and sample rows
# The hackathon prompt explicitly says to confirm the schema before building prompts.

dq_df = spark.table(SOURCE_TABLE)

display(dq_df.limit(10))
dq_df.printSchema()
print(f"Row count in source table: {dq_df.count()}")

# COMMAND ----------

# COMMAND ----------
# Optional: normalize column names if the DE pipeline used singular/plural variation.
# The user example shows `affected_record`; the prompt text mentions `affected_records`.
# This block makes the notebook more resilient without hiding schema drift.

columns = set(dq_df.columns)

required_base = ["issue_id", "table_name", "column_name", "issue_description", "severity", "detected_at"]
missing_base = [c for c in required_base if c not in columns]
if missing_base:
    raise ValueError(f"Missing expected columns in {SOURCE_TABLE}: {missing_base}")

affected_col = None
for candidate in ["affected_records", "affected_record"]:
    if candidate in columns:
        affected_col = candidate
        break

if affected_col is None:
    raise ValueError("Could not find `affected_records` or `affected_record` in source table.")

# Optional technical fields mentioned in the problem statement
optional_cols = [c for c in ["rule_name", "affected_ratio", "suggested_fix"] if c in columns]

selected_cols = required_base + [affected_col] + optional_cols
dq_df = dq_df.select(*selected_cols)

print("Columns used for prompting:")
print(dq_df.columns)

# COMMAND ----------

# COMMAND ----------
# 3) Databricks model client

DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
WORKSPACE_URL = spark.conf.get("spark.databricks.workspaceUrl")

client = OpenAI(
    api_key=DATABRICKS_TOKEN,
    base_url=f"https://{WORKSPACE_URL}/serving-endpoints"
)

# COMMAND ----------

# COMMAND ----------
# 4) Response parser for databricks-gpt-oss-20b
# Important: this model returns structured content blocks.
# We extract ONLY the final text block.

def extract_text_from_databricks_response(response):
    try:
        content = response.choices[0].message.content

        if isinstance(content, str):
            parsed = json.loads(content)
        else:
            parsed = content

        if isinstance(parsed, list):
            for block in parsed:
                if isinstance(block, dict) and block.get("type") == "text":
                    return block.get("text", "").strip()

        return ""
    except Exception as e:
        return f"PARSE_ERROR: {str(e)}"

# COMMAND ----------

# COMMAND ----------
# 5) Prompt builder
# Goal: convert technical DQ metadata into a fixed, compliance-friendly explanation.

def build_messages(issue_row: dict):
    system_prompt = '''
You are a compliance-facing data quality explainer for an insurance company.

Your audience is a compliance officer, not an engineer.
Translate technical data quality issues into plain business language.

Rules:
- Use only the facts provided in the input.
- Do not invent missing facts.
- Do not mention prompts, LLMs, SQL, Spark, schemas, notebooks, pipelines, or JSON.
- Keep the tone clear, professional, and actionable.
- If the cause or fix is not explicit, say "based on the issue details provided".
- Return the result in EXACTLY this format with these headings:

What was found:
<1-3 sentences>

Why it matters:
<1-3 sentences>

Likely cause:
<1-3 sentences>

What was done to fix it:
<1-3 sentences>

How to prevent recurrence:
<1-3 sentences>
'''.strip()

    user_payload = {
        "issue_id": issue_row.get("issue_id"),
        "table_name": issue_row.get("table_name"),
        "column_name": issue_row.get("column_name"),
        "issue_description": issue_row.get("issue_description"),
        "severity": issue_row.get("severity"),
        "detected_at": str(issue_row.get("detected_at")),
        "affected_record": issue_row.get("affected_record"),
        "affected_records": issue_row.get("affected_records"),
        "rule_name": issue_row.get("rule_name"),
        "affected_ratio": issue_row.get("affected_ratio"),
        "suggested_fix": issue_row.get("suggested_fix")
    }

    user_prompt = "Explain this data quality issue for a compliance officer:\n" + json.dumps(user_payload, default=str, ensure_ascii=False)

    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

# COMMAND ----------

# COMMAND ----------
# 6) Inference function

def generate_explanation(issue_row: dict):
    try:
        messages = build_messages(issue_row)

        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=messages,
            temperature=0.1,
            max_tokens=500
        )

        explanation_text = extract_text_from_databricks_response(response)

        status = "SUCCESS"
        error_message = None

        if explanation_text.startswith("PARSE_ERROR:"):
            status = "PARSE_ERROR"
            error_message = explanation_text
            explanation_text = None

        return {
            "ai_explanation": explanation_text,
            "generation_status": status,
            "error_message": error_message,
        }

    except Exception as e:
        return {
            "ai_explanation": None,
            "generation_status": "FAILED",
            "error_message": str(e),
        }

# COMMAND ----------

# COMMAND ----------
# 7) Run on every row from dq_issues
# For hackathon simplicity, collect + iterate is acceptable for moderate volumes.
# For larger volumes, switch to batch processing or pandas UDF patterns.

work_df = dq_df
if BATCH_LIMIT is not None:
    work_df = work_df.limit(BATCH_LIMIT)

source_rows = [r.asDict(recursive=True) for r in work_df.collect()]
print(f"Processing {len(source_rows)} DQ issues...")

# COMMAND ----------

# COMMAND ----------
# 8) Generate business explanations

result_rows = []
generated_at_utc = datetime.now(timezone.utc)

for row in source_rows:
    result = generate_explanation(row)

    result_rows.append({
        "issue_id": row.get("issue_id"),
        "table_name": row.get("table_name"),
        "column_name": row.get("column_name"),
        "severity": row.get("severity"),
        "issue_description": row.get("issue_description"),
        "affected_records": row.get("affected_records", row.get("affected_record")),
        "detected_at": row.get("detected_at"),
        "rule_name": row.get("rule_name"),
        "affected_ratio": row.get("affected_ratio"),
        "suggested_fix": row.get("suggested_fix"),
        "ai_generated_explanation": result["ai_explanation"],
        "model_name": MODEL_NAME,
        "generation_status": result["generation_status"],
        "error_message": result["error_message"],
        "generated_at": generated_at_utc,
    })

print(f"Generated {len(result_rows)} output rows")

# COMMAND ----------

# COMMAND ----------
# 9) Create output dataframe

output_schema = T.StructType([
    T.StructField("issue_id", T.StringType(), True),
    T.StructField("table_name", T.StringType(), True),
    T.StructField("column_name", T.StringType(), True),
    T.StructField("severity", T.StringType(), True),
    T.StructField("issue_description", T.StringType(), True),
    T.StructField("affected_records", T.StringType(), True),
    T.StructField("detected_at", T.StringType(), True),
    T.StructField("rule_name", T.StringType(), True),
    T.StructField("affected_ratio", T.StringType(), True),
    T.StructField("suggested_fix", T.StringType(), True),
    T.StructField("ai_generated_explanation", T.StringType(), True),
    T.StructField("model_name", T.StringType(), True),
    T.StructField("generation_status", T.StringType(), True),
    T.StructField("error_message", T.StringType(), True),
    T.StructField("generated_at", T.TimestampType(), True),
])

output_df = spark.createDataFrame(result_rows, schema=output_schema)
display(output_df.limit(20))

# COMMAND ----------

# COMMAND ----------
# 10) Write to Gold

(output_df
 .write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(TARGET_TABLE))

print(f"Wrote results to {TARGET_TABLE}")

# COMMAND ----------

# COMMAND ----------
# 11) Validate output in Unity Catalog

final_df = spark.table(TARGET_TABLE)
print(f"Target row count: {final_df.count()}")
display(final_df.orderBy(F.col("generated_at").desc()))