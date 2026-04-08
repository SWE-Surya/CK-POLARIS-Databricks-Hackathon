import dlt
from pyspark.sql import functions as F

# The root directory containing all the folders and root files
VOLUME_ROOT = "/Volumes/primeinsurance/bronze/raw_data_primeinsurance/autoinsurancedata"

# --------------------------------------------------------------------------
# HELPER FUNCTION
# --------------------------------------------------------------------------
def read_bronze_stream(file_format, glob_filter):
    opts = {
        "cloudFiles.format": file_format,
        "cloudFiles.inferColumnTypes": "true",
        "cloudFiles.schemaEvolutionMode": "addNewColumns", 
        "pathGlobFilter": glob_filter,
        "rescuedDataColumn": "_rescued_data" 
    }
    
    if file_format == "csv":
        opts.update({"header": "true"})
    elif file_format == "json":
        opts.update({"multiLine": "false"})

    return (
        spark.readStream
        .format("cloudFiles")
        .options(**opts)
        .load(VOLUME_ROOT) 
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_loaded_at", F.current_timestamp())
    )

# --------------------------------------------------------------------------
# DLT TABLE DEFINITIONS
# --------------------------------------------------------------------------

@dlt.expect("valid_lineage_customers", "_source_file IS NOT NULL")
@dlt.table(
    #  Using the 3-part name completely overrides the UI's default schema!
    name="primeinsurance.bronze.customers",
    comment="Raw customers data loaded exactly as received.",
    table_properties={
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5"
    }
)
def customers_bronze():
    return read_bronze_stream("csv", "customers*.csv")


@dlt.expect("valid_lineage_sales", "_source_file IS NOT NULL")
@dlt.table(
    name="primeinsurance.bronze.sales",
    comment="Raw sales data loaded exactly as received.",
    table_properties={
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5"
    }
)
def sales_bronze():
    return read_bronze_stream("csv", "[sS]ales*.csv")


@dlt.expect("valid_lineage_cars", "_source_file IS NOT NULL")
@dlt.table(
    name="primeinsurance.bronze.cars",
    comment="Raw cars data loaded exactly as received.",
    table_properties={
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5"
    }
)
def cars_bronze():
    return read_bronze_stream("csv", "cars*.csv")


@dlt.expect("valid_lineage_policy", "_source_file IS NOT NULL")
@dlt.table(
    name="primeinsurance.bronze.policy",
    comment="Raw policy data loaded exactly as received.",
    table_properties={
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5"
    }
)
def policy_bronze():
    return read_bronze_stream("csv", "policy*.csv")


@dlt.expect("valid_lineage_claims", "_source_file IS NOT NULL")
@dlt.table(
    name="primeinsurance.bronze.claims",
    comment="Raw JSON claims data loaded exactly as received.",
    table_properties={
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5"
    }
)
def claims_bronze():
    return read_bronze_stream("json", "claims*.json")