# Databricks notebook source
# MAGIC %md
# MAGIC #### Since in this section Orchestrating the Policy Intelligence Assistant, we can only upload 1 file. So we are adding screenshots and other asked content here in these cells itself.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Screenshot of primeins.gold.rag_query_history in Unity Catalog showing at least 5 queries
# MAGIC
# MAGIC
# MAGIC #####Screenshot -a, since all columns are not commnig in one screen-shot, adding multiple to cover all screenshots
# MAGIC ![image_1774609307204.png](./image_1774609307204.png "image_1774609307204.png")
# MAGIC ![image_1774609345608.png](./image_1774609345608.png "image_1774609345608.png")
# MAGIC
# MAGIC #####Screenshot -b, since all columns are not commnig in one screen-shot, adding multiple to cover all screenshots
# MAGIC ![image_1774609510202.png](./image_1774609510202.png "image_1774609510202.png")
# MAGIC ![image_1774609688045.png](./image_1774609688045.png "image_1774609688045.png")
# MAGIC
# MAGIC
# MAGIC #####Screenshot -c, since all columns are not commnig in one screen-shot, adding multiple to cover all screenshots
# MAGIC ![image_1774609770190.png](./image_1774609770190.png "image_1774609770190.png")
# MAGIC ![image_1774609897319.png](./image_1774609897319.png "image_1774609897319.png")
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sample output: 3 Q&A pairs showing question, retrieved sources (with policy numbers), and LLM answer
# MAGIC ![image_1774610507023.png](./image_1774610507023.png "image_1774610507023.png")
# MAGIC ![image_1774610537104.png](./image_1774610537104.png "image_1774610537104.png")
# MAGIC ![image_1774610571538.png](./image_1774610571538.png "image_1774610571538.png")
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Adding Screenshot of your Genie Space setup showing the tables added and context instructions
# MAGIC ### 

# COMMAND ----------

# MAGIC %md
# MAGIC ####Screenshot of 3 natural language queries answered by Genie with results visible
# MAGIC ![image_1774614708126.png](./image_1774614708126.png "image_1774614708126.png")
# MAGIC ![image_1774614738190.png](./image_1774614738190.png "image_1774614738190.png")
# MAGIC ![image_1774614770969.png](./image_1774614770969.png "image_1774614770969.png")

# COMMAND ----------

# DBTITLE 1,Set Spark Catalog and Schema for Data Access
# Set catalog and schema for Spark table access
catalog = 'primeinsurance'
schema = 'gold'

# COMMAND ----------

# DBTITLE 1,Set Catalog Context for Prime Insurance Database
# MAGIC %sql
# MAGIC --Set the catalog to primeinsurance
# MAGIC use catalog 'primeinsurance';

# COMMAND ----------

# DBTITLE 1,Install Libraries for Embeddings and Indexing
# Install required libraries for embeddings and FAISS indexing
%pip install sentence-transformers faiss-cpu

# COMMAND ----------

# DBTITLE 1,Install OpenAI Library for API Integration
# Install OpenAI library for LLM API access
%pip install openai

# COMMAND ----------

# DBTITLE 1,Restart Python Environment After Library Installation
# Restart Python after install to ensure libraries are available
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Configure OpenAI Client for Databricks Integration
from openai import OpenAI
import pyspark.sql.functions as F   # use F.col(), F.max(), etc. instead of *
import pyspark.sql.types as T
from datetime import datetime

# Databricks workspace URL and token for Foundation Model API
WORKSPACE_HOST = "dbc-2926c9ce-e635.cloud.databricks.com"
DATABRICKS_TOKEN = 'dapi36a38122af594d843737d47ea020e056'

# Model to use (free on Databricks Free Edition)
MODEL_NAME = "databricks-gpt-oss-20b"

# Create OpenAI-compatible client pointing to Databricks serving endpoint
client = OpenAI(
    api_key=DATABRICKS_TOKEN,
    base_url=f"https://{WORKSPACE_HOST}/serving-endpoints"
)

print(f"Workspace: {WORKSPACE_HOST}")
print(f"Model: {MODEL_NAME}")
print("Client ready!")
print("Configuration loaded.")

# COMMAND ----------

# DBTITLE 1,Extract and Parse Text Data from JSON Content
# Helper function to extract answer text from LLM response content.
# Handles plain strings, JSON lists, and dicts with "text" keys.
def _extract_text(content):
    if content is None: return ""
    if isinstance(content, str):
        try: parsed = json.loads(content)
        except: return content.strip()
    else: parsed = content
    if isinstance(parsed, list):
        for block in parsed:
            if isinstance(block, dict) and block.get("type") == "text":
                return str(block.get("text", "")).strip()
    if isinstance(parsed, dict) and "text" in parsed:
        return str(parsed["text"]).strip()
    return str(content).strip()

# COMMAND ----------

# DBTITLE 1,Load and Preview Prime Insurance Policy DataFrame
from pyspark.sql import functions as F

# Read the source table containing policy data
df_policy = spark.table("primeinsurance.gold.dim_policy")

# Basic checks
print("Row count:", df_policy.count())
print("Columns:", len(df_policy.columns))

# Show schema
df_policy.printSchema()

# Preview data
display(df_policy.limit(10))

# COMMAND ----------

# DBTITLE 1,Clean and Trim String Columns in Policy DataFrame
from pyspark.sql import functions as F

# Clean policy DataFrame: replace invalid values, trim strings, filter null policy_id, if exists, but in our case all the data are clean, but I have added to double make sure of everything
df_policy_clean = df_policy.replace(["?", "NULL", "null", ""], None)

string_cols = [c for c, t in df_policy_clean.dtypes if t == "string"]

for col_name in string_cols:
    df_policy_clean = df_policy_clean.withColumn(col_name, F.trim(F.col(col_name)))

df_policy_clean = df_policy_clean.filter(F.col("policy_id").isNotNull())

display(df_policy_clean.limit(10))

# COMMAND ----------

# DBTITLE 1,Generate Document Text from Policy DataFrame Columns
# Create a document-style text column for each policy, concatenating key fields for semantic search
df_docs = df_policy_clean.withColumn(
    "document_text",
    F.concat_ws(
        " ",
        F.concat(F.lit("Policy "), F.col("policy_id"), F.lit(".")),
        F.concat(F.lit("Policy bind date is "), F.col("policy_bind_date"), F.lit(".")),
        F.concat(F.lit("Policy state is "), F.col("policy_state"), F.lit(".")),
        F.concat(F.lit("Combined single limit is "), F.col("policy_csl"), F.lit(".")),
        F.concat(F.lit("Policy deductible is "), F.col("policy_deductable"), F.lit(".")),
        F.concat(F.lit("Annual premium is "), F.col("policy_annual_premium"), F.lit(".")),
        F.concat(F.lit("Umbrella limit is "), F.col("umbrella_limit"), F.lit("."))
    )
)

df_docs = df_docs.select("policy_id", "document_text")

display(df_docs.limit(5))

# COMMAND ----------

# DBTITLE 1,Collect Rows from Document DataFrame for Analysis
# Collect document rows from Spark DataFrame for chunking and embedding
doc_rows = df_docs.collect()

# COMMAND ----------

# DBTITLE 1,Split Text into Chunks with Overlap for Processing
def split_text(text, chunk_size=400, overlap=80):
    # Split text into overlapping chunks for embedding and semantic search
    chunks = []
    start = 0
    text_length = len(text)

    while start < text_length:
        end = start + chunk_size
        chunk = text[start:end]
        chunks.append(chunk)
        start = end - overlap

    return chunks

# COMMAND ----------

# DBTITLE 1,Chunk Texts and Metadata Preparation for Analysis
# Split document_text into overlapping chunks and collect chunk metadata for each policy
chunk_texts = []
chunk_metadata = []

for row in doc_rows:
    chunks = split_text(row["document_text"], chunk_size=400, overlap=80)

    for chunk in chunks:
        chunk_texts.append(chunk)
        chunk_metadata.append({
            "policy_id": row["policy_id"]
        })

print("Total chunks:", len(chunk_texts))
print("Sample:", chunk_texts[0])

# COMMAND ----------

# DBTITLE 1,Generate and Prepare Text Embeddings for Indexing
# creates one embedding per chunk
# prepares data for FAISS indexing

# Load embedding model and generate embeddings for chunked texts
from sentence_transformers import SentenceTransformer
import numpy as np

print("Loading embedding model (all-MiniLM-L6-v2)...")
embed_model = SentenceTransformer("all-MiniLM-L6-v2")

print("Generating embeddings for chunks...")
embeddings = embed_model.encode(
    chunk_texts,
    convert_to_numpy=True,
    show_progress_bar=True
)

embeddings = np.array(embeddings, dtype="float32")

print("Embeddings shape:", embeddings.shape)

# COMMAND ----------

# DBTITLE 1,Build FAISS Index with Normalized Vectors for Similarit ...
# IndexFlatIP + normalized vectors = cosine similarity
# cosine similarity is better than plain L2 for semantic retrieval

import faiss
import numpy as np

# Normalize embeddings for cosine similarity
faiss.normalize_L2(embeddings)

# Build FAISS index using Inner Product (IP)
# With normalized vectors, IP is equivalent to cosine similarity
dimension = embeddings.shape[1]
index = faiss.IndexFlatIP(dimension)

# Add embeddings to index
index.add(embeddings)

print(f"FAISS index built successfully.")
print(f"Total indexed chunks: {index.ntotal}")
print(f"Embedding dimension: {dimension}")

# COMMAND ----------

# DBTITLE 1,Retrieve Top Similar Text Chunks Using FAISS Embeddings
def retrieve_relevant_chunks(question, top_k=5):
    # Embed the query
    query_embedding = embed_model.encode([question], convert_to_numpy=True)
    query_embedding = np.array(query_embedding, dtype="float32")

    # Normalize for cosine similarity
    faiss.normalize_L2(query_embedding)

    # Search FAISS index for top_k most similar chunks
    scores, indices = index.search(query_embedding, top_k)

    results = []
    for score, idx in zip(scores[0], indices[0]):
        if idx != -1:
            results.append({
                "score": float(score),
                "chunk_text": chunk_texts[idx],
                "metadata": chunk_metadata[idx]
            })

    return results

# COMMAND ----------

# DBTITLE 1,Retrieve and Display Top Chunks for Umbrella Coverage Q ...
# Test retrieval of relevant chunks for a sample umbrella coverage question
test_question = "Which policies have umbrella coverage?"
retrieved_chunks = retrieve_relevant_chunks(test_question, top_k=5)

for i, item in enumerate(retrieved_chunks, 1):
    print(f"\n--- Result {i} ---")
    print("Score:", item["score"])
    print("Metadata:", item["metadata"])
    print("Chunk Text:", item["chunk_text"])

# COMMAND ----------

# DBTITLE 1,Format Retrieved Text Chunks into LLM Context String
# Now create a helper function to format retrieved chunks into context for the LLM.

def build_context(retrieved_chunks):
    context_parts = []

    for i, item in enumerate(retrieved_chunks, 1):
        policy_id = item["metadata"]["policy_id"]
        chunk_text = item["chunk_text"]

        context_parts.append(
            f"[Source {i}] "
            f"Policy ID: {policy_id}\n"
            f"{chunk_text}"
        )

    return "\n\n".join(context_parts)

# COMMAND ----------

# DBTITLE 1,Build and Print Context from Retrieved Text Chunks
# Format retrieved chunks into context for LLM prompt
context_text = build_context(retrieved_chunks)
print(context_text)

# COMMAND ----------

# DBTITLE 1,Define Function to Answer Questions Using Retrieved Pol ...
def answer_question(question, top_k=5):
    # Retrieve relevant chunks
    retrieved_chunks = retrieve_relevant_chunks(question, top_k=top_k)

    # Build context
    context_text = build_context(retrieved_chunks)

    # Extract unique source policies
    # Use dict.fromkeys to preserve order and remove duplicates
    source_policies = list(dict.fromkeys(
        [item["metadata"]["policy_id"] for item in retrieved_chunks]
    ))

    prompt = f"""
You are an insurance policy assistant.

Answer the user's question using ONLY the retrieved context below.
Do not make up information.
If the answer is not clearly available in the context, say that the information is not available in the retrieved records.
Always cite the relevant policy IDs explicitly in the final answer.
For filter-based questions, include only policies that actually satisfy the condition.
Do not include policies that do not match the requested criteria.

Retrieved Context:
{context_text}

User Question:
{question}
"""

    response = client.chat.completions.create(
        model=MODEL_NAME,
        messages=[
            {"role": "system", "content": "You answer insurance policy questions using retrieved records only."},
            {"role": "user", "content": prompt}
        ],
        temperature=0
    )

    content = response.choices[0].message.content

    # Use your helper function
    final_answer = _extract_text(content)

    return {
        "question": question,
        "answer": final_answer,
        "source_policies": source_policies,
        "retrieved_chunks": retrieved_chunks
    }

# COMMAND ----------

# DBTITLE 1,Compute Composite Confidence Score and Band for Retriev ...
def calculate_confidence(retrieved_chunks):
    # Calculate a composite confidence score and band for retrieved chunks
    if not retrieved_chunks:
        return {"score": 0.0, "band": "LOW", "explanation": "No chunks retrieved."}

    # Convert PySpark Rows to plain dicts if needed
    chunks = [c.asDict(recursive=True) if hasattr(c, "asDict") else c for c in retrieved_chunks]

    scores = [float(c.get("score") or c.get("similarity_score") or 0.5) for c in chunks]

    mean_score      = sum(scores) / len(scores)
    top_score       = max(scores)
    unique_policies = len({c["metadata"]["policy_id"] for c in chunks})
    diversity       = min(unique_policies / 4.0, 1.0)

    composite = round(0.5 * mean_score + 0.3 * top_score + 0.2 * diversity, 4)

    band = "HIGH" if composite >= 0.75 else "MEDIUM" if composite >= 0.50 else "LOW"

    return {"score": composite, "band": band, "unique_policies": unique_policies}

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from primeinsurance.gold.dim_policy;

# COMMAND ----------

# DBTITLE 1,Create RAG Query History Table in Prime Insurance Schem ...
# Create table to log RAG query results and metadata for insurance policy questions
spark.sql("""
CREATE TABLE IF NOT EXISTS primeinsurance.gold.rag_query_history (
    question STRING,
    answer STRING,
    confidence_score DOUBLE,
    confidence_band STRING,
    unique_policies_retrieved INT,
    source_policies STRING,
    query_timestamp TIMESTAMP
)
""")

# COMMAND ----------

# DBTITLE 1,Show Schema Details of Prime Insurance RAG Query Histor ...
# Show schema and column details for the RAG query history table
display(spark.sql("DESCRIBE primeinsurance.gold.rag_query_history"))

# COMMAND ----------

# DBTITLE 1,Log Query Results with Confidence and Source Metadata
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
)
from datetime import datetime

# Log RAG query result and metadata to primeinsurance.gold.rag_query_history table
def log_query_result(result, confidence):
    log_schema = StructType([
        StructField("question", StringType(), True),
        StructField("answer", StringType(), True),
        StructField("confidence_score", DoubleType(), True),
        StructField("confidence_band", StringType(), True),
        StructField("unique_policies_retrieved", IntegerType(), True),
        StructField("source_policies", StringType(), True),
        StructField("query_timestamp", TimestampType(), True)
    ])

    log_data = [(
        str(result["question"]),
        str(result["answer"]),
        float(confidence["score"]),
        str(confidence["band"]),
        int(confidence["unique_policies"]),
        ",".join([str(p) for p in result["source_policies"]]),
        datetime.now()
    )]

    log_df = spark.createDataFrame(log_data, schema=log_schema)

    (
        log_df
        .selectExpr(
            "CAST(question AS STRING) AS question",
            "CAST(answer AS STRING) AS answer",
            "CAST(confidence_score AS DOUBLE) AS confidence_score",
            "CAST(confidence_band AS STRING) AS confidence_band",
            "CAST(unique_policies_retrieved AS INT) AS unique_policies_retrieved",
            "CAST(source_policies AS STRING) AS source_policies",
            "CAST(query_timestamp AS TIMESTAMP) AS query_timestamp"
        )
        .write
        .format("delta")
        .mode("append")
        .saveAsTable("primeinsurance.gold.rag_query_history")
    )

# COMMAND ----------

# DBTITLE 1,Run Test Questions Log Results and Display Confidence S ...
# Run batch of test questions, log results to RAG query history table, and print summary for each
test_questions = [
    "Which policies in South Carolina (SC) have zero umbrella coverage and what insights can be derived?",
    "Which policies have high premiums and what does that indicate about their coverage?",
    "What patterns can be observed for policies with high deductibles?",
    "Compare policies with umbrella coverage versus those without it in terms of risk exposure.",
    "What insights can be derived from policies with low deductibles and high coverage limits?"
]

all_results = []

for q in test_questions:
    result = answer_question(q, top_k=10)
    confidence = calculate_confidence(result["retrieved_chunks"])
    
    log_query_result(result, confidence)
    
    all_results.append({
        "question": result["question"],
        "answer": result["answer"],
        "confidence_score": confidence["score"],
        "confidence_band": confidence["band"],
        "source_policies": ",".join(result["source_policies"])
    })

    print("\n" + "=" * 100)
    print("Question:", result["question"])
    print("\nAnswer:\n", result["answer"])
    print("\nConfidence Score:", confidence["score"])
    print("Confidence Band:", confidence["band"])
    print("Source Policies:", result["source_policies"])

# COMMAND ----------

# DBTITLE 1,Show Recent RAG Query History Ordered by Timestamp
# Display all RAG query history records ordered by most recent timestamp
display(
    spark.table("primeinsurance.gold.rag_query_history")
    .orderBy(F.col("query_timestamp").desc())
)

# COMMAND ----------

result = answer_question("Which policies have high premiums and what does that indicate about their coverage?", top_k=10)

print("Question:", result["question"])
print("\nAnswer:\n", result["answer"])
print("\nSource Policies:", result["source_policies"])

# COMMAND ----------

result = answer_question("Which policies in South Carolina (SC) have zero umbrella coverage and what insights can be derived?", top_k=10)

print("Question:", result["question"])
print("\nAnswer:\n", result["answer"])
print("\nSource Policies:", result["source_policies"])

# COMMAND ----------

result = answer_question("What patterns can be observed for policies with high deductibles?", top_k=10)

print("Question:", result["question"])
print("\nAnswer:\n", result["answer"])
print("\nSource Policies:", result["source_policies"])