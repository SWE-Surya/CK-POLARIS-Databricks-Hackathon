# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC #### Since in this section Synthesizing Executive Business Insights, we can only upload 1 file. So we are adding screenshots and other asked content here in these cells itself.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Screenshot of primeins.gold.ai_business_insights in Unity Catalog with rows visible
# MAGIC
# MAGIC #### Screenshot of Claims Performance Overview
# MAGIC
# MAGIC ##### a. Screenshot of Claims Performance Overview
# MAGIC ![image_1774611556437.png](./image_1774611556437.png "image_1774611556437.png")
# MAGIC ##### b. Screenshot of Claims Performance Overview
# MAGIC ![image_1774611650993.png](./image_1774611650993.png "image_1774611650993.png")
# MAGIC ##### c. Screenshot of Claims Performance Overview
# MAGIC ![image_1774611684237.png](./image_1774611684237.png "image_1774611684237.png")
# MAGIC
# MAGIC
# MAGIC #### Screenshot of Customer Profile Overview
# MAGIC
# MAGIC ##### a. Screenshot of Customer Profile Overview
# MAGIC ![image_1774611822878.png](./image_1774611822878.png "image_1774611822878.png")
# MAGIC ##### b. Screenshot of Customer Profile Overview
# MAGIC ![image_1774611946792.png](./image_1774611946792.png "image_1774611946792.png")
# MAGIC ##### c. Screenshot of Customer Profile Overview
# MAGIC ![image_1774611905005.png](./image_1774611905005.png "image_1774611905005.png")
# MAGIC
# MAGIC
# MAGIC #### Screenshot of Policy Portfolio Overview
# MAGIC
# MAGIC ##### a. Screenshot of Policy Portfolio Overview
# MAGIC ![image_1774612166780.png](./image_1774612166780.png "image_1774612166780.png")
# MAGIC ##### b. Screenshot of Policy Portfolio Overview
# MAGIC ![image_1774612201270.png](./image_1774612201270.png "image_1774612201270.png")
# MAGIC ##### c. Screenshot of Policy Portfolio Overview
# MAGIC ![image_1774612331620.png](./image_1774612331620.png "image_1774612331620.png")
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sample output: paste or screenshot 1 full executive summary (any domain) 
# MAGIC
# MAGIC ### Ans:
# MAGIC ####  Full executive summary = claims_performance
# MAGIC
# MAGIC Query:
# MAGIC `SELECT 
# MAGIC   domain_name,
# MAGIC   summary_title,
# MAGIC   executive_summary
# MAGIC FROM primeinsurance.gold.ai_business_insights
# MAGIC WHERE domain_name = 'claims_performance'
# MAGIC LIMIT 1;
# MAGIC `
# MAGIC
# MAGIC Result:
# MAGIC
# MAGIC domain_name	summary_title	executive_summary
# MAGIC claims_performance	Claims Performance Overview	"**Executive Summary – Claims KPI Review**
# MAGIC
# MAGIC | KPI | Value | Benchmark / Insight |
# MAGIC |-----|-------|---------------------|
# MAGIC | **Total Claims** | 227 | Modest volume – allows detailed review |
# MAGIC | **Total Claim Amount** | $2,798,325 | Avg. $12,327 per claim |
# MAGIC | **Avg. Claim Amount** | $12,327 | Above industry average for similar lines |
# MAGIC | **Avg. Processing Time** | 2.95 days | Fast turnaround, but may mask incomplete reviews |
# MAGIC | **Rejection Rate** | **38.8 %** | **Anomaly – far above typical 10‑15 %** |
# MAGIC | **Major Severity %** | 25.6 % | High proportion of severe claims |
# MAGIC | **Avg. Vehicles Involved** | 1.73 | Indicates multi‑vehicle incidents are common |
# MAGIC | **Avg. Bodily Injuries** | 1.09 | One injury per claim on average |
# MAGIC | **Police Report %** | **31.3 %** | **Anomaly – low capture rate** |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Key Insights
# MAGIC
# MAGIC 1. **High Rejection Rate**  
# MAGIC    - 38.8 % of claims are rejected, suggesting either overly stringent criteria or a high incidence of fraud/false reporting.  
# MAGIC    - This rate is likely inflating administrative costs and eroding customer trust.
# MAGIC
# MAGIC 2. **Low Police Report Capture**  
# MAGIC    - Only 31 % of claims have an accompanying police report, which is a critical evidence source for validating claim severity and reducing fraud.  
# MAGIC    - The low capture rate may be contributing to the high rejection rate and the elevated average claim amount.
# MAGIC
# MAGIC 3. **Elevated Claim Severity**  
# MAGIC    - 25.6 % of claims are classified as major severity, and the average number of vehicles involved (1.73) and bodily injuries (1.09) confirm a high‑risk portfolio.  
# MAGIC    - This drives the average claim amount upward and increases exposure.
# MAGIC
# MAGIC 4. **Rapid Processing Time**  
# MAGIC    - 2.95 days is commendably fast, but the combination of high rejection and low evidence suggests the speed may come at the expense of thoroughness.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Risks
# MAGIC
# MAGIC | Risk | Impact | Likelihood |
# MAGIC |------|--------|------------|
# MAGIC | **Fraud Exposure** | High claim payouts, regulatory scrutiny | Medium‑High |
# MAGIC | **Customer Attrition** | Rejections and lack of transparency | Medium |
# MAGIC | **Operational Overhead** | Re‑work on rejected claims, appeals | Medium |
# MAGIC | **Reputation Damage** | Perceived unfairness, negative reviews | Low‑Medium |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Recommendations
# MAGIC
# MAGIC 1. **Revise Rejection Criteria**  
# MAGIC    - Conduct a root‑cause analysis of rejected claims.  
# MAGIC    - Tighten only the most egregious fraud indicators; consider a “pre‑review” step to flag borderline cases for human review.
# MAGIC
# MAGIC 2. **Improve Police Report Capture**  
# MAGIC    - Automate prompts for policyholders to upload police reports at the time of claim filing.  
# MAGIC    - Partner with local law‑enforcement portals to pull reports directly where possible.
# MAGIC
# MAGIC 3. **Enhance Evidence‑Based Triage**  
# MAGIC    - Deploy AI‑driven triage tools that weigh evidence quality (photos, police reports, witness statements) before deciding on acceptance or rejection.  
# MAGIC    - Use the triage score to flag high‑severity, high‑risk claims for expedited human review.
# MAGIC
# MAGIC 4. **Targeted Training for Claims Adjusters**  
# MAGIC    - Focus on interpreting severity indicators and evidence thresholds.  
# MAGIC    - Emphasize the importance of evidence capture and documentation.
# MAGIC
# MAGIC 5. **Monitor Fraud Indicators**  
# MAGIC    - Implement a fraud‑risk score that updates in real time as new evidence is added.  
# MAGIC    - Flag claims with low police‑report coverage and high severity for additional scrutiny.
# MAGIC
# MAGIC 6. **Customer Communication Protocol**  
# MAGIC    - Provide clear, transparent explanations for rejections and next‑step options.  
# MAGIC    - Offer a streamlined appeal process to reduce churn.
# MAGIC
# MAGIC 7. **Benchmarking & Continuous Improvement**  
# MAGIC    - Track the same KPIs quarterly and compare against industry peers.  
# MAGIC    - Set realistic targets: reduce rejection rate to <20 %, increase police report capture to >70 %, and maintain processing time ≤3 days.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Bottom Line:**  
# MAGIC The portfolio is exposed to higher-than‑expected claim amounts and a high rejection rate, largely driven by insufficient evidence capture (police reports). By tightening evidence requirements, improving triage, and refining rejection criteria, the organization can reduce fraud exposure, lower administrative costs, and improve customer satisfaction while maintaining rapid claim processing.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sample output: 2 ai_query() SQL results
# MAGIC
# MAGIC #### Ans- 1st ai_qyery() result :
# MAGIC `%sql
# MAGIC SELECT 
# MAGIC   ai_query(
# MAGIC     'databricks-gpt-oss-20b',
# MAGIC     'What does a high claim rejection rate indicate in insurance?'
# MAGIC   ) AS explanation;`
# MAGIC
# MAGIC A high claim‑rejection rate in an insurance portfolio is a red flag that something in the underwriting‑to‑claims chain is not working as intended. It can point to several underlying issues:
# MAGIC
# MAGIC | What the high rate may signal | Why it matters | Typical causes |
# MAGIC |------------------------------|----------------|----------------|
# MAGIC | **Aggressive fraud detection** | Protects the insurer’s bottom line, but can backfire if legitimate claims are flagged. | Over‑tight fraud rules, automated systems that misclassify claims, lack of human review. |
# MAGIC | **Poor underwriting quality** | Policies are sold to risk profiles that are too high for the premiums charged. | Inadequate risk assessment, outdated underwriting guidelines, insufficient data. |
# MAGIC | **Misaligned policy terms** | Policyholders misunderstand coverage, leading to claims that are technically out of scope. | Ambiguous wording, frequent changes to terms, inadequate disclosure. |
# MAGIC | **Claims‑processing inefficiencies** | Delays and errors increase rejection rates and erode customer trust. | Manual bottlenecks, lack of training, outdated technology. |
# MAGIC | **High‑risk portfolio** | The insurer is knowingly taking on risk that is hard to cover. | Targeted niche markets, aggressive growth strategies, insufficient pricing. |
# MAGIC | **Regulatory or compliance pressure** | Stricter rules force more denials to avoid penalties. | New solvency or consumer‑protection regulations. |
# MAGIC
# MAGIC ### Why it matters
# MAGIC - **Financial impact** – A high rejection rate can reduce claim payouts, but it can also hurt revenue if policyholders cancel or switch carriers.
# MAGIC - **Reputation risk** – Customers who feel unfairly denied may spread negative word‑of‑mouth, damaging brand equity.
# MAGIC - **Regulatory scrutiny** – Persistent denials can trigger investigations or fines if they appear discriminatory or arbitrary.
# MAGIC
# MAGIC ### What to do
# MAGIC 1. **Audit the claims data** – Identify patterns (e.g., specific lines of business, geographic areas, or claim types) that drive rejections.
# MAGIC 2. **Review underwriting guidelines** – Ensure they match current risk profiles and market conditions.
# MAGIC 3. **Improve communication** – Provide clear, timely explanations to policyholders about why a claim was denied and how to appeal.
# MAGIC 4. **Invest in technology** – Use data analytics and AI to flag only truly suspicious claims, reducing false positives.
# MAGIC 5. **Train staff** – Equip underwriters and claims adjusters with the latest risk assessment tools and customer‑service skills.
# MAGIC 6. **Monitor regulatory changes** – Stay ahead of new compliance requirements that could affect denial criteria.
# MAGIC
# MAGIC In short, a high claim rejection rate is a symptom of deeper operational, underwriting, or market‑risk issues. Addressing it requires a balanced approach that protects the insurer’s financial health while maintaining fair, transparent treatment of policyholders.
# MAGIC
# MAGIC
# MAGIC #### Ans- 2nd ai_qyery() result :
# MAGIC
# MAGIC `%sql
# MAGIC SELECT 
# MAGIC   ai_query(
# MAGIC     'databricks-gpt-oss-20b',
# MAGIC     CONCAT(
# MAGIC       'Explain business impact of average claim amount: ',
# MAGIC       CAST(AVG(injury + property + vehicle) AS STRING)
# MAGIC     )
# MAGIC   ) AS explanation
# MAGIC FROM primeinsurance.gold.fact_claims;
# MAGIC `
# MAGIC
# MAGIC **Average Claim Amount (≈ $12,327.42)**  
# MAGIC *What it means for the business*
# MAGIC
# MAGIC | Area | Impact of a $12k average claim | Why it matters |
# MAGIC |------|--------------------------------|----------------|
# MAGIC | **Profitability** | Higher average claims raise the loss ratio (claims ÷ earned premiums). If the loss ratio climbs above target, underwriting profit shrinks or turns negative. | Directly reduces net income and shareholder returns. |
# MAGIC | **Pricing & Underwriting** | To keep the loss ratio in line, premiums may need to be increased or underwriting criteria tightened. | Ensures the product remains financially viable and competitive. |
# MAGIC | **Reserves & Cash Flow** | Larger claims require larger loss‑reserve provisions and more immediate cash outlays. | Affects liquidity, working‑capital needs, and solvency ratios. |
# MAGIC | **Capital & Solvency** | Higher claim amounts increase the risk‑adjusted capital required (e.g., under Solvency II or IFRS 17). | Impacts regulatory capital, cost of capital, and ability to write new business. |
# MAGIC | **Reinsurance** | A higher average claim may push the company to purchase more or higher‑limit reinsurance to cap exposure. | Alters the cost structure and risk‑transfer strategy. |
# MAGIC | **Product Design** | If the average claim approaches or exceeds policy limits, the product may need redesign (e.g., higher limits, new riders). | Keeps the product attractive while managing risk. |
# MAGIC | **Claims Management** | A high average claim can signal inefficiencies in claims handling or emerging loss trends. | Drives investment in claims technology, fraud detection, and loss‑control programs. |
# MAGIC | **Risk Appetite & Strategy** | The average claim informs the risk appetite framework—whether the company is comfortable with high‑severity, low‑frequency risks. | Guides portfolio mix, diversification, and strategic focus. |
# MAGIC | **Competitive Positioning** | Competitors with lower average claims may offer lower premiums or better coverage terms. | Influences market share and brand perception. |
# MAGIC
# MAGIC **Key Take‑aways**
# MAGIC
# MAGIC 1. **Monitor the trend** – A rising average claim signals emerging risks or pricing gaps that need addressing.
# MAGIC 2. **Adjust pricing or underwriting** – If the average claim is above industry benchmarks, consider premium hikes or stricter underwriting.
# MAGIC 3. **Re‑evaluate reinsurance** – Higher average claims may justify additional or higher‑limit reinsurance to protect capital.
# MAGIC 4. **Invest in loss prevention** – Target high‑severity loss categories with prevention programs to reduce the average claim size.
# MAGIC 5. **Use it for forecasting** – Combine the average claim with claim frequency to project future losses, set reserves, and plan capital.
# MAGIC
# MAGIC In short, an average claim of ~$12,327 is a critical lever that influences profitability, pricing, capital, and risk management. Keeping it in check—or understanding why it’s high—helps maintain a healthy, competitive insurance portfolio.
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# DBTITLE 1,Initialize Databricks Environment with Libraries and Im ...
# Databricks Notebook Setup

from pyspark.sql import functions as F
from datetime import datetime
import json

# COMMAND ----------

# DBTITLE 1,Load primeinsurance Gold Tables into Spark DataFrames
# Load gold tables from primeinsurance database into Spark DataFrames
claims_df = spark.table("primeinsurance.gold.fact_claims")
policy_df = spark.table("primeinsurance.gold.dim_policy")
customer_df = spark.table("primeinsurance.gold.dim_customer")
car_df = spark.table("primeinsurance.gold.dim_car")
sales_df = spark.table("primeinsurance.gold.fact_sales")

# COMMAND ----------

# DBTITLE 1,Calculate Summary KPIs for Claims Dataset with Aggregat ...
# Calculate claims KPIs from claims_df
claims_kpi = claims_df.select(
    F.count("*").alias("total_claims"),  # Total number of claims
    F.avg(F.col("injury") + F.col("property") + F.col("vehicle")).alias("avg_claim_amount"),  # Average claim amount
    F.sum(F.col("injury") + F.col("property") + F.col("vehicle")).alias("total_claim_amount"),  # Total claim amount
    F.avg("processing_time_days").alias("avg_processing_time"),  # Average processing time in days
    (F.sum(F.when(F.col("claim_rejected") == True, 1).otherwise(0)) * 100.0 / F.count("*")).alias("rejection_rate"),  # Claim rejection rate (%)
    (F.sum(F.when(F.col("incident_severity") == "Major Damage", 1).otherwise(0)) * 100.0 / F.count("*")).alias("major_severity_pct"),  # % of claims with major severity
    F.avg("number_of_vehicles_involved").alias("avg_vehicles_involved"),  # Average number of vehicles involved per claim
    F.avg("bodily_injuries").alias("avg_bodily_injuries"),  # Average bodily injuries per claim
    (F.sum(F.when(F.col("police_report_available") == True, 1).otherwise(0)) * 100.0 / F.count("*")).alias("police_report_pct")  # % of claims with police report available
)

# Convert KPIs to JSON for downstream use
claims_kpi_dict = claims_kpi.toPandas().to_dict(orient="records")[0]
claims_kpi_json = json.dumps(claims_kpi_dict)

# COMMAND ----------

# DBTITLE 1,Aggregate Policy Metrics Including Claims Participation ...
# Calculate policy KPIs from policy_df joined with claims_df
policy_join_df = policy_df.join(claims_df, "policy_id", "left")

policy_kpi = policy_join_df.select(
    F.countDistinct("policy_id").alias("total_policies"),  # Total number of policies
    F.avg("policy_annual_premium").alias("avg_premium"),  # Average annual premium
    F.sum("umbrella_limit").alias("total_umbrella_coverage"),  # Total umbrella coverage
    F.avg("policy_deductable").alias("avg_deductible"),  # Average deductible
    (F.sum(F.when(F.col("claim_id").isNotNull(), 1).otherwise(0)) * 100.0 / F.countDistinct("policy_id")).alias("policies_with_claim_pct")  # % of policies with at least one claim
)

policy_kpi_dict = policy_kpi.toPandas().to_dict(orient="records")[0]
policy_kpi_json = json.dumps(policy_kpi_dict)

# COMMAND ----------

# DBTITLE 1,Aggregate Customer KPIs Including Defaults Claims and L ...
# Calculate customer KPIs from customer_df joined with claims_df
customer_join_df = customer_df.join(claims_df, "customer_id", "left")

customer_kpi = customer_join_df.select(
    F.countDistinct("customer_id").alias("total_customers"),  # Total number of customers
    F.avg("balance").alias("avg_balance"),  # Average customer balance
    (F.sum(F.when(F.col("default_flag") == 1, 1).otherwise(0)) * 100.0 / F.count("*")).alias("default_rate"),  # Default rate (%)
    F.avg(F.when(F.col("claim_id").isNotNull(), 1).otherwise(0)).alias("claims_per_customer"),  # Average claims per customer
    (F.sum(F.when(F.col("CarLoan") == 1, 1).otherwise(0)) * 100.0 / F.count("*")).alias("car_loan_pct"),  # % of customers with car loan
    (F.sum(F.when(F.col("HHInsurance") == 1, 1).otherwise(0)) * 100.0 / F.count("*")).alias("home_insurance_pct")  # % of customers with home insurance
)

# Convert KPIs to JSON for downstream use
customer_kpi_dict = customer_kpi.toPandas().to_dict(orient="records")[0]
customer_kpi_json = json.dumps(customer_kpi_dict)

# COMMAND ----------

# DBTITLE 1,Generate Executive Summary Prompt for Insurance KPIs
def build_prompt(domain, kpi_json):
    # Build prompt for insurance executive summary generation
    # Returns a formatted prompt string for AI executive summary based on domain and KPI JSON
    return f"""
You are a senior insurance business analyst.

Domain: {domain}

Given the following KPI summary:
{kpi_json}

Write an executive summary that includes:
- Key insights
- Risks
- Opportunities
- Any anomalies

Keep it concise and decision-focused.
"""

# COMMAND ----------

# DBTITLE 1,Create Temporary View for Policy KPI JSON Data
# Create a temporary view for policy KPIs as JSON for downstream use
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW policy_kpi_view AS
SELECT '{policy_kpi_json}' AS kpi_json
""")

# COMMAND ----------

# DBTITLE 1,Create Temporary View to Store Claims KPI JSON Data
# Create a temporary view for claims KPIs as JSON for downstream use
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW claims_kpi_view AS
SELECT '{claims_kpi_json}' AS kpi_json
""")

# COMMAND ----------

# DBTITLE 1,Establish Temporary View for Customer KPI JSON Data
# Create a temporary view for customer KPIs as JSON for downstream use
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW customer_kpi_view AS
SELECT '{customer_kpi_json}' AS kpi_json
""")

# COMMAND ----------

# DBTITLE 1,Combine Policy, Claims, and Customer KPI Data Views
# MAGIC %sql
# MAGIC -- Combine KPI JSON views for policy, claims, and customer domains
# MAGIC SELECT * FROM policy_kpi_view
# MAGIC union all
# MAGIC SELECT * FROM claims_kpi_view
# MAGIC union all
# MAGIC SELECT * FROM customer_kpi_view;

# COMMAND ----------

# DBTITLE 1,Generate Claims Performance Executive Summary View
# MAGIC %sql
# MAGIC -- Create a temporary view for claims executive summary using AI
# MAGIC CREATE OR REPLACE TEMP VIEW claims_summary_view AS
# MAGIC SELECT 
# MAGIC   'claims_performance' AS domain_name,
# MAGIC   'Claims Performance Overview' AS summary_title,
# MAGIC
# MAGIC   ai_query(
# MAGIC     'databricks-gpt-oss-20b',
# MAGIC     CONCAT(
# MAGIC       'You are a senior insurance executive advisor. ',
# MAGIC       'Analyze the KPI JSON and produce a concise executive summary with insights, risks, and recommendations. ',
# MAGIC       'Highlight anomalies if any: ',
# MAGIC       kpi_json
# MAGIC     )
# MAGIC   ) AS executive_summary,
# MAGIC
# MAGIC   kpi_json,
# MAGIC   current_timestamp() AS generation_timestamp,
# MAGIC   'databricks-gpt-oss-20b' AS model_name,
# MAGIC   'SUCCESS' AS status
# MAGIC
# MAGIC FROM claims_kpi_view;

# COMMAND ----------

# DBTITLE 1,Generate Customer Insights Summary with AI Analysis
# MAGIC %sql
# MAGIC -- Create a temporary view for customer executive summary using AI
# MAGIC CREATE OR REPLACE TEMP VIEW customer_summary_view AS
# MAGIC SELECT 
# MAGIC   'customer_profile' AS domain_name,
# MAGIC   'Customer Profile Overview' AS summary_title,
# MAGIC
# MAGIC   ai_query(
# MAGIC     'databricks-gpt-oss-20b',
# MAGIC     CONCAT(
# MAGIC       'You are a senior insurance executive advisor. ',
# MAGIC       'Analyze the KPI JSON and produce a concise executive summary with insights, risks, and recommendations. ',
# MAGIC       'Highlight customer risk segments and opportunities: ',
# MAGIC       kpi_json
# MAGIC     )
# MAGIC   ) AS executive_summary,
# MAGIC
# MAGIC   kpi_json,
# MAGIC   current_timestamp() AS generation_timestamp,
# MAGIC   'databricks-gpt-oss-20b' AS model_name,
# MAGIC   'SUCCESS' AS status
# MAGIC
# MAGIC FROM customer_kpi_view;

# COMMAND ----------

# DBTITLE 1,Construct Policy Summary View with Executive Insights
# MAGIC %sql
# MAGIC -- Create a temporary view for policy executive summary using AI
# MAGIC CREATE OR REPLACE TEMP VIEW policy_summary_view AS
# MAGIC SELECT 
# MAGIC   'policy_portfolio' AS domain_name,
# MAGIC   'Policy Portfolio Overview' AS summary_title,
# MAGIC
# MAGIC   ai_query(
# MAGIC     'databricks-gpt-oss-20b',
# MAGIC     CONCAT(
# MAGIC       'You are a senior insurance executive advisor. ',
# MAGIC       'Analyze the KPI JSON and produce a concise executive summary with insights, risks, and recommendations: ',
# MAGIC       kpi_json
# MAGIC     )
# MAGIC   ) AS executive_summary,
# MAGIC
# MAGIC   kpi_json,
# MAGIC   current_timestamp() AS generation_timestamp,
# MAGIC   'databricks-gpt-oss-20b' AS model_name,
# MAGIC   'SUCCESS' AS status
# MAGIC
# MAGIC FROM policy_kpi_view;

# COMMAND ----------

# DBTITLE 1,Create Gold Table for AI Business Insights in Prime Ins ...
# MAGIC %sql
# MAGIC -- Table to store AI-generated business insights summaries for insurance domains
# MAGIC CREATE TABLE IF NOT EXISTS primeinsurance.gold.ai_business_insights (
# MAGIC   domain_name STRING,           -- Domain of the summary (e.g., claims, policy, customer)
# MAGIC   summary_title STRING,         -- Title of the executive summary
# MAGIC   executive_summary STRING,     -- AI-generated executive summary text
# MAGIC   kpi_json STRING,              -- KPI metrics in JSON format
# MAGIC   generation_timestamp TIMESTAMP,-- Timestamp when summary was generated
# MAGIC   model_name STRING,            -- Name of the AI model used
# MAGIC   status STRING                 -- Status of the summary generation
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Insert Combined Claims, Policy, and Customer Insights D ...
# MAGIC %sql
# MAGIC -- Insert AI-generated executive summaries for claims, policy, and customer domains into gold insights table
# MAGIC INSERT INTO primeinsurance.gold.ai_business_insights
# MAGIC SELECT * FROM (
# MAGIC   -- Claims
# MAGIC   SELECT * FROM claims_summary_view
# MAGIC   UNION ALL
# MAGIC   -- Policy
# MAGIC   SELECT * FROM policy_summary_view
# MAGIC   UNION ALL
# MAGIC   -- Customer
# MAGIC   SELECT * FROM customer_summary_view
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table primeinsurance.gold.ai_business_insights;

# COMMAND ----------

# DBTITLE 1,Retrieve Business Insights from Gold AI Database
# MAGIC %sql
# MAGIC -- Retrieve all AI-generated business insights summaries for insurance domains
# MAGIC SELECT * 
# MAGIC FROM primeinsurance.gold.ai_business_insights;

# COMMAND ----------

# DBTITLE 1,Analyze High Claim Rejection Rate Insights for Insuranc ...
# MAGIC %sql
# MAGIC -- Use AI to explain implications of a high claim rejection rate in insurance
# MAGIC SELECT 
# MAGIC   ai_query(
# MAGIC     'databricks-gpt-oss-20b',
# MAGIC     'What does a high claim rejection rate indicate in insurance?'
# MAGIC   ) AS explanation;

# COMMAND ----------

# DBTITLE 1,Generate Business Impact Analysis for Average Claim Amo ...
# MAGIC %sql
# MAGIC -- Use AI to explain the business impact of the average claim amount in insurance
# MAGIC SELECT 
# MAGIC   ai_query(
# MAGIC     'databricks-gpt-oss-20b',
# MAGIC     CONCAT(
# MAGIC       'Explain business impact of this average claim amount: ',
# MAGIC       CAST(AVG(injury + property + vehicle) AS STRING)
# MAGIC     )
# MAGIC   ) AS explanation
# MAGIC FROM primeinsurance.gold.fact_claims;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   domain_name,
# MAGIC   summary_title,
# MAGIC   executive_summary
# MAGIC FROM primeinsurance.gold.ai_business_insights
# MAGIC WHERE domain_name = 'claims_performance'
# MAGIC LIMIT 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   ai_query(
# MAGIC     'databricks-gpt-oss-20b',
# MAGIC     'What does a high claim rejection rate indicate in insurance?'
# MAGIC   ) AS explanation;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   ai_query(
# MAGIC     'databricks-gpt-oss-20b',
# MAGIC     CONCAT(
# MAGIC       'Explain business impact of average claim amount: ',
# MAGIC       CAST(AVG(injury + property + vehicle) AS STRING)
# MAGIC     )
# MAGIC   ) AS explanation
# MAGIC FROM primeinsurance.gold.fact_claims;