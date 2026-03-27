/* ==============================================================================
   PRIME INSURANCE: GOLD LAYER (STAR SCHEMA)
   Architecture: Streaming Tables (Append-Only/Upsert) reading from Silver.
   ============================================================================== */

-- ==============================================================================
-- 1. FACT TABLE: CLAIMS
-- Purpose: Core transactional table for claims analysis.
-- Note: This performs a stream-to-static join to enrich claims with policy data.
-- ==============================================================================
CREATE OR REFRESH STREAMING TABLE primeinsurance.gold.fact_claims AS
SELECT
    c.ClaimID        AS claim_id,
    c.PolicyID       AS policy_id,

    -- Pulling foreign keys from the policy dimension for easier slicing in BI
    p.customer_id,
    p.car_id,

    --  TIMELINE RECONSTRUCTION
    -- The source system provided cryptic integer offsets (day buckets). 
    -- We are translating these back into physical calendar dates using the policy bind date.
    DATE_ADD(p.policy_bind_date, c.indicate_day_bucket)           AS incident_date,
    DATE_ADD(p.policy_bind_date, c.Claim_Logged_On_day_bucket)    AS claim_logged_on,
    DATE_ADD(p.policy_bind_date, c.Claim_Processed_On_day_bucket) AS claim_processed_on,

    c.Claim_Rejected AS claim_rejected,

    -- Incident details
    c.incident_severity,
    c.incident_type,
    c.collision_type,

    -- Financial Impact (Cleaned in Silver to guarantee >= 0)
    c.injury,
    c.property,
    c.vehicle,

    -- Physical Impact
    c.number_of_vehicles_involved,
    c.bodily_injuries,
    c.witnesses,

    -- Flags
    c.property_damage,
    c.police_report_available,

    --  DERIVED BI METRICS
    -- Pre-calculating processing SLA so Tableau/PowerBI developers don't have to write DAX.
    DATEDIFF(
        DATE_ADD(p.policy_bind_date, c.Claim_Processed_On_day_bucket),
        DATE_ADD(p.policy_bind_date, c.Claim_Logged_On_day_bucket)
    ) AS processing_time_days

FROM STREAM(primeinsurance.silver.claims_silver) c
-- Left join ensures we don't drop claims if the policy record is temporarily delayed
LEFT JOIN primeinsurance.silver.policy_silver p
    ON c.PolicyID = p.policy_number;


-- ==============================================================================
-- 2. DIMENSION TABLE: POLICY
-- Purpose: Attributes related to the insurance contract.
-- ==============================================================================
CREATE OR REFRESH STREAMING TABLE primeinsurance.gold.dim_policy AS
SELECT
    policy_number      AS policy_id,

    -- Foreign Keys bridging to Customer and Car dimensions
    customer_id,
    car_id,

    policy_bind_date,
    policy_state,

    -- Coverage & Financial Limits
    policy_csl,
    policy_deductable,
    policy_annual_premium,
    umbrella_limit

FROM STREAM(primeinsurance.silver.policy_silver);


-- ==============================================================================
-- 3. DIMENSION TABLE: CUSTOMER
-- Purpose: Demographic and financial profile of the policyholder.
-- Note: Standardizing all CamelCase source columns to snake_case for Gold standards.
-- ==============================================================================
CREATE OR REFRESH STREAMING TABLE primeinsurance.gold.dim_customer AS
SELECT
    CustomerID AS customer_id,

    -- Geography (Standardized to full words in Silver)
    Region     AS region,
    State      AS state,
    City       AS city,

    -- Demographics
    Job        AS job,
    Marital    AS marital,
    Education  AS education,

    -- Financial Standing
    Default    AS default_flag,
    Balance    AS balance,
    HHInsurance,
    CarLoan

FROM STREAM(primeinsurance.silver.customers_silver);


-- ==============================================================================
-- 4. DIMENSION TABLE: CAR
-- Purpose: Physical asset details.
-- Note: All string-contaminated metrics (e.g., "23.4 kmpl") were scrubbed into 
--       pure numerics in Silver, making this instantly ready for aggregations.
-- ==============================================================================
CREATE OR REFRESH STREAMING TABLE primeinsurance.gold.dim_car AS
SELECT
    car_id,

    name,
    model,

    fuel,
    transmission,

    -- Clean Numerical Metrics
    km_driven,
    mileage,
    engine,
    max_power,
    
    -- Torque remains a string due to highly unstructured source formats
    torque,

    seats

FROM STREAM(primeinsurance.silver.cars_silver);


-- ==============================================================================
-- 5. FACT TABLE: SALES
-- Purpose: Transactional record of vehicle sales.
-- ==============================================================================
CREATE OR REFRESH STREAMING TABLE primeinsurance.gold.fact_sales AS
SELECT
    sales_id,
    car_id,

    -- Timestamps physically cast in Silver
    ad_placed_on,
    sold_on,

    Region AS region,
    State  AS state,
    City   AS city,

    original_selling_price,

    seller_type,
    owner,

    --  DERIVED BI METRICS
    -- Helps analysts calculate average time-on-market for unsold inventory
    DATEDIFF(CURRENT_DATE, ad_placed_on) AS days_since_listing,

    -- Binary flag for easy funnel conversion counting in BI tools
    CASE 
        WHEN sold_on IS NULL THEN 0 
        ELSE 1 
    END AS is_sold

FROM STREAM(primeinsurance.silver.sales_silver);