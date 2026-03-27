-- ==============================================================================
-- PRIMEINSURANCE GOLD LAYER: BUSINESS AGGREGATIONS
-- ==============================================================================
-- These Materialized Views power the executive dashboards. They are pre-computed 
-- to ensure lightning-fast load times for BI tools and guarantee that metrics 
-- stay perfectly synced with the unified dimensional model.
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- AGGREGATION 1: CUSTOMER IDENTITY & COMPLIANCE
-- Business Failure Addressed: Regulatory Pressure (Inflated customer counts)
-- ------------------------------------------------------------------------------
-- Logic: Queries the deduplicated 'dim_customer' table (which survived our Silver 
-- layer CDC logic) to provide regulators with an exact, auditable count of 
-- unique policyholders grouped by Region.
-- ------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW primeinsurance.gold.agg_customer_identity AS
SELECT 
    region, 
    
    -- Count of strictly unique customers per region
    COUNT(customer_id) AS total_unique_customers, 
    
    -- Financial health metric for the region
    ROUND(AVG(balance), 2) AS average_account_balance

FROM primeinsurance.gold.dim_customer
GROUP BY region;


-- ------------------------------------------------------------------------------
-- AGGREGATION 2: CLAIM PERFORMANCE & BOTTLENECKS
-- Business Failure Addressed: Claims Backlog & No Unified View
-- ------------------------------------------------------------------------------
-- Logic: Joins the Fact table to Customer and Policy dimensions to analyze 
-- processing times and rejection rates by Region and Policy Type (policy_csl).
-- ------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW primeinsurance.gold.agg_claim_performance AS
SELECT
    c.region,
    p.policy_csl AS policy_type,
    
    -- Volume metric
    COUNT(f.claim_id) AS total_claims,
    
    -- Efficiency metric: Identifies if processing time exceeds the 7-day benchmark
    ROUND(AVG(f.processing_time_days), 1) AS avg_processing_days,
    
    -- Quality metric: Calculates percentage of claims rejected
    ROUND(
        (SUM(CASE WHEN f.claim_rejected = true THEN 1 ELSE 0 END) / COUNT(f.claim_id)) * 100, 
        2
    ) AS rejection_rate_percent,
    
    -- Financial Impact: Sums all damage types into a total claim value
    SUM(f.vehicle + f.property + f.injury) AS total_claim_financial_value

FROM primeinsurance.gold.fact_claims f
-- Join to get the geographical region
LEFT JOIN primeinsurance.gold.dim_customer c 
    ON f.customer_id = c.customer_id
-- Join to get the policy type grouping
LEFT JOIN primeinsurance.gold.dim_policy p 
    ON f.policy_id = p.policy_id
GROUP BY c.region, p.policy_csl;


-- ------------------------------------------------------------------------------
-- AGGREGATION 3: INVENTORY STATUS & AGING ALERTS
-- Business Failure Addressed: Revenue Leakage (Unsold cars aging unnoticed)
-- ------------------------------------------------------------------------------
-- Logic: Scans the fact_sales table for active inventory (is_sold = 0) that has 
-- sat for more than 60 days. Joins to dim_car to group by the specific Model.
-- ------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW primeinsurance.gold.agg_inventory_status AS
SELECT
    s.region,
    c.model,
    
    -- Identifies how many vehicles are taking up physical lot space
    COUNT(s.sales_id) AS aging_unsold_cars,
    
    -- Identifies the total capital frozen in this aging inventory
    SUM(s.original_selling_price) AS locked_revenue_at_risk,
    
    -- Identifies the worst-offender (the car sitting the longest in this group)
    MAX(s.days_since_listing) AS max_days_on_market

FROM primeinsurance.gold.fact_sales s
-- Join to get the vehicle model name
LEFT JOIN primeinsurance.gold.dim_car c 
    ON s.car_id = c.car_id
-- FILTER: Only look at unsold cars that are strictly older than 60 days
WHERE s.is_sold = 0 AND s.days_since_listing > 60
GROUP BY s.region, c.model;