-- =============================================================================
-- Data Quality Checks - Azure Synapse Analytics
-- Azure Real-Time Data Quality & Anomaly Detection Pipeline
-- =============================================================================
-- Run these queries against the Silver and Gold Delta tables registered
-- as external tables or views in Azure Synapse Analytics.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 1. Overall Data Quality Score by Date and Source
-- -----------------------------------------------------------------------------
SELECT
    report_date,
    source_system,
    SUM(total_records)   AS total_records,
    SUM(anomaly_count)   AS total_anomalies,
    SUM(clean_count)     AS total_clean,
    ROUND(
        CAST(SUM(clean_count) AS FLOAT) / NULLIF(SUM(total_records), 0) * 100,
        2
    )                    AS overall_dq_score_pct
FROM gold_dq_scorecard
GROUP BY report_date, source_system
ORDER BY report_date DESC;


-- -----------------------------------------------------------------------------
-- 2. Ticker-Level Anomaly Rate (Last 30 Days)
-- -----------------------------------------------------------------------------
SELECT
    ticker,
    COUNT(*)                                        AS total_records,
    SUM(CASE WHEN is_anomaly = TRUE THEN 1 ELSE 0 END) AS anomaly_count,
    ROUND(
        CAST(SUM(CASE WHEN is_anomaly = TRUE THEN 1 ELSE 0 END) AS FLOAT)
        / NULLIF(COUNT(*), 0) * 100, 2
    )                                               AS anomaly_rate_pct
FROM gold_anomaly_records
WHERE trade_date >= DATEADD(DAY, -30, GETDATE())
GROUP BY ticker
ORDER BY anomaly_rate_pct DESC;


-- -----------------------------------------------------------------------------
-- 3. Quarantined Records Summary
-- -----------------------------------------------------------------------------
SELECT
    batch_id,
    quarantine_reason,
    COUNT(*)             AS quarantine_count,
    MIN(quarantine_timestamp) AS first_seen,
    MAX(quarantine_timestamp) AS last_seen
FROM silver_quarantine
GROUP BY batch_id, quarantine_reason
ORDER BY last_seen DESC;


-- -----------------------------------------------------------------------------
-- 4. Null Check per Column (Silver Layer)
-- -----------------------------------------------------------------------------
SELECT
    COUNT(*)                                              AS total_records,
    SUM(CASE WHEN ticker      IS NULL THEN 1 ELSE 0 END)  AS null_ticker,
    SUM(CASE WHEN close_price IS NULL THEN 1 ELSE 0 END)  AS null_close_price,
    SUM(CASE WHEN open_price  IS NULL THEN 1 ELSE 0 END)  AS null_open_price,
    SUM(CASE WHEN volume      IS NULL THEN 1 ELSE 0 END)  AS null_volume,
    SUM(CASE WHEN trade_date  IS NULL THEN 1 ELSE 0 END)  AS null_trade_date,
    ROUND(
        CAST(SUM(CASE WHEN ticker IS NOT NULL AND close_price IS NOT NULL
                           AND volume IS NOT NULL AND trade_date IS NOT NULL
                      THEN 1 ELSE 0 END) AS FLOAT)
        / NULLIF(COUNT(*), 0) * 100, 2
    )                                                     AS completeness_score_pct
FROM silver_validated;


-- -----------------------------------------------------------------------------
-- 5. Business Rule Violation Breakdown
-- -----------------------------------------------------------------------------
SELECT
    'Price Not Positive'  AS rule_name,
    COUNT(*) AS violation_count
FROM silver_validated WHERE rule_price_positive = FALSE
UNION ALL
SELECT
    'Volume Not Positive',
    COUNT(*) FROM silver_validated WHERE rule_volume_positive = FALSE
UNION ALL
SELECT
    'High < Low Price',
    COUNT(*) FROM silver_validated WHERE rule_high_gte_low = FALSE
UNION ALL
SELECT
    'Null Ticker',
    COUNT(*) FROM silver_validated WHERE rule_no_null_ticker = FALSE
UNION ALL
SELECT
    'Null Trade Date',
    COUNT(*) FROM silver_validated WHERE rule_no_null_date = FALSE
ORDER BY violation_count DESC;


-- -----------------------------------------------------------------------------
-- 6. Rolling 7-Day DQ Score Trend per Ticker
-- -----------------------------------------------------------------------------
SELECT
    report_date,
    ticker,
    dq_score,
    AVG(dq_score) OVER (
        PARTITION BY ticker
        ORDER BY report_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7d_avg_dq_score
FROM gold_dq_scorecard
ORDER BY ticker, report_date DESC;


-- -----------------------------------------------------------------------------
-- 7. Pipeline SLA Compliance (Records processed within 5 minutes)
-- -----------------------------------------------------------------------------
SELECT
    batch_id,
    MIN(ingestion_timestamp)               AS pipeline_start,
    MAX(ingestion_timestamp)               AS pipeline_end,
    DATEDIFF(
        MINUTE,
        MIN(ingestion_timestamp),
        MAX(ingestion_timestamp)
    )                                      AS duration_minutes,
    COUNT(*)                               AS records_processed,
    CASE
        WHEN DATEDIFF(MINUTE, MIN(ingestion_timestamp), MAX(ingestion_timestamp)) <= 5
        THEN 'PASS' ELSE 'FAIL'
    END                                    AS sla_status
FROM bronze_raw
GROUP BY batch_id
ORDER BY pipeline_start DESC;
