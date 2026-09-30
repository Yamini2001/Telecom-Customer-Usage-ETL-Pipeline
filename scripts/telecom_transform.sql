CREATE DATABASE IF NOT EXISTS telecom_dw;

DROP TABLE IF EXISTS telecom_dw.customer_usage_raw;

CREATE EXTERNAL TABLE telecom_dw.customer_usage_raw (
    usage_id INT,
    customer_id STRING,
    plan STRING,
    call_minutes INT,
    sms_count INT,
    data_gb DECIMAL(10,2),
    usage_date DATE,
    circle STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/telecom/raw/customer_usage';

DROP TABLE IF EXISTS telecom_dw.customer_usage_fact;

CREATE TABLE telecom_dw.customer_usage_fact AS
SELECT
    usage_id,
    customer_id,
    UPPER(plan) AS plan,
    call_minutes,
    sms_count,
    data_gb,
    call_minutes * 1.0 AS call_usage_score,
    usage_date,
    UPPER(circle) AS circle
FROM telecom_dw.customer_usage_raw
WHERE call_minutes >= 0
  AND sms_count >= 0
  AND data_gb >= 0;

DROP TABLE IF EXISTS telecom_dw.customer_usage_summary;

CREATE TABLE telecom_dw.customer_usage_summary AS
SELECT
    customer_id,
    MAX(plan) AS plan,
    SUM(call_minutes) AS total_call_minutes,
    SUM(sms_count) AS total_sms,
    SUM(data_gb) AS total_data_gb
FROM telecom_dw.customer_usage_fact
GROUP BY customer_id;