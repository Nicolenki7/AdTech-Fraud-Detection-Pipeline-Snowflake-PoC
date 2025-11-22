-- 1. CAMBIAR AL ROL CON PERMISOS DE CREACIÓN
USE ROLE SYSADMIN;

-- 2. CREAR O REEMPLAZAR UN WAREHOUSE PEQUEÑO (X-SMALL) PARA PRUEBAS (1 crédito/hora)
CREATE OR REPLACE WAREHOUSE FRAUD_ANALYSIS_WH
WITH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60 -- Suspender tras 60 segundos de inactividad para ahorrar costos
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = FALSE;

-- 3. ESTABLECER EL WAREHOUSE RECIÉN CREADO COMO EL RECURSO DE CÓMPUTO ACTIVO
USE WAREHOUSE FRAUD_ANALYSIS_WH;

-- 4. VERIFICAR TU CONTEXTO ACTUAL
SELECT CURRENT_ROLE(), CURRENT_WAREHOUSE();
-- 1. SWITCH TO THE SYSADMIN ROLE (for resource creation)
USE ROLE SYSADMIN;

-- 2. CREATE DATABASE FOR THE PROJECT
CREATE OR REPLACE DATABASE FRAUD_DETECTION_DB;

-- 3. SET THE DATABASE AND CREATE/USE THE WORKING SCHEMA
USE DATABASE FRAUD_DETECTION_DB;
CREATE OR REPLACE SCHEMA RAW_LOGS;
USE SCHEMA RAW_LOGS;

-- 4. CREATE THE LOGS TABLE (Simulating the 10TB data structure)
CREATE OR REPLACE TABLE BID_LOGS (
    TIMESTAMP TIMESTAMP_NTZ,      -- Timestamp of the event
    IP_ADDRESS VARCHAR,           -- The key for our analysis (IP)
    USER_AGENT VARCHAR,           -- User Agent string (for bot detection)
    TIME_TO_CLICK_MS INT,         -- Click latency in milliseconds (for speed detection)
    BID_REQUEST_ID VARCHAR
);

-- 5. INSERT TEST DATA (including some 'suspects' for our heuristics)
INSERT INTO BID_LOGS (TIMESTAMP, IP_ADDRESS, USER_AGENT, TIME_TO_CLICK_MS, BID_REQUEST_ID)
VALUES
    -- Normal Traffic (Slower and varied)
    (CURRENT_TIMESTAMP(), '192.168.1.1', 'Mozilla/5.0 (Windows NT 10.0)', 500, 'N1'),
    (CURRENT_TIMESTAMP(), '192.168.1.2', 'Chrome/100', 650, 'N2'),
    -- Suspect Traffic 1: Rapid-Fire Bot (IP 10.0.0.1, very fast clicks)
    (DATEADD(second, -5, CURRENT_TIMESTAMP()), '10.0.0.1', 'Bot/Script', 5, 'S1_1'),
    (DATEADD(second, -4, CURRENT_TIMESTAMP()), '10.0.0.1', 'Bot/Script', 5, 'S1_2'),
    (DATEADD(second, -3, CURRENT_TIMESTAMP()), '10.0.0.1', 'Bot/Script', 6, 'S1_3'),
    -- Suspect Traffic 2: High Volume/Rotating UAs (IP 10.0.0.2)
    (CURRENT_TIMESTAMP(), '10.0.0.2', 'Headless/Chrome', 80, 'S2_1'),
    (CURRENT_TIMESTAMP(), '10.0.0.2', 'Headless/Firefox', 75, 'S2_2'),
    (CURRENT_TIMESTAMP(), '10.0.0.2', 'Headless/Chrome', 78, 'S2_3'),
    -- More normal traffic
    (DATEADD(minute, -1, CURRENT_TIMESTAMP()), '192.168.1.3', 'Safari/15', 700, 'N3'),
    (DATEADD(minute, -2, CURRENT_TIMESTAMP()), '192.168.1.4', 'Edge/95', 850, 'N4');

-- 6. VERIFY CREATION (Expected result: 10 rows)
SELECT COUNT(*) FROM BID_LOGS;

-- 1. CONTEXT (Always ensures we are in the right place)
USE ROLE SYSADMIN;
USE WAREHOUSE FRAUD_ANALYSIS_WH;
USE DATABASE FRAUD_DETECTION_DB;
USE SCHEMA RAW_LOGS;

-- 2. EXECUTE DETECTION LOGIC
-- We use a CTE to calculate the speed (Window Function) first
CREATE OR REPLACE TABLE SUSPECT_IPS AS
WITH Velocity_Metrics AS (
    SELECT 
        IP_ADDRESS,
        TIMESTAMP,
        USER_AGENT,
        -- LAG: Look at the previous row for this IP to calculate speed
        TIMESTAMPDIFF(MILLISECOND, 
                      LAG(TIMESTAMP) OVER (PARTITION BY IP_ADDRESS ORDER BY TIMESTAMP), 
                      TIMESTAMP) AS time_diff_ms
    FROM BID_LOGS
)
-- Main Aggregation: Filter based on Volume OR Velocity
SELECT 
    IP_ADDRESS,
    COUNT(*) AS total_events,
    COUNT(DISTINCT USER_AGENT) AS distinct_uas,
    MIN(time_diff_ms) AS fastest_click_ms,
    -- Assign a Label for the Snowpark Phase
    CASE 
        WHEN MIN(time_diff_ms) < 1000 THEN 'HIGH_VELOCITY_BOT' -- < 1 second in our sample
        WHEN COUNT(DISTINCT USER_AGENT) > 1 THEN 'UA_ROTATION_BOT'
        ELSE 'VOLUME_SUSPECT'
    END AS FRAUD_REASON
FROM Velocity_Metrics
GROUP BY IP_ADDRESS
-- THE FILTER: Only keep IPs that break the rules
HAVING total_events >= 3 OR fastest_click_ms < 1000;

-- 3. VIEW THE RESULTS (Expectation: Only IPs 10.0.0.1 and 10.0.0.2)
SELECT * FROM SUSPECT_IPS;
