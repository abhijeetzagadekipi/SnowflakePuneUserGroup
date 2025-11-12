CREATE DATABASE SF_DE_FEATURES;
CREATE WAREHOUSE SF_DE_WH;
CREATE SCHEMA DE_SCH;
-----------------------------------------------------------------------------------
-- =====================================================================
-- SNOWFLAKE ASYNC STORED PROCEDURES + DATA LINEAGE DEMO
-- Demonstrating: 
--   1. Async Execution (90 seconds → 30 seconds)
--   2. Data Lineage Tracking (automatic flow tracking)
-- =====================================================================

-- =====================================================================
-- STEP 1: CREATE MINIMAL SOURCE AND TARGET TABLES FOR LINEAGE
-- =====================================================================

CREATE OR REPLACE TABLE source_sales (id INT, amount DECIMAL(10, 2));
CREATE OR REPLACE TABLE processed_sales (id INT, amount DECIMAL(10, 2), processed_at TIMESTAMP);

CREATE OR REPLACE TABLE source_inventory (id INT, qty INT);
CREATE OR REPLACE TABLE processed_inventory (id INT, qty INT, processed_at TIMESTAMP);

CREATE OR REPLACE TABLE source_customers (id INT, name VARCHAR);
CREATE OR REPLACE TABLE processed_customers (id INT, name VARCHAR, processed_at TIMESTAMP);

-- Insert minimal sample data
INSERT INTO source_sales VALUES (1, 1000);
INSERT INTO source_inventory VALUES (1, 100);
INSERT INTO source_customers VALUES (1, 'John');


-- =====================================================================
-- STEP 2: CREATE THREE INDIVIDUAL STORED PROCEDURES
-- Each simulates 30-second processing + inserts one row (minimal interaction)
-- =====================================================================

CREATE OR REPLACE PROCEDURE process_sales()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    CALL SYSTEM$WAIT(30);
    INSERT INTO processed_sales SELECT id, amount, CURRENT_TIMESTAMP() FROM source_sales LIMIT 1;
    RETURN 'Sales processed';
END;
$$;


CREATE OR REPLACE PROCEDURE process_inventory()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    CALL SYSTEM$WAIT(30);
    INSERT INTO processed_inventory SELECT id, qty, CURRENT_TIMESTAMP() FROM source_inventory LIMIT 1;
    RETURN 'Inventory processed';
END;
$$;

CREATE OR REPLACE PROCEDURE process_customers()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    CALL SYSTEM$WAIT(30);
    INSERT INTO processed_customers SELECT id, name, CURRENT_TIMESTAMP() FROM source_customers LIMIT 1;
    RETURN 'Customers processed';
END;
$$;


-- =====================================================================
-- STEP 3: THE OLD WAY - SEQUENTIAL EXECUTION
-- Total Execution Time: ~90 seconds (30 + 30 + 30)
-- =====================================================================

CREATE OR REPLACE PROCEDURE etl_pipeline_old_way()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    elapsed_seconds INT;
BEGIN
    start_time := CURRENT_TIMESTAMP();
    
    -- Sequential execution - waits for each to complete
    CALL process_sales();      -- Waits 30 seconds
    CALL process_inventory();  -- Waits 30 seconds
    CALL process_customers();  -- Waits 30 seconds
    
    end_time := CURRENT_TIMESTAMP();
    elapsed_seconds := DATEDIFF(SECOND, start_time, end_time);
    
    RETURN CONCAT('SEQUENTIAL MODE: Completed in ', elapsed_seconds, ' seconds');
END;
$$;


-- =====================================================================
-- STEP 4: THE NEW WAY - ASYNC EXECUTION
-- Total Execution Time: ~30 seconds (all run in parallel)
-- =====================================================================

CREATE OR REPLACE PROCEDURE etl_pipeline_async_way()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    elapsed_seconds INT;
BEGIN
    start_time := CURRENT_TIMESTAMP();
    
    -- Launch all procedures asynchronously
    ASYNC (CALL process_sales());
    ASYNC (CALL process_inventory());
    ASYNC (CALL process_customers());
    
    -- Wait for all async operations to complete
    AWAIT ALL;
    
    end_time := CURRENT_TIMESTAMP();
    elapsed_seconds := DATEDIFF(SECOND, start_time, end_time);
    
    RETURN CONCAT('ASYNC MODE: Completed in ', elapsed_seconds, ' seconds');
END;
$$;


-- =====================================================================
-- STEP 5: RUN BOTH MODES
-- =====================================================================

-- RUN 1: Sequential Mode (~90 seconds)
CALL etl_pipeline_old_way();

-- RUN 2: Async Mode (~30 seconds)
CALL etl_pipeline_async_way();


-- =====================================================================
-- STEP 6: VIEW DATA LINEAGE
-- =====================================================================


-- =====================================================================
-- SUMMARY - ASYNC + DATA LINEAGE
-- =====================================================================
--
-- FEATURE 1: ASYNC EXECUTION
-- Sequential: 30 + 30 + 30 = 90 seconds
-- Async:      Max(30, 30, 30) = 30 seconds
-- Improvement: 66.7% faster (3x speedup) ✓
--
-- FEATURE 2: DATA LINEAGE TRACKING
-- Automatic tracking shows:
--   • SOURCE_SALES     → PROCESSED_SALES
--   • SOURCE_INVENTORY → PROCESSED_INVENTORY
--   • SOURCE_CUSTOMERS → PROCESSED_CUSTOMERS
--   • All transformations tracked in access_history ✓
--
-- KEY INSIGHTS:
-- 1. Async runs all 3 processes in parallel = 67% faster
-- 2. Data lineage automatically tracks data dependencies
-- 3. Access history shows what broke if you change a source table
-- 4. Perfect for identifying: "If I change SOURCE_SALES, what breaks?"
--    Answer: PROCESSED_SALES (shown in access_history)
-- =====================================================================

-- =====================================================================
-- =====================================================================
-- SNOWFLAKE REAL-TIME ALERTS DEMO
-- Using existing tables from Async SP demo
-- Demonstrating: Instant alerts vs periodic checks
-- =====================================================================

USE DATABASE SF_DE_FEATURES;
USE SCHEMA DE_SCH;
USE WAREHOUSE SF_DE_WH;

-- =====================================================================
-- STEP 0: CREATE EMAIL INTEGRATION (Required for alerts)
-- =====================================================================

-- Switch to ACCOUNTADMIN role (required for creating integrations)
USE ROLE ACCOUNTADMIN;

-- Create notification integration for email
CREATE OR REPLACE NOTIFICATION INTEGRATION my_email_integration
  TYPE = EMAIL
  ENABLED = TRUE;

-- Grant usage to your role
GRANT USAGE ON INTEGRATION my_email_integration TO ROLE SYSADMIN;

-- Grant EXECUTE ALERT privilege (required to create alerts)
GRANT EXECUTE ALERT ON ACCOUNT TO ROLE SYSADMIN;

-- Switch back to your working role
USE ROLE SYSADMIN;

-- Verify integration created
SHOW INTEGRATIONS LIKE 'my_email_integration';

-- =====================================================================
-- USING EXISTING TABLES (from Async SP demo):
-- • processed_sales (id, amount, processed_at)
-- • processed_inventory (id, qty, processed_at)
-- =====================================================================

-- =====================================================================
-- STEP 1: SCHEDULED ALERT (Periodic Check - Every 1 Minute for Testing)
-- Monitors sales amount periodically
-- =====================================================================
CREATE OR REPLACE ALERT high_sales_monitor
  WAREHOUSE = SF_DE_WH
  SCHEDULE = '1 MINUTE'  -- 
  IF (EXISTS (
    SELECT 1 
    FROM processed_sales 
    WHERE amount > 5000
      AND processed_at >= SNOWFLAKE.ALERT.LAST_SUCCESSFUL_SCHEDULED_TIME()
  ))
  THEN CALL SYSTEM$SEND_EMAIL(
    'my_email_integration',
    'antariksha.a.labade@kipi.ai, rupesh.s.neve@kipi.ai',
    'High Sales Alert',
    'Sales amount exceeded $5000.'
  );

-- =====================================================================
-- STEP 2: REAL-TIME ALERT (Instant Trigger on New Data)
-- Triggers immediately when low inventory detected
-- =====================================================================

CREATE OR REPLACE ALERT low_inventory_alert
  WAREHOUSE = SF_DE_WH
  -- NO SCHEDULE = triggers instantly on INSERT
  IF (EXISTS (
    SELECT 1 
    FROM processed_inventory 
    WHERE qty < 10
      AND processed_at >= SNOWFLAKE.ALERT.LAST_SUCCESSFUL_SCHEDULED_TIME()
  ))
  THEN CALL SYSTEM$SEND_EMAIL(
    'my_email_integration',
    'antariksha.a.labade@kipi.ai, rupesh.s.neve@kipi.ai',
    'URGENT: Low Inventory Alert',
    'Inventory quantity dropped below 10 units. Immediate restocking required.'
  );

-- =====================================================================
-- STEP 3: ACTIVATE ALERTS
-- =====================================================================

ALTER ALERT high_sales_monitor RESUME;
ALTER ALERT low_inventory_alert RESUME;

-- =====================================================================
-- STEP 4: TEST THE ALERTS
-- =====================================================================

-- Test 1: Normal sales (no alert)
INSERT INTO processed_sales (id, amount, processed_at) 
VALUES (10, 2000, CURRENT_TIMESTAMP());

-- Test 2: High sales (scheduled alert )
INSERT INTO processed_sales (id, amount, processed_at) 
VALUES (11, 7500, CURRENT_TIMESTAMP());
EXECUTE ALERT high_sales_monitor;

-- Test 3: Normal inventory (no alert)
INSERT INTO processed_inventory (id, qty, processed_at) 
VALUES (10, 50, CURRENT_TIMESTAMP());

-- Test 4: Low inventory (real-time alert - fires IMMEDIATELY)
INSERT INTO processed_inventory (id, qty, processed_at) 
VALUES (11, 5, CURRENT_TIMESTAMP());

-- =====================================================================
-- STEP 5: MONITOR ALERT EXECUTION
-- =====================================================================

-- View alert history
-- View alert history (last 1 hour only)
SELECT 
    *
FROM TABLE(INFORMATION_SCHEMA.ALERT_HISTORY())
WHERE name IN ('HIGH_SALES_MONITOR', 'LOW_INVENTORY_ALERT')
  AND scheduled_time >= DATEADD(MIN, -10, CURRENT_TIMESTAMP())
ORDER BY scheduled_time DESC;

-- Check alert status
SHOW ALERTS LIKE '%SALES%';
SHOW ALERTS LIKE '%INVENTORY%';

-- =====================================================================
-- STEP 6: VIEW DATA
-- =====================================================================

SELECT * FROM processed_sales ORDER BY processed_at DESC LIMIT 5;
SELECT * FROM processed_inventory ORDER BY processed_at DESC LIMIT 5;

-- =====================================================================
-- CLEANUP
-- =====================================================================

-- Suspend alerts when done testing
ALTER ALERT high_sales_monitor SUSPEND;
ALTER ALERT low_inventory_alert SUSPEND;

-- =====================================================================
-- KEY DIFFERENCES
-- =====================================================================
--
-- SCHEDULED ALERT (high_sales_monitor):
-- ✓ SCHEDULE = '60 MINUTE'
-- ✓ Checks every hour
-- ✓ Use case: Sales thresholds, periodic monitoring
-- ✓ Latency: Up to 1 hour
--
-- REAL-TIME ALERT (low_inventory_alert):
-- ✓ NO SCHEDULE
-- ✓ Triggers on INSERT instantly
-- ✓ Use case: Critical inventory levels, immediate action needed
-- ✓ Latency: Seconds
--
-- WHEN TO USE WHICH:
-- • Scheduled → Cost monitoring, daily reports, non-urgent checks
-- • Real-Time → Critical errors, inventory stockouts, fraud detection
--
-- =====================================================================


-- =====================================================================
-- DEVELOPER PRODUCTIVITY BOOST DEMO
-- Features: INSERT ALL (Multi-Table) + MERGE ALL BY NAME
-- =====================================================================
USE DATABASE SF_DE_FEATURES;
USE SCHEMA DE_SCH;
USE WAREHOUSE SF_DE_WH;

-- =====================================================================
-- FEATURE 1: MERGE ALL BY NAME
-- Eliminates manual column mapping - reduces code by 80%
-- =====================================================================

-- Create a staging table with updates
CREATE OR REPLACE TABLE sales_updates (
    id INT,
    amount DECIMAL(10, 2)
);
-- Insert some updates
INSERT INTO sales_updates VALUES 
    (1, 1500),  
    (2, 2500),   
    (99, 9999);  

-- View current data
SELECT * FROM source_sales;
SELECT * FROM sales_updates;

-- THE OLD WAY: Manual column mapping (error-prone, verbose)
/*
MERGE INTO source_sales t 
USING sales_updates s 
ON t.id = s.id
WHEN MATCHED THEN 
  UPDATE SET 
    t.amount = s.amount
WHEN NOT MATCHED THEN 
  INSERT (id, amount) 
  VALUES (s.id, s.amount);
*/

-- THE NEW WAY: MERGE ALL BY NAME (automatic, clean, no typos!)
MERGE INTO source_sales t 
USING sales_updates s 
ON t.id = s.id
WHEN MATCHED THEN 
  UPDATE ALL BY NAME
WHEN NOT MATCHED THEN 
  INSERT ALL BY NAME;

-- View updated data
SELECT * FROM source_sales ORDER BY id;

-- =====================================================================
-- FEATURE 2: INSERT ALL (Multi-Table Insert)
-- Route data to multiple tables with single source scan
-- =====================================================================

-- Create target tables for sales categorization
CREATE OR REPLACE TABLE high_value_sales (
    id INT,
    amount DECIMAL(10, 2),
    category VARCHAR DEFAULT 'HIGH'
);

CREATE OR REPLACE TABLE medium_value_sales (
    id INT,
    amount DECIMAL(10, 2),
    category VARCHAR DEFAULT 'MEDIUM'
);

CREATE OR REPLACE TABLE low_value_sales (
    id INT,
    amount DECIMAL(10, 2),
    category VARCHAR DEFAULT 'LOW'
);

-- Create staging data
CREATE OR REPLACE TABLE sales_staging (
    id INT,
    amount DECIMAL(10, 2)
);

INSERT INTO sales_staging VALUES 
    (101, 8000),   -- High
    (102, 3500),   -- Medium
    (103, 500),    -- Low
    (104, 12000),  -- High
    (105, 4500),   -- Medium
    (106, 800);    -- Low

-- THE OLD WAY: Multiple INSERT statements (scans source 3 times!)
/*
INSERT INTO high_value_sales( id, amount) SELECT  id, amount FROM sales_staging WHERE amount > 5000;
INSERT INTO medium_value_sales( id, amount) SELECT id, amount FROM sales_staging WHERE amount BETWEEN 1000 AND 5000;
INSERT INTO low_value_sales( id, amount)  SELECT  id, amount FROM sales_staging WHERE amount < 1000;
*/

-- THE NEW WAY: INSERT ALL (scans source only ONCE!)
INSERT ALL
  WHEN amount > 5000 THEN 
    INTO high_value_sales (id, amount)
  WHEN amount >= 1000 THEN 
    INTO medium_value_sales (id, amount)
  ELSE 
    INTO low_value_sales (id, amount)
SELECT id, amount FROM sales_staging;

-- View results
SELECT 'HIGH' as category, COUNT(*) as count, SUM(amount) as total FROM high_value_sales
UNION ALL
SELECT 'MEDIUM', COUNT(*), SUM(amount) FROM medium_value_sales
UNION ALL
SELECT 'LOW', COUNT(*), SUM(amount) FROM low_value_sales;

-- =====================================================================
-- BONUS: COMBINE BOTH FEATURES
-- Use INSERT ALL with complex routing + MERGE ALL BY NAME pattern
-- ====================================================================
-- =====================================================================
-- KEY BENEFITS SUMMARY
-- =====================================================================
--
-- MERGE ALL BY NAME:
-- ✓ 80% less code compared to manual column mapping
-- ✓ Zero typo errors - automatic column matching
-- ✓ Schema evolution friendly - column order doesn't matter
-- ✓ Cleaner, more maintainable code
--
-- INSERT ALL (Multi-Table):
-- ✓ Single source scan vs multiple scans
-- ✓ Huge performance improvement for multi-table routing
-- ✓ Conditional routing based on business logic
-- ✓ Reduces query complexity
--
-- WHEN TO USE:
-- • MERGE ALL BY NAME: UPSERT operations, CDC, data synchronization
-- • INSERT ALL: Data categorization, multi-warehouse routing, conditional splits
--
-- REAL-WORLD USE CASES:
-- • E-commerce: Route orders by value to different fulfillment centers
-- • Finance: Categorize transactions by amount/type into different ledgers
-- • Logistics: Distribute inventory across warehouses based on stock levels
-- • Analytics: Split data into hot/warm/cold storage tiers
--
-- =====================================================================