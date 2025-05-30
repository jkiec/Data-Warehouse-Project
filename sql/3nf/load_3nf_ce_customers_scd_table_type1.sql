-- =============================================================================
-- FN_GET_CUSTOMERS FUNCTION
-- Extracts and transforms customer data from staging layer
-- Splits full name into first/last name components and handles date parsing
-- Links customers to cities for complete geographic hierarchy
-- =============================================================================

CREATE OR REPLACE FUNCTION bl_3nf.fn_get_customers()
RETURNS TABLE (
    customer_src_id         VARCHAR(255),
    customer_first_name     VARCHAR(30),
    customer_last_name      VARCHAR(30),
    customer_gender         VARCHAR(10),
    customer_birthday_dt    DATE,
    customer_city_id        BIGINT,
    source_system           VARCHAR(255),
    source_entity           VARCHAR(255)
) AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT
        CAST(COALESCE(s."CustomerKey", d.CUSTOMER_SRC_ID) AS VARCHAR(255)) AS customer_src_id,
        -- Split full name into first and last name components
        CAST(COALESCE(SPLIT_PART(s."Name", ' ', 1), d.CUSTOMER_FIRST_NAME) AS VARCHAR(30)) AS customer_first_name,
        CAST(COALESCE(SPLIT_PART(s."Name", ' ', 2), d.CUSTOMER_LAST_NAME) AS VARCHAR(30)) AS customer_last_name,
        CAST(COALESCE(s."Gender", d.CUSTOMER_GENDER) AS VARCHAR(10)) AS customer_gender,
        -- Parse birthday from MM/DD/YYYY format
        COALESCE(TO_DATE(s."Birthday", 'MM/DD/YYYY'), d.CUSTOMER_BIRTHDAY_DT) AS customer_birthday_dt,
        -- Lookup city ID for geographic hierarchy linkage
        CAST(COALESCE(ci.CITY_ID, d.CUSTOMER_CITY_ID) AS BIGINT) AS customer_city_id,
        CAST('Staging' AS VARCHAR(255)) AS source_system,
        CAST('Customers' AS VARCHAR(255)) AS source_entity
    FROM staging.customers s
    -- Handle NULL values using default row
    LEFT JOIN bl_3nf.ce_customers d 
        ON d.CUSTOMER_ID = -1
    -- Link to city for complete geographic hierarchy (customer -> city -> state -> country -> continent)
    LEFT JOIN bl_3nf.ce_cities ci 
        ON UPPER(ci.CITY_NAME) = UPPER(s."City");

END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- SP_LOAD_CE_CUSTOMERS PROCEDURE
-- Implements SCD Type 1 (Slowly Changing Dimension) for customers
-- Updates existing records with new values (no history preservation)
-- Uses UPSERT pattern with ON CONFLICT for efficient processing
-- Change detection: first name, last name, city changes trigger updates
-- =============================================================================

CREATE OR REPLACE PROCEDURE bl_3nf.sp_load_ce_customers()
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;                -- Record for processing each customer
    v_rows_affected INT := 0;  -- Counter for inserted/updated rows
BEGIN
    -- ==========================================================================
    -- STEP 1: ENSURE DEFAULT ROW EXISTS FOR NULL VALUE HANDLING
    -- ==========================================================================
    
    INSERT INTO bl_3nf.ce_customers (
        CUSTOMER_ID,
        CUSTOMER_SRC_ID,
        CUSTOMER_FIRST_NAME,
        CUSTOMER_LAST_NAME,
        CUSTOMER_GENDER,
        CUSTOMER_BIRTHDAY_DT,
        CUSTOMER_CITY_ID,
        INSERT_DT,
        UPDATE_DT,
        SOURCE_SYSTEM,
        SOURCE_ENTITY
    )
    SELECT 
        -1,
        'n.a.',
        'n.a.',
        'n.a.',
        'n.a.',
        '1900-01-01',
        -1,
        '1900-01-01',
        '1900-01-01',
        'MANUAL',
        'MANUAL'
    WHERE NOT EXISTS (
        SELECT 1 
        FROM bl_3nf.ce_customers 
        WHERE CUSTOMER_ID = -1
    );

    -- ==========================================================================
    -- STEP 2: PROCESS CUSTOMER DATA WITH SCD TYPE 1 LOGIC (UPSERT)
    -- ==========================================================================
    
    FOR rec IN
        SELECT * FROM bl_3nf.fn_get_customers()
    LOOP
        -- UPSERT: Insert new customer or update existing (SCD Type 1)
        INSERT INTO bl_3nf.ce_customers (
            CUSTOMER_ID,
            CUSTOMER_SRC_ID,
            CUSTOMER_FIRST_NAME,
            CUSTOMER_LAST_NAME,
            CUSTOMER_GENDER,
            CUSTOMER_BIRTHDAY_DT,
            CUSTOMER_CITY_ID,
            INSERT_DT,
            UPDATE_DT,
            SOURCE_SYSTEM,
            SOURCE_ENTITY
        )
        VALUES (
            NEXTVAL('bl_3nf.ce_customers_seq'),
            rec.customer_src_id,
            rec.customer_first_name,
            rec.customer_last_name,
            rec.customer_gender,
            rec.customer_birthday_dt,
            rec.customer_city_id,
            CURRENT_DATE,
            CURRENT_DATE,
            rec.source_system,
            rec.source_entity
        )
        ON CONFLICT (CUSTOMER_SRC_ID)
        DO UPDATE SET
            CUSTOMER_FIRST_NAME = EXCLUDED.CUSTOMER_FIRST_NAME,
            CUSTOMER_LAST_NAME = EXCLUDED.CUSTOMER_LAST_NAME,
            CUSTOMER_CITY_ID = EXCLUDED.CUSTOMER_CITY_ID,
            UPDATE_DT = CURRENT_DATE
        WHERE 
            -- Only update if there are actual changes (change detection)
            ce_customers.CUSTOMER_FIRST_NAME IS DISTINCT FROM EXCLUDED.CUSTOMER_FIRST_NAME OR
            ce_customers.CUSTOMER_LAST_NAME IS DISTINCT FROM EXCLUDED.CUSTOMER_LAST_NAME OR
            ce_customers.CUSTOMER_CITY_ID IS DISTINCT FROM EXCLUDED.CUSTOMER_CITY_ID;

        -- Count successful operations (insert or update)
        IF FOUND THEN
            v_rows_affected := v_rows_affected + 1;
        END IF;
    END LOOP;

    -- ==========================================================================
    -- STEP 3: LOG ETL OPERATION RESULTS
    -- ==========================================================================
    
    CALL bl_cl.sp_insert_etl_log(
        'bl_3nf.sp_load_ce_customers',
        CASE 
            WHEN v_rows_affected = 0 THEN 'No new customers or changes to process - 3NF table up to date'
            ELSE 'Customers dimension loaded successfully with Type 1 SCD updates'
        END,
        v_rows_affected,
        NULL
    );

EXCEPTION
    WHEN OTHERS THEN
        -- Log error details and rollback transaction
        CALL bl_cl.sp_insert_etl_log(
            'bl_3nf.sp_load_ce_customers', 
            'Error occurred during customers dimension load', 
            NULL, 
            SQLERRM
        );
        RAISE NOTICE 'Error in bl_3nf.sp_load_ce_customers: %', SQLERRM;
        ROLLBACK;
END;
$$;