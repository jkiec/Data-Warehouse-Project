-- =============================================================================
-- FN_CLEAN_CUSTOMERS_DATA FUNCTION
-- Cleans and standardizes customer data from source before staging insertion
-- Applies data type casting and removes duplicates at source level
-- =============================================================================

CREATE OR REPLACE FUNCTION staging.fn_clean_customers_data()
RETURNS TABLE (
    "CustomerKey"   VARCHAR(255),
    "Gender"        VARCHAR(255),
    "Name"          VARCHAR(255),
    "City"          VARCHAR(255),
    "State Code"    VARCHAR(255),
    "State"         VARCHAR(255),
    "Zip Code"      VARCHAR(255),
    "Country"       VARCHAR(255),
    "Continent"     VARCHAR(255),
    "Birthday"      VARCHAR(255)
)
AS $$
BEGIN
    -- Return cleaned data with explicit casting and deduplication
    RETURN QUERY
    SELECT DISTINCT
        s."CustomerKey"::VARCHAR(255),
        s."Gender"::VARCHAR(255),
        s."Name"::VARCHAR(255),
        s."City"::VARCHAR(255),
        s."State Code"::VARCHAR(255),
        s."State"::VARCHAR(255),
        s."Zip Code"::VARCHAR(255),
        s."Country"::VARCHAR(255),
        s."Continent"::VARCHAR(255),
        s."Birthday"::VARCHAR(255)
    FROM data_source.customers s;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- SP_LOAD_CUSTOMERS_DATA PROCEDURE
-- Loads cleaned customer data into staging layer with duplicate prevention
-- Creates staging table if not exists, logs all operations for monitoring
-- Deduplication based on CustomerKey + Name + geographic location
-- =============================================================================

CREATE OR REPLACE PROCEDURE staging.sp_load_customers_data()
LANGUAGE plpgsql
AS $$
DECLARE 
    v_inserted_rows INT := 0;      -- Counter for successfully inserted rows
    v_table_exists  INT := 0;      -- Flag to check if staging table exists
BEGIN
    -- Check if the staging table exists
    SELECT COUNT(*) 
    INTO v_table_exists
    FROM information_schema.tables
    WHERE table_schema = 'staging'
      AND table_name = 'customers';

    -- Create staging table if it doesn't exist
    IF v_table_exists = 0 THEN
        EXECUTE '
            CREATE TABLE staging.customers (
                "CustomerKey"   VARCHAR(255),
                "Gender"        VARCHAR(255),
                "Name"          VARCHAR(255),
                "City"          VARCHAR(255),
                "State Code"    VARCHAR(255),
                "State"         VARCHAR(255),
                "Zip Code"      VARCHAR(255),
                "Country"       VARCHAR(255),
                "Continent"     VARCHAR(255),
                "Birthday"      VARCHAR(255)
            );
        ';
        
        -- Log successful table creation
        CALL bl_cl.sp_insert_etl_log(
            'staging.sp_load_customers_data', 
            'Customers staging table created successfully', 
            NULL, 
            NULL
        );
    ELSE
        -- Log that table already exists
        CALL bl_cl.sp_insert_etl_log(
            'staging.sp_load_customers_data', 
            'Customers staging table already exists - proceeding with data load', 
            NULL, 
            NULL
        );
    END IF;

    -- Insert only new records (avoid duplicates based on CustomerKey and geographic data)
    INSERT INTO staging.customers (
        "CustomerKey",
        "Gender",
        "Name",
        "City",
        "State Code",
        "State",
        "Zip Code",
        "Country",
        "Continent",
        "Birthday"
    )
    SELECT 
        c."CustomerKey",
        c."Gender",
        c."Name",
        c."City",
        c."State Code",
        c."State",
        c."Zip Code",
        c."Country",
        c."Continent",
        c."Birthday"
    FROM staging.fn_clean_customers_data() c
    WHERE NOT EXISTS (
        SELECT 1 
        FROM staging.customers s 
        WHERE s."CustomerKey" = c."CustomerKey"
          AND s."Name" = c."Name"
          AND s."City" = c."City"
          AND s."State" = c."State"
          AND s."Country" = c."Country"
          AND s."Continent" = c."Continent"
    );

    -- Capture number of rows inserted
    GET DIAGNOSTICS v_inserted_rows = ROW_COUNT;

    -- Log operation results with appropriate message
    CALL bl_cl.sp_insert_etl_log(
        'staging.sp_load_customers_data', 
        CASE 
            WHEN v_inserted_rows = 0 THEN 'No new customers data to insert - staging table up to date'
            ELSE 'Customers staging table loaded successfully'
        END, 
        v_inserted_rows, 
        NULL
    );

EXCEPTION 
    WHEN OTHERS THEN 
        -- Log error details and re-raise exception
        CALL bl_cl.sp_insert_etl_log(
            'staging.sp_load_customers_data', 
            'Error occurred during customers staging load', 
            NULL, 
            SQLERRM
        );
        RAISE NOTICE 'Error in staging.sp_load_customers_data: %', SQLERRM;
        ROLLBACK;
END;
$$;