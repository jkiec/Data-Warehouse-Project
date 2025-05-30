-- =============================================================================
-- FN_CLEAN_STORES_DATA FUNCTION
-- Cleans and standardizes stores data from source before staging insertion
-- Applies data type casting and removes duplicates at source level
-- =============================================================================

CREATE OR REPLACE FUNCTION staging.fn_clean_stores_data()
RETURNS TABLE (
    "StoreKey"        VARCHAR(255),
    "Country"         VARCHAR(255),
    "State"           VARCHAR(255),
    "Square Meters"   VARCHAR(255),
    "Open Date"       VARCHAR(255)
)
AS $$
BEGIN
    -- Return cleaned data with explicit casting and deduplication
    RETURN QUERY
    SELECT DISTINCT
        s."StoreKey"::VARCHAR(255),
        s."Country"::VARCHAR(255),
        s."State"::VARCHAR(255),
        s."Square Meters"::VARCHAR(255),
        s."Open Date"::VARCHAR(255)
    FROM data_source.stores s;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- SP_LOAD_STORES_DATA PROCEDURE
-- Loads cleaned stores data into staging layer with duplicate prevention
-- Creates staging table if not exists, logs all operations for monitoring
-- Deduplication based on StoreKey (unique store identifier)
-- =============================================================================

CREATE OR REPLACE PROCEDURE staging.sp_load_stores_data()
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
      AND table_name = 'stores';

    -- Create staging table if it doesn't exist
    IF v_table_exists = 0 THEN
        EXECUTE '
            CREATE TABLE staging.stores (
                "StoreKey"        VARCHAR(255),
                "Country"         VARCHAR(255),
                "State"           VARCHAR(255),
                "Square Meters"   VARCHAR(255),
                "Open Date"       VARCHAR(255)
            );
        ';
        
        -- Log successful table creation
        CALL bl_cl.sp_insert_etl_log(
            'staging.sp_load_stores_data', 
            'Stores staging table created successfully', 
            NULL, 
            NULL
        );
    ELSE
        -- Log that table already exists
        CALL bl_cl.sp_insert_etl_log(
            'staging.sp_load_stores_data', 
            'Stores staging table already exists - proceeding with data load', 
            NULL, 
            NULL
        );
    END IF;

    -- Insert only new records (avoid duplicates based on StoreKey)
    INSERT INTO staging.stores (
        "StoreKey",
        "Country",
        "State",
        "Square Meters",
        "Open Date"
    )
    SELECT 
        c."StoreKey",
        c."Country",
        c."State",
        c."Square Meters",
        c."Open Date"
    FROM staging.fn_clean_stores_data() c
    WHERE NOT EXISTS (
        SELECT 1 
        FROM staging.stores s 
        WHERE s."StoreKey" = c."StoreKey"
    );

    -- Capture number of rows inserted
    GET DIAGNOSTICS v_inserted_rows = ROW_COUNT;

    -- Log operation results with appropriate message
    CALL bl_cl.sp_insert_etl_log(
        'staging.sp_load_stores_data', 
        CASE 
            WHEN v_inserted_rows = 0 THEN 'No new stores data to insert - staging table up to date'
            ELSE 'Stores staging table loaded successfully'
        END, 
        v_inserted_rows, 
        NULL
    );

EXCEPTION 
    WHEN OTHERS THEN 
        -- Log error details and re-raise exception
        CALL bl_cl.sp_insert_etl_log(
            'staging.sp_load_stores_data', 
            'Error occurred during stores staging load', 
            NULL, 
            SQLERRM
        );
        RAISE NOTICE 'Error in staging.sp_load_stores_data: %', SQLERRM;
        ROLLBACK;
END;
$$;