-- =============================================================================
-- FN_CLEAN_SALES_DATA FUNCTION
-- Cleans and standardizes sales data from source before staging insertion
-- Applies data type casting and removes duplicates at source level
-- =============================================================================

CREATE OR REPLACE FUNCTION staging.fn_clean_sales_data()
RETURNS TABLE (
    "Order Number"    VARCHAR(255),
    "Line Item"       VARCHAR(255),
    "Order Date"      VARCHAR(255),
    "Delivery Date"   VARCHAR(255),
    "CustomerKey"     VARCHAR(255),
    "StoreKey"        VARCHAR(255),
    "ProductKey"      VARCHAR(255),
    "Quantity"        VARCHAR(255),
    "Currency Code"   VARCHAR(255)
)
AS $$
BEGIN
    -- Return cleaned data with explicit casting and deduplication
    RETURN QUERY
    SELECT DISTINCT
        s."Order Number"::VARCHAR(255),
        s."Line Item"::VARCHAR(255),
        s."Order Date"::VARCHAR(255),
        s."Delivery Date"::VARCHAR(255),
        s."CustomerKey"::VARCHAR(255),
        s."StoreKey"::VARCHAR(255),
        s."ProductKey"::VARCHAR(255),
        s."Quantity"::VARCHAR(255),
        s."Currency Code"::VARCHAR(255)
    FROM data_source.sales s;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- SP_LOAD_SALES_DATA PROCEDURE
-- Loads cleaned sales data into staging layer with duplicate prevention
-- Creates staging table if not exists, logs all operations for monitoring
-- Deduplication based on Order Number (may need review for line items)
-- WARNING: Current logic may miss duplicate line items within same order
-- =============================================================================

CREATE OR REPLACE PROCEDURE staging.sp_load_sales_data()
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
      AND table_name = 'sales';

    -- Create staging table if it doesn't exist
    IF v_table_exists = 0 THEN
        EXECUTE '
            CREATE TABLE staging.sales (
                "Order Number"    VARCHAR(255),
                "Line Item"       VARCHAR(255),
                "Order Date"      VARCHAR(255),
                "Delivery Date"   VARCHAR(255),
                "CustomerKey"     VARCHAR(255),
                "StoreKey"        VARCHAR(255),
                "ProductKey"      VARCHAR(255),
                "Quantity"        VARCHAR(255),
                "Currency Code"   VARCHAR(255)
            );
        ';
        
        -- Log successful table creation
        CALL bl_cl.sp_insert_etl_log(
            'staging.sp_load_sales_data', 
            'Sales staging table created successfully', 
            NULL, 
            NULL
        );
    ELSE
        -- Log that table already exists
        CALL bl_cl.sp_insert_etl_log(
            'staging.sp_load_sales_data', 
            'Sales staging table already exists - proceeding with data load', 
            NULL, 
            NULL
        );
    END IF;

    -- Insert only new records (avoid duplicates based on Order Number)
    -- NOTE: This logic may need adjustment if line items should be considered
    INSERT INTO staging.sales (
        "Order Number",
        "Line Item",
        "Order Date",
        "Delivery Date",
        "CustomerKey",
        "StoreKey",
        "ProductKey",
        "Quantity",
        "Currency Code"
    )
    SELECT 
        c."Order Number",
        c."Line Item",
        c."Order Date",
        c."Delivery Date",
        c."CustomerKey",
        c."StoreKey",
        c."ProductKey",
        c."Quantity",
        c."Currency Code"
    FROM staging.fn_clean_sales_data() c
    WHERE NOT EXISTS (
        SELECT 1 
        FROM staging.sales s 
        WHERE s."Order Number" = c."Order Number"
				AND s."Line Item" = c."Line Item"
    );

    -- Capture number of rows inserted
    GET DIAGNOSTICS v_inserted_rows = ROW_COUNT;

    -- Log operation results with appropriate message
    CALL bl_cl.sp_insert_etl_log(
        'staging.sp_load_sales_data', 
        CASE 
            WHEN v_inserted_rows = 0 THEN 'No new sales data to insert - staging table up to date'
            ELSE 'Sales staging table loaded successfully'
        END, 
        v_inserted_rows, 
        NULL
    );

EXCEPTION 
    WHEN OTHERS THEN 
        -- Log error details and re-raise exception
        CALL bl_cl.sp_insert_etl_log(
            'staging.sp_load_sales_data', 
            'Error occurred during sales staging load', 
            NULL, 
            SQLERRM
        );
        RAISE NOTICE 'Error in staging.sp_load_sales_data: %', SQLERRM;
        ROLLBACK;
END;
$$;