-- =============================================================================
-- FN_CLEAN_PRODUCTS_DATA FUNCTION
-- Cleans and standardizes product data from source before staging insertion
-- Applies data type casting and removes duplicates at source level
-- =============================================================================

CREATE OR REPLACE FUNCTION staging.fn_clean_products_data()
RETURNS TABLE (
    "ProductKey"        VARCHAR(255),
    "Product Name"      VARCHAR(255),
    "Brand"             VARCHAR(255),
    "Color"             VARCHAR(255),
    "Unit Cost USD"     VARCHAR(255),
    "Unit Price USD"    VARCHAR(255),
    "SubcategoryKey"    VARCHAR(255),
    "Subcategory"       VARCHAR(255),
    "CategoryKey"       VARCHAR(255),
    "Category"          VARCHAR(255)
)
AS $$
BEGIN
    -- Return cleaned data with explicit casting and deduplication
    RETURN QUERY
    SELECT DISTINCT
        s."ProductKey"::VARCHAR(255),
        s."Product Name"::VARCHAR(255),
        s."Brand"::VARCHAR(255),
        s."Color"::VARCHAR(255),
        s."Unit Cost USD"::VARCHAR(255),
        s."Unit Price USD"::VARCHAR(255),
        s."SubcategoryKey"::VARCHAR(255),
        s."Subcategory"::VARCHAR(255),
        s."CategoryKey"::VARCHAR(255),
        s."Category"::VARCHAR(255)
    FROM data_source.products s;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- SP_LOAD_PRODUCTS_DATA PROCEDURE
-- Loads cleaned product data into staging layer with duplicate prevention
-- Creates staging table if not exists, logs all operations for monitoring
-- =============================================================================

CREATE OR REPLACE PROCEDURE staging.sp_load_products_data()
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
      AND table_name = 'products';

    -- Create staging table if it doesn't exist
    IF v_table_exists = 0 THEN
        EXECUTE '
            CREATE TABLE staging.products (
                "ProductKey"        VARCHAR(255),
                "Product Name"      VARCHAR(255),
                "Brand"             VARCHAR(255),
                "Color"             VARCHAR(255),
                "Unit Cost USD"     VARCHAR(255),
                "Unit Price USD"    VARCHAR(255),
                "SubcategoryKey"    VARCHAR(255),
                "Subcategory"       VARCHAR(255),
                "CategoryKey"       VARCHAR(255),
                "Category"          VARCHAR(255)
            );
        ';
        
        -- Log successful table creation
        CALL bl_cl.sp_insert_etl_log(
            'staging.sp_load_products_data', 
            'Products staging table created successfully', 
            NULL, 
            NULL
        );
    ELSE
        -- Log that table already exists
        CALL bl_cl.sp_insert_etl_log(
            'staging.sp_load_products_data', 
            'Products staging table already exists - proceeding with data load', 
            NULL, 
            NULL
        );
    END IF;

    -- Insert only new records (avoid duplicates based on ProductKey and pricing)
    INSERT INTO staging.products (
        "ProductKey",
        "Product Name",
        "Brand",
        "Color",
        "Unit Cost USD",
        "Unit Price USD",
        "SubcategoryKey",
        "Subcategory",
        "CategoryKey",
        "Category"
    )
    SELECT 
        c."ProductKey",
        c."Product Name",
        c."Brand",
        c."Color",
        c."Unit Cost USD",
        c."Unit Price USD",
        c."SubcategoryKey",
        c."Subcategory",
        c."CategoryKey",
        c."Category"
    FROM staging.fn_clean_products_data() c
    WHERE NOT EXISTS (
        SELECT 1 
        FROM staging.products s 
        WHERE s."ProductKey" = c."ProductKey"
          AND s."Unit Price USD" = c."Unit Price USD"
    );

    -- Capture number of rows inserted
    GET DIAGNOSTICS v_inserted_rows = ROW_COUNT;

    -- Log operation results with appropriate message
    CALL bl_cl.sp_insert_etl_log(
        'staging.sp_load_products_data', 
        CASE 
            WHEN v_inserted_rows = 0 THEN 'No new products data to insert - staging table up to date'
            ELSE 'Products staging table loaded successfully'
        END, 
        v_inserted_rows, 
        NULL
    );

EXCEPTION 
    WHEN OTHERS THEN 
        -- Log error details and re-raise exception
        CALL bl_cl.sp_insert_etl_log(
            'staging.sp_load_products_data', 
            'Error occurred during products staging load', 
            NULL, 
            SQLERRM
        );
        RAISE NOTICE 'Error in staging.sp_load_products_data: %', SQLERRM;
        ROLLBACK;
END;
$$;