-- =============================================================================
-- FN_GET_CATEGORIES FUNCTION
-- Extracts and transforms product category data from staging layer
-- Maps CategoryKey to category dimension with NULL value handling
-- =============================================================================

CREATE OR REPLACE FUNCTION bl_3nf.fn_get_categories()
RETURNS TABLE (
    category_src_id VARCHAR(255),
    category_name   VARCHAR(255),
    source_system   VARCHAR(255),
    source_entity   VARCHAR(255)
) AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT
        COALESCE(s."CategoryKey", d.CATEGORY_SRC_ID)::VARCHAR(255) AS category_src_id,
        COALESCE(s."Category", d.CATEGORY_NAME)::VARCHAR(255) AS category_name,
        'Staging'::VARCHAR(255) AS source_system,
        'Products'::VARCHAR(255) AS source_entity
    FROM staging.products s
    -- Handle NULL values using default row
    LEFT JOIN bl_3nf.ce_categories d 
        ON d.CATEGORY_ID = -1;
        
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- SP_LOAD_CE_CATEGORIES PROCEDURE
-- Loads product category dimension data into 3NF layer
-- Creates default row for NULL handling and prevents duplicates
-- =============================================================================

CREATE OR REPLACE PROCEDURE bl_3nf.sp_load_ce_categories()
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;                -- Record for processing each category
    v_rows_affected INT := 0;  -- Counter for inserted rows
BEGIN
    -- ==========================================================================
    -- STEP 1: ENSURE DEFAULT ROW EXISTS FOR NULL VALUE HANDLING
    -- ==========================================================================
    
    INSERT INTO bl_3nf.ce_categories (
        CATEGORY_ID,
        CATEGORY_SRC_ID,
        CATEGORY_NAME,
        INSERT_DT,
        UPDATE_DT,
        SOURCE_SYSTEM,
        SOURCE_ENTITY
    )
    SELECT 
        -1,
        'n.a.',
        'n.a.',
        '1900-01-01',
        '1900-01-01',
        'MANUAL',
        'MANUAL'
    WHERE NOT EXISTS (
        SELECT 1 
        FROM bl_3nf.ce_categories 
        WHERE CATEGORY_ID = -1
    );

    -- ==========================================================================
    -- STEP 2: PROCESS NEW CATEGORY DATA FROM PRODUCTS SOURCE
    -- ==========================================================================
    
    FOR rec IN 
        SELECT * FROM bl_3nf.fn_get_categories()
    LOOP
        -- Insert only new categories (avoid duplicates based on SRC_ID)
        INSERT INTO bl_3nf.ce_categories (
            CATEGORY_ID,
            CATEGORY_SRC_ID,
            CATEGORY_NAME,
            INSERT_DT,
            UPDATE_DT,
            SOURCE_SYSTEM,
            SOURCE_ENTITY
        )
        SELECT 
            NEXTVAL('bl_3nf.ce_categories_seq'),
            rec.category_src_id,
            rec.category_name,
            CURRENT_DATE,
            CURRENT_DATE,
            rec.source_system,
            rec.source_entity
        WHERE NOT EXISTS (
            SELECT 1 
            FROM bl_3nf.ce_categories c
            WHERE c.CATEGORY_SRC_ID = rec.category_src_id
        );

        -- Count successful insertions
        IF FOUND THEN
            v_rows_affected := v_rows_affected + 1;
        END IF;
    END LOOP;

    -- ==========================================================================
    -- STEP 3: LOG ETL OPERATION RESULTS
    -- ==========================================================================
    
    CALL bl_cl.sp_insert_etl_log(
        'bl_3nf.sp_load_ce_categories',
        CASE 
            WHEN v_rows_affected = 0 THEN 'No new categories to process - 3NF table up to date'
            ELSE 'Categories dimension loaded successfully into 3NF layer'
        END,
        v_rows_affected,
        NULL
    );

EXCEPTION 
    WHEN OTHERS THEN 
        -- Log error details and rollback transaction
        CALL bl_cl.sp_insert_etl_log(
            'bl_3nf.sp_load_ce_categories', 
            'Error occurred during categories dimension load', 
            NULL, 
            SQLERRM
        );
        RAISE NOTICE 'Error in bl_3nf.sp_load_ce_categories: %', SQLERRM;
        ROLLBACK;
END;
$$;