-- =============================================================================
-- FN_GET_SUBCATEGORIES FUNCTION
-- Extracts and transforms product subcategory data from staging layer
-- Maps SubcategoryKey to subcategory dimension with category lookup
-- Links subcategories to their parent categories for product hierarchy
-- =============================================================================

CREATE OR REPLACE FUNCTION bl_3nf.fn_get_subcategories()
RETURNS TABLE (
    subcategory_src_id          VARCHAR(255),
    subcategory_name            VARCHAR(255),
    subcategory_category_id     BIGINT,
    source_system               VARCHAR(255),
    source_entity               VARCHAR(255)
) AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT
        COALESCE(s."SubcategoryKey", d.SUBCATEGORY_SRC_ID)::VARCHAR(255) AS subcategory_src_id,
        COALESCE(s."Subcategory", d.SUBCATEGORY_NAME)::VARCHAR(255) AS subcategory_name,
        COALESCE(c.CATEGORY_ID, d.SUBCATEGORY_CATEGORY_ID)::BIGINT AS subcategory_category_id,
        'Staging'::VARCHAR(255) AS source_system,
        'Products'::VARCHAR(255) AS source_entity
    FROM staging.products s
    -- Handle NULL values using default row
    LEFT JOIN bl_3nf.CE_SUBCATEGORIES d 
        ON d.SUBCATEGORY_ID = -1
    -- Lookup parent category ID for referential integrity
    LEFT JOIN bl_3nf.CE_CATEGORIES c
        ON c.CATEGORY_SRC_ID = s."CategoryKey";
        
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- SP_LOAD_CE_SUBCATEGORIES PROCEDURE
-- Loads product subcategory dimension data into 3NF layer
-- Creates default row for NULL handling and prevents duplicates
-- Uses insert-only pattern - second level of product hierarchy
-- =============================================================================

CREATE OR REPLACE PROCEDURE bl_3nf.sp_load_ce_subcategories()
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;                -- Record for processing each subcategory
    v_rows_affected INT := 0;  -- Counter for inserted rows
BEGIN
    -- ==========================================================================
    -- STEP 1: ENSURE DEFAULT ROW EXISTS FOR NULL VALUE HANDLING
    -- ==========================================================================
    
    INSERT INTO bl_3nf.ce_subcategories (
        SUBCATEGORY_ID,
        SUBCATEGORY_SRC_ID,
        SUBCATEGORY_NAME,
        SUBCATEGORY_CATEGORY_ID,
        INSERT_DT,
        UPDATE_DT,
        SOURCE_SYSTEM,
        SOURCE_ENTITY
    )
    SELECT 
        -1,
        'n.a.',
        'n.a.',
        -1,  -- References default category row
        '1900-01-01',
        '1900-01-01',
        'MANUAL',
        'MANUAL'
    WHERE NOT EXISTS (
        SELECT 1 
        FROM bl_3nf.ce_subcategories 
        WHERE SUBCATEGORY_ID = -1
    );

    -- ==========================================================================
    -- STEP 2: PROCESS NEW SUBCATEGORY DATA FROM PRODUCTS SOURCE
    -- ==========================================================================
    
    FOR rec IN 
        SELECT * FROM bl_3nf.fn_get_subcategories()
    LOOP
        -- Insert only new subcategories (avoid duplicates based on SRC_ID)
        INSERT INTO bl_3nf.ce_subcategories (
            SUBCATEGORY_ID,
            SUBCATEGORY_SRC_ID,
            SUBCATEGORY_NAME,
            SUBCATEGORY_CATEGORY_ID,
            INSERT_DT,
            UPDATE_DT,
            SOURCE_SYSTEM,
            SOURCE_ENTITY
        )
        SELECT 
            NEXTVAL('bl_3nf.ce_subcategories_seq'),
            rec.subcategory_src_id,
            rec.subcategory_name,
            rec.subcategory_category_id,
            CURRENT_DATE,
            CURRENT_DATE,
            rec.source_system,
            rec.source_entity
        WHERE NOT EXISTS (
            SELECT 1 
            FROM bl_3nf.ce_subcategories sc
            WHERE sc.SUBCATEGORY_SRC_ID = rec.subcategory_src_id
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
        'bl_3nf.sp_load_ce_subcategories',
        CASE 
            WHEN v_rows_affected = 0 THEN 'No new subcategories to process - 3NF table up to date'
            ELSE 'Subcategories dimension loaded successfully into 3NF layer'
        END,
        v_rows_affected,
        NULL
    );

EXCEPTION 
    WHEN OTHERS THEN 
        -- Log error details and rollback transaction
        CALL bl_cl.sp_insert_etl_log(
            'bl_3nf.sp_load_ce_subcategories', 
            'Error occurred during subcategories dimension load', 
            NULL, 
            SQLERRM
        );
        RAISE NOTICE 'Error in bl_3nf.sp_load_ce_subcategories: %', SQLERRM;
        ROLLBACK;
END;
$$;