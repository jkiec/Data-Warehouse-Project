-- =============================================================================
-- FN_GET_CONTINENTS FUNCTION
-- Extracts and transforms continent data from staging layer
-- Handles NULL values using default row (-1) and applies data quality rules
-- Uses INITCAP for consistent naming convention
-- =============================================================================

CREATE OR REPLACE FUNCTION bl_3nf.fn_get_continents()
RETURNS TABLE (
    continent_src_id    VARCHAR(255),
    continent_name      VARCHAR(255),
    source_system       VARCHAR(255),
    source_entity       VARCHAR(255)
) AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT
        COALESCE(s."Continent", d.CONTINENT_SRC_ID)::VARCHAR(255) AS continent_src_id,
        COALESCE(INITCAP(s."Continent"), d.CONTINENT_NAME)::VARCHAR(255) AS continent_name,
        'Staging'::VARCHAR(255) AS source_system,
        'Customers'::VARCHAR(255) AS source_entity
    FROM staging.customers s
    -- Handle NULL values by joining with default row (CONTINENT_ID = -1)
    LEFT JOIN bl_3nf.ce_continents d 
        ON d.CONTINENT_ID = -1;

END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- SP_LOAD_CE_CONTINENTS PROCEDURE
-- Loads continent dimension data into 3NF layer with data quality controls
-- Creates default row for NULL handling and prevents duplicates
-- Uses insert-only pattern - no updates to existing records
-- =============================================================================

CREATE OR REPLACE PROCEDURE bl_3nf.sp_load_ce_continents()
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;                -- Record for processing each continent
    v_rows_affected INT := 0;  -- Counter for inserted rows
BEGIN
    -- ==========================================================================
    -- STEP 1: ENSURE DEFAULT ROW EXISTS FOR NULL VALUE HANDLING
    -- ==========================================================================
    
    INSERT INTO bl_3nf.ce_continents (
        CONTINENT_ID, 
        CONTINENT_SRC_ID, 
        CONTINENT_NAME,
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
        FROM bl_3nf.ce_continents 
        WHERE CONTINENT_ID = -1
    );

    -- ==========================================================================
    -- STEP 2: PROCESS NEW CONTINENT DATA
    -- ==========================================================================
    
    FOR rec IN 
        SELECT * FROM bl_3nf.fn_get_continents()
    LOOP
        -- Insert only new continents (avoid duplicates based on SRC_ID)
        INSERT INTO bl_3nf.ce_continents (
            CONTINENT_ID,
            CONTINENT_SRC_ID,
            CONTINENT_NAME,
            INSERT_DT,
            UPDATE_DT,
            SOURCE_SYSTEM,
            SOURCE_ENTITY
        )
        SELECT 
            NEXTVAL('bl_3nf.ce_continents_seq'),
            rec.continent_src_id,
            rec.continent_name,
            CURRENT_DATE,
            CURRENT_DATE,
            rec.source_system,
            rec.source_entity
        WHERE NOT EXISTS (
            SELECT 1 
            FROM bl_3nf.ce_continents c
            WHERE c.CONTINENT_SRC_ID = rec.continent_src_id
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
        'bl_3nf.sp_load_ce_continents',
        CASE 
            WHEN v_rows_affected = 0 THEN 'No new continents to process - 3NF table up to date'
            ELSE 'Continents dimension loaded successfully into 3NF layer'
        END,
        v_rows_affected,
        NULL
    );

EXCEPTION 
    WHEN OTHERS THEN 
        -- Log error details and rollback transaction
        CALL bl_cl.sp_insert_etl_log(
            'bl_3nf.sp_load_ce_continents', 
            'Error occurred during continents dimension load', 
            NULL, 
            SQLERRM
        );
        RAISE NOTICE 'Error in bl_3nf.sp_load_ce_continents: %', SQLERRM;
        ROLLBACK;
END;
$$;