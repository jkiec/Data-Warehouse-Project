-- =============================================================================
-- FN_GET_CITIES FUNCTION
-- Extracts and transforms city data from customers staging table
-- Links cities to states for geographic hierarchy maintenance
-- Handles NULL values using default row (-1) and applies data quality rules
-- =============================================================================

CREATE OR REPLACE FUNCTION bl_3nf.fn_get_cities()
RETURNS TABLE (
    city_src_id     VARCHAR(255),
    city_name       VARCHAR(255),
    city_state_id   BIGINT,
    source_system   VARCHAR(255),
    source_entity   VARCHAR(255)
) AS $$
BEGIN
    RETURN QUERY
    
    -- Cities from customers table with state lookup
    SELECT DISTINCT
        COALESCE(s."City", d.CITY_SRC_ID)::VARCHAR(255) AS city_src_id,
        COALESCE(INITCAP(s."City"), d.CITY_NAME)::VARCHAR(255) AS city_name,
        COALESCE(st.STATE_ID, d.CITY_STATE_ID)::BIGINT AS city_state_id,
        'Staging'::VARCHAR(255) AS source_system,
        'Customers'::VARCHAR(255) AS source_entity
    FROM staging.customers s
    -- Handle NULL values using default row
    LEFT JOIN bl_3nf.CE_CITIES d 
        ON d.CITY_ID = -1
    -- Lookup state ID for referential integrity
    LEFT JOIN bl_3nf.CE_STATES st
        ON upper(st.STATE_NAME) = upper(s."State");
        
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- SP_LOAD_CE_CITIES PROCEDURE
-- Loads city dimension data into 3NF layer with referential integrity
-- Creates default row for NULL handling and prevents duplicates
-- Uses insert-only pattern with enhanced deduplication logic
-- =============================================================================

CREATE OR REPLACE PROCEDURE bl_3nf.sp_load_ce_cities()
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;                -- Record for processing each city
    v_rows_affected INT := 0;  -- Counter for inserted rows
BEGIN
    -- ==========================================================================
    -- STEP 1: ENSURE DEFAULT ROW EXISTS FOR NULL VALUE HANDLING
    -- ==========================================================================
    
    INSERT INTO bl_3nf.ce_cities (
        CITY_ID,
        CITY_SRC_ID,
        CITY_NAME,
        CITY_STATE_ID,
        INSERT_DT,
        UPDATE_DT,
        SOURCE_SYSTEM,
        SOURCE_ENTITY
    )
    SELECT 
        -1,
        'n.a.',
        'n.a.',
        -1,  -- References default state row
        '1900-01-01',
        '1900-01-01',
        'MANUAL',
        'MANUAL'
    WHERE NOT EXISTS (
        SELECT 1 
        FROM bl_3nf.ce_cities 
        WHERE CITY_ID = -1
    );

    -- ==========================================================================
    -- STEP 2: PROCESS NEW CITY DATA FROM CUSTOMERS SOURCE
    -- ==========================================================================
    
    FOR rec IN 
        SELECT * FROM bl_3nf.fn_get_cities()
    LOOP
        -- Insert only new cities based on SRC_ID
        INSERT INTO bl_3nf.ce_cities (
            CITY_ID,
            CITY_SRC_ID,
            CITY_NAME,
            CITY_STATE_ID,
            INSERT_DT,
            UPDATE_DT,
            SOURCE_SYSTEM,
            SOURCE_ENTITY
        )
        SELECT 
            NEXTVAL('bl_3nf.ce_cities_seq'),
            rec.city_src_id,
            rec.city_name,
            rec.city_state_id,
            CURRENT_DATE,
            CURRENT_DATE,
            rec.source_system,
            rec.source_entity
        WHERE NOT EXISTS (
            SELECT 1 
            FROM bl_3nf.ce_cities c
            WHERE c.CITY_SRC_ID = rec.city_src_id 
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
        'bl_3nf.sp_load_ce_cities',
        CASE 
            WHEN v_rows_affected = 0 THEN 'No new cities to process - 3NF table up to date'
            ELSE 'Cities dimension loaded successfully into 3NF layer'
        END,
        v_rows_affected,
        NULL
    );

EXCEPTION 
    WHEN OTHERS THEN 
        -- Log error details and rollback transaction
        CALL bl_cl.sp_insert_etl_log(
            'bl_3nf.sp_load_ce_cities', 
            'Error occurred during cities dimension load', 
            NULL, 
            SQLERRM
        );
        RAISE NOTICE 'Error in bl_3nf.sp_load_ce_cities: %', SQLERRM;
        ROLLBACK;
END;
$$;