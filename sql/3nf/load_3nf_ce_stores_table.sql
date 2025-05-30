-- =============================================================================
-- FN_GET_STORES FUNCTION
-- Extracts and transforms store data from staging layer
-- Handles date parsing (MM/DD/YYYY format) and state lookup for geographic hierarchy
-- =============================================================================

CREATE OR REPLACE FUNCTION bl_3nf.fn_get_stores()
RETURNS TABLE (
    store_src_id            VARCHAR(255),
    store_state_id          BIGINT,
    store_square_meters     DECIMAL(8,2),
    store_open_dt           DATE,
    source_system           VARCHAR(255),
    source_entity           VARCHAR(255)
) AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT
        COALESCE(CAST(s."StoreKey" AS VARCHAR(255)), CAST(d.STORE_SRC_ID AS VARCHAR(255))) AS store_src_id,
        COALESCE(st.STATE_ID, d.STORE_STATE_ID)::BIGINT AS store_state_id,
        COALESCE(CAST(s."Square Meters" AS DECIMAL(8,2)), d.STORE_SQUARE_METERS) AS store_square_meters,
        -- Parse date from MM/DD/YYYY format
        COALESCE(TO_DATE(s."Open Date", 'MM/DD/YYYY'), d.STORE_OPEN_DT) AS store_open_dt,
        CAST('Staging' AS VARCHAR(255)) AS source_system,
        CAST('Stores' AS VARCHAR(255)) AS source_entity
    FROM staging.stores s
    -- Handle NULL values using default row
    LEFT JOIN bl_3nf.CE_STORES d 
        ON d.STORE_ID = -1
    -- Lookup state ID for geographic hierarchy (stores link to states, not cities)
    LEFT JOIN bl_3nf.CE_STATES st 
        ON UPPER(st.STATE_NAME) = UPPER(s."State");
        
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- SP_LOAD_CE_STORES PROCEDURE
-- Loads store dimension data into 3NF layer with geographic linking
-- Creates default row for NULL handling and prevents duplicates
-- Uses insert-only pattern - stores are typically static once created
-- Links to state level (not city) in geographic hierarchy
-- =============================================================================

CREATE OR REPLACE PROCEDURE bl_3nf.sp_load_ce_stores()
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;                -- Record for processing each store
    v_rows_affected INT := 0;  -- Counter for inserted rows
BEGIN
    -- ==========================================================================
    -- STEP 1: ENSURE DEFAULT ROW EXISTS FOR NULL VALUE HANDLING
    -- ==========================================================================
    
    INSERT INTO bl_3nf.ce_stores (
        STORE_ID,
        STORE_SRC_ID,
        STORE_STATE_ID,
        STORE_SQUARE_METERS,
        STORE_OPEN_DT,
        INSERT_DT,
        UPDATE_DT,
        SOURCE_SYSTEM,
        SOURCE_ENTITY
    )
    SELECT 
        -1,
        'n.a.',
        -1,  -- References default state row
        0.00,
        '1900-01-01',
        '1900-01-01',
        '1900-01-01',
        'MANUAL',
        'MANUAL'
    WHERE NOT EXISTS (
        SELECT 1 
        FROM bl_3nf.ce_stores 
        WHERE STORE_ID = -1
    );

    -- ==========================================================================
    -- STEP 2: PROCESS NEW STORE DATA
    -- ==========================================================================
    
    FOR rec IN 
        SELECT * FROM bl_3nf.fn_get_stores()
    LOOP
        -- Insert only new stores (avoid duplicates based on SRC_ID)
        INSERT INTO bl_3nf.ce_stores (
            STORE_ID,
            STORE_SRC_ID,
            STORE_STATE_ID,
            STORE_SQUARE_METERS,
            STORE_OPEN_DT,
            INSERT_DT,
            UPDATE_DT,
            SOURCE_SYSTEM,
            SOURCE_ENTITY
        )
        SELECT 
            NEXTVAL('bl_3nf.ce_stores_seq'),
            rec.store_src_id,
            rec.store_state_id,
            rec.store_square_meters,
            rec.store_open_dt,
            CURRENT_DATE,
            CURRENT_DATE,
            rec.source_system,
            rec.source_entity
        WHERE NOT EXISTS (
            SELECT 1 
            FROM bl_3nf.ce_stores st
            WHERE st.STORE_SRC_ID = rec.store_src_id 
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
        'bl_3nf.sp_load_ce_stores',
        CASE 
            WHEN v_rows_affected = 0 THEN 'No new stores to process - 3NF table up to date'
            ELSE 'Stores dimension loaded successfully into 3NF layer'
        END,
        v_rows_affected,
        NULL
    );

EXCEPTION 
    WHEN OTHERS THEN 
        -- Log error details and rollback transaction
        CALL bl_cl.sp_insert_etl_log(
            'bl_3nf.sp_load_ce_stores', 
            'Error occurred during stores dimension load', 
            NULL, 
            SQLERRM
        );
        RAISE NOTICE 'Error in bl_3nf.sp_load_ce_stores: %', SQLERRM;
        ROLLBACK;
END;
$$;