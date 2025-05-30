-- =============================================================================
-- FN_GET_STATES FUNCTION
-- Extracts and transforms state data from multiple staging sources
-- Combines data from customers and stores tables with country lookups
-- Handles NULL values using default row (-1) and applies data quality rules
-- =============================================================================

CREATE OR REPLACE FUNCTION bl_3nf.fn_get_states()
RETURNS TABLE (
    state_src_id        VARCHAR(255),
    state_name          VARCHAR(255),
    state_country_id    BIGINT,
    source_system       VARCHAR(255),
    source_entity       VARCHAR(255)
) AS $$
BEGIN
    RETURN QUERY
    
    -- States from customers table with country lookup
    SELECT DISTINCT
        COALESCE(s."State", d.STATE_SRC_ID)::VARCHAR(255) AS state_src_id,
        COALESCE(INITCAP(s."State"), d.STATE_NAME)::VARCHAR(255) AS state_name,
        COALESCE(c.COUNTRY_ID, d.STATE_COUNTRY_ID)::BIGINT AS state_country_id,
        'Staging'::VARCHAR(255) AS source_system,
        'Customers'::VARCHAR(255) AS source_entity
    FROM staging.customers s
    -- Handle NULL values using default row
    LEFT JOIN bl_3nf.CE_STATES d 
        ON d.STATE_ID = -1
    -- Lookup country ID for referential integrity
    LEFT JOIN bl_3nf.CE_COUNTRIES c
        ON upper(c.COUNTRY_NAME) = upper(s."Country")
        
    UNION 
    
    -- States from stores table with country lookup
    SELECT DISTINCT
        COALESCE(s."State", d.STATE_SRC_ID)::VARCHAR(255) AS state_src_id,
        COALESCE(INITCAP(s."State"), d.STATE_NAME)::VARCHAR(255) AS state_name,
        COALESCE(c.COUNTRY_ID, d.STATE_COUNTRY_ID)::BIGINT AS state_country_id,
        'Staging'::VARCHAR(255) AS source_system,
        'Stores'::VARCHAR(255) AS source_entity
    FROM staging.stores s
    -- Handle NULL values using default row
    LEFT JOIN bl_3nf.CE_STATES d 
        ON d.STATE_ID = -1
    -- Lookup country ID for referential integrity
    LEFT JOIN bl_3nf.CE_COUNTRIES c
        ON upper(c.COUNTRY_NAME) = upper(s."Country");
       
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- SP_LOAD_CE_STATES PROCEDURE
-- Loads state dimension data into 3NF layer with referential integrity
-- Creates default row for NULL handling and prevents duplicates
-- Uses insert-only pattern with enhanced deduplication logic
-- =============================================================================

CREATE OR REPLACE PROCEDURE bl_3nf.sp_load_ce_states()
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;                -- Record for processing each state
    v_rows_affected INT := 0;  -- Counter for inserted rows
BEGIN
    -- ==========================================================================
    -- STEP 1: ENSURE DEFAULT ROW EXISTS FOR NULL VALUE HANDLING
    -- ==========================================================================
    
    INSERT INTO bl_3nf.ce_states (
        STATE_ID,
        STATE_SRC_ID,
        STATE_NAME,
        STATE_COUNTRY_ID,
        INSERT_DT,
        UPDATE_DT,
        SOURCE_SYSTEM,
        SOURCE_ENTITY
    )
    SELECT 
        -1,
        'n.a.',
        'n.a.',
        -1,  -- References default country row
        '1900-01-01',
        '1900-01-01',
        'MANUAL',
        'MANUAL'
    WHERE NOT EXISTS (
        SELECT 1 
        FROM bl_3nf.ce_states 
        WHERE STATE_ID = -1
    );

    -- ==========================================================================
    -- STEP 2: PROCESS NEW STATE DATA FROM MULTIPLE SOURCES
    -- ==========================================================================
    
    FOR rec IN 
        SELECT * FROM bl_3nf.fn_get_states()
    LOOP
        -- Insert only new states (enhanced deduplication: SRC_ID + SOURCE_SYSTEM)
        INSERT INTO bl_3nf.ce_states (
            STATE_ID,
            STATE_SRC_ID,
            STATE_NAME,
            STATE_COUNTRY_ID,
            INSERT_DT,
            UPDATE_DT,
            SOURCE_SYSTEM,
            SOURCE_ENTITY
        )
        SELECT 
            NEXTVAL('bl_3nf.ce_states_seq'),
            rec.state_src_id,
            rec.state_name,
            rec.state_country_id,
            CURRENT_DATE,
            CURRENT_DATE,
            rec.source_system,
            rec.source_entity
        WHERE NOT EXISTS (
            SELECT 1 
            FROM bl_3nf.ce_states s
            WHERE s.STATE_SRC_ID = rec.state_src_id 
              AND s.SOURCE_SYSTEM = rec.source_system
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
        'bl_3nf.sp_load_ce_states',
        CASE 
            WHEN v_rows_affected = 0 THEN 'No new states to process - 3NF table up to date'
            ELSE 'States dimension loaded successfully into 3NF layer'
        END,
        v_rows_affected,
        NULL
    );

EXCEPTION 
    WHEN OTHERS THEN 
        -- Log error details and rollback transaction
        CALL bl_cl.sp_insert_etl_log(
            'bl_3nf.sp_load_ce_states', 
            'Error occurred during states dimension load', 
            NULL, 
            SQLERRM
        );
        RAISE NOTICE 'Error in bl_3nf.sp_load_ce_states: %', SQLERRM;
        ROLLBACK;
END;
$$;