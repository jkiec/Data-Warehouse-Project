-- =============================================================================
-- FN_GET_COUNTRIES FUNCTION
-- Extracts and transforms country data from multiple staging sources
-- Combines data from customers and stores tables with continent lookups
-- Handles NULL values using default row (-1) and applies data quality rules
-- =============================================================================

CREATE OR REPLACE FUNCTION bl_3nf.fn_get_countries()
RETURNS TABLE (
    country_src_id          VARCHAR(255),
    country_name            VARCHAR(255),
    country_continent_id    BIGINT,
    source_system           VARCHAR(255),
    source_entity           VARCHAR(255)
) AS $$
BEGIN
    RETURN QUERY
    
    -- Countries from customers table with continent lookup
    SELECT DISTINCT
        COALESCE(s."Country", d.COUNTRY_SRC_ID)::VARCHAR(255) AS country_src_id,
        COALESCE(INITCAP(s."Country"), d.COUNTRY_NAME)::VARCHAR(255) AS country_name,
        COALESCE(c.CONTINENT_ID, d.COUNTRY_CONTINENT_ID)::BIGINT AS country_continent_id,
        'Staging'::VARCHAR(255) AS source_system,
        'Customers'::VARCHAR(255) AS source_entity
    FROM staging.customers s
    -- Handle NULL values using default row
    LEFT JOIN bl_3nf.CE_COUNTRIES d 
        ON d.COUNTRY_ID = -1
    -- Lookup continent ID for referential integrity
    LEFT JOIN bl_3nf.CE_CONTINENTS c 
        ON UPPER(c.CONTINENT_NAME) = UPPER(s."Continent")
       
    UNION
    
    -- Countries from stores table with continent lookup via customers
    SELECT DISTINCT
        COALESCE(st."Country", d.COUNTRY_SRC_ID)::VARCHAR(255) AS country_src_id,
        COALESCE(INITCAP(st."Country"), d.COUNTRY_NAME)::VARCHAR(255) AS country_name,
        COALESCE(c.CONTINENT_ID, d.COUNTRY_CONTINENT_ID)::BIGINT AS country_continent_id,
        'Staging'::VARCHAR(255) AS source_system,
        'Stores'::VARCHAR(255) AS source_entity
    FROM staging.stores st
    -- Handle NULL values using default row
    LEFT JOIN bl_3nf.CE_COUNTRIES d 
        ON d.COUNTRY_ID = -1
    -- Complex lookup: stores -> customers -> continents (for continent mapping)
    LEFT JOIN staging.customers sc 
        ON UPPER(sc."Country") = UPPER(st."Country")
    LEFT JOIN bl_3nf.CE_CONTINENTS c 
        ON UPPER(c.CONTINENT_NAME) = UPPER(sc."Continent");
        
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- SP_LOAD_CE_COUNTRIES PROCEDURE
-- Loads country dimension data into 3NF layer with referential integrity
-- Creates default row for NULL handling and prevents duplicates
-- Uses insert-only pattern - no updates to existing records
-- =============================================================================

CREATE OR REPLACE PROCEDURE bl_3nf.sp_load_ce_countries()
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;                -- Record for processing each country
    v_rows_affected INT := 0;  -- Counter for inserted rows
BEGIN
    -- ==========================================================================
    -- STEP 1: ENSURE DEFAULT ROW EXISTS FOR NULL VALUE HANDLING
    -- ==========================================================================
    
    INSERT INTO bl_3nf.ce_countries (
        COUNTRY_ID,
        COUNTRY_SRC_ID,
        COUNTRY_NAME,
        COUNTRY_CONTINENT_ID,
        INSERT_DT,
        UPDATE_DT,
        SOURCE_SYSTEM,
        SOURCE_ENTITY
    )
    SELECT 
        -1,
        'n.a.',
        'n.a.',
        -1,  -- References default continent row
        '1900-01-01',
        '1900-01-01',
        'MANUAL',
        'MANUAL'
    WHERE NOT EXISTS (
        SELECT 1 
        FROM bl_3nf.ce_countries 
        WHERE COUNTRY_ID = -1
    );

    -- ==========================================================================
    -- STEP 2: PROCESS NEW COUNTRY DATA FROM MULTIPLE SOURCES
    -- ==========================================================================
    
    FOR rec IN 
        SELECT * FROM bl_3nf.fn_get_countries()
    LOOP
        -- Insert only new countries (avoid duplicates based on SRC_ID)
        INSERT INTO bl_3nf.ce_countries (
            COUNTRY_ID,
            COUNTRY_SRC_ID,
            COUNTRY_NAME,
            COUNTRY_CONTINENT_ID,
            INSERT_DT,
            UPDATE_DT,
            SOURCE_SYSTEM,
            SOURCE_ENTITY
        )
        SELECT 
            NEXTVAL('bl_3nf.ce_countries_seq'),
            rec.country_src_id,
            rec.country_name,
            rec.country_continent_id,
            CURRENT_DATE,
            CURRENT_DATE,
            rec.source_system,
            rec.source_entity
        WHERE NOT EXISTS (
            SELECT 1 
            FROM bl_3nf.ce_countries c
            WHERE c.COUNTRY_SRC_ID = rec.country_src_id
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
        'bl_3nf.sp_load_ce_countries',
        CASE 
            WHEN v_rows_affected = 0 THEN 'No new countries to process - 3NF table up to date'
            ELSE 'Countries dimension loaded successfully into 3NF layer'
        END,
        v_rows_affected,
        NULL
    );

EXCEPTION 
    WHEN OTHERS THEN 
        -- Log error details and rollback transaction
        CALL bl_cl.sp_insert_etl_log(
            'bl_3nf.sp_load_ce_countries', 
            'Error occurred during countries dimension load', 
            NULL, 
            SQLERRM
        );
        RAISE NOTICE 'Error in bl_3nf.sp_load_ce_countries: %', SQLERRM;
        ROLLBACK;
END;
$$;