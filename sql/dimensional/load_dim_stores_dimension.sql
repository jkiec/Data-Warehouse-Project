-- =============================================================================
-- SP_LOAD_DIM_STORES PROCEDURE
-- Loads denormalized store dimension from 3NF normalized tables
-- Flattens geographic hierarchy (continent->country->state) into single table
-- Transforms normalized 3NF structure into star schema dimension for analytics
-- =============================================================================

CREATE OR REPLACE PROCEDURE bl_dm.sp_load_dim_stores()
LANGUAGE plpgsql
AS $$
DECLARE
    default_rec RECORD;        -- Default row for NULL value handling
    rec RECORD;                -- Record for processing each store
    v_inserted_rows INT := 0;  -- Counter for inserted rows
BEGIN
    -- ==========================================================================
    -- STEP 1: ENSURE DEFAULT ROW EXISTS FOR NULL VALUE HANDLING
    -- ==========================================================================
    
    -- Insert default store row for NULL handling in fact table
    INSERT INTO bl_dm.dim_stores (
        STORE_SURR_ID,
        STORE_SRC_ID,
        STORE_STATE,
        STORE_COUNTRY,
        STORE_CONTINENT,
        STORE_SQUARE_METERS,
        STORE_OPEN_DT,
        INSERT_DT,
        UPDATE_DT,
        SOURCE_SYSTEM,
        SOURCE_ENTITY
    )
    SELECT 
        -1,
        -1,
        'n.a.',
        'n.a.',
        'n.a.',
        0.00,
        '1900-01-01',
        '1900-01-01',
        '1900-01-01',
        'MANUAL',
        'MANUAL'
    WHERE NOT EXISTS (
        SELECT 1 
        FROM bl_dm.dim_stores 
        WHERE STORE_SURR_ID = -1
    );

    -- Cache default row for COALESCE operations
    SELECT * 
    INTO default_rec 
    FROM bl_dm.dim_stores 
    WHERE STORE_SURR_ID = -1;

    -- ==========================================================================
    -- STEP 2: DENORMALIZE AND LOAD STORE DATA FROM 3NF LAYER
    -- ==========================================================================
    
    FOR rec IN 
        SELECT 
            s.STORE_ID AS STORE_SRC_ID,
            -- Denormalize geographic hierarchy into flat structure
            COALESCE(st.STATE_NAME, default_rec.STORE_STATE) AS STORE_STATE,
            COALESCE(co.COUNTRY_NAME, default_rec.STORE_COUNTRY) AS STORE_COUNTRY,
            COALESCE(cn.CONTINENT_NAME, default_rec.STORE_CONTINENT) AS STORE_CONTINENT,
            -- Store physical attributes
            COALESCE(s.STORE_SQUARE_METERS, default_rec.STORE_SQUARE_METERS) AS STORE_SQUARE_METERS,
            COALESCE(s.STORE_OPEN_DT, default_rec.STORE_OPEN_DT) AS STORE_OPEN_DT,
            -- Audit fields
            CURRENT_DATE AS INSERT_DT,
            CURRENT_DATE AS UPDATE_DT,
            'BL_3NF' AS SOURCE_SYSTEM,
            'CE_STORES, CE_STATES, CE_COUNTRIES, CE_CONTINENTS' AS SOURCE_ENTITY
        FROM bl_3nf.ce_stores s
        -- Join geographic hierarchy to denormalize into single row
        LEFT JOIN bl_3nf.ce_states st 
            ON s.STORE_STATE_ID = st.STATE_ID
        LEFT JOIN bl_3nf.ce_countries co 
            ON st.STATE_COUNTRY_ID = co.COUNTRY_ID
        LEFT JOIN bl_3nf.ce_continents cn 
            ON co.COUNTRY_CONTINENT_ID = cn.CONTINENT_ID
        WHERE s.STORE_ID != -1  -- Exclude default record from 3NF layer
    LOOP
        -- Insert only new stores (avoid duplicates)
        IF NOT EXISTS (
            SELECT 1 
            FROM bl_dm.dim_stores st 
            WHERE st.STORE_SRC_ID = rec.STORE_SRC_ID 
        ) THEN
            INSERT INTO bl_dm.dim_stores (
                STORE_SURR_ID,
                STORE_SRC_ID,
                STORE_STATE,
                STORE_COUNTRY,
                STORE_CONTINENT,
                STORE_SQUARE_METERS,
                STORE_OPEN_DT,
                INSERT_DT,
                UPDATE_DT,
                SOURCE_SYSTEM,
                SOURCE_ENTITY
            )
            VALUES (
                NEXTVAL('bl_dm.dim_stores_seq'),
                rec.STORE_SRC_ID,
                rec.STORE_STATE,
                rec.STORE_COUNTRY,
                rec.STORE_CONTINENT,
                rec.STORE_SQUARE_METERS,
                rec.STORE_OPEN_DT,
                rec.INSERT_DT,
                rec.UPDATE_DT,
                rec.SOURCE_SYSTEM,
                rec.SOURCE_ENTITY
            );
            
            v_inserted_rows := v_inserted_rows + 1;
        END IF;
    END LOOP;

    -- ==========================================================================
    -- STEP 3: LOG ETL OPERATION RESULTS
    -- ==========================================================================
    
    CALL bl_cl.sp_insert_etl_log(
        'bl_dm.sp_load_dim_stores',
        CASE 
            WHEN v_inserted_rows = 0 THEN 'No new stores to process - dimension table up to date'
            ELSE 'Stores dimension loaded successfully with geographic denormalization'
        END,
        v_inserted_rows,
        NULL
    );

EXCEPTION 
    WHEN OTHERS THEN 
        -- Log error details and rollback transaction
        CALL bl_cl.sp_insert_etl_log(
            'bl_dm.sp_load_dim_stores',
            'Error occurred during stores dimension load',
            NULL,
            SQLERRM
        );
        RAISE NOTICE 'Error in bl_dm.sp_load_dim_stores: %', SQLERRM;
        ROLLBACK;
END;
$$;