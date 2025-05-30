-- =============================================================================
-- SP_LOAD_DIM_CUSTOMERS PROCEDURE
-- Loads denormalized customer dimension with SCD Type 1 logic
-- Flattens complete geographic hierarchy (continent->country->state->city)
-- Uses UPSERT pattern for efficient insert/update operations
-- Maintains current state only (no history preservation)
-- =============================================================================

CREATE OR REPLACE PROCEDURE bl_dm.sp_load_dim_customers()
LANGUAGE plpgsql
AS $$
DECLARE
    default_rec RECORD;        -- Default row for NULL value handling
    rec RECORD;                -- Record for processing each customer
    v_total_affected INT := 0; -- Counter for total affected rows
    v_current_row_count INT := 0; -- Counter for current operation
BEGIN
    -- ==========================================================================
    -- STEP 1: ENSURE DEFAULT ROW EXISTS FOR NULL VALUE HANDLING
    -- ==========================================================================
    
    -- Insert default customer row for NULL handling in fact table
    INSERT INTO bl_dm.dim_customers (
        CUSTOMER_SURR_ID,
        CUSTOMER_SRC_ID,
        CUSTOMER_FIRST_NAME,
        CUSTOMER_LAST_NAME,
        CUSTOMER_GENDER,
        CUSTOMER_BIRTHDAY_DT,
        CUSTOMER_CITY,
        CUSTOMER_STATE,
        CUSTOMER_COUNTRY,
        CUSTOMER_CONTINENT,
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
        '1900-01-01',
        'n.a.',
        'n.a.',
        'n.a.',
        'n.a.',
        '1900-01-01',
        '1900-01-01',
        'MANUAL',
        'MANUAL'
    WHERE NOT EXISTS (
        SELECT 1 
        FROM bl_dm.dim_customers 
        WHERE CUSTOMER_SURR_ID = -1
    );

    -- Cache default row for COALESCE operations
    SELECT * 
    INTO default_rec 
    FROM bl_dm.dim_customers 
    WHERE CUSTOMER_SURR_ID = -1;

    -- ==========================================================================
    -- STEP 2: DENORMALIZE AND UPSERT CUSTOMER DATA FROM 3NF LAYER
    -- ==========================================================================
    
    FOR rec IN 
        SELECT 
            s.CUSTOMER_ID AS customer_src_id,
            -- Customer basic attributes
            COALESCE(s.CUSTOMER_FIRST_NAME, default_rec.CUSTOMER_FIRST_NAME) AS customer_first_name,
            COALESCE(s.CUSTOMER_LAST_NAME, default_rec.CUSTOMER_LAST_NAME) AS customer_last_name,
            COALESCE(s.CUSTOMER_GENDER, default_rec.CUSTOMER_GENDER) AS customer_gender,
            COALESCE(s.CUSTOMER_BIRTHDAY_DT, default_rec.CUSTOMER_BIRTHDAY_DT) AS customer_birthday_dt,
            -- Denormalize complete geographic hierarchy into flat structure
            COALESCE(c.CITY_NAME, default_rec.CUSTOMER_CITY) AS customer_city,
            COALESCE(st.STATE_NAME, default_rec.CUSTOMER_STATE) AS customer_state,
            COALESCE(co.COUNTRY_NAME, default_rec.CUSTOMER_COUNTRY) AS customer_country,
            COALESCE(con.CONTINENT_NAME, default_rec.CUSTOMER_CONTINENT) AS customer_continent,
            -- Source metadata
            'BL_3NF' AS source_system,
            'CE_CUSTOMERS, CE_CITIES, CE_STATES, CE_COUNTRIES, CE_CONTINENTS' AS source_entity
        FROM bl_3nf.ce_customers s
        -- Join complete geographic hierarchy to denormalize
        LEFT JOIN bl_3nf.ce_cities c 
            ON s.CUSTOMER_CITY_ID = c.CITY_ID 
        LEFT JOIN bl_3nf.ce_states st 
            ON c.CITY_STATE_ID = st.STATE_ID
        LEFT JOIN bl_3nf.ce_countries co 
            ON st.STATE_COUNTRY_ID = co.COUNTRY_ID 
        LEFT JOIN bl_3nf.ce_continents con 
            ON co.COUNTRY_CONTINENT_ID = con.CONTINENT_ID 
        WHERE s.CUSTOMER_ID != -1  -- Exclude default record from 3NF layer
    LOOP
        -- UPSERT: Insert new customer or update existing (SCD Type 1)
        INSERT INTO bl_dm.dim_customers (
            CUSTOMER_SURR_ID,
            CUSTOMER_SRC_ID,
            CUSTOMER_FIRST_NAME,
            CUSTOMER_LAST_NAME,
            CUSTOMER_GENDER,
            CUSTOMER_BIRTHDAY_DT,
            CUSTOMER_CITY,
            CUSTOMER_STATE,
            CUSTOMER_COUNTRY,
            CUSTOMER_CONTINENT,
            INSERT_DT,
            UPDATE_DT,
            SOURCE_SYSTEM,
            SOURCE_ENTITY
        )
        VALUES (
            NEXTVAL('bl_dm.dim_customers_seq'),
            rec.customer_src_id,
            rec.customer_first_name,
            rec.customer_last_name,
            rec.customer_gender,
            rec.customer_birthday_dt,
            rec.customer_city,
            rec.customer_state,
            rec.customer_country,
            rec.customer_continent,
            CURRENT_DATE,
            CURRENT_DATE,
            rec.source_system,
            rec.source_entity
        )
        ON CONFLICT (CUSTOMER_SRC_ID)
        DO UPDATE SET
            CUSTOMER_FIRST_NAME = EXCLUDED.CUSTOMER_FIRST_NAME,
            CUSTOMER_LAST_NAME = EXCLUDED.CUSTOMER_LAST_NAME,
            CUSTOMER_CITY = EXCLUDED.CUSTOMER_CITY,
            CUSTOMER_STATE = EXCLUDED.CUSTOMER_STATE,
            CUSTOMER_COUNTRY = EXCLUDED.CUSTOMER_COUNTRY,
            CUSTOMER_CONTINENT = EXCLUDED.CUSTOMER_CONTINENT,
            UPDATE_DT = CURRENT_DATE
        WHERE 
            -- Only update if there are actual changes (change detection)
            dim_customers.CUSTOMER_FIRST_NAME IS DISTINCT FROM EXCLUDED.CUSTOMER_FIRST_NAME OR
            dim_customers.CUSTOMER_LAST_NAME IS DISTINCT FROM EXCLUDED.CUSTOMER_LAST_NAME OR
            dim_customers.CUSTOMER_CITY IS DISTINCT FROM EXCLUDED.CUSTOMER_CITY OR
            dim_customers.CUSTOMER_STATE IS DISTINCT FROM EXCLUDED.CUSTOMER_STATE OR
            dim_customers.CUSTOMER_COUNTRY IS DISTINCT FROM EXCLUDED.CUSTOMER_COUNTRY OR
            dim_customers.CUSTOMER_CONTINENT IS DISTINCT FROM EXCLUDED.CUSTOMER_CONTINENT;
            
        -- Count actual operations (INSERT=1, UPDATE=1, NO_CHANGE=0)
        GET DIAGNOSTICS v_current_row_count = ROW_COUNT;
        v_total_affected := v_total_affected + v_current_row_count;
    END LOOP;

    -- ==========================================================================
    -- STEP 3: LOG ETL OPERATION RESULTS
    -- ==========================================================================
    
    CALL bl_cl.sp_insert_etl_log(
        'bl_dm.sp_load_dim_customers',
        CASE 
            WHEN v_total_affected = 0 THEN 'No new customers or changes to process - dimension table up to date'
            ELSE 'Customers dimension loaded successfully with Type 1 SCD and denormalization'
        END,
        v_total_affected,
        NULL
    );

EXCEPTION 
    WHEN OTHERS THEN 
        -- Log error details and rollback transaction
        CALL bl_cl.sp_insert_etl_log(
            'bl_dm.sp_load_dim_customers',
            'Error occurred during customers dimension load',
            NULL,
            SQLERRM
        );
        RAISE NOTICE 'Error in bl_dm.sp_load_dim_customers: %', SQLERRM;
        ROLLBACK;
END;
$$;