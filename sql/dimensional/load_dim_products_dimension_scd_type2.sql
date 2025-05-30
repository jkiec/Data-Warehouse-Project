-- =============================================================================
-- FN_GET_DIM_PRODUCTS FUNCTION
-- Extracts and transforms active product data from 3NF SCD table
-- Denormalizes product hierarchy (category->subcategory->product)
-- Only returns active products for dimensional loading
-- =============================================================================

CREATE OR REPLACE FUNCTION bl_dm.fn_get_dim_products()
RETURNS TABLE (
    product_src_id          BIGINT,        
    product_name            VARCHAR(100),
    product_brand           VARCHAR(30),
    product_color           VARCHAR(20),
    product_unit_cost       DECIMAL(7,2),
    product_unit_price      DECIMAL(7,2),
    product_subcategory     VARCHAR(40),
    product_category        VARCHAR(40),
    start_dt                TIMESTAMP,
    end_dt                  TIMESTAMP,
    is_active               VARCHAR(1),
    source_system           VARCHAR(255),
    source_entity           VARCHAR(255)
) AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT
        COALESCE(p.PRODUCT_ID, d.PRODUCT_SRC_ID) AS product_src_id,  
        COALESCE(p.PRODUCT_NAME, d.PRODUCT_NAME)::VARCHAR(100) AS product_name,
        COALESCE(p.PRODUCT_BRAND, d.PRODUCT_BRAND)::VARCHAR(30) AS product_brand,
        COALESCE(p.PRODUCT_COLOR, d.PRODUCT_COLOR)::VARCHAR(20) AS product_color,
        COALESCE(p.PRODUCT_UNIT_COST, d.PRODUCT_UNIT_COST)::DECIMAL(7,2) AS product_unit_cost,
        COALESCE(p.PRODUCT_UNIT_PRICE, d.PRODUCT_UNIT_PRICE)::DECIMAL(7,2) AS product_unit_price,
        -- Denormalize product hierarchy
        COALESCE(s.SUBCATEGORY_NAME, d.PRODUCT_SUBCATEGORY)::VARCHAR(40) AS product_subcategory,
        COALESCE(c.CATEGORY_NAME, d.PRODUCT_CATEGORY)::VARCHAR(40) AS product_category,
        -- SCD Type 2 attributes
        COALESCE(p.START_DT, d.START_DT) AS start_dt,
        COALESCE(p.END_DT, d.END_DT) AS end_dt,
        COALESCE(p.IS_ACTIVE, d.IS_ACTIVE) AS is_active,
        -- Source metadata
        CAST('BL_3NF' AS VARCHAR(255)) AS source_system,
        CAST('CE_PRODUCTS_SCD, CE_SUBCATEGORIES, CE_CATEGORIES' AS VARCHAR(255)) AS source_entity
    FROM bl_3nf.CE_PRODUCTS_SCD p
    -- Handle NULL values using default row
    LEFT JOIN bl_dm.dim_products_scd d
        ON d.PRODUCT_SURR_ID = -1
    -- Join product hierarchy for denormalization
    LEFT JOIN bl_3nf.CE_SUBCATEGORIES s
        ON s.SUBCATEGORY_ID = p.PRODUCT_SUBCATEGORY_ID
    LEFT JOIN bl_3nf.CE_CATEGORIES c
        ON c.CATEGORY_ID = s.SUBCATEGORY_CATEGORY_ID
    WHERE p.PRODUCT_ID != -1 
      AND p.IS_ACTIVE = 'Y';  -- Only active products for dimensional loading
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- SP_LOAD_DIM_PRODUCTS_SCD PROCEDURE
-- Loads denormalized product dimension with SCD Type 2 logic
-- Maintains price change history while denormalizing category hierarchy
-- Implements proper change detection and version management
-- =============================================================================

CREATE OR REPLACE PROCEDURE bl_dm.sp_load_dim_products_scd()
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;                -- Record for processing each product
    v_rows_affected INT := 0;  -- Counter for affected rows
BEGIN
    -- ==========================================================================
    -- STEP 1: ENSURE DEFAULT ROW EXISTS FOR NULL VALUE HANDLING
    -- ==========================================================================
    
    INSERT INTO bl_dm.dim_products_scd (
        product_surr_id,
        product_src_id,
        product_name,
        product_brand,
        product_color,
        product_unit_cost,
        product_unit_price,
        product_subcategory,
        product_category,
        start_dt,
        end_dt,
        is_active,
        insert_dt,
        source_system,
        source_entity
    )
    SELECT 
        -1,
        -1,
        'n.a.',
        'n.a.',
        'n.a.',
        0.00,
        0.00,
        'n.a.',
        'n.a.',
        '1900-01-01',
        '9999-12-31',
        'N',
        '1900-01-01',
        'MANUAL',
        'MANUAL'
    WHERE NOT EXISTS (
        SELECT 1 
        FROM bl_dm.dim_products_scd 
        WHERE product_surr_id = -1
    );

    -- ==========================================================================
    -- STEP 2: PROCESS CHANGED PRODUCTS (SCD TYPE 2 LOGIC)
    -- ==========================================================================
    
    FOR rec IN 
        SELECT DISTINCT s.*  
        FROM bl_dm.fn_get_dim_products() s
        LEFT JOIN bl_dm.dim_products_scd d  
            ON d.PRODUCT_SRC_ID = s.product_src_id 
            AND d.IS_ACTIVE = 'Y'
        WHERE d.PRODUCT_SRC_ID IS NULL 
           OR d.PRODUCT_UNIT_PRICE IS DISTINCT FROM s.product_unit_price
    LOOP
        -- Deactivate existing active record if price changed
        IF EXISTS (
            SELECT 1 
            FROM bl_dm.dim_products_scd dim 
            WHERE dim.PRODUCT_SRC_ID = rec.product_src_id
              AND dim.PRODUCT_UNIT_PRICE <> rec.product_unit_price  
              AND dim.SOURCE_SYSTEM = rec.source_system  
              AND dim.IS_ACTIVE = 'Y'
        ) THEN  
            -- Deactivate old version
            UPDATE bl_dm.dim_products_scd 
            SET end_dt = CURRENT_TIMESTAMP - INTERVAL '1 second', 
                is_active = 'N'
            WHERE PRODUCT_SRC_ID = rec.product_src_id
              AND IS_ACTIVE = 'Y';

            v_rows_affected := v_rows_affected + 1;
        END IF;

        -- Insert new version (either completely new product or new price version)
        INSERT INTO bl_dm.dim_products_scd (
            product_surr_id,
            product_src_id,
            product_name,
            product_brand,
            product_color,
            product_unit_cost,
            product_unit_price,
            product_subcategory,
            product_category,
            start_dt,
            end_dt,
            is_active,
            insert_dt,
            source_system,
            source_entity
        )
        VALUES (
            NEXTVAL('bl_dm.dim_products_scd_seq'),
            rec.product_src_id,
            rec.product_name,
            rec.product_brand,
            rec.product_color,
            rec.product_unit_cost,
            rec.product_unit_price,
            rec.product_subcategory,
            rec.product_category,
            CURRENT_TIMESTAMP,
            '9999-12-31',
            'Y',
            CURRENT_DATE,
            rec.source_system,
            rec.source_entity
        );
        
        v_rows_affected := v_rows_affected + 1;
    END LOOP;

    -- ==========================================================================
    -- STEP 3: INSERT COMPLETELY NEW PRODUCTS (NO HISTORY EXISTS)
    -- ==========================================================================
    
    FOR rec IN 
        SELECT DISTINCT s.* 
        FROM bl_dm.fn_get_dim_products() s
        WHERE NOT EXISTS (
            SELECT 1 
            FROM bl_dm.dim_products_scd d 
            WHERE d.product_src_id = s.product_src_id 
        )
    LOOP
        INSERT INTO bl_dm.dim_products_scd (
            product_surr_id,
            product_src_id,
            product_name,
            product_brand,
            product_color,
            product_unit_cost,
            product_unit_price,
            product_subcategory,
            product_category,
            start_dt,
            end_dt,
            is_active,
            insert_dt,
            source_system,
            source_entity
        )
        VALUES (
            NEXTVAL('bl_dm.dim_products_scd_seq'),
            rec.product_src_id,
            rec.product_name,
            rec.product_brand,
            rec.product_color,
            rec.product_unit_cost,
            rec.product_unit_price,
            rec.product_subcategory,
            rec.product_category,
            CURRENT_TIMESTAMP,
            '9999-12-31',
            'Y',
            CURRENT_DATE,
            rec.source_system,
            rec.source_entity
        );
        
        v_rows_affected := v_rows_affected + 1;
    END LOOP;

    -- ==========================================================================
    -- STEP 4: LOG ETL OPERATION RESULTS
    -- ==========================================================================
    
    CALL bl_cl.sp_insert_etl_log(
        'bl_dm.sp_load_dim_products_scd',
        CASE   
            WHEN v_rows_affected = 0 THEN 'No new products or price changes to process - SCD dimension up to date'
            ELSE 'Products SCD dimension loaded successfully.'
        END,
        v_rows_affected,
        NULL
    );

EXCEPTION 
    WHEN OTHERS THEN 
        -- Log error details and rollback transaction
        CALL bl_cl.sp_insert_etl_log(
            'bl_dm.sp_load_dim_products_scd',
            'Error occurred during products SCD dimension load',
            NULL,
            SQLERRM
        );
        RAISE NOTICE 'Error in bl_dm.sp_load_dim_products_scd: %', SQLERRM;
        ROLLBACK;
END;
$$;