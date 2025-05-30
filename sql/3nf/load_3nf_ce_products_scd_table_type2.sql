-- =============================================================================
-- FN_GET_PRODUCTS FUNCTION
-- Extracts and transforms product data from staging for SCD Type 2 processing
-- Handles price data cleaning (removes $ and commas) and subcategory lookup
-- Includes all product attributes for change detection and history tracking
-- =============================================================================

CREATE OR REPLACE FUNCTION bl_3nf.fn_get_products()
RETURNS TABLE (
    product_src_id          VARCHAR(255),
    product_name            VARCHAR(100),
    product_brand           VARCHAR(30),
    product_color           VARCHAR(20),
    product_unit_cost       DECIMAL(7,2),
    product_unit_price      DECIMAL(7,2),
    product_subcategory_id  BIGINT,
    source_system           VARCHAR(255),
    source_entity           VARCHAR(255)
) AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT
        CAST(COALESCE(s."ProductKey", d.PRODUCT_SRC_ID) AS VARCHAR(255)) AS product_src_id,
        COALESCE(s."Product Name", d.PRODUCT_NAME) AS product_name,
        COALESCE(s."Brand", d.PRODUCT_BRAND) AS product_brand,
        COALESCE(s."Color", d.PRODUCT_COLOR) AS product_color,
        -- Clean price data: remove $ and commas, convert to decimal
        COALESCE(
            CAST(TRIM(REPLACE(REPLACE(s."Unit Cost USD", '$', ''), ',', '')) AS DECIMAL(7,2)), 
            d.PRODUCT_UNIT_COST
        ) AS product_unit_cost,
        COALESCE(
            CAST(TRIM(REPLACE(REPLACE(s."Unit Price USD", '$', ''), ',', '')) AS DECIMAL(7,2)), 
            d.PRODUCT_UNIT_PRICE
        ) AS product_unit_price,
        -- Lookup subcategory ID for referential integrity
        CAST(COALESCE(sub.SUBCATEGORY_ID, d.PRODUCT_SUBCATEGORY_ID) AS BIGINT) AS product_subcategory_id,
        CAST('Staging' AS VARCHAR(255)) AS source_system,
        CAST('Products' AS VARCHAR(255)) AS source_entity
    FROM staging.products s
    -- Handle NULL values using default row
    LEFT JOIN bl_3nf.CE_PRODUCTS_SCD d 
        ON d.PRODUCT_ID = -1
    -- Lookup subcategory ID for product hierarchy
    LEFT JOIN bl_3nf.CE_SUBCATEGORIES sub 
        ON sub.SUBCATEGORY_SRC_ID = s."SubcategoryKey";
        
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- SP_LOAD_CE_PRODUCTS_SCD PROCEDURE
-- Implements SCD Type 2 (Slowly Changing Dimension) for products
-- Tracks price changes over time by creating new versions and deactivating old ones
-- Change detection based on PRODUCT_UNIT_PRICE differences
-- Maintains full history with START_DT, END_DT, and IS_ACTIVE flags
-- =============================================================================

CREATE OR REPLACE PROCEDURE bl_3nf.sp_load_ce_products_scd()
LANGUAGE plpgsql  
AS $$  
DECLARE  
    rec RECORD;                -- Record for processing each product
    v_rows_affected INT := 0;  -- Counter for inserted/updated rows
BEGIN  
    -- ==========================================================================
    -- STEP 1: ENSURE DEFAULT ROW EXISTS FOR NULL VALUE HANDLING
    -- ==========================================================================
    
    INSERT INTO bl_3nf.CE_PRODUCTS_SCD (  
        PRODUCT_ID,
        PRODUCT_SRC_ID,
        PRODUCT_NAME,
        PRODUCT_BRAND,   
        PRODUCT_COLOR,
        PRODUCT_UNIT_COST,
        PRODUCT_UNIT_PRICE,
        PRODUCT_SUBCATEGORY_ID,  
        START_DT,
        END_DT,
        IS_ACTIVE,
        INSERT_DT,
        SOURCE_SYSTEM,
        SOURCE_ENTITY  
    )  
    SELECT 
        -1,
        'n.a.',
        'n.a.',
        'n.a.',
        'n.a.',
        0.00,
        0.00,
        -1,
        '1900-01-01',
        '9999-12-31',
        'N',
        '1900-01-01',
        'MANUAL',
        'MANUAL'
    WHERE NOT EXISTS (  
        SELECT 1 
        FROM bl_3nf.CE_PRODUCTS_SCD 
        WHERE PRODUCT_ID = -1
    );  

    -- ==========================================================================
    -- STEP 2: PROCESS CHANGED PRODUCTS (SCD TYPE 2 LOGIC)
    -- ==========================================================================
    
    FOR rec IN   
        SELECT DISTINCT s.*  
        FROM bl_3nf.fn_get_products() s
        LEFT JOIN bl_3nf.CE_PRODUCTS_SCD d  
            ON d.PRODUCT_SRC_ID = s.product_src_id 
            AND d.IS_ACTIVE = 'Y'    
        WHERE d.PRODUCT_SRC_ID IS NULL 
           OR d.PRODUCT_UNIT_PRICE IS DISTINCT FROM s.product_unit_price
    LOOP  
        -- Deactivate current active record if it exists (price changed)
        IF EXISTS (
            SELECT 1 
            FROM bl_3nf.CE_PRODUCTS_SCD  
            WHERE PRODUCT_SRC_ID = rec.product_src_id  
              AND SOURCE_SYSTEM = rec.source_system  
              AND IS_ACTIVE = 'Y'
        ) THEN  
            UPDATE bl_3nf.CE_PRODUCTS_SCD  
            SET END_DT = CURRENT_TIMESTAMP - INTERVAL '1 second', 
                IS_ACTIVE = 'N'  
            WHERE PRODUCT_SRC_ID = rec.product_src_id    
              AND IS_ACTIVE = 'Y';  
        
            v_rows_affected := v_rows_affected + 1;
        END IF;

        -- Insert new version of product (new price or completely new product)
        INSERT INTO bl_3nf.CE_PRODUCTS_SCD (  
            PRODUCT_ID,
            PRODUCT_SRC_ID,
            PRODUCT_NAME,
            PRODUCT_BRAND,   
            PRODUCT_COLOR,
            PRODUCT_UNIT_COST,
            PRODUCT_UNIT_PRICE,
            PRODUCT_SUBCATEGORY_ID,  
            START_DT,
            END_DT,
            IS_ACTIVE,
            INSERT_DT,
            SOURCE_SYSTEM,
            SOURCE_ENTITY
        )  
        VALUES (  
            NEXTVAL('bl_3nf.ce_products_scd_seq'),  
            rec.product_src_id,  
            rec.product_name,  
            rec.product_brand,  
            rec.product_color,  
            rec.product_unit_cost,  
            rec.product_unit_price,  
            rec.product_subcategory_id,  
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
        FROM bl_3nf.fn_get_products() s
        WHERE NOT EXISTS (
            SELECT 1 
            FROM bl_3nf.CE_PRODUCTS_SCD  
            WHERE PRODUCT_SRC_ID = s.product_src_id  
              AND SOURCE_SYSTEM = s.source_system  
        )
    LOOP  
        INSERT INTO bl_3nf.CE_PRODUCTS_SCD (  
            PRODUCT_ID,
            PRODUCT_SRC_ID,
            PRODUCT_NAME,
            PRODUCT_BRAND,   
            PRODUCT_COLOR,
            PRODUCT_UNIT_COST,
            PRODUCT_UNIT_PRICE,
            PRODUCT_SUBCATEGORY_ID,  
            START_DT,
            END_DT,
            IS_ACTIVE,
            INSERT_DT,
            SOURCE_SYSTEM,
            SOURCE_ENTITY  
        )  
        VALUES (  
            NEXTVAL('bl_3nf.ce_products_scd_seq'),  
            rec.product_src_id,  
            rec.product_name,  
            rec.product_brand,  
            rec.product_color,  
            rec.product_unit_cost,  
            rec.product_unit_price,  
            rec.product_subcategory_id,  
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
        'bl_3nf.sp_load_ce_products_scd',  
        CASE   
            WHEN v_rows_affected = 0 THEN 'No new products or price changes to process - SCD table up to date'
            ELSE 'Products SCD dimension loaded successfully with history tracking'
        END,  
        v_rows_affected,  
        NULL  
    );  

EXCEPTION   
    WHEN OTHERS THEN  
        CALL bl_cl.sp_insert_etl_log(
            'bl_3nf.sp_load_ce_products_scd', 
            'Error occurred during products SCD load', 
            NULL, 
            SQLERRM
        );  
        RAISE NOTICE 'Error in bl_3nf.sp_load_ce_products_scd: %', SQLERRM;  
        ROLLBACK;  
END;
$$;