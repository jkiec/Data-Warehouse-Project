-- =============================================================================
-- SP_LOAD_CE_SALES PROCEDURE
-- Loads sales fact data into 3NF layer with dimension key lookups
-- Implements incremental loading based on order date and deduplication
-- Calculates total sales amount (quantity * unit price) from product dimension
-- Links to all dimension tables via foreign key relationships
-- =============================================================================

CREATE OR REPLACE PROCEDURE bl_3nf.sp_load_ce_sales()
LANGUAGE plpgsql
AS $$
DECLARE
    v_last_update       DATE;  -- Watermark for incremental loading
    v_rows_inserted     INT := 0;  -- Counter for inserted rows
BEGIN
    -- ==========================================================================
    -- STEP 1: DETERMINE INCREMENTAL LOADING WATERMARK
    -- ==========================================================================
    
    -- Get last update date from 3NF sales table for incremental processing
    SELECT COALESCE(MAX(UPDATE_DT), '2000-01-01') 
    INTO v_last_update 
    FROM bl_3nf.CE_SALES;

    -- ==========================================================================
    -- STEP 2: LOAD NEW SALES DATA WITH DIMENSION KEY LOOKUPS
    -- ==========================================================================
    
    INSERT INTO bl_3nf.CE_SALES (
        SALE_ID,
        SALE_SRC_ID,
        SALE_ORDER_DT,
        SALE_DELIVERY_DT,
        SALE_PRODUCT_ID,
        SALE_CUSTOMER_ID,
        SALE_STORE_ID,
        SALE_QUANTITY,
        SALE_TOTAL_SUM,
        INSERT_DT,
        UPDATE_DT,
        SOURCE_SYSTEM,
        SOURCE_ENTITY
    )
    SELECT
        NEXTVAL('bl_3nf.ce_sales_seq') AS SALE_ID,
        -- Create composite source ID from Order Number + Line Item
        o."Order Number" || '-' || o."Line Item" AS SALE_SRC_ID,
        -- Parse order and delivery dates
        COALESCE(TO_DATE(o."Order Date", 'MM/DD/YYYY'), '1900-01-01') AS SALE_ORDER_DT,
        COALESCE(TO_DATE(o."Delivery Date", 'MM/DD/YYYY'), '1900-01-01') AS SALE_DELIVERY_DT,
        
        -- Lookup product dimension key (active version for SCD Type 2)
        COALESCE(
            (SELECT p.PRODUCT_ID 
             FROM bl_3nf.CE_PRODUCTS_SCD p
             WHERE p.PRODUCT_SRC_ID = o."ProductKey" 
               AND p.IS_ACTIVE = 'Y'),
            -1
        ) AS SALE_PRODUCT_ID,

        -- Lookup customer dimension key
        COALESCE(
            (SELECT c.CUSTOMER_ID 
             FROM bl_3nf.CE_CUSTOMERS c
             WHERE c.CUSTOMER_SRC_ID = o."CustomerKey"),
            -1
        ) AS SALE_CUSTOMER_ID,

        -- Lookup store dimension key
        COALESCE(
            (SELECT s.STORE_ID 
             FROM bl_3nf.CE_STORES s
             WHERE s.STORE_SRC_ID = o."StoreKey"),
            -1
        ) AS SALE_STORE_ID,

        -- Sales measures
        CAST(o."Quantity" AS INT) AS SALE_QUANTITY,
        -- Calculate total amount: quantity * current unit price from product dimension
        CAST(o."Quantity"::INT * p.PRODUCT_UNIT_PRICE AS DECIMAL(8,2)) AS SALE_TOTAL_SUM,
        
        -- Audit fields
        CURRENT_DATE AS INSERT_DT,
        CURRENT_DATE AS UPDATE_DT,
        'Staging' AS SOURCE_SYSTEM,
        'Sales' AS SOURCE_ENTITY
    FROM staging.sales o
    -- Join with product dimension to get current unit price for calculation
    LEFT JOIN bl_3nf.ce_products_scd p 
        ON p.PRODUCT_SRC_ID = o."ProductKey" 
        AND p.IS_ACTIVE = 'Y'
    WHERE 
        -- Incremental loading: only process orders after last update
        TO_DATE(o."Order Date", 'MM/DD/YYYY') > v_last_update
        -- Deduplication: avoid inserting duplicate sales records
        AND NOT EXISTS (
            SELECT 1 
            FROM bl_3nf.CE_SALES s 
            WHERE s.SALE_SRC_ID = o."Order Number" || '-' || o."Line Item"
        );

    -- Capture number of rows inserted
    GET DIAGNOSTICS v_rows_inserted = ROW_COUNT;

    -- ==========================================================================
    -- STEP 3: LOG ETL OPERATION RESULTS
    -- ==========================================================================
    
    CALL bl_cl.sp_insert_etl_log(
        'bl_3nf.sp_load_ce_sales',
        CASE 
            WHEN v_rows_inserted = 0 THEN 'No new sales data to process - fact table up to date'
            ELSE 'Sales fact table loaded successfully with dimension lookups'
        END,
        v_rows_inserted,
        NULL
    );

EXCEPTION
    WHEN OTHERS THEN
        -- Log error details and rollback transaction
        CALL bl_cl.sp_insert_etl_log(
            'bl_3nf.sp_load_ce_sales',
            'Error occurred during sales fact table load',
            NULL,
            SQLERRM
        );
        RAISE;
END;
$$;