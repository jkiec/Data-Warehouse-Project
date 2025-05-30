-- =============================================================================
-- SP_LOAD_FCT_SALES PROCEDURE
-- Loads sales fact data from 3NF layer into dimensional model fact table
-- Implements automatic partition management based on sales order dates
-- Performs dimension key lookups and calculates derived measures
-- Creates monthly partitions for current and recent data plus catch-all for older data
-- =============================================================================

CREATE OR REPLACE PROCEDURE bl_dm.sp_load_fct_sales()
LANGUAGE plpgsql
AS $$
DECLARE
    v_latest_month          DATE;           -- Latest sales month for partition planning
    v_existing_partitions   TEXT[];         -- Array of existing partition names
    v_partition_name        TEXT;           -- Current partition name being processed
    v_partition_start       DATE;           -- Partition start date
    v_partition_end         DATE;           -- Partition end date
    v_rows_inserted         INT := 0;       -- Counter for inserted rows
    v_old_data_partition    TEXT := 'FCT_SALES_OLDER';  -- Name for historical data partition
BEGIN
    -- ==========================================================================
    -- STEP 1: DETERMINE PARTITIONING STRATEGY BASED ON DATA
    -- ==========================================================================
    
    -- Get the latest sales month to determine partition requirements
    SELECT DATE_TRUNC('month', MAX(SALE_ORDER_DT)) 
    INTO v_latest_month 
    FROM bl_3nf.CE_SALES;
    
    -- Exit if no sales data exists
    IF v_latest_month IS NULL THEN 
        CALL bl_cl.sp_insert_etl_log(
            'bl_dm.sp_load_fct_sales', 
            'No sales data found - skipping fact table load', 
            0, 
            NULL
        );
        RETURN; 
    END IF;

    -- ==========================================================================
    -- STEP 2: DISCOVER EXISTING PARTITIONS
    -- ==========================================================================
    
    -- Get list of existing partitions for FCT_SALES table
    SELECT ARRAY(
        SELECT c.relname 
        FROM pg_catalog.pg_inherits i
        JOIN pg_catalog.pg_class c ON i.inhrelid = c.oid
        JOIN pg_catalog.pg_class p ON i.inhparent = p.oid
        WHERE p.relname = 'fct_sales'
    ) INTO v_existing_partitions;

    -- ==========================================================================
    -- STEP 3: CREATE MONTHLY PARTITIONS FOR RECENT DATA (LAST 3 MONTHS)
    -- ==========================================================================
    
    -- Create partitions for the last 3 months (current + 2 previous)
    FOR i IN 0..2 LOOP
        v_partition_start := v_latest_month - (i * INTERVAL '1 month');
        v_partition_end := v_partition_start + INTERVAL '1 month';
        v_partition_name := 'FCT_SALES_' || TO_CHAR(v_partition_start, 'YYYYMM');

        -- Create partition if it doesn't exist
        IF NOT (v_partition_name = ANY(v_existing_partitions)) THEN
            EXECUTE FORMAT(
                'CREATE TABLE IF NOT EXISTS %I PARTITION OF bl_dm.FCT_SALES 
                 FOR VALUES FROM (%L) TO (%L)',
                v_partition_name, v_partition_start, v_partition_end
            );

            -- Log partition creation
            CALL bl_cl.sp_insert_etl_log(
                'bl_dm.sp_load_fct_sales', 
                FORMAT('Created monthly partition: %s for period %s to %s', 
                       v_partition_name, v_partition_start, v_partition_end), 
                NULL, 
                NULL
            );
        END IF;
    END LOOP;

    -- ==========================================================================
    -- STEP 4: CREATE CATCH-ALL PARTITION FOR HISTORICAL DATA
    -- ==========================================================================
    
    -- Ensure older data has a partition (everything before the 3-month window)
    IF NOT (v_old_data_partition = ANY(v_existing_partitions)) THEN
        EXECUTE FORMAT(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF bl_dm.FCT_SALES 
             FOR VALUES FROM (MINVALUE) TO (%L)',
            v_old_data_partition, v_partition_start
        );

        CALL bl_cl.sp_insert_etl_log(
            'bl_dm.sp_load_fct_sales', 
            FORMAT('Created historical data partition: %s for data before %s', 
                   v_old_data_partition, v_partition_start), 
            NULL, 
            NULL
        );
    END IF;

    -- ==========================================================================
    -- STEP 5: LOAD FACT DATA WITH DIMENSION KEY LOOKUPS
    -- ==========================================================================
    
    INSERT INTO bl_dm.FCT_SALES (
        SALE_SRC_ID,
        SALE_ORDER_SURR_DT,
        SALE_DELIVERY_SURR_DT,
        SALE_PRODUCT_SURR_ID,
        SALE_CUSTOMER_SURR_ID,
        SALE_STORE_SURR_ID,
        FCT_QUANTITY_NUM,
        FCT_TOTAL_SUM_$,
        INSERT_DT,
        UPDATE_DT
    )
    SELECT 
        s.SALE_ID AS SALE_SRC_ID,
        s.SALE_ORDER_DT AS SALE_ORDER_SURR_DT,
        s.SALE_DELIVERY_DT AS SALE_DELIVERY_SURR_DT,
        -- Dimension key lookups with fallback to default (-1)
        COALESCE(p.PRODUCT_SURR_ID, -1) AS SALE_PRODUCT_SURR_ID,
        COALESCE(c.CUSTOMER_SURR_ID, -1) AS SALE_CUSTOMER_SURR_ID,
        COALESCE(st.STORE_SURR_ID, -1) AS SALE_STORE_SURR_ID,
        -- Fact measures
        s.SALE_QUANTITY AS FCT_QUANTITY_NUM,
        s.SALE_TOTAL_SUM AS FCT_TOTAL_SUM_$,
        -- Audit fields
        s.INSERT_DT,
        s.UPDATE_DT
    FROM bl_3nf.CE_SALES s
    -- Dimension lookups to get surrogate keys
    LEFT JOIN bl_dm.DIM_PRODUCTS_SCD p 
        ON s.SALE_PRODUCT_ID = p.PRODUCT_SRC_ID 
        AND p.IS_ACTIVE = 'Y'  -- Get current active product version
    LEFT JOIN bl_dm.DIM_CUSTOMERS c 
        ON s.SALE_CUSTOMER_ID = c.CUSTOMER_SRC_ID
    LEFT JOIN bl_dm.DIM_STORES st 
        ON s.SALE_STORE_ID = st.STORE_SRC_ID
    -- Incremental loading: avoid duplicates
    WHERE NOT EXISTS (
        SELECT 1 
        FROM bl_dm.fct_sales f 
        WHERE f.SALE_SRC_ID = s.SALE_ID
    );

    -- Capture number of rows inserted
    GET DIAGNOSTICS v_rows_inserted = ROW_COUNT;

    -- ==========================================================================
    -- STEP 6: LOG ETL OPERATION RESULTS
    -- ==========================================================================
    
    CALL bl_cl.sp_insert_etl_log(
        'bl_dm.sp_load_fct_sales',
        CASE 
            WHEN v_rows_inserted = 0 THEN 'No new sales data to process - fact table up to date'
            ELSE 'Sales fact table loaded successfully with automatic partitioning'
        END,
        v_rows_inserted,
        NULL
    );

EXCEPTION
    WHEN OTHERS THEN
        -- Log error details and re-raise exception
        CALL bl_cl.sp_insert_etl_log(
            'bl_dm.sp_load_fct_sales',
            'Error occurred during sales fact table load',
            NULL,
            SQLERRM
        );
        RAISE;
END;
$$;