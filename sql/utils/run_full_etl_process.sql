-- =============================================================================
-- SP_RUN_FULL_ETL PROCEDURE
-- Master orchestration procedure for complete data warehouse ETL pipeline
-- Executes all layers in proper sequence: Staging → 3NF → Dimensional Model
-- Provides comprehensive logging, timing, progress tracking, and error handling
-- Implements proper dependency order for referential integrity
-- =============================================================================

CREATE OR REPLACE PROCEDURE bl_cl.sp_run_full_etl()
LANGUAGE plpgsql AS $$
DECLARE
    v_start_time    TIMESTAMP := CLOCK_TIMESTAMP();  -- ETL process start time
    v_end_time      TIMESTAMP;                       -- ETL process end time
    v_duration      INTERVAL;                        -- Total execution duration
    v_step_name     VARCHAR(100);                    -- Current step description
    v_total_steps   INT := 3;                        -- Total number of major stages
    v_current_step  INT := 0;                        -- Current stage counter
BEGIN
    RAISE NOTICE '=======================================================';
    RAISE NOTICE 'STARTING FULL DATA WAREHOUSE ETL PROCESS';
    RAISE NOTICE 'Start Time: %', v_start_time;
    RAISE NOTICE '=======================================================';
    
    -- ==========================================================================
    -- STAGE 1: STAGING LAYER - RAW DATA INGESTION
    -- Load cleaned data from source systems into staging tables
    -- ==========================================================================
    
    v_current_step := v_current_step + 1;
    v_step_name := 'STAGING LAYER - Raw data ingestion and cleaning';
    RAISE NOTICE '';
    RAISE NOTICE '=== [%/%] %s ===', v_current_step, v_total_steps, v_step_name;
    RAISE NOTICE 'Loading data from source systems into staging tables...';
    
    -- Load staging tables in optimal order (no dependencies)
    CALL staging.sp_load_sales_data();
    CALL staging.sp_load_stores_data();
    CALL staging.sp_load_customers_data();
    CALL staging.sp_load_products_data();
    
    RAISE NOTICE 'Staging layer completed successfully';
    
    -- ==========================================================================
    -- STAGE 2: 3NF LAYER - NORMALIZED BUSINESS MODEL
    -- Transform staging data into normalized 3NF structure with business rules
    -- ==========================================================================
    
    v_current_step := v_current_step + 1;
    v_step_name := '3NF LAYER - Normalized business model creation';
    RAISE NOTICE '';
    RAISE NOTICE '=== [%/%] %s ===', v_current_step, v_total_steps, v_step_name;
    RAISE NOTICE 'Creating normalized 3NF layer with proper referential integrity...';
    
    -- Create 3NF DDL objects first
    CALL bl_3nf.sp_create_ddl_objects();
    
    -- Load dimensions in hierarchical order (parent → child)
    RAISE NOTICE 'Loading geographic hierarchy...';
    CALL bl_3nf.sp_load_ce_continents();
    CALL bl_3nf.sp_load_ce_countries();
    CALL bl_3nf.sp_load_ce_states();
    CALL bl_3nf.sp_load_ce_cities();
    
    RAISE NOTICE 'Loading product hierarchy...';
    CALL bl_3nf.sp_load_ce_categories();
    CALL bl_3nf.sp_load_ce_subcategories();
    
    RAISE NOTICE 'Loading master entities...';
    CALL bl_3nf.sp_load_ce_stores();
    CALL bl_3nf.sp_load_ce_customers();
    CALL bl_3nf.sp_load_ce_products_scd();
    
    RAISE NOTICE 'Loading fact data...';
    CALL bl_3nf.sp_load_ce_sales();
    
    RAISE NOTICE '3NF layer completed successfully';
    
    -- ==========================================================================
    -- STAGE 3: DIMENSIONAL MODEL - STAR SCHEMA FOR ANALYTICS
    -- Transform 3NF data into denormalized star schema for optimal query performance
    -- ==========================================================================
    
    v_current_step := v_current_step + 1;
    v_step_name := 'DIMENSIONAL MODEL - Star schema for analytics';
    RAISE NOTICE '';
    RAISE NOTICE '=== [%/%] %s ===', v_current_step, v_total_steps, v_step_name;
    RAISE NOTICE 'Creating dimensional model with denormalized star schema...';
    
    -- Create dimensional DDL objects first
    CALL bl_dm.sp_create_dim_ddl_objects();
    
    -- Load date dimension (independent)
    RAISE NOTICE 'Populating date dimension...';
    CALL bl_dm.populate_dim_dates_auto();
    
    -- Load denormalized dimensions
    RAISE NOTICE 'Loading denormalized dimensions...';
    CALL bl_dm.sp_load_dim_stores();
    CALL bl_dm.sp_load_dim_customers();
    CALL bl_dm.sp_load_dim_products_scd();
    
    -- Load fact table with automatic partitioning
    RAISE NOTICE 'Loading partitioned fact table...';
    CALL bl_dm.sp_load_fct_sales();
    
    RAISE NOTICE 'Dimensional model completed successfully';
    
    -- ==========================================================================
    -- COMPLETION SUMMARY AND LOGGING
    -- ==========================================================================
    
    -- Calculate total execution time
    v_end_time := CLOCK_TIMESTAMP();
    v_duration := v_end_time - v_start_time;
    
    RAISE NOTICE '';
    RAISE NOTICE '=======================================================';
    RAISE NOTICE 'ETL PROCESS COMPLETED SUCCESSFULLY!';
    RAISE NOTICE '=======================================================';
    RAISE NOTICE 'Pipeline Summary:';
    RAISE NOTICE '  • Staging Layer: Raw data ingestion ✓';
    RAISE NOTICE '  • 3NF Layer: Normalized business model ✓';
    RAISE NOTICE '  • Dimensional Model: Analytics-ready star schema ✓';
    RAISE NOTICE '';
    RAISE NOTICE 'Execution Details:';
    RAISE NOTICE '  • Start Time: %', v_start_time;
    RAISE NOTICE '  • End Time: %', v_end_time;
    RAISE NOTICE '  • Total Duration: %', v_duration;
    RAISE NOTICE '  • Steps Completed: %/%', v_current_step, v_total_steps;
    RAISE NOTICE '=======================================================';
    
    -- Log successful completion to ETL log table
    CALL bl_cl.sp_insert_etl_log(
        'bl_cl.sp_run_full_etl',
        'Complete data warehouse ETL pipeline executed successfully. Duration: ' || v_duration::TEXT,
        NULL,
        NULL
    );

EXCEPTION 
    WHEN OTHERS THEN
        -- Calculate duration up to failure point
        v_end_time := CLOCK_TIMESTAMP();
        v_duration := v_end_time - v_start_time;
        
        RAISE NOTICE '';
        RAISE NOTICE '=======================================================';
        RAISE NOTICE 'ETL PROCESS FAILED!';
        RAISE NOTICE '=======================================================';
        RAISE NOTICE 'Failure Details:';
        RAISE NOTICE '  • Failed at Stage: [%/%] %', v_current_step, v_total_steps, v_step_name;
        RAISE NOTICE '  • Error Message: %', SQLERRM;
        RAISE NOTICE '  • Duration Before Failure: %', v_duration;
        RAISE NOTICE '  • Failure Time: %', v_end_time;
        RAISE NOTICE '';
        RAISE NOTICE 'Troubleshooting:';
        RAISE NOTICE '  • Check ETL log table: bl_cl.etl_log';
        RAISE NOTICE '  • Review data quality in source systems';
        RAISE NOTICE '  • Verify schema dependencies and constraints';
        RAISE NOTICE '=======================================================';
        
        -- Log the error with full context
        CALL bl_cl.sp_insert_etl_log(
            'bl_cl.sp_run_full_etl',
            'ETL pipeline failed at stage: ' || v_step_name || '. Error: ' || SQLERRM || '. Duration: ' || v_duration::TEXT,
            NULL,
            SQLERRM
        );
        
        -- Re-raise the exception for calling applications
        RAISE;
END;
$$;