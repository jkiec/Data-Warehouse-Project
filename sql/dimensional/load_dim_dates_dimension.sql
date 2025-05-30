-- =============================================================================
-- GENERATE_DIM_DATES FUNCTION
-- Generates comprehensive date dimension data for specified date range
-- Uses recursive CTE to create date series with all time-based attributes
-- Includes business calendar features (weekends, quarters, leap years)
-- =============================================================================

CREATE OR REPLACE FUNCTION bl_dm.generate_dim_dates(start_date DATE, end_date DATE)
RETURNS TABLE (
    date_surr_id                DATE,
    date_day_name               VARCHAR(10),
    date_day_of_week_number     INT,
    date_day_of_month_number    INT,
    date_iso_week_number        INT,
    date_weekend_flag           INT,
    date_week_ending_day_dt     DATE,
    date_month_number           INT,
    date_days_in_month          INT,
    date_end_of_month_dt        DATE,
    date_month_name             VARCHAR(10),
    date_quarter_number         INT,
    date_days_in_quarter        INT,
    date_end_of_quarter_dt      DATE,
    date_year                   INT,
    date_days_in_year           INT,
    date_end_of_year_dt         DATE
) 
LANGUAGE sql AS $$
    -- Generate continuous date series using recursive CTE
    WITH RECURSIVE date_series AS (
        SELECT start_date AS date_surr_id
        UNION ALL
        SELECT (date_series.date_surr_id + INTERVAL '1 day')::DATE
        FROM date_series
        WHERE date_series.date_surr_id < end_date
    )
    SELECT 
        date_series.date_surr_id,
        -- Day attributes
        TRIM(TO_CHAR(date_series.date_surr_id, 'Day')) AS date_day_name,
        EXTRACT(ISODOW FROM date_series.date_surr_id) AS date_day_of_week_number,
        EXTRACT(DAY FROM date_series.date_surr_id) AS date_day_of_month_number,
        -- Week attributes
        EXTRACT(WEEK FROM date_series.date_surr_id) AS date_iso_week_number,
        CASE 
            WHEN EXTRACT(ISODOW FROM date_series.date_surr_id) IN (6,7) THEN 1 
            ELSE 0 
        END AS date_weekend_flag,
        (date_series.date_surr_id + (7 - EXTRACT(ISODOW FROM date_series.date_surr_id)) * INTERVAL '1 day')::DATE AS date_week_ending_day_dt,
        -- Month attributes
        EXTRACT(MONTH FROM date_series.date_surr_id) AS date_month_number,
        EXTRACT(DAY FROM (DATE_TRUNC('month', date_series.date_surr_id) + INTERVAL '1 month' - INTERVAL '1 day')) AS date_days_in_month,
        (DATE_TRUNC('month', date_series.date_surr_id) + INTERVAL '1 month' - INTERVAL '1 day')::DATE AS date_end_of_month_dt,
        TRIM(TO_CHAR(date_series.date_surr_id, 'Month')) AS date_month_name,
        -- Quarter attributes
        EXTRACT(QUARTER FROM date_series.date_surr_id) AS date_quarter_number,
        CASE 
            WHEN EXTRACT(QUARTER FROM date_series.date_surr_id) = 1 THEN 90
            WHEN EXTRACT(QUARTER FROM date_series.date_surr_id) = 2 THEN 91
            WHEN EXTRACT(QUARTER FROM date_series.date_surr_id) = 3 THEN 92
            WHEN EXTRACT(QUARTER FROM date_series.date_surr_id) = 4 THEN 92
        END AS date_days_in_quarter,
        (DATE_TRUNC('quarter', date_series.date_surr_id) + INTERVAL '3 months' - INTERVAL '1 day')::DATE AS date_end_of_quarter_dt,
        -- Year attributes with leap year calculation
        EXTRACT(YEAR FROM date_series.date_surr_id) AS date_year,
        CASE 
            WHEN EXTRACT(YEAR FROM date_series.date_surr_id) % 4 = 0 AND 
                 (EXTRACT(YEAR FROM date_series.date_surr_id) % 100 <> 0 OR 
                  EXTRACT(YEAR FROM date_series.date_surr_id) % 400 = 0) THEN 366
            ELSE 365
        END AS date_days_in_year,
        (DATE_TRUNC('year', date_series.date_surr_id) + INTERVAL '1 year' - INTERVAL '1 day')::DATE AS date_end_of_year_dt
    FROM date_series;
$$;

-- =============================================================================
-- SP_POPULATE_DIM_DATES PROCEDURE
-- Populates date dimension table for specified date range
-- Creates default row and prevents duplicate date entries
-- Supports incremental loading for new date ranges
-- =============================================================================

CREATE OR REPLACE PROCEDURE bl_dm.sp_populate_dim_dates(start_date DATE, end_date DATE)
LANGUAGE plpgsql AS $$
DECLARE
    v_inserted_rows     INT := 0;  -- Counter for new date records
    v_default_inserted  INT := 0;  -- Counter for default row insertion
BEGIN
    -- ==========================================================================
    -- STEP 1: ENSURE DEFAULT ROW EXISTS FOR NULL DATE HANDLING
    -- ==========================================================================
    
    -- Insert default date row (1900-01-01) for NULL date handling
    INSERT INTO bl_dm.dim_dates (
        date_surr_id,
        date_day_name,
        date_day_of_week_number,
        date_day_of_month_number,
        date_iso_week_number,
        date_weekend_flag,
        date_week_ending_day_dt,
        date_month_number,
        date_days_in_month,
        date_end_of_month_dt,
        date_month_name,
        date_quarter_number,
        date_days_in_quarter,
        date_end_of_quarter_dt,
        date_year,
        date_days_in_year,
        date_end_of_year_dt
    )
    SELECT 
        gdd.date_surr_id,
        gdd.date_day_name,
        gdd.date_day_of_week_number,
        gdd.date_day_of_month_number,
        gdd.date_iso_week_number,
        gdd.date_weekend_flag,
        gdd.date_week_ending_day_dt,
        gdd.date_month_number,
        gdd.date_days_in_month,
        gdd.date_end_of_month_dt,
        gdd.date_month_name,
        gdd.date_quarter_number,
        gdd.date_days_in_quarter,
        gdd.date_end_of_quarter_dt,
        gdd.date_year,
        gdd.date_days_in_year,
        gdd.date_end_of_year_dt
    FROM bl_dm.generate_dim_dates('1900-01-01', '1900-01-01') AS gdd
    WHERE NOT EXISTS (
        SELECT 1 
        FROM bl_dm.dim_dates d 
        WHERE d.date_surr_id = '1900-01-01'
    );

    GET DIAGNOSTICS v_default_inserted = ROW_COUNT;

    -- ==========================================================================
    -- STEP 2: INSERT NEW DATES FOR SPECIFIED RANGE
    -- ==========================================================================
    
    -- Insert new dates, avoiding duplicates
    INSERT INTO bl_dm.dim_dates (
        date_surr_id,
        date_day_name,
        date_day_of_week_number,
        date_day_of_month_number,
        date_iso_week_number,
        date_weekend_flag,
        date_week_ending_day_dt,
        date_month_number,
        date_days_in_month,
        date_end_of_month_dt,
        date_month_name,
        date_quarter_number,
        date_days_in_quarter,
        date_end_of_quarter_dt,
        date_year,
        date_days_in_year,
        date_end_of_year_dt
    )
    SELECT 
        gdd.date_surr_id,
        gdd.date_day_name,
        gdd.date_day_of_week_number,
        gdd.date_day_of_month_number,
        gdd.date_iso_week_number,
        gdd.date_weekend_flag,
        gdd.date_week_ending_day_dt,
        gdd.date_month_number,
        gdd.date_days_in_month,
        gdd.date_end_of_month_dt,
        gdd.date_month_name,
        gdd.date_quarter_number,
        gdd.date_days_in_quarter,
        gdd.date_end_of_quarter_dt,
        gdd.date_year,
        gdd.date_days_in_year,
        gdd.date_end_of_year_dt
    FROM bl_dm.generate_dim_dates(start_date, end_date) AS gdd
    WHERE NOT EXISTS (
        SELECT 1 
        FROM bl_dm.dim_dates d 
        WHERE d.date_surr_id = gdd.date_surr_id
    );

    GET DIAGNOSTICS v_inserted_rows = ROW_COUNT;

    -- ==========================================================================
    -- STEP 3: LOG ETL OPERATION RESULTS
    -- ==========================================================================
    
    CALL bl_cl.sp_insert_etl_log(
        'bl_dm.sp_populate_dim_dates',
        CASE 
            WHEN (v_inserted_rows + v_default_inserted) = 0 THEN 'No new dates to populate - dimension table up to date'
            ELSE 'Date dimension populated successfully for specified range'
        END,
        (v_inserted_rows + v_default_inserted),
        NULL
    );

EXCEPTION 
    WHEN OTHERS THEN 
        -- Log error details and re-raise exception
        CALL bl_cl.sp_insert_etl_log(
            'bl_dm.sp_populate_dim_dates',
            'Error occurred during date dimension population',
            NULL,
            SQLERRM
        );
        RAISE NOTICE 'Error in bl_dm.sp_populate_dim_dates: %', SQLERRM;
        RAISE; 
END;
$$;

-- =============================================================================
-- POPULATE_DIM_DATES_AUTO PROCEDURE
-- Automatically determines date range based on sales data and populates dates
-- Adds buffer months before/after actual sales dates for comprehensive coverage
-- Simplifies date dimension maintenance by auto-calculating required range
-- =============================================================================

CREATE OR REPLACE PROCEDURE bl_dm.populate_dim_dates_auto()
LANGUAGE plpgsql AS $$
DECLARE
    start_dt DATE;  -- Calculated start date with buffer
    end_dt   DATE;  -- Calculated end date with buffer
BEGIN
    -- Calculate date range from sales data with 1-month buffer on each side
    SELECT 
        (MIN(sale_order_dt) - INTERVAL '1 month')::DATE, 
        (MAX(sale_order_dt) + INTERVAL '1 month')::DATE
    INTO start_dt, end_dt
    FROM bl_3nf.ce_sales;
    
    -- Populate date dimension for calculated range
    CALL bl_dm.sp_populate_dim_dates(start_dt, end_dt);
    
    -- Log auto-population execution
    CALL bl_cl.sp_insert_etl_log(
        'bl_dm.populate_dim_dates_auto',
        'Date dimension auto-populated based on sales data range',
        0,
        NULL
    );
    
END;
$$;