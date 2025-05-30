-- =============================================================================
-- SP_CREATE_DIM_DDL_OBJECTS PROCEDURE
-- Creates all necessary sequences and tables for Dimensional Model (DM) layer
-- Implements star schema design with denormalized dimensions and fact table
-- Includes date partitioning on fact table for performance optimization
-- =============================================================================

CREATE OR REPLACE PROCEDURE bl_dm.sp_create_dim_ddl_objects() 
LANGUAGE plpgsql
AS $$
BEGIN
    -- ==========================================================================
    -- STEP 1: CREATE SEQUENCES FOR SURROGATE KEYS
    -- ==========================================================================
    
    EXECUTE 'CREATE SEQUENCE IF NOT EXISTS bl_dm.dim_customers_seq START 1;';
    EXECUTE 'CREATE SEQUENCE IF NOT EXISTS bl_dm.dim_products_scd_seq START 1;';
    EXECUTE 'CREATE SEQUENCE IF NOT EXISTS bl_dm.dim_stores_seq START 1;';

    -- ==========================================================================
    -- STEP 2: CREATE DIMENSIONAL TABLES (DENORMALIZED FOR PERFORMANCE)
    -- ==========================================================================

    -- DIM_CUSTOMERS - Denormalized customer dimension with geography
    EXECUTE '
    CREATE TABLE IF NOT EXISTS bl_dm.DIM_CUSTOMERS (
        CUSTOMER_SURR_ID        BIGINT PRIMARY KEY,
        CUSTOMER_SRC_ID         BIGINT NOT NULL UNIQUE,
        CUSTOMER_FIRST_NAME     VARCHAR(30) NOT NULL,
        CUSTOMER_LAST_NAME      VARCHAR(30) NOT NULL,
        CUSTOMER_GENDER         VARCHAR(10) NOT NULL,
        CUSTOMER_BIRTHDAY_DT    DATE NOT NULL,
        CUSTOMER_CITY           VARCHAR(50) NOT NULL,
        CUSTOMER_STATE          VARCHAR(40) NOT NULL,
        CUSTOMER_COUNTRY        VARCHAR(20) NOT NULL,
        CUSTOMER_CONTINENT      VARCHAR(20) NOT NULL,
        INSERT_DT               DATE NOT NULL,
        UPDATE_DT               DATE NOT NULL,
        SOURCE_SYSTEM           VARCHAR(255) NOT NULL,
        SOURCE_ENTITY           VARCHAR(255) NOT NULL
    );';

    -- DIM_PRODUCTS_SCD - Denormalized product dimension with category hierarchy
    EXECUTE 'CREATE TABLE IF NOT EXISTS bl_dm.DIM_PRODUCTS_SCD (
        PRODUCT_SURR_ID         BIGINT PRIMARY KEY,
        PRODUCT_SRC_ID          BIGINT NOT NULL,
        PRODUCT_NAME            VARCHAR(100) NOT NULL,
        PRODUCT_BRAND           VARCHAR(30) NOT NULL,
        PRODUCT_COLOR           VARCHAR(20) NOT NULL,
        PRODUCT_UNIT_COST       DECIMAL(7,2) NOT NULL,
        PRODUCT_UNIT_PRICE      DECIMAL(7,2) NOT NULL,
        PRODUCT_SUBCATEGORY     VARCHAR(40) NOT NULL,
        PRODUCT_CATEGORY        VARCHAR(40) NOT NULL,
        START_DT                TIMESTAMP NOT NULL,
        END_DT                  TIMESTAMP NOT NULL,
        IS_ACTIVE               VARCHAR(1) NOT NULL,
        INSERT_DT               DATE NOT NULL,
        SOURCE_SYSTEM           VARCHAR(255) NOT NULL,
        SOURCE_ENTITY           VARCHAR(255) NOT NULL
    );';

    -- DIM_STORES - Denormalized store dimension with geography
    EXECUTE 'CREATE TABLE IF NOT EXISTS bl_dm.DIM_STORES (
        STORE_SURR_ID           BIGINT PRIMARY KEY,
        STORE_SRC_ID            BIGINT NOT NULL UNIQUE,
        STORE_STATE             VARCHAR(40) NOT NULL,
        STORE_COUNTRY           VARCHAR(20) NOT NULL,
        STORE_CONTINENT         VARCHAR(20) NOT NULL,
        STORE_SQUARE_METERS     DECIMAL(8,2) NOT NULL,
        STORE_OPEN_DT           DATE NOT NULL,
        INSERT_DT               DATE NOT NULL,
        UPDATE_DT               DATE NOT NULL,
        SOURCE_SYSTEM           VARCHAR(255) NOT NULL,
        SOURCE_ENTITY           VARCHAR(255) NOT NULL
    );';

    -- DIM_DATES - Comprehensive date dimension for time-based analysis
    EXECUTE 'CREATE TABLE IF NOT EXISTS bl_dm.DIM_DATES (
        date_surr_id                DATE PRIMARY KEY,
        date_day_name               VARCHAR(10) NOT NULL,
        date_day_of_week_number     INT NOT NULL,
        date_day_of_month_number    INT NOT NULL,
        date_iso_week_number        INT NOT NULL,
        date_weekend_flag           INT NOT NULL,
        date_week_ending_day_dt     DATE NOT NULL,
        date_month_number           INT NOT NULL,
        date_days_in_month          INT NOT NULL,
        date_end_of_month_dt        DATE NOT NULL,
        date_month_name             VARCHAR(10) NOT NULL,
        date_quarter_number         INT NOT NULL,
        date_days_in_quarter        INT NOT NULL,
        date_end_of_quarter_dt      DATE NOT NULL,
        date_year                   INT NOT NULL,
        date_days_in_year           INT NOT NULL,
        date_end_of_year_dt         DATE NOT NULL
    );';

    -- ==========================================================================
    -- STEP 3: CREATE FACT TABLE WITH PARTITIONING AND FOREIGN KEYS
    -- ==========================================================================

    -- FCT_SALES - Central fact table with measures and dimension references
    EXECUTE 'CREATE TABLE IF NOT EXISTS bl_dm.FCT_SALES (
        SALE_SRC_ID                 BIGINT,
        SALE_ORDER_SURR_DT          DATE NOT NULL,
        SALE_DELIVERY_SURR_DT       DATE NOT NULL,
        SALE_PRODUCT_SURR_ID        BIGINT NOT NULL,
        SALE_CUSTOMER_SURR_ID       BIGINT NOT NULL,
        SALE_STORE_SURR_ID          BIGINT NOT NULL,
        FCT_QUANTITY_NUM            INT NOT NULL,
        FCT_TOTAL_SUM_$             DECIMAL(8,2),
        INSERT_DT                   DATE NOT NULL,
        UPDATE_DT                   DATE NOT NULL,
        PRIMARY KEY (SALE_SRC_ID, SALE_ORDER_SURR_DT),
        CONSTRAINT fk_fct_sales_order_date 
            FOREIGN KEY (SALE_ORDER_SURR_DT) 
            REFERENCES bl_dm.DIM_DATES(date_surr_id),
        CONSTRAINT fk_fct_sales_delivery_date 
            FOREIGN KEY (SALE_DELIVERY_SURR_DT) 
            REFERENCES bl_dm.DIM_DATES(date_surr_id),
        CONSTRAINT fk_fct_sales_product 
            FOREIGN KEY (SALE_PRODUCT_SURR_ID) 
            REFERENCES bl_dm.DIM_PRODUCTS_SCD(PRODUCT_SURR_ID),
        CONSTRAINT fk_fct_sales_customer 
            FOREIGN KEY (SALE_CUSTOMER_SURR_ID) 
            REFERENCES bl_dm.DIM_CUSTOMERS(CUSTOMER_SURR_ID),
        CONSTRAINT fk_fct_sales_store 
            FOREIGN KEY (SALE_STORE_SURR_ID) 
            REFERENCES bl_dm.DIM_STORES(STORE_SURR_ID)
    ) PARTITION BY RANGE (SALE_ORDER_SURR_DT);';

    -- ==========================================================================
    -- STEP 4: LOG SUCCESSFUL COMPLETION
    -- ==========================================================================

    CALL bl_cl.sp_insert_etl_log(
        'bl_dm.sp_create_dim_ddl_objects', 
        'All required DDL objects in Dimensional Layer created successfully', 
        0, 
        NULL
    );

END;
$$;