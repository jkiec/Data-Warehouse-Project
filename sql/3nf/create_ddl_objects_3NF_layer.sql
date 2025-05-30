-- =============================================================================
-- SP_CREATE_DDL_OBJECTS PROCEDURE
-- Creates all necessary sequences and tables for 3NF (Third Normal Form) layer
-- Establishes proper referential integrity with foreign key constraints
-- Includes audit fields (INSERT_DT, UPDATE_DT, SOURCE_SYSTEM, SOURCE_ENTITY)
-- =============================================================================

CREATE OR REPLACE PROCEDURE bl_3nf.sp_create_ddl_objects()
LANGUAGE plpgsql
AS $$
BEGIN
    -- ==========================================================================
    -- STEP 1: CREATE SEQUENCES FOR SURROGATE KEYS
    -- ==========================================================================
    
    EXECUTE 'CREATE SEQUENCE IF NOT EXISTS bl_3nf.ce_sales_seq START 1;';
    EXECUTE 'CREATE SEQUENCE IF NOT EXISTS bl_3nf.ce_customers_seq START 1;';
    EXECUTE 'CREATE SEQUENCE IF NOT EXISTS bl_3nf.ce_products_scd_seq START 1;';
    EXECUTE 'CREATE SEQUENCE IF NOT EXISTS bl_3nf.ce_categories_seq START 1;';
    EXECUTE 'CREATE SEQUENCE IF NOT EXISTS bl_3nf.ce_subcategories_seq START 1;';
    EXECUTE 'CREATE SEQUENCE IF NOT EXISTS bl_3nf.ce_stores_seq START 1;';
    EXECUTE 'CREATE SEQUENCE IF NOT EXISTS bl_3nf.ce_cities_seq START 1;';
    EXECUTE 'CREATE SEQUENCE IF NOT EXISTS bl_3nf.ce_continents_seq START 1;';
    EXECUTE 'CREATE SEQUENCE IF NOT EXISTS bl_3nf.ce_countries_seq START 1;';
    EXECUTE 'CREATE SEQUENCE IF NOT EXISTS bl_3nf.ce_states_seq START 1;';

    -- ==========================================================================
    -- STEP 2: CREATE TABLES WITH PROPER HIERARCHY (PARENT TABLES FIRST)
    -- ==========================================================================

    -- CE_CONTINENTS - Top level of geographic hierarchy
    EXECUTE 'CREATE TABLE IF NOT EXISTS bl_3nf.CE_CONTINENTS (
        CONTINENT_ID        BIGINT PRIMARY KEY,
        CONTINENT_SRC_ID    VARCHAR(255) NOT NULL UNIQUE,
        CONTINENT_NAME      VARCHAR(20) NOT NULL,
        INSERT_DT           DATE NOT NULL,
        UPDATE_DT           DATE NOT NULL,
        SOURCE_SYSTEM       VARCHAR(255) NOT NULL,
        SOURCE_ENTITY       VARCHAR(255) NOT NULL
    );';

    -- CE_COUNTRIES - References continents
    EXECUTE 'CREATE TABLE IF NOT EXISTS bl_3nf.CE_COUNTRIES (
        COUNTRY_ID              BIGINT PRIMARY KEY,
        COUNTRY_SRC_ID          VARCHAR(255) NOT NULL UNIQUE,
        COUNTRY_NAME            VARCHAR(20) NOT NULL,
        COUNTRY_CONTINENT_ID    BIGINT NOT NULL,
        INSERT_DT               DATE NOT NULL,
        UPDATE_DT               DATE NOT NULL,
        SOURCE_SYSTEM           VARCHAR(255) NOT NULL,
        SOURCE_ENTITY           VARCHAR(255) NOT NULL,
        CONSTRAINT fk_country_continent 
            FOREIGN KEY (COUNTRY_CONTINENT_ID) 
            REFERENCES bl_3nf.CE_CONTINENTS(CONTINENT_ID)
    );';

    -- CE_STATES - References countries
    EXECUTE 'CREATE TABLE IF NOT EXISTS bl_3nf.CE_STATES (
        STATE_ID            BIGINT PRIMARY KEY,
        STATE_SRC_ID        VARCHAR(255) NOT NULL UNIQUE,
        STATE_NAME          VARCHAR(40) NOT NULL,
        STATE_COUNTRY_ID    BIGINT NOT NULL,
        INSERT_DT           DATE NOT NULL,
        UPDATE_DT           DATE NOT NULL,
        SOURCE_SYSTEM       VARCHAR(255) NOT NULL,
        SOURCE_ENTITY       VARCHAR(255) NOT NULL,
        CONSTRAINT fk_state_country 
            FOREIGN KEY (STATE_COUNTRY_ID) 
            REFERENCES bl_3nf.CE_COUNTRIES(COUNTRY_ID)
    );';

    -- CE_CITIES - References states
    EXECUTE 'CREATE TABLE IF NOT EXISTS bl_3nf.CE_CITIES (
        CITY_ID         BIGINT PRIMARY KEY,
        CITY_SRC_ID     VARCHAR(255) NOT NULL UNIQUE,
        CITY_NAME       VARCHAR(50) NOT NULL,
        CITY_STATE_ID   BIGINT NOT NULL,
        INSERT_DT       DATE NOT NULL,
        UPDATE_DT       DATE NOT NULL,
        SOURCE_SYSTEM   VARCHAR(255) NOT NULL,
        SOURCE_ENTITY   VARCHAR(255) NOT NULL,
        CONSTRAINT fk_city_state 
            FOREIGN KEY (CITY_STATE_ID) 
            REFERENCES bl_3nf.CE_STATES(STATE_ID)
    );';

    -- CE_CUSTOMERS - References cities
    EXECUTE 'CREATE TABLE IF NOT EXISTS bl_3nf.CE_CUSTOMERS (
        CUSTOMER_ID             BIGINT PRIMARY KEY,
        CUSTOMER_SRC_ID         VARCHAR(255) NOT NULL UNIQUE,
        CUSTOMER_FIRST_NAME     VARCHAR(30) NOT NULL,
        CUSTOMER_LAST_NAME      VARCHAR(30) NOT NULL,
        CUSTOMER_GENDER         VARCHAR(10) NOT NULL,
        CUSTOMER_BIRTHDAY_DT    DATE NOT NULL,
        CUSTOMER_CITY_ID        BIGINT NOT NULL,
        INSERT_DT               DATE NOT NULL,
        UPDATE_DT               DATE NOT NULL,
        SOURCE_SYSTEM           VARCHAR(255) NOT NULL,
        SOURCE_ENTITY           VARCHAR(255) NOT NULL,
        CONSTRAINT fk_customer_city 
            FOREIGN KEY (CUSTOMER_CITY_ID) 
            REFERENCES bl_3nf.CE_CITIES(CITY_ID)
    );';

    -- CE_CATEGORIES - Product category master
    EXECUTE 'CREATE TABLE IF NOT EXISTS bl_3nf.CE_CATEGORIES (
        CATEGORY_ID         BIGINT PRIMARY KEY,
        CATEGORY_SRC_ID     VARCHAR(255) NOT NULL UNIQUE,
        CATEGORY_NAME       VARCHAR(50) NOT NULL,
        INSERT_DT           DATE NOT NULL,
        UPDATE_DT           DATE NOT NULL,
        SOURCE_SYSTEM       VARCHAR(255) NOT NULL,
        SOURCE_ENTITY       VARCHAR(255) NOT NULL
    );';

    -- CE_SUBCATEGORIES - References categories
    EXECUTE 'CREATE TABLE IF NOT EXISTS bl_3nf.CE_SUBCATEGORIES (
        SUBCATEGORY_ID              BIGINT PRIMARY KEY,
        SUBCATEGORY_SRC_ID          VARCHAR(255) NOT NULL UNIQUE,
        SUBCATEGORY_NAME            VARCHAR(50) NOT NULL,
        SUBCATEGORY_CATEGORY_ID     BIGINT NOT NULL,
        INSERT_DT                   DATE NOT NULL,
        UPDATE_DT                   DATE NOT NULL,
        SOURCE_SYSTEM               VARCHAR(255) NOT NULL,
        SOURCE_ENTITY               VARCHAR(255) NOT NULL,
        CONSTRAINT fk_subcategory_category 
            FOREIGN KEY (SUBCATEGORY_CATEGORY_ID) 
            REFERENCES bl_3nf.CE_CATEGORIES(CATEGORY_ID)
    );';

    -- CE_PRODUCTS_SCD - Slowly Changing Dimension Type 2 for products
    EXECUTE 'CREATE TABLE IF NOT EXISTS bl_3nf.CE_PRODUCTS_SCD (
        PRODUCT_ID                  BIGINT PRIMARY KEY,
        PRODUCT_SRC_ID              VARCHAR(255) NOT NULL,
        PRODUCT_NAME                VARCHAR(100) NOT NULL,
        PRODUCT_BRAND               VARCHAR(30) NOT NULL,
        PRODUCT_COLOR               VARCHAR(20) NOT NULL,
        PRODUCT_UNIT_COST           DECIMAL(7,2) NOT NULL,
        PRODUCT_UNIT_PRICE          DECIMAL(7,2) NOT NULL,
        PRODUCT_SUBCATEGORY_ID      BIGINT NOT NULL,
        START_DT                    TIMESTAMP NOT NULL,
        END_DT                      TIMESTAMP NOT NULL,
        IS_ACTIVE                   VARCHAR(5) NOT NULL,
        INSERT_DT                   DATE NOT NULL,
        SOURCE_SYSTEM               VARCHAR(255) NOT NULL,
        SOURCE_ENTITY               VARCHAR(255) NOT NULL,
        CONSTRAINT fk_product_subcategory 
            FOREIGN KEY (PRODUCT_SUBCATEGORY_ID) 
            REFERENCES bl_3nf.CE_SUBCATEGORIES(SUBCATEGORY_ID)
    );';

    -- CE_STORES - References states
    EXECUTE 'CREATE TABLE IF NOT EXISTS bl_3nf.CE_STORES (
        STORE_ID                BIGINT PRIMARY KEY,
        STORE_SRC_ID            VARCHAR(255) NOT NULL UNIQUE,
        STORE_STATE_ID          BIGINT NOT NULL,
        STORE_SQUARE_METERS     DECIMAL(8,2) NOT NULL,
        STORE_OPEN_DT           DATE NOT NULL,
        INSERT_DT               DATE NOT NULL,
        UPDATE_DT               DATE NOT NULL,
        SOURCE_SYSTEM           VARCHAR(255) NOT NULL,
        SOURCE_ENTITY           VARCHAR(255) NOT NULL,
        CONSTRAINT fk_store_state 
            FOREIGN KEY (STORE_STATE_ID) 
            REFERENCES bl_3nf.CE_STATES(STATE_ID)
    );';

    -- CE_SALES - Fact table referencing all dimension tables
    EXECUTE 'CREATE TABLE IF NOT EXISTS bl_3nf.CE_SALES (
        SALE_ID             BIGINT PRIMARY KEY,
        SALE_SRC_ID         VARCHAR(255) NOT NULL UNIQUE,
        SALE_ORDER_DT       DATE NOT NULL,
        SALE_DELIVERY_DT    DATE NOT NULL,
        SALE_PRODUCT_ID     BIGINT NOT NULL,
        SALE_CUSTOMER_ID    BIGINT NOT NULL,
        SALE_STORE_ID       BIGINT NOT NULL,
        SALE_QUANTITY       INT,
        SALE_TOTAL_SUM      DECIMAL(8,2),
        INSERT_DT           DATE NOT NULL,
        UPDATE_DT           DATE NOT NULL,
        SOURCE_SYSTEM       VARCHAR(255) NOT NULL,
        SOURCE_ENTITY       VARCHAR(255) NOT NULL,
        CONSTRAINT fk_sales_customer 
            FOREIGN KEY (SALE_CUSTOMER_ID) 
            REFERENCES bl_3nf.CE_CUSTOMERS(CUSTOMER_ID),
        CONSTRAINT fk_sales_store 
            FOREIGN KEY (SALE_STORE_ID) 
            REFERENCES bl_3nf.CE_STORES(STORE_ID),
        CONSTRAINT fk_sales_product 
            FOREIGN KEY (SALE_PRODUCT_ID) 
            REFERENCES bl_3nf.CE_PRODUCTS_SCD(PRODUCT_ID)
    );';

    -- Log successful completion
    CALL bl_cl.sp_insert_etl_log(
        'bl_3nf.sp_create_ddl_objects', 
        'All required 3NF layer DDL objects created successfully', 
        0, 
        NULL
    );

END;
$$;