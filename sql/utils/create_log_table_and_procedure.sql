-- =============================================================================
-- ETL_LOG TABLE
-- Centralized logging table for tracking ETL procedures execution across all data layers
-- Stores both successful operations and error information for monitoring and debugging
-- =============================================================================

CREATE TABLE IF NOT EXISTS bl_cl.etl_log (
    log_id          SERIAL PRIMARY KEY,                    
    log_timestamp   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  
    procedure_name  VARCHAR(255),                          
    log_message     TEXT,                                  
    rows_affected   INT,                                   
    error_message   TEXT                                   
);

-- =============================================================================
-- SP_INSERT_ETL_LOG PROCEDURE  
-- Inserts standardized log entries into etl_log table
-- Used by all ETL procedures to maintain consistent logging format
-- =============================================================================

CREATE OR REPLACE PROCEDURE bl_cl.sp_insert_etl_log(
    p_procedure_name  VARCHAR(255),  
    p_log_message     TEXT,          
    p_rows_affected   INT  DEFAULT NULL, 
    p_error_message   TEXT DEFAULT NULL   
)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO bl_cl.etl_log (
        procedure_name,
        log_message,
        rows_affected,
        error_message
    )
    VALUES (
        p_procedure_name,
        p_log_message,
        p_rows_affected,
        p_error_message
    );
END;
$$;