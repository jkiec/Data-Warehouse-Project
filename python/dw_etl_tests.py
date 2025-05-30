"""
Data Warehouse ETL Testing Suite
Comprehensive tests for Global Electronics Retailers DW pipeline
Test Levels: Smoke → Critical → Extended
"""
import psycopg2
from psycopg2.extras import RealDictCursor
from sqlalchemy import create_engine
import logging
from datetime import datetime
from typing import Dict, List


class DataWarehouseTestConfig:
    """Configuration for DW tests"""
    def __init__(self):
        self.db_config = {
            'host': 'localhost',
            'port': '5432', 
            'database': 'global_electronics_retailers',
            'user': 'postgres',
            'password': '1234'
        }
        self.conn_string = f"postgresql://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
        self.engine = create_engine(self.conn_string)


class DataWarehouseTestSuite:
    """Main test suite for DW ETL processes"""
    
    def __init__(self):
        self.config = DataWarehouseTestConfig()
        self.setup_logging()

    def setup_logging(self):
        """Setup test logging"""
        import sys
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('dw_tests.log', encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def execute_query(self, query: str) -> List[Dict]:
        """Execute SQL query and return results"""
        try:
            with psycopg2.connect(**self.config.db_config) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(query)
                    if cursor.description:
                        results = cursor.fetchall()
                        return [dict(row) for row in results]
                    return []
        except Exception as e:
            self.logger.error(f"Query execution failed: {e}")
            raise
    
    def execute_procedure(self, procedure_name: str, params: List = None):
        """Execute stored procedure"""
        try:
            with psycopg2.connect(**self.config.db_config) as conn:
                with conn.cursor() as cursor:
                    if params:
                        cursor.callproc(procedure_name, params)
                    else:
                        cursor.execute(f"CALL {procedure_name}()")
                    conn.commit()
                    self.logger.info(f"Procedure {procedure_name} executed successfully")
        except Exception as e:
            self.logger.error(f"Procedure {procedure_name} failed: {e}")
            raise


# =============================================================================
# SMOKE TESTS - Basic connectivity and structure validation
# =============================================================================

class SmokeTests(DataWarehouseTestSuite):
    """Level 1: Basic smoke tests to verify system is operational"""
    
    def test_database_connectivity(self):
        """Test basic database connection"""
        self.logger.info("🔥 SMOKE TEST: Database Connectivity")
        
        try:
            result = self.execute_query("SELECT version()")
            assert len(result) > 0
            self.logger.info("✅ Database connection successful")
            return True
        except Exception as e:
            self.logger.error(f"❌ Database connection failed: {e}")
            return False
    
    def test_schemas_exist(self):
        """Test that all required schemas exist"""
        self.logger.info("🔥 SMOKE TEST: Schema Existence")
        
        required_schemas = ['data_source', 'staging', 'bl_3nf', 'bl_dm', 'bl_cl']
        
        query = """
        SELECT schema_name 
        FROM information_schema.schemata 
        WHERE schema_name IN ('data_source', 'staging', 'bl_3nf', 'bl_dm', 'bl_cl')
        ORDER BY schema_name
        """
        
        results = self.execute_query(query)
        existing_schemas = [row['schema_name'] for row in results]
        
        missing_schemas = set(required_schemas) - set(existing_schemas)
        
        if missing_schemas:
            self.logger.error(f"❌ Missing schemas: {missing_schemas}")
            return False
        
        self.logger.info("✅ All required schemas exist")
        return True
    
    def test_source_data_exists(self):
        """Test that source data tables have records"""
        self.logger.info("🔥 SMOKE TEST: Source Data Existence")
        
        source_tables = ['sales', 'customers', 'products', 'stores']
        results = {}
        
        for table in source_tables:
            query = f"SELECT COUNT(*) as count FROM data_source.{table}"
            try:
                result = self.execute_query(query)
                count = result[0]['count']
                results[table] = count
                
                if count == 0:
                    self.logger.warning(f"⚠️ No data in data_source.{table}")
                else:
                    self.logger.info(f"✅ data_source.{table}: {count:,} rows")
                    
            except Exception as e:
                self.logger.error(f"❌ Error checking {table}: {e}")
                return False
        
        total_rows = sum(results.values())
        if total_rows == 0:
            self.logger.error("❌ No source data found")
            return False
            
        self.logger.info(f"✅ Total source rows: {total_rows:,}")
        return True
    
    def test_etl_log_table(self):
        """Test ETL logging infrastructure"""
        self.logger.info("🔥 SMOKE TEST: ETL Log Infrastructure")
        
        # Check if log table exists and is accessible
        query = "SELECT COUNT(*) as count FROM bl_cl.etl_log"
        try:
            result = self.execute_query(query)
            count = result[0]['count']
            self.logger.info(f"✅ ETL log table accessible with {count} entries")
            return True
        except Exception as e:
            self.logger.error(f"❌ ETL log table error: {e}")
            return False


# =============================================================================
# CRITICAL TESTS - Core ETL functionality validation
# =============================================================================

class CriticalTests(DataWarehouseTestSuite):
    """Level 2: Critical business logic and ETL process validation"""

    def test_full_etl_execution(self):
        """Test complete ETL pipeline execution once"""
        self.logger.info("🎯 CRITICAL TEST: Full ETL Pipeline Execution")

        try:
            # Execute the complete ETL pipeline once
            start_time = datetime.now()
            self.execute_procedure('bl_cl.sp_run_full_etl')
            end_time = datetime.now()
            duration = end_time - start_time

            self.logger.info(f"✅ Full ETL pipeline completed in {duration}")
            return True

        except Exception as e:
            self.logger.error(f"❌ Full ETL pipeline failed: {e}")
            return False

    def test_data_validation_post_etl(self):
        """Validate data after ETL completion"""
        self.logger.info("🎯 CRITICAL TEST: Post-ETL Data Validation")

        # Check row counts across all layers
        validation_queries = {
            'staging_sales': 'SELECT COUNT(*) as count FROM staging.sales',
            'staging_customers': 'SELECT COUNT(*) as count FROM staging.customers',
            'staging_products': 'SELECT COUNT(*) as count FROM staging.products',
            'staging_stores': 'SELECT COUNT(*) as count FROM staging.stores',
            '3nf_sales': 'SELECT COUNT(*) as count FROM bl_3nf.ce_sales',
            '3nf_products_scd': 'SELECT COUNT(*) as count FROM bl_3nf.ce_products_scd WHERE product_id != -1',
            '3nf_subcategories': 'SELECT COUNT(*) as count FROM bl_3nf.ce_subcategories WHERE subcategory_id != -1',
            '3nf_categories': 'SELECT COUNT(*) as count FROM bl_3nf.ce_categories WHERE category_id != -1',
            '3nf_stores': 'SELECT COUNT(*) as count FROM bl_3nf.ce_stores WHERE store_id != -1',
            '3nf_customers': 'SELECT COUNT(*) as count FROM bl_3nf.ce_customers WHERE customer_id != -1',
            '3nf_cities': 'SELECT COUNT(*) as count FROM bl_3nf.ce_cities WHERE city_id != -1',
            '3nf_states': 'SELECT COUNT(*) as count FROM bl_3nf.ce_states WHERE state_id != -1',
            '3nf_countries': 'SELECT COUNT(*) as count FROM bl_3nf.ce_countries WHERE country_id != -1',
            '3nf_continents': 'SELECT COUNT(*) as count FROM bl_3nf.ce_continents WHERE continent_id != -1',
            'dm_customers': 'SELECT COUNT(*) as count FROM bl_dm.dim_customers WHERE customer_surr_id != -1',
            'dm_products': 'SELECT COUNT(*) as count FROM bl_dm.dim_products_scd WHERE product_surr_id != -1',
            'dm_stores': 'SELECT COUNT(*) as count FROM bl_dm.dim_stores WHERE store_surr_id != -1',
            'fact_sales': 'SELECT COUNT(*) as count FROM bl_dm.fct_sales'
        }

        results = {}
        for name, query in validation_queries.items():
            try:
                result = self.execute_query(query)
                count = result[0]['count']
                results[name] = count
                self.logger.info(f"  ✅ {name}: {count:,} records")
            except Exception as e:
                self.logger.error(f"  ❌ {name}: Error - {e}")
                return False

        # Validate data flow consistency
        if results.get('staging_sales', 0) > 0:
            if results.get('fact_sales', 0) == 0:
                self.logger.error("❌ No fact data despite source data existing")
                return False

            # Check approximate data flow (allowing for some default records)
            staging_sales = results.get('staging_sales', 0)
            fact_sales = results.get('fact_sales', 0)

            if abs(staging_sales - fact_sales) > staging_sales * 0.1:  # Allow 10% variance
                self.logger.warning(f"⚠️ Data flow variance: Staging {staging_sales:,} vs Fact {fact_sales:,}")
            else:
                self.logger.info("✅ Data flow consistency validated")

        return True

    def test_data_quality_checks(self):
        """Critical data quality validations"""
        self.logger.info("🎯 CRITICAL TEST: Data Quality Checks")

        quality_checks = []

        # Check 1: No NULL values in key business fields
        null_checks = [
            ("staging.sales", "\"Order Number\""),
            ("staging.customers", "\"CustomerKey\""),
            ("staging.products", "\"ProductKey\""),
            ("staging.stores", "\"StoreKey\""),
        ]

        for table, column in null_checks:
            query = f"SELECT COUNT(*) as nulls FROM {table} WHERE {column} IS NULL"
            result = self.execute_query(query)
            null_count = result[0]['nulls']

            if null_count > 0:
                quality_checks.append(f"❌ {table}.{column}: {null_count} NULL values")
            else:
                quality_checks.append(f"✅ {table}.{column}: No NULL values")

        # Check 2: Referential integrity in fact table
        ref_integrity_query = """
        SELECT 
            COUNT(*) as total_sales,
            COUNT(CASE WHEN sale_product_surr_id = -1 THEN 1 END) as missing_products,
            COUNT(CASE WHEN sale_customer_surr_id = -1 THEN 1 END) as missing_customers,
            COUNT(CASE WHEN sale_store_surr_id = -1 THEN 1 END) as missing_stores
        FROM bl_dm.fct_sales
        """

        try:
            result = self.execute_query(ref_integrity_query)
            stats = result[0]

            total = stats['total_sales']
            missing_products = stats['missing_products']
            missing_customers = stats['missing_customers']
            missing_stores = stats['missing_stores']

            quality_checks.append(f"ℹ️ Total sales: {total:,}")

            if missing_products > 0:
                quality_checks.append(f"⚠️ Missing product references: {missing_products}")
            if missing_customers > 0:
                quality_checks.append(f"⚠️ Missing customer references: {missing_customers}")
            if missing_stores > 0:
                quality_checks.append(f"⚠️ Missing store references: {missing_stores}")

            if missing_products == 0 and missing_customers == 0 and missing_stores == 0:
                quality_checks.append("✅ All fact table references resolved")

        except Exception as e:
            quality_checks.append(f"❌ Could not verify referential integrity: {e}")

        # Log all quality check results
        for check in quality_checks:
            self.logger.info(check)

        # Return True if no critical issues (NULLs in keys)
        critical_issues = [check for check in quality_checks if "NULL values" in check and "❌" in check]
        return len(critical_issues) == 0

    def test_scd_functionality(self):
        """Test SCD Type 1 and Type 2 implementations"""
        self.logger.info("🎯 CRITICAL TEST: SCD Functionality")

        try:
            # Test SCD Type 2 (Products) - check for history tracking capability
            scd2_query = """
            SELECT 
                COUNT(*) as total_products,
                COUNT(CASE WHEN is_active = 'Y' THEN 1 END) as active_products,
                COUNT(CASE WHEN is_active = 'N' THEN 1 END) as inactive_products
            FROM bl_3nf.ce_products_scd 
            WHERE product_id != -1
            """

            scd2_results = self.execute_query(scd2_query)
            if scd2_results:
                stats = scd2_results[0]
                self.logger.info(
                    f"✅ SCD Type 2 Products: {stats['total_products']:,} total, {stats['active_products']:,} active")

                if stats['inactive_products'] > 0:
                    self.logger.info(f"✅ Found {stats['inactive_products']:,} historical product versions")
                else:
                    self.logger.info("ℹ️ No historical versions yet (expected for initial load)")

            # Test SCD Type 1 (Customers) - check for update capability
            scd1_query = """
            SELECT COUNT(*) as customer_count
            FROM bl_3nf.ce_customers
            WHERE customer_id != -1
            """

            scd1_results = self.execute_query(scd1_query)
            customer_count = scd1_results[0]['customer_count']

            if customer_count > 0:
                self.logger.info(f"✅ SCD Type 1 Customers: {customer_count:,} loaded")
            else:
                self.logger.error("❌ No customers found")
                return False

            return True

        except Exception as e:
            self.logger.error(f"❌ SCD functionality test failed: {e}")
            return False


# =============================================================================
# EXTENDED TESTS - Comprehensive validation and performance testing
# =============================================================================

class ExtendedTests(DataWarehouseTestSuite):
    """Level 3: Extended testing for performance, edge cases, and business rules"""
    
    def test_full_etl_pipeline(self):
        """Test complete end-to-end ETL pipeline"""
        self.logger.info("🚀 EXTENDED TEST: Full ETL Pipeline")
        
        start_time = datetime.now()
        
        try:
            # Execute full ETL pipeline
            self.execute_procedure('bl_cl.sp_run_full_etl')
            
            end_time = datetime.now()
            duration = end_time - start_time
            
            self.logger.info(f"✅ Full ETL pipeline completed in {duration}")
            
            # Validate final results
            validation_queries = {
                'staging_sales': 'SELECT COUNT(*) as count FROM staging.sales',
                'dm_customers': 'SELECT COUNT(*) as count FROM bl_dm.dim_customers WHERE customer_surr_id != -1',
                'dm_products': 'SELECT COUNT(*) as count FROM bl_dm.dim_products_scd WHERE product_surr_id != -1',
                'dm_stores': 'SELECT COUNT(*) as count FROM bl_dm.dim_stores WHERE store_surr_id != -1',
                'fact_sales': 'SELECT COUNT(*) as count FROM bl_dm.fct_sales'
            }
            
            pipeline_results = {}
            for name, query in validation_queries.items():
                result = self.execute_query(query)
                pipeline_results[name] = result[0]['count']
                self.logger.info(f"  {name}: {pipeline_results[name]:,} records")
            
            # Basic validation: fact table should have data if source has data
            if pipeline_results['staging_sales'] > 0 and pipeline_results['fact_sales'] == 0:
                self.logger.error("❌ No fact data despite source data existing")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Full ETL pipeline failed: {e}")
            return False
    
    def test_business_rules_validation(self):
        """Test business rules and calculated fields"""
        self.logger.info("🚀 EXTENDED TEST: Business Rules Validation")
        
        business_rule_tests = []
        
        try:
            # Test 1: Date dimension completeness
            date_completeness_query = """
            SELECT 
                MIN(date_surr_id) as min_date,
                MAX(date_surr_id) as max_date,
                COUNT(*) as total_dates,
                COUNT(DISTINCT date_year) as years_covered
            FROM bl_dm.dim_dates
            WHERE date_surr_id != '1900-01-01'
            """
            
            date_results = self.execute_query(date_completeness_query)
            if date_results and date_results[0]['total_dates'] > 0:
                stats = date_results[0]
                business_rule_tests.append(f"✅ Date dimension: {stats['total_dates']} dates, {stats['years_covered']} years")
            
            # Test 2: Geographic hierarchy integrity
            geo_hierarchy_query = """
            SELECT 
                customer_continent,
                customer_country,
                customer_state,
                COUNT(*) as customer_count
            FROM bl_dm.dim_customers 
            WHERE customer_surr_id != -1
            GROUP BY customer_continent, customer_country, customer_state
            HAVING COUNT(*) > 10
            ORDER BY customer_count DESC
            """
            
            geo_results = self.execute_query(geo_hierarchy_query)
            if geo_results:
                business_rule_tests.append(f"✅ Geographic hierarchy: {len(geo_results)} major geo combinations")
            
            # Log all business rule test results
            for test in business_rule_tests:
                self.logger.info(test)
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Business rules validation failed: {e}")
            return False
    
    def test_performance_benchmarks(self):
        """Test query performance on dimensional model"""
        self.logger.info("🚀 EXTENDED TEST: Performance Benchmarks")
        
        performance_tests = [
            {
                'name': 'Simple fact aggregation',
                'query': '''
                SELECT 
                    COUNT(*) as total_sales,
                    SUM(fct_quantity_num) as total_quantity,
                    SUM(fct_total_sum_$) as total_revenue
                FROM bl_dm.fct_sales
                '''
            },
            {
                'name': 'Sales by product category',
                'query': '''
                SELECT 
                    p.product_category,
                    COUNT(*) as sales_count,
                    SUM(f.fct_total_sum_$) as category_revenue
                FROM bl_dm.fct_sales f
                JOIN bl_dm.dim_products_scd p ON f.sale_product_surr_id = p.product_surr_id
                WHERE p.product_surr_id != -1
                GROUP BY p.product_category
                ORDER BY category_revenue DESC
                '''
            },
            {
                'name': 'Sales by customer geography',
                'query': '''
                SELECT 
                    c.customer_country,
                    c.customer_state,
                    COUNT(*) as sales_count,
                    SUM(f.fct_total_sum_$) as geo_revenue
                FROM bl_dm.fct_sales f
                JOIN bl_dm.dim_customers c ON f.sale_customer_surr_id = c.customer_surr_id
                WHERE c.customer_surr_id != -1
                GROUP BY c.customer_country, c.customer_state
                ORDER BY geo_revenue DESC
                LIMIT 10
                '''
            }
        ]
        
        performance_results = []
        
        for test in performance_tests:
            start_time = datetime.now()
            try:
                results = self.execute_query(test['query'])
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                
                performance_results.append({
                    'test': test['name'],
                    'duration': duration,
                    'rows_returned': len(results),
                    'status': 'SUCCESS'
                })
                
                self.logger.info(f"✅ {test['name']}: {duration:.3f}s, {len(results)} rows")
                
            except Exception as e:
                performance_results.append({
                    'test': test['name'],
                    'duration': None,
                    'rows_returned': 0,
                    'status': 'FAILED',
                    'error': str(e)
                })
                self.logger.error(f"❌ {test['name']} failed: {e}")
        
        # Performance threshold validation (adjust based on your requirements)
        slow_queries = [r for r in performance_results if r['duration'] and r['duration'] > 10.0]
        
        if slow_queries:
            self.logger.warning(f"⚠️ {len(slow_queries)} queries exceeded 10s threshold")
        else:
            self.logger.info("✅ All queries performed within acceptable time")
        
        return len([r for r in performance_results if r['status'] == 'SUCCESS']) == len(performance_tests)
    
    def test_data_lineage_and_audit_trail(self):
        """Test data lineage and audit capabilities"""
        self.logger.info("🚀 EXTENDED TEST: Data Lineage & Audit Trail")
        
        try:
            # Check ETL log entries
            log_query = """
            SELECT 
                procedure_name,
                COUNT(*) as executions,
                MAX(log_timestamp) as last_execution,
                SUM(CASE WHEN error_message IS NOT NULL THEN 1 ELSE 0 END) as error_count
            FROM bl_cl.etl_log
            WHERE log_timestamp >= CURRENT_DATE - INTERVAL '1 day'
            GROUP BY procedure_name
            ORDER BY last_execution DESC
            """
            
            log_results = self.execute_query(log_query)
            
            if log_results:
                self.logger.info(f"✅ Audit trail: {len(log_results)} procedures logged")
                
                for log in log_results[:5]:  # Show top 5
                    status = "✅" if log['error_count'] == 0 else f"❌({log['error_count']} errors)"
                    self.logger.info(f"  {log['procedure_name']}: {log['executions']} runs {status}")

            # Check source system tracking
            source_tracking_query = """
            SELECT 
                source_system,
                source_entity,
                COUNT(*) as record_count
            FROM bl_dm.dim_customers
            WHERE customer_surr_id != -1
            GROUP BY source_system, source_entity
            """

            source_results = self.execute_query(source_tracking_query)

            if source_results:
                self.logger.info("✅ Source system tracking working")
                for source in source_results:
                    self.logger.info(f"  {source['source_system']}.{source['source_entity']}: {source['record_count']:,} records")

            return True

        except Exception as e:
            self.logger.error(f"❌ Data lineage test failed: {e}")
            return False


# =============================================================================
# TEST RUNNER AND ORCHESTRATION
# =============================================================================

class TestRunner:
    """Main test orchestrator"""
    
    def __init__(self):
        self.smoke_tests = SmokeTests()
        self.critical_tests = CriticalTests()
        self.extended_tests = ExtendedTests()
        self.logger = logging.getLogger(__name__)
    
    def run_smoke_tests(self) -> bool:
        """Run smoke tests"""
        self.logger.info("🔥🔥🔥 STARTING SMOKE TESTS 🔥🔥🔥")
        
        tests = [
            self.smoke_tests.test_database_connectivity,
            self.smoke_tests.test_schemas_exist,
            self.smoke_tests.test_source_data_exists,
            self.smoke_tests.test_etl_log_table
        ]
        
        results = []
        for test in tests:
            try:
                result = test()
                results.append(result)
            except Exception as e:
                self.logger.error(f"Smoke test failed: {e}")
                results.append(False)
        
        success_rate = sum(results) / len(results) * 100
        self.logger.info(f"🔥 SMOKE TESTS COMPLETED: {success_rate:.1f}% success rate")
        
        return all(results)

    def run_critical_tests(self) -> bool:
        """Run streamlined critical tests"""
        self.logger.info("🎯🎯🎯 STARTING CRITICAL TESTS 🎯🎯🎯")

        # Streamlined test list - no redundant procedure calls
        tests = [
            self.critical_tests.test_full_etl_execution,  # Execute ETL once
            self.critical_tests.test_data_validation_post_etl,  # Validate results
            self.critical_tests.test_data_quality_checks,  # Quality checks
            self.critical_tests.test_scd_functionality  # SCD validation
        ]

        results = []
        for test in tests:
            try:
                result = test()
                results.append(result)
            except Exception as e:
                self.logger.error(f"Critical test failed: {e}")
                results.append(False)

        success_rate = sum(results) / len(results) * 100
        self.logger.info(f"🎯 CRITICAL TESTS COMPLETED: {success_rate:.1f}% success rate")

        return all(results)
    
    def run_extended_tests(self) -> bool:
        """Run extended tests"""
        self.logger.info("🚀🚀🚀 STARTING EXTENDED TESTS 🚀🚀🚀")
        
        tests = [
            self.extended_tests.test_full_etl_pipeline,
            self.extended_tests.test_business_rules_validation,
            self.extended_tests.test_performance_benchmarks,
            self.extended_tests.test_data_lineage_and_audit_trail
        ]
        
        results = []
        for test in tests:
            try:
                result = test()
                results.append(result)
            except Exception as e:
                self.logger.error(f"Extended test failed: {e}")
                results.append(False)
        
        success_rate = sum(results) / len(results) * 100
        self.logger.info(f"🚀 EXTENDED TESTS COMPLETED: {success_rate:.1f}% success rate")
        
        return all(results)
    
    def run_all_tests(self):
        """Run complete test suite"""
        self.logger.info("=" * 80)
        self.logger.info("DATA WAREHOUSE ETL TEST SUITE")
        self.logger.info("=" * 80)
        
        start_time = datetime.now()
        
        # Run tests in sequence
        smoke_passed = self.run_smoke_tests()
        
        if not smoke_passed:
            self.logger.error("❌ SMOKE TESTS FAILED - Stopping execution")
            return False
        
        critical_passed = self.run_critical_tests()
        
        if not critical_passed:
            self.logger.error("❌ CRITICAL TESTS FAILED - Extended tests may not be reliable")
        
        extended_passed = self.run_extended_tests()
        
        # Final summary
        end_time = datetime.now()
        duration = end_time - start_time
        
        self.logger.info("=" * 80)
        self.logger.info("TEST EXECUTION SUMMARY")
        self.logger.info("=" * 80)
        self.logger.info(f"🔥 Smoke Tests: {'✅ PASSED' if smoke_passed else '❌ FAILED'}")
        self.logger.info(f"🎯 Critical Tests: {'✅ PASSED' if critical_passed else '❌ FAILED'}")
        self.logger.info(f"🚀 Extended Tests: {'✅ PASSED' if extended_passed else '❌ FAILED'}")
        self.logger.info(f"⏱️ Total Duration: {duration}")
        self.logger.info("=" * 80)
        
        return smoke_passed and critical_passed and extended_passed


# =============================================================================
# MAIN EXECUTION BLOCK
# =============================================================================

if __name__ == "__main__":
    import sys

    print("=" * 80)
    print("🏢 GLOBAL ELECTRONICS DW - ETL TEST SUITE")
    print("=" * 80)
    print("🚀 Initializing test environment...")

    try:
        # Initialize test runner
        runner = TestRunner()

        # Check command line arguments for specific test types
        if len(sys.argv) > 1:
            test_type = sys.argv[1].lower()

            if test_type == "smoke":
                print("🔥 Running SMOKE TESTS only...")
                success = runner.run_smoke_tests()
            elif test_type == "critical":
                print("🎯 Running CRITICAL TESTS only...")
                success = runner.run_critical_tests()
            elif test_type == "extended":
                print("🚀 Running EXTENDED TESTS only...")
                success = runner.run_extended_tests()
            else:
                print(f"❓ Unknown test type: {test_type}")
                print("📋 Usage: python dw_etl_tests.py [smoke|critical|extended]")
                print("📋 Available options:")
                print("   - smoke    : Basic connectivity and structure tests")
                print("   - critical : Core ETL functionality tests")
                print("   - extended : Performance and business rules tests")
                sys.exit(1)
        else:
            # Run all tests in sequence
            print("🚀 Running COMPLETE TEST SUITE (all levels)...")
            success = runner.run_all_tests()

        # Final status
        print()
        if success:
            print("🎉 ALL TESTS COMPLETED SUCCESSFULLY! 🎉")
            print("✅ Your data warehouse is ready for production!")
        else:
            print("❌ SOME TESTS FAILED!")
            print("🔍 Check the detailed log output above for specific issues")
            print("📝 Review 'dw_tests.log' file for complete details")

    except KeyboardInterrupt:
        print("\n⏹️ Test execution interrupted by user")

    except Exception as e:
        print(f"\n💥 CRITICAL ERROR: {e}")
        print("🔧 Troubleshooting suggestions:")
        print("   1. Check PostgreSQL is running")
        print("   2. Verify database connection settings")
        print("   3. Ensure all required schemas exist")
        print("   4. Check if source data is loaded")

        import traceback

        print("\n📋 Detailed error information:")
        traceback.print_exc()

    finally:
        print("=" * 80)
        print("✅ Test execution completed!")
        print("📝 Detailed results saved to: dw_tests.log")
        print("📊 Check Power BI dashboards for data validation")
        print("=" * 80)