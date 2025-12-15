"""
Azure SQL Database and Blob Storage client configuration.
Uses pymssql (pure Python driver) for SQL Server connectivity.
Handles missing environment variables gracefully to prevent startup crashes.
"""
import os
import logging
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ============================================================================
# AZURE SQL DATABASE CONNECTION
# ============================================================================
conn_str = None
db_available = False

try:
    # Get Azure SQL credentials from environment
    server = os.getenv('AZURE_SQL_SERVER')  # e.g., 'your-server.database.windows.net'
    database = os.getenv('AZURE_SQL_DATABASE')
    username = os.getenv('AZURE_SQL_USERNAME')
    password = os.getenv('AZURE_SQL_PASSWORD')
    
    # Validate all required credentials are present
    if all([server, database, username, password]):
        # Store connection parameters for pymssql
        # pymssql is a pure Python driver - no ODBC dependency needed
        conn_str = {
            'server': server,
            'database': database,
            'user': username,
            'password': password,
            'timeout': 30
        }
        
        # Test connection
        try:
            import pymssql
            test_conn = pymssql.connect(**conn_str)
            test_conn.close()
            db_available = True
            logger.info("✅ Azure SQL Database connection successful")
        except Exception as conn_error:
            logger.error(f"⚠️ Azure SQL connection test failed: {conn_error}")
            logger.warning("App will start but database operations will fail")
            # Don't raise - let app start anyway
    else:
        missing = []
        if not server: missing.append('AZURE_SQL_SERVER')
        if not database: missing.append('AZURE_SQL_DATABASE')
        if not username: missing.append('AZURE_SQL_USERNAME')
        if not password: missing.append('AZURE_SQL_PASSWORD')
        
        logger.warning(f"⚠️ Missing Azure SQL environment variables: {', '.join(missing)}")
        logger.warning("Database operations will not be available")

except ImportError as e:
    logger.error(f"⚠️ pyodbc not available: {e}")
    logger.error("Install pyodbc to use Azure SQL Database")
except Exception as e:
    logger.error(f"⚠️ Error initializing Azure SQL connection: {e}")

# ============================================================================
# AZURE BLOB STORAGE CONNECTION
# ============================================================================
blob_service_client = None
blob_available = False

try:
    AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    
    if AZURE_STORAGE_CONNECTION_STRING:
        blob_service_client = BlobServiceClient.from_connection_string(
            AZURE_STORAGE_CONNECTION_STRING
        )
        blob_available = True
        logger.info("✅ Azure Blob Storage initialized successfully")
    else:
        logger.warning("⚠️ AZURE_STORAGE_CONNECTION_STRING not set")
        logger.warning("Blob storage operations will not be available")

except Exception as e:
    logger.error(f"⚠️ Error initializing Azure Blob Storage: {e}")
    logger.warning("Blob storage operations will not be available")

# ============================================================================
# STARTUP STATUS LOG
# ============================================================================
logger.info("=== Azure Services Status ===")
logger.info(f"  Azure SQL Database: {'✅ Available' if db_available else '❌ Unavailable'}")
logger.info(f"  Azure Blob Storage: {'✅ Available' if blob_available else '❌ Unavailable'}")

if not db_available or not blob_available:
    logger.warning("⚠️ Some Azure services are unavailable - check environment variables")