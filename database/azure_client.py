"""
Azure SQL Database client with FULL Managed Identity support using pyodbc.
Uses the recommended Authentication=ActiveDirectoryMsi method.

Requirements:
- pyodbc
- azure-identity (for Blob Storage only)
- Microsoft ODBC Driver 18 for SQL Server (pre-installed on Azure Web Apps)
"""
import os
import logging
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential, ManagedIdentityCredential
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ============================================================================
# AZURE SQL DATABASE CONNECTION WITH MANAGED IDENTITY
# ============================================================================
conn_str = None
connection_string = None  # Full connection string for pyodbc
db_available = False
use_managed_identity = os.getenv('USE_MANAGED_IDENTITY', 'false').lower() == 'true'


def get_db_connection():
    """
    Get a database connection using pyodbc.
    Supports both Managed Identity and SQL Authentication.
    Uses the RECOMMENDED Authentication=ActiveDirectoryMsi method for Managed Identity.
    """
    import pyodbc
    
    if use_managed_identity:
        # Use Azure AD Managed Identity authentication
        server = os.getenv('AZURE_SQL_SERVER')
        database = os.getenv('AZURE_SQL_DATABASE')
        
        # RECOMMENDED METHOD: Use Authentication=ActiveDirectoryMsi
        # This lets pyodbc handle the entire Managed Identity flow
        connection_string = (
            f"Driver={{ODBC Driver 18 for SQL Server}};"
            f"Server=tcp:{server},1433;"
            f"Database={database};"
            f"Authentication=ActiveDirectoryMsi;"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
            f"Connection Timeout=30;"
        )
        
        # No token needed - pyodbc handles Managed Identity automatically
        conn = pyodbc.connect(connection_string)
        return conn
    else:
        # Use SQL Authentication
        server = os.getenv('AZURE_SQL_SERVER')
        database = os.getenv('AZURE_SQL_DATABASE')
        username = os.getenv('AZURE_SQL_USERNAME')
        password = os.getenv('AZURE_SQL_PASSWORD')
        
        connection_string = (
            f"Driver={{ODBC Driver 18 for SQL Server}};"
            f"Server=tcp:{server},1433;"
            f"Database={database};"
            f"Uid={username};"
            f"Pwd={{password}};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
            f"Connection Timeout=30;"
        )
        
        conn = pyodbc.connect(connection_string)
        return conn


# Test connection at startup
try:
    server = os.getenv('AZURE_SQL_SERVER')
    database = os.getenv('AZURE_SQL_DATABASE')
    
    if not all([server, database]):
        missing = []
        if not server: missing.append('AZURE_SQL_SERVER')
        if not database: missing.append('AZURE_SQL_DATABASE')
        logger.warning(f"⚠️ Missing required variables: {', '.join(missing)}")
        raise ValueError("Server and database are required")
    
    # ========================================================================
    # MANAGED IDENTITY AUTHENTICATION
    # ========================================================================
    if use_managed_identity:
        logger.info("🔐 Using Azure AD Managed Identity authentication")
        logger.info(f"🔄 Testing Azure SQL connection with Managed Identity to: {server}/{database}")
        
        try:
            import pyodbc
            
            # Test connection using the recommended method
            test_conn = get_db_connection()
            cursor = test_conn.cursor()
            cursor.execute("SELECT 1 AS test, CURRENT_USER AS current_user")
            result = cursor.fetchone()
            cursor.close()
            test_conn.close()
            
            db_available = True
            logger.info("✅ Azure SQL connection successful with Managed Identity")
            logger.info(f"✅ Connected as: {result[1] if result else 'unknown'}")
            
            # Store for use throughout the app
            conn_str = {'use_managed_identity': True}
            
        except ImportError:
            logger.error("❌ pyodbc not available. Install with: pip install pyodbc")
            logger.error("Note: Azure Web Apps have ODBC Driver 18 pre-installed")
            raise
        except Exception as mi_error:
            logger.error(f"❌ Managed Identity connection failed: {mi_error}")
            logger.error("Troubleshooting checklist:")
            logger.error("  1. System-assigned Managed Identity is enabled on your Web App")
            logger.error("  2. SQL Server firewall allows Azure services (Networking → Firewalls)")
            logger.error("  3. Database user exists: CREATE USER [test-acu-backend] FROM EXTERNAL PROVIDER")
            logger.error("  4. User has permissions: ALTER ROLE db_datareader ADD MEMBER [test-acu-backend]")
            logger.error("  5. Check SQL Server → Azure Active Directory → Set admin")
            raise
    
    # ========================================================================
    # SQL AUTHENTICATION
    # ========================================================================
    else:
        logger.info("🔐 Using SQL Authentication (username/password)")
        
        username = os.getenv('AZURE_SQL_USERNAME')
        password = os.getenv('AZURE_SQL_PASSWORD')
        
        if not all([username, password]):
            missing = []
            if not username: missing.append('AZURE_SQL_USERNAME')
            if not password: missing.append('AZURE_SQL_PASSWORD')
            logger.warning(f"⚠️ Missing SQL auth credentials: {', '.join(missing)}")
            raise ValueError("Username and password required for SQL Authentication")
        
        try:
            import pyodbc
            
            logger.info(f"🔄 Testing Azure SQL connection to: {server}/{database}")
            
            test_conn = get_db_connection()
            cursor = test_conn.cursor()
            cursor.execute("SELECT 1 AS test")
            result = cursor.fetchone()
            cursor.close()
            test_conn.close()
            
            db_available = True
            logger.info("✅ Azure SQL connection successful with SQL Authentication")
            
            # Store connection string for use throughout the app
            conn_str = {'use_managed_identity': False}
            
        except ImportError:
            logger.error("❌ pyodbc not available")
            raise
        except Exception as conn_error:
            logger.error(f"❌ SQL Authentication connection failed: {conn_error}")
            raise

except Exception as e:
    logger.error(f"❌ Error initializing Azure SQL: {e}")
    logger.warning("⚠️ App will start but database operations will fail")
    import traceback
    logger.error(traceback.format_exc())

# ============================================================================
# AZURE BLOB STORAGE WITH MANAGED IDENTITY
# ============================================================================
blob_service_client = None
blob_available = False

try:
    AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    AZURE_STORAGE_ACCOUNT_URL = os.getenv("AZURE_STORAGE_ACCOUNT_URL")
    
    if AZURE_STORAGE_CONNECTION_STRING:
        logger.info("🔐 Using Blob Storage connection string")
        blob_service_client = BlobServiceClient.from_connection_string(
            AZURE_STORAGE_CONNECTION_STRING
        )
        blob_available = True
        logger.info("✅ Azure Blob Storage initialized")
        
    elif AZURE_STORAGE_ACCOUNT_URL:
        logger.info("🔐 Using Blob Storage with Managed Identity")
        credential = DefaultAzureCredential()
        blob_service_client = BlobServiceClient(
            account_url=AZURE_STORAGE_ACCOUNT_URL,
            credential=credential
        )
        # Test connection
        list(blob_service_client.list_containers(max_results=1))
        blob_available = True
        logger.info("✅ Azure Blob Storage initialized with Managed Identity")
    else:
        logger.warning("⚠️ No Blob Storage credentials configured")
        logger.warning("Set either AZURE_STORAGE_CONNECTION_STRING or AZURE_STORAGE_ACCOUNT_URL")

except Exception as e:
    logger.error(f"❌ Error initializing Blob Storage: {e}")
    logger.warning("⚠️ Blob Storage operations will fail")

# ============================================================================
# STARTUP STATUS
# ============================================================================
logger.info("=" * 60)
logger.info("=== Azure Services Status ===")
logger.info(f"  Auth Mode: {'🔐 Managed Identity' if use_managed_identity else '🔑 SQL Auth'}")
logger.info(f"  Azure SQL: {'✅ Available' if db_available else '❌ Unavailable'}")
logger.info(f"  Blob Storage: {'✅ Available' if blob_available else '❌ Unavailable'}")
logger.info("=" * 60)