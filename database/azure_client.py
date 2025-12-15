"""
Azure SQL Database client with FULL Managed Identity support using pyodbc.
pyodbc has native support for Azure AD authentication, making it ideal for Azure deployments.

Requirements:
- pyodbc
- azure-identity
- Microsoft ODBC Driver 18 for SQL Server (pre-installed on Azure Web Apps)
"""
import os
import logging
import struct
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

def get_azure_sql_token():
    """
    Get Azure AD access token for Azure SQL Database.
    Returns the token as bytes in the format required by pyodbc.
    """
    try:
        # Try Managed Identity first (best for Azure Web Apps)
        credential = ManagedIdentityCredential()
        token = credential.get_token("https://database.windows.net/.default")
        logger.info("✅ Acquired Azure SQL token using Managed Identity")
    except Exception as mi_error:
        logger.warning(f"⚠️ Managed Identity not available: {mi_error}")
        try:
            # Fallback to DefaultAzureCredential (works locally)
            credential = DefaultAzureCredential()
            token = credential.get_token("https://database.windows.net/.default")
            logger.info("✅ Acquired Azure SQL token using DefaultAzureCredential")
        except Exception as default_error:
            logger.error(f"❌ Failed to acquire Azure AD token: {default_error}")
            raise
    
    # Convert token to format required by pyodbc
    token_bytes = token.token.encode("UTF-16-LE")
    token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)
    return token_struct


def get_db_connection():
    """
    Get a database connection using pyodbc.
    Supports both Managed Identity and SQL Authentication.
    """
    import pyodbc
    
    if use_managed_identity:
        # Use Azure AD token authentication
        server = os.getenv('AZURE_SQL_SERVER')
        database = os.getenv('AZURE_SQL_DATABASE')
        
        connection_string = (
            f"Driver={{ODBC Driver 18 for SQL Server}};"
            f"Server=tcp:{server},1433;"
            f"Database={database};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
            f"Connection Timeout=60;"
        )
        
        # Get fresh token
        token = get_azure_sql_token()
        
        # Connect with token
        conn = pyodbc.connect(connection_string, attrs_before={1256: token})
        return conn
    else:
        # Use SQL Authentication
        return pyodbc.connect(connection_string)


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
        
        try:
            import pyodbc
            
            # Build connection string for Managed Identity
            connection_string = (
                f"Driver={{ODBC Driver 18 for SQL Server}};"
                f"Server=tcp:{server},1433;"
                f"Database={database};"
                f"Encrypt=yes;"
                f"TrustServerCertificate=no;"
                f"Connection Timeout=60;"
            )
            
            logger.info(f"🔄 Testing Azure SQL connection with Managed Identity to: {server}/{database}")
            
            # Get token and connect
            token = get_azure_sql_token()
            test_conn = pyodbc.connect(connection_string, attrs_before={1256: token})
            
            # Test query
            cursor = test_conn.cursor()
            cursor.execute("SELECT 1 AS test, CURRENT_USER AS user")
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
            logger.error("Make sure:")
            logger.error("  1. System-assigned Managed Identity is enabled on your Web App")
            logger.error("  2. Managed Identity has access to Azure SQL Database")
            logger.error("  3. Azure SQL admin ran: CREATE USER [your-webapp-name] FROM EXTERNAL PROVIDER")
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
            raise ValueError("Username and password required")
        
        try:
            import pyodbc
            
            connection_string = (
                f"Driver={{ODBC Driver 18 for SQL Server}};"
                f"Server=tcp:{server},1433;"
                f"Database={database};"
                f"Uid={username};"
                f"Pwd={password};"
                f"Encrypt=yes;"
                f"TrustServerCertificate=no;"
                f"Connection Timeout=60;"
            )
            
            logger.info(f"🔄 Testing Azure SQL connection to: {server}/{database}")
            
            test_conn = pyodbc.connect(connection_string)
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
        list(blob_service_client.list_containers(max_results=1))
        blob_available = True
        logger.info("✅ Azure Blob Storage initialized with Managed Identity")
    else:
        logger.warning("⚠️ No Blob Storage credentials configured")

except Exception as e:
    logger.error(f"❌ Error initializing Blob Storage: {e}")

# ============================================================================
# STARTUP STATUS
# ============================================================================
logger.info("=" * 60)
logger.info("=== Azure Services Status ===")
logger.info(f"  Auth Mode: {'🔐 Managed Identity' if use_managed_identity else '🔑 SQL Auth'}")
logger.info(f"  Azure SQL: {'✅ Available' if db_available else '❌ Unavailable'}")
logger.info(f"  Blob Storage: {'✅ Available' if blob_available else '❌ Unavailable'}")
logger.info("=" * 60)