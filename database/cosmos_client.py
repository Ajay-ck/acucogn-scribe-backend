"""
Azure Cosmos DB client - MUCH simpler than Azure SQL!
No Managed Identity headaches, just works with a connection string.

Automatically creates database and containers on startup if they don't exist.
Supports both Serverless and Provisioned throughput modes.
"""
import os
import logging
from azure.cosmos import CosmosClient, PartitionKey, exceptions
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ============================================================================
# AZURE COSMOS DB CONNECTION (Simple and Reliable!)
# ============================================================================
cosmos_client = None
database = None
users_container = None
patients_container = None
soap_container = None
voice_container = None
db_available = False


def create_container_safe(database, container_id: str, partition_key_path: str):
    """
    Create container if it doesn't exist.
    Handles both Serverless and Provisioned throughput modes.
    """
    try:
        # Try serverless mode first (no throughput specified)
        container = database.create_container_if_not_exists(
            id=container_id,
            partition_key=PartitionKey(path=partition_key_path)
        )
        logger.info(f"✅ Container created/verified: {container_id}")
        return container
    except exceptions.CosmosHttpResponseError as e:
        # If serverless fails, try with manual throughput
        if "offer" in str(e).lower() or "throughput" in str(e).lower():
            logger.info(f"Serverless not available, using provisioned throughput for {container_id}")
            container = database.create_container_if_not_exists(
                id=container_id,
                partition_key=PartitionKey(path=partition_key_path),
                offer_throughput=400  # Minimum for manual throughput
            )
            logger.info(f"✅ Container created/verified: {container_id}")
            return container
        else:
            raise


try:
    COSMOS_ENDPOINT = os.getenv("COSMOS_ENDPOINT")
    COSMOS_KEY = os.getenv("COSMOS_KEY")
    
    if not COSMOS_ENDPOINT or not COSMOS_KEY:
        logger.warning("⚠️ Cosmos DB credentials not configured")
        logger.warning("Set COSMOS_ENDPOINT and COSMOS_KEY in environment variables")
        logger.warning("Example:")
        logger.warning("  COSMOS_ENDPOINT=https://your-account.documents.azure.com:443/")
        logger.warning("  COSMOS_KEY=your-primary-key")
    else:
        logger.info("🔐 Connecting to Azure Cosmos DB...")
        logger.info(f"   Endpoint: {COSMOS_ENDPOINT}")
        
        # Simple connection - just endpoint and key!
        cosmos_client = CosmosClient(COSMOS_ENDPOINT, COSMOS_KEY)
        
        # Create database if it doesn't exist
        logger.info("📦 Creating/verifying database: medical_db")
        database = cosmos_client.create_database_if_not_exists(id="medical_db")
        logger.info("✅ Database ready: medical_db")
        
        # Create containers (like tables in SQL)
        # These will be automatically created on first run!
        logger.info("📦 Creating/verifying containers...")
        
        # Container 1: logged_users (partition key: /id)
        users_container = create_container_safe(
            database=database,
            container_id="logged_users",
            partition_key_path="/id"
        )
        
        # Container 2: patients (partition key: /user_id)
        # This groups all patients for the same user together for fast queries
        patients_container = create_container_safe(
            database=database,
            container_id="patients",
            partition_key_path="/user_id"
        )
        
        # Container 3: soap_records (partition key: /patient_id)
        # This groups all SOAP records for the same patient together
        soap_container = create_container_safe(
            database=database,
            container_id="soap_records",
            partition_key_path="/patient_id"
        )
        
        # Container 4: voice_recordings (partition key: /patient_id)
        voice_container = create_container_safe(
            database=database,
            container_id="voice_recordings",
            partition_key_path="/patient_id"
        )
        
        db_available = True
        logger.info("=" * 60)
        logger.info("✅ Cosmos DB initialized successfully - NO TIMEOUTS! 🎉")
        logger.info("   All containers are ready to use!")
        logger.info("=" * 60)

except exceptions.CosmosHttpResponseError as e:
    logger.error(f"❌ Cosmos DB HTTP Error: {e.status_code} - {e.message}")
    logger.error("   Check your COSMOS_ENDPOINT and COSMOS_KEY are correct")
    logger.warning("⚠️ App will start but database operations will fail")
except Exception as e:
    logger.error(f"❌ Error initializing Cosmos DB: {e}")
    logger.warning("⚠️ App will start but database operations will fail")
    import traceback
    logger.error(traceback.format_exc())

# ============================================================================
# AZURE BLOB STORAGE (same as before)
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
logger.info(f"  Cosmos DB: {'✅ Available' if db_available else '❌ Unavailable'}")
if db_available:
    logger.info(f"    - Database: medical_db")
    logger.info(f"    - Containers: logged_users, patients, soap_records, voice_recordings")
logger.info(f"  Blob Storage: {'✅ Available' if blob_available else '❌ Unavailable'}")
logger.info("=" * 60)