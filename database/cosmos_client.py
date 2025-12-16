"""
Azure Cosmos DB client with Managed Identity support.
Replaces Azure SQL Database for better connectivity and reliability.

Requirements:
- azure-cosmos
- azure-identity (for Managed Identity)
"""
import os
import logging
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from azure.cosmos import CosmosClient, PartitionKey, exceptions
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ============================================================================
# AZURE COSMOS DB CONNECTION WITH MANAGED IDENTITY
# ============================================================================
cosmos_client = None
database = None
db_available = False

# Container names (equivalent to SQL tables)
CONTAINER_PATIENTS = "patients"
CONTAINER_SOAP_RECORDS = "soap_records"
CONTAINER_VOICE_RECORDINGS = "voice_recordings"
CONTAINER_LOGGED_USERS = "logged_users"

# Container references
containers = {
    CONTAINER_PATIENTS: None,
    CONTAINER_SOAP_RECORDS: None,
    CONTAINER_VOICE_RECORDINGS: None,
    CONTAINER_LOGGED_USERS: None,
}

# Initialize Cosmos DB connection
try:
    COSMOS_ENDPOINT = os.getenv('COSMOS_ENDPOINT')
    COSMOS_DATABASE_NAME = os.getenv('COSMOS_DATABASE_NAME', 'medical-db')
    COSMOS_KEY = os.getenv('COSMOS_KEY')  # Optional: for key-based auth
    
    if not COSMOS_ENDPOINT:
        logger.warning("⚠️ COSMOS_ENDPOINT not set. Cosmos DB operations will fail.")
        raise ValueError("COSMOS_ENDPOINT is required")
    
    # Initialize Cosmos DB client
    if COSMOS_KEY:
        logger.info("🔐 Using Cosmos DB with key-based authentication")
        cosmos_client = CosmosClient(COSMOS_ENDPOINT, COSMOS_KEY)
    else:
        logger.info("🔐 Using Cosmos DB with Managed Identity")
        credential = DefaultAzureCredential()
        cosmos_client = CosmosClient(COSMOS_ENDPOINT, credential=credential)
    
    # Get or create database
    try:
        database = cosmos_client.get_database_client(COSMOS_DATABASE_NAME)
        database.read()
        logger.info(f"✅ Connected to existing Cosmos DB database: {COSMOS_DATABASE_NAME}")
    except exceptions.CosmosResourceNotFoundError:
        logger.info(f"📦 Creating new Cosmos DB database: {COSMOS_DATABASE_NAME}")
        database = cosmos_client.create_database(COSMOS_DATABASE_NAME)
        logger.info(f"✅ Created Cosmos DB database: {COSMOS_DATABASE_NAME}")
    
    db_available = True
    logger.info("✅ Azure Cosmos DB initialized successfully")
    
except Exception as e:
    logger.error(f"❌ Error initializing Cosmos DB: {e}")
    logger.warning("⚠️ App will start but database operations will fail")
    import traceback
    logger.error(traceback.format_exc())


def ensure_containers_exist():
    """
    Ensure all required containers exist. Create them if they don't.
    This function should be called on app startup.
    """
    if not db_available or not database:
        logger.error("❌ Cannot create containers: Cosmos DB not available")
        return False
    
    container_configs = [
        {
            'name': CONTAINER_PATIENTS,
            'partition_key': PartitionKey(path="/id"),
            'description': 'Patient records'
        },
        {
            'name': CONTAINER_SOAP_RECORDS,
            'partition_key': PartitionKey(path="/id"),
            'description': 'SOAP medical records'
        },
        {
            'name': CONTAINER_VOICE_RECORDINGS,
            'partition_key': PartitionKey(path="/id"),
            'description': 'Voice recording metadata'
        },
        {
            'name': CONTAINER_LOGGED_USERS,
            'partition_key': PartitionKey(path="/id"),
            'description': 'Logged user records'
        },
    ]
    
    for config in container_configs:
        container_name = config['name']
        try:
            # Try to get existing container
            container = database.get_container_client(container_name)
            container.read()
            containers[container_name] = container
            logger.info(f"✅ Container '{container_name}' already exists")
        except exceptions.CosmosResourceNotFoundError:
            # Container doesn't exist, create it
            try:
                logger.info(f"📦 Creating container '{container_name}'...")
                # For serverless Cosmos DB accounts, don't specify offer_throughput
                container = database.create_container(
                    id=container_name,
                    partition_key=config['partition_key']
                )
                containers[container_name] = container
                logger.info(f"✅ Created container '{container_name}'")
            except Exception as create_error:
                logger.error(f"❌ Failed to create container '{container_name}': {create_error}")
                raise
        except Exception as e:
            logger.error(f"❌ Error checking/creating container '{container_name}': {e}")
            raise
    
    logger.info("✅ All Cosmos DB containers are ready")
    return True


def get_container(container_name: str):
    """Get a container client by name."""
    if container_name not in containers or containers[container_name] is None:
        if not db_available or not database:
            raise RuntimeError("Cosmos DB is not available")
        try:
            containers[container_name] = database.get_container_client(container_name)
        except Exception as e:
            logger.error(f"❌ Failed to get container '{container_name}': {e}")
            raise
    return containers[container_name]


# ============================================================================
# AZURE BLOB STORAGE WITH MANAGED IDENTITY (UNCHANGED)
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
logger.info(f"  Blob Storage: {'✅ Available' if blob_available else '❌ Unavailable'}")
logger.info("=" * 60)

