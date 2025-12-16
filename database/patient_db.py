"""
Patient database operations using Azure Cosmos DB.
Migrated from Azure SQL for better connectivity and reliability.
"""
import os
import uuid
import json
import time
import logging
from datetime import datetime
from typing import List, Dict, Optional
import base64
import hashlib
from azure.storage.blob import BlobServiceClient

from database.cosmos_client import (
    get_container,
    CONTAINER_PATIENTS,
    CONTAINER_SOAP_RECORDS,
    CONTAINER_VOICE_RECORDINGS,
    CONTAINER_LOGGED_USERS,
    blob_service_client,
    db_available,
    blob_available,
)
from utils.encryption import (
    encrypt_text,
    decrypt_text,
    encrypt_json,
    decrypt_json,
    encrypt_bytes,
    decrypt_bytes,
)

logger = logging.getLogger("PatientDB")

CONTAINER_NAME = "voice-recordings"


def check_db_available():
    """Check if database is available, raise exception if not."""
    if not db_available:
        raise RuntimeError(
            "Azure Cosmos DB is not available. "
            "Check configuration and ensure COSMOS_ENDPOINT is set."
        )


def check_blob_available():
    """Check if blob storage is available, raise exception if not."""
    if not blob_available or not blob_service_client:
        raise RuntimeError(
            "Azure Blob Storage is not available. "
            "Please configure AZURE_STORAGE_CONNECTION_STRING or AZURE_STORAGE_ACCOUNT_URL."
        )


def generate_token_id() -> str:
    return str(uuid.uuid4())


def generate_user_id() -> str:
    return str(uuid.uuid4())


def generate_numeric_id() -> int:
    """Generate a numeric ID using timestamp and random component for Cosmos DB compatibility."""
    # Use timestamp (seconds since epoch) + random component to create a unique numeric ID
    import random
    timestamp_sec = int(time.time())
    random_component = random.randint(100, 999)
    # Combine: timestamp (10 digits) + random (3 digits) = 13 digit number
    # This ensures uniqueness while keeping reasonable size
    numeric_id = timestamp_sec * 1000 + random_component
    return numeric_id


def convert_datetime_fields(data: Dict) -> Dict:
    """Convert datetime objects to ISO format strings"""
    if data is None:
        return data
    for key in data:
        if isinstance(data[key], datetime):
            data[key] = data[key].isoformat()
    return data


def _ensure_datetime_fields(doc: Dict) -> Dict:
    """Ensure datetime fields are properly handled for Cosmos DB"""
    if doc is None:
        return doc
    
    # Cosmos DB stores dates as ISO strings, but we need to handle them properly
    for key in ['created_at', 'updated_at']:
        if key in doc and isinstance(doc[key], str):
            try:
                # Try to parse ISO string back to datetime if needed
                doc[key] = datetime.fromisoformat(doc[key].replace('Z', '+00:00'))
            except:
                pass
    
    return doc


def create_patient(name: str, address: str = '', phone_number: str = '', problem: str = '', user_id: str = '') -> Dict:
    """Create a patient linked to a logged user."""
    check_db_available()
    
    if not user_id:
        raise ValueError('user_id is required to create a patient')

    try:
        container = get_container(CONTAINER_PATIENTS)
        
        # Generate numeric ID for backward compatibility with frontend
        patient_id = generate_numeric_id()
        # Use string ID for Cosmos DB document ID (partition key)
        patient_id_str = str(patient_id)
        created_at = datetime.utcnow().isoformat() + 'Z'
        
        patient_doc = {
            'id': patient_id_str,  # Cosmos DB document ID (string)
            'patient_id': patient_id,  # Numeric ID for frontend compatibility
            'user_id': user_id,
            'name': encrypt_text(name),
            'address': encrypt_text(address),
            'phone_number': encrypt_text(phone_number),
            'problem': encrypt_text(problem),
            'created_at': created_at,
        }
        
        # Create item in Cosmos DB
        container.create_item(body=patient_doc)
        
        logger.info(f"Patient created for user_id: {user_id}, patient_id: {patient_id}")
        
        # Retrieve and decrypt
        patient = container.read_item(item=patient_id_str, partition_key=patient_id_str)
        
        # Decrypt sensitive fields
        patient['name'] = decrypt_text(patient['name'])
        patient['address'] = decrypt_text(patient['address'])
        patient['phone_number'] = decrypt_text(patient['phone_number'])
        patient['problem'] = decrypt_text(patient['problem'])
        
        # Use numeric patient_id for frontend compatibility
        if 'patient_id' in patient:
            patient['id'] = patient['patient_id']
        else:
            patient['id'] = int(patient['id']) if patient['id'].isdigit() else patient_id
        
        convert_datetime_fields(patient)
        
        return patient
    except Exception as e:
        logger.error(f"create_patient error: {e}")
        raise Exception(f"Failed to create patient: {e}")


def get_all_patients(user_id: str = '') -> List[Dict]:
    """Get all patients for a user."""
    check_db_available()
    
    try:
        container = get_container(CONTAINER_PATIENTS)
        
        if user_id:
            # Query patients by user_id
            query = "SELECT * FROM c WHERE c.user_id = @user_id ORDER BY c.created_at DESC"
            parameters = [{"name": "@user_id", "value": user_id}]
            items = container.query_items(
                query=query,
                parameters=parameters,
                enable_cross_partition_query=True
            )
        else:
            # Get all patients
            query = "SELECT * FROM c ORDER BY c.created_at DESC"
            items = container.query_items(
                query=query,
                enable_cross_partition_query=True
            )
        
        patients = list(items)
        
        # Decrypt patient fields and normalize IDs
        for patient in patients:
            try:
                if patient.get('name'):
                    patient['name'] = decrypt_text(patient['name'])
                if patient.get('address'):
                    patient['address'] = decrypt_text(patient['address'])
                if patient.get('phone_number'):
                    patient['phone_number'] = decrypt_text(patient['phone_number'])
                if patient.get('problem'):
                    patient['problem'] = decrypt_text(patient['problem'])
                
                # Use numeric patient_id for frontend compatibility
                if 'patient_id' in patient:
                    patient['id'] = patient['patient_id']
                else:
                    # Try to convert string ID to int if possible
                    try:
                        patient['id'] = int(patient['id'])
                    except (ValueError, TypeError):
                        pass
                
                convert_datetime_fields(patient)
            except Exception:
                logger.exception('Failed to decrypt patient fields')
        
        return patients
    except Exception as e:
        logger.error(f"get_all_patients error: {e}")
        raise Exception(f"Failed to get patients: {e}")


def get_patient_by_id(patient_id: int, user_id: str = '') -> Optional[Dict]:
    """Get a patient by ID."""
    check_db_available()
    
    try:
        container = get_container(CONTAINER_PATIENTS)
        
        # Convert patient_id to string for Cosmos DB lookup
        patient_id_str = str(patient_id)
        
        # Try to find by document ID first, then by patient_id field
        try:
            patient = container.read_item(item=patient_id_str, partition_key=patient_id_str)
        except Exception:
            # If not found by ID, query by patient_id field
            query = "SELECT * FROM c WHERE c.patient_id = @patient_id"
            parameters = [{"name": "@patient_id", "value": patient_id}]
            items = list(container.query_items(
                query=query,
                parameters=parameters,
                enable_cross_partition_query=True
            ))
            if not items:
                return None
            patient = items[0]
        
        # Check ownership
        if user_id and patient.get('user_id') != user_id:
            logger.warning(f"Access denied: user {user_id} tried to access patient {patient_id}")
            return None
        
        # Decrypt patient fields
        try:
            if patient.get('name'):
                patient['name'] = decrypt_text(patient['name'])
            if patient.get('address'):
                patient['address'] = decrypt_text(patient['address'])
            if patient.get('phone_number'):
                patient['phone_number'] = decrypt_text(patient['phone_number'])
            if patient.get('problem'):
                patient['problem'] = decrypt_text(patient['problem'])
            
            # Use numeric patient_id for frontend compatibility
            if 'patient_id' in patient:
                patient['id'] = patient['patient_id']
            else:
                try:
                    patient['id'] = int(patient['id'])
                except (ValueError, TypeError):
                    pass
            
            convert_datetime_fields(patient)
        except Exception:
            logger.exception('Failed to decrypt patient')
        
        return patient
    except Exception as e:
        logger.error(f"get_patient_by_id error: {e}")
        raise Exception(f"Failed to get patient: {e}")


def save_soap_record(patient_id: int, audio_file_name: str = None, audio_local_path: str = None,
                     transcript: str = '', original_transcript: Optional[str] = None,
                     soap_sections: Optional[Dict] = None) -> Dict:
    """Save a SOAP record and optionally upload audio to Azure Blob Storage."""
    check_db_available()
    
    storage_path = None
    
    try:
        # Upload audio to Azure Blob Storage if provided
        if audio_local_path and os.path.exists(audio_local_path):
            check_blob_available()
            
            timestamp = int(time.time())
            filename = audio_file_name or os.path.basename(audio_local_path)
            storage_path = f"{patient_id}/{timestamp}_{filename}"
            
            with open(audio_local_path, 'rb') as f:
                raw = f.read()
            
            try:
                # Encrypt audio data
                enc_b64 = encrypt_bytes(raw)
                enc_bytes = base64.b64decode(enc_b64)
                
                # Upload to Azure Blob Storage
                blob_client = blob_service_client.get_blob_client(
                    container=CONTAINER_NAME,
                    blob=storage_path
                )
                blob_client.upload_blob(enc_bytes, overwrite=True)
                logger.info(f"Uploaded audio to Blob Storage: {storage_path}")
            except Exception as upload_error:
                logger.exception('Failed to encrypt/upload audio')
                raise Exception(f"Audio upload failed: {upload_error}")
        
        # Save SOAP record to Cosmos DB
        soap_container = get_container(CONTAINER_SOAP_RECORDS)
        
        # Generate numeric record ID for backward compatibility
        record_id = generate_numeric_id()
        record_id_str = str(record_id)
        created_at = datetime.utcnow().isoformat() + 'Z'
        
        audio_file_to_store = storage_path if storage_path else audio_file_name
        
        logger.info(f"SOAP sections before encryption: {soap_sections}")
        
        soap_doc = {
            'id': record_id_str,  # Cosmos DB document ID (string)
            'record_id': record_id,  # Numeric ID for frontend compatibility
            'patient_id': patient_id,  # Keep as int for queries
            'audio_file_name': audio_file_to_store,
            'transcript': encrypt_text(transcript),
            'original_transcript': encrypt_text(original_transcript) if original_transcript is not None else None,
            'soap_sections': encrypt_json(soap_sections or {}),
            'created_at': created_at,
            'updated_at': created_at,
        }
        
        logger.info(f"SOAP sections encrypted: {soap_doc['soap_sections']}")
        soap_container.create_item(body=soap_doc)
        
        # Retrieve the inserted record
        record = soap_container.read_item(item=record_id_str, partition_key=record_id_str)
        
        if not record:
            raise Exception('Failed to retrieve inserted soap record')
        
        # Create voice_recordings entry if audio was uploaded
        if storage_path:
            voice_container = get_container(CONTAINER_VOICE_RECORDINGS)
            voice_id = generate_numeric_id()
            voice_id_str = str(voice_id)
            
            voice_doc = {
                'id': voice_id_str,
                'voice_id': voice_id,
                'patient_id': patient_id,
                'soap_record_id': record_id,
                'storage_path': storage_path,
                'file_name': audio_file_name or os.path.basename(audio_local_path),
                'is_realtime': False,
                'created_at': created_at,
            }
            
            voice_container.create_item(body=voice_doc)
        
        record['storage_path'] = storage_path
        
        # Decrypt fields and normalize ID
        try:
            if record.get('transcript'):
                record['transcript'] = decrypt_text(record['transcript'])
            if record.get('original_transcript'):
                record['original_transcript'] = decrypt_text(record['original_transcript'])
            if record.get('soap_sections'):
                record['soap_sections'] = decrypt_json(record['soap_sections'])
            
            # Use numeric record_id for frontend compatibility
            if 'record_id' in record:
                record['id'] = record['record_id']
            else:
                try:
                    record['id'] = int(record['id'])
                except (ValueError, TypeError):
                    pass
        except Exception:
            logger.exception('Failed to decrypt soap record')
        
        convert_datetime_fields(record)
        
        return record
    except Exception as e:
        logger.error(f"Error saving SOAP record: {e}")
        raise


def get_patient_soap_records(patient_id: int) -> List[Dict]:
    """Get all SOAP records for a patient."""
    check_db_available()
    
    try:
        container = get_container(CONTAINER_SOAP_RECORDS)
        
        # FIX: Pass patient_id as INTEGER, not string
        query = "SELECT * FROM c WHERE c.patient_id = @patient_id ORDER BY c.created_at DESC"
        parameters = [{"name": "@patient_id", "value": patient_id}]  # ✅ Keep as int
        
        items = container.query_items(
            query=query,
            parameters=parameters,
            enable_cross_partition_query=True
        )
        
        records = list(items)
        logger.info(f"Retrieved {len(records)} SOAP records for patient {patient_id}")
        
        # Add debug logging to see what's in the database
        if len(records) == 0:
            logger.warning(f"No SOAP records found for patient_id={patient_id}")
            # Debug: Check what patient_ids exist
            debug_query = "SELECT DISTINCT c.patient_id FROM c"
            debug_items = list(container.query_items(
                query=debug_query,
                enable_cross_partition_query=True
            ))
            logger.info(f"Available patient_ids in database: {debug_items}")
        
        # Decrypt fields and normalize IDs
        for i, record in enumerate(records):
            try:
                logger.debug(f"Record {i}: soap_sections type = {type(record.get('soap_sections'))}, value = {record.get('soap_sections')}")
                
                if record.get('transcript'):
                    record['transcript'] = decrypt_text(record['transcript'])
                if record.get('original_transcript'):
                    record['original_transcript'] = decrypt_text(record['original_transcript'])
                if record.get('soap_sections'):
                    decrypted_soap = decrypt_json(record['soap_sections'])
                    logger.info(f"Record {i}: Decrypted SOAP sections: {decrypted_soap}")
                    record['soap_sections'] = decrypted_soap or {}
                else:
                    logger.warning(f"Record {i}: soap_sections is empty or None")
                    record['soap_sections'] = {}
                
                # Use numeric record_id for frontend compatibility
                if 'record_id' in record:
                    record['id'] = record['record_id']
                else:
                    try:
                        record['id'] = int(record['id'])
                    except (ValueError, TypeError):
                        pass
                
                convert_datetime_fields(record)
            except Exception as e:
                logger.exception(f'Failed to decrypt soap record {i}: {e}')
                record['soap_sections'] = {}
        
        return records
    except Exception as e:
        logger.error(f"get_patient_soap_records error: {e}")
        raise Exception(f"Failed to get SOAP records: {e}")

def update_soap_record(record_id: int, soap_sections: Dict) -> bool:
    """Update SOAP sections for a record."""
    check_db_available()
    
    try:
        container = get_container(CONTAINER_SOAP_RECORDS)
        
        record_id_str = str(record_id)
        
        # Try to find by document ID first, then by record_id field
        try:
            record = container.read_item(item=record_id_str, partition_key=record_id_str)
        except Exception:
            # If not found by ID, query by record_id field
            query = "SELECT * FROM c WHERE c.record_id = @record_id"
            parameters = [{"name": "@record_id", "value": record_id}]
            items = list(container.query_items(
                query=query,
                parameters=parameters,
                enable_cross_partition_query=True
            ))
            if not items:
                raise Exception(f"SOAP record {record_id} not found")
            record = items[0]
            record_id_str = record['id']  # Use the actual document ID
        
        # Update SOAP sections
        record['soap_sections'] = encrypt_json(soap_sections)
        record['updated_at'] = datetime.utcnow().isoformat() + 'Z'
        
        # Replace item
        container.replace_item(item=record_id_str, body=record)
        
        return True
    except Exception as e:
        logger.error(f"update_soap_record error: {e}")
        raise Exception(f"Failed to update SOAP record: {e}")


def get_voice_recordings(patient_id: int) -> List[Dict]:
    """Get all voice recordings for a patient."""
    check_db_available()
    
    try:
        container = get_container(CONTAINER_VOICE_RECORDINGS)
        
        patient_id_str = str(patient_id)
        query = "SELECT * FROM c WHERE c.patient_id = @patient_id ORDER BY c.created_at DESC"
        parameters = [{"name": "@patient_id", "value": patient_id_str}]
        
        items = container.query_items(
            query=query,
            parameters=parameters,
            enable_cross_partition_query=True
        )
        
        recordings = list(items)
        
        for recording in recordings:
            convert_datetime_fields(recording)
        
        return recordings
    except Exception as e:
        logger.error(f"get_voice_recordings error: {e}")
        raise Exception(f"Failed to get voice recordings: {e}")


def create_logged_user(email: str) -> Dict:
    """Create a logged user record."""
    check_db_available()
    
    user_id = generate_user_id()
    email_norm = (email or '').strip().lower()
    email_hash = hashlib.sha256(email_norm.encode('utf-8')).hexdigest()
    
    try:
        container = get_container(CONTAINER_LOGGED_USERS)
        
        created_at = datetime.utcnow().isoformat() + 'Z'
        
        user_doc = {
            'id': user_id,
            'email': encrypt_text(email),
            'email_hash': email_hash,
            'created_at': created_at,
        }
        
        container.create_item(body=user_doc)
        
        logger.info(f"Logged user created with id: {user_id}")
        
        # Retrieve user
        user = container.read_item(item=user_id, partition_key=user_id)
        
        if user and user.get('email'):
            user['email'] = decrypt_text(user['email'])
        
        return user
    except Exception as e:
        logger.error(f"create_logged_user error: {e}")
        raise Exception(f"Failed to create logged user: {e}")


def get_logged_user_by_email(email: str) -> Optional[Dict]:
    """Lookup logged user by email hash."""
    check_db_available()
    
    email_norm = (email or '').strip().lower()
    email_hash = hashlib.sha256(email_norm.encode('utf-8')).hexdigest()
    
    try:
        container = get_container(CONTAINER_LOGGED_USERS)
        
        query = "SELECT * FROM c WHERE c.email_hash = @email_hash"
        parameters = [{"name": "@email_hash", "value": email_hash}]
        
        items = container.query_items(
            query=query,
            parameters=parameters,
            enable_cross_partition_query=True
        )
        
        users = list(items)
        
        if not users:
            return None
        
        user = users[0]  # Get first match
        
        try:
            if user.get('email'):
                user['email'] = decrypt_text(user['email'])
        except Exception:
            logger.exception('Failed to decrypt logged user email')
        
        return user
    except Exception as e:
        logger.error(f"get_logged_user_by_email error: {e}")
        raise Exception(f"Failed to get logged user: {e}")


def get_or_create_logged_user(email: str) -> Dict:
    """Return existing user or create new one."""
    existing = get_logged_user_by_email(email)
    if existing:
        return existing
    return create_logged_user(email)
