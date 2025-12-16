"""
Patient database operations using Azure Cosmos DB.
Much simpler and more reliable than Azure SQL!
"""
import os
import uuid
import json
import time
import logging
from datetime import datetime
from typing import List, Dict, Optional
import io
import base64
import hashlib
from azure.cosmos import exceptions

from database.cosmos_client import (
    users_container, 
    patients_container, 
    soap_container, 
    voice_container,
    blob_service_client, 
    db_available, 
    blob_available
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
            "Check configuration and ensure COSMOS_ENDPOINT and COSMOS_KEY are set."
        )


def check_blob_available():
    """Check if blob storage is available, raise exception if not."""
    if not blob_available or not blob_service_client:
        raise RuntimeError(
            "Azure Blob Storage is not available. "
            "Please configure AZURE_STORAGE_CONNECTION_STRING or AZURE_STORAGE_ACCOUNT_URL."
        )


def generate_uuid() -> str:
    """Generate a UUID for document IDs"""
    return str(uuid.uuid4())


def convert_datetime_fields(data: Dict) -> Dict:
    """Cosmos DB stores datetimes as strings, so this is mainly for consistency"""
    # In Cosmos DB, we store everything as ISO strings already
    return data


# ============================================================================
# LOGGED USERS
# ============================================================================

def create_logged_user(email: str) -> Dict:
    """Create a logged user record."""
    check_db_available()
    
    user_id = generate_uuid()
    email_norm = (email or '').strip().lower()
    email_hash = hashlib.sha256(email_norm.encode('utf-8')).hexdigest()
    
    try:
        user_doc = {
            "id": user_id,  # Partition key
            "email": encrypt_text(email),
            "email_hash": email_hash,
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat()
        }
        
        # Create user - single line!
        created = users_container.create_item(body=user_doc)
        logger.info(f"Logged user created with id: {user_id}")
        
        # Decrypt for return
        created['email'] = decrypt_text(created['email'])
        return created
        
    except exceptions.CosmosResourceExistsError:
        logger.warning(f"User already exists with id: {user_id}")
        raise Exception("User already exists")
    except Exception as e:
        logger.error(f"create_logged_user error: {e}")
        raise Exception(f"Failed to create logged user: {e}")


def get_logged_user_by_email(email: str) -> Optional[Dict]:
    """Lookup logged user by email hash."""
    check_db_available()
    
    email_norm = (email or '').strip().lower()
    email_hash = hashlib.sha256(email_norm.encode('utf-8')).hexdigest()
    
    try:
        # Query by email_hash
        query = "SELECT * FROM c WHERE c.email_hash = @email_hash"
        parameters = [{"name": "@email_hash", "value": email_hash}]
        
        items = list(users_container.query_items(
            query=query,
            parameters=parameters,
            enable_cross_partition_query=True  # Since we're not querying by partition key
        ))
        
        if not items:
            return None
        
        user = items[0]
        
        # Decrypt email
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


# ============================================================================
# PATIENTS
# ============================================================================

def create_patient(name: str, address: str = '', phone_number: str = '', 
                   problem: str = '', user_id: str = '') -> Dict:
    """Create a patient linked to a logged user."""
    check_db_available()
    
    if not user_id:
        raise ValueError('user_id is required to create a patient')

    try:
        patient_id = generate_uuid()
        
        patient_doc = {
            "id": patient_id,
            "user_id": user_id,  # Partition key - groups all patients for this user
            "name": encrypt_text(name),
            "address": encrypt_text(address),
            "phone_number": encrypt_text(phone_number),
            "problem": encrypt_text(problem),
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat()
        }
        
        # Create patient - single line!
        created = patients_container.create_item(body=patient_doc)
        logger.info(f"Patient created for user_id: {user_id}, patient_id: {patient_id}")
        
        # Decrypt sensitive fields
        created['name'] = decrypt_text(created['name'])
        created['address'] = decrypt_text(created['address'])
        created['phone_number'] = decrypt_text(created['phone_number'])
        created['problem'] = decrypt_text(created['problem'])
        
        return created
        
    except Exception as e:
        logger.error(f"create_patient error: {e}")
        raise Exception(f"Failed to create patient: {e}")


def get_all_patients(user_id: str = '') -> List[Dict]:
    """Get all patients for a user."""
    check_db_available()
    
    try:
        if user_id:
            # Fast query using partition key!
            query = "SELECT * FROM c WHERE c.user_id = @user_id ORDER BY c.created_at DESC"
            parameters = [{"name": "@user_id", "value": user_id}]
            
            # partition_key makes this SUPER fast
            items = list(patients_container.query_items(
                query=query,
                parameters=parameters,
                partition_key=user_id
            ))
        else:
            # Get all patients (cross-partition query)
            query = "SELECT * FROM c ORDER BY c.created_at DESC"
            items = list(patients_container.query_items(
                query=query,
                enable_cross_partition_query=True
            ))
        
        # Decrypt patient fields
        for patient in items:
            try:
                if patient.get('name'):
                    patient['name'] = decrypt_text(patient['name'])
                if patient.get('address'):
                    patient['address'] = decrypt_text(patient['address'])
                if patient.get('phone_number'):
                    patient['phone_number'] = decrypt_text(patient['phone_number'])
                if patient.get('problem'):
                    patient['problem'] = decrypt_text(patient['problem'])
            except Exception:
                logger.exception('Failed to decrypt patient fields')
        
        return items
        
    except Exception as e:
        logger.error(f"get_all_patients error: {e}")
        raise Exception(f"Failed to get patients: {e}")


def get_patient_by_id(patient_id: str, user_id: str = '') -> Optional[Dict]:
    """Get a patient by ID."""
    check_db_available()
    
    try:
        # Try to read directly if we have user_id (partition key)
        if user_id:
            try:
                patient = patients_container.read_item(
                    item=patient_id,
                    partition_key=user_id
                )
            except exceptions.CosmosResourceNotFoundError:
                return None
        else:
            # Query without partition key (slower)
            query = "SELECT * FROM c WHERE c.id = @patient_id"
            parameters = [{"name": "@patient_id", "value": patient_id}]
            
            items = list(patients_container.query_items(
                query=query,
                parameters=parameters,
                enable_cross_partition_query=True
            ))
            
            if not items:
                return None
            
            patient = items[0]
        
        # Check ownership if user_id provided
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
        except Exception:
            logger.exception('Failed to decrypt patient')
        
        return patient
        
    except Exception as e:
        logger.error(f"get_patient_by_id error: {e}")
        raise Exception(f"Failed to get patient: {e}")


# ============================================================================
# SOAP RECORDS
# ============================================================================

def save_soap_record(patient_id: str, audio_file_name: str = None, 
                     audio_local_path: str = None, transcript: str = '', 
                     original_transcript: Optional[str] = None,
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
        record_id = generate_uuid()
        audio_file_to_store = storage_path if storage_path else audio_file_name
        
        soap_doc = {
            "id": record_id,
            "patient_id": patient_id,  # Partition key
            "audio_file_name": audio_file_to_store,
            "transcript": encrypt_text(transcript),
            "original_transcript": encrypt_text(original_transcript) if original_transcript is not None else None,
            "soap_sections": encrypt_json(soap_sections or {}),
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat()
        }
        
        # Create SOAP record - single line!
        created = soap_container.create_item(body=soap_doc)
        logger.info(f"SOAP record created with id: {record_id}")
        
        # Create voice_recordings entry if audio was uploaded
        if storage_path:
            voice_doc = {
                "id": generate_uuid(),
                "patient_id": patient_id,  # Partition key
                "soap_record_id": record_id,
                "storage_path": storage_path,
                "file_name": audio_file_name or os.path.basename(audio_local_path),
                "is_realtime": False,
                "created_at": datetime.utcnow().isoformat()
            }
            voice_container.create_item(body=voice_doc)
            logger.info(f"Voice recording entry created")
        
        created['storage_path'] = storage_path
        
        # Decrypt fields
        try:
            if created.get('transcript'):
                created['transcript'] = decrypt_text(created['transcript'])
            if created.get('original_transcript'):
                created['original_transcript'] = decrypt_text(created['original_transcript'])
            if created.get('soap_sections'):
                created['soap_sections'] = decrypt_json(created['soap_sections'])
        except Exception:
            logger.exception('Failed to decrypt soap record')
        
        return created
        
    except Exception as e:
        logger.error(f"Error saving SOAP record: {e}")
        raise


def get_patient_soap_records(patient_id: str) -> List[Dict]:
    """Get all SOAP records for a patient."""
    check_db_available()
    
    try:
        # Fast query using partition key!
        query = "SELECT * FROM c WHERE c.patient_id = @patient_id ORDER BY c.created_at DESC"
        parameters = [{"name": "@patient_id", "value": patient_id}]
        
        records = list(soap_container.query_items(
            query=query,
            parameters=parameters,
            partition_key=patient_id  # Super fast!
        ))
        
        # Decrypt fields
        for record in records:
            try:
                if record.get('transcript'):
                    record['transcript'] = decrypt_text(record['transcript'])
                if record.get('original_transcript'):
                    record['original_transcript'] = decrypt_text(record['original_transcript'])
                if record.get('soap_sections'):
                    record['soap_sections'] = decrypt_json(record['soap_sections'])
            except Exception:
                logger.exception('Failed to decrypt soap record')
        
        return records
        
    except Exception as e:
        logger.error(f"get_patient_soap_records error: {e}")
        raise Exception(f"Failed to get SOAP records: {e}")


def update_soap_record(record_id: str, soap_sections: Dict, patient_id: str = None) -> bool:
    """Update SOAP sections for a record."""
    check_db_available()
    
    try:
        # Read the existing record first
        if patient_id:
            # Fast read with partition key
            record = soap_container.read_item(
                item=record_id,
                partition_key=patient_id
            )
        else:
            # Query without partition key
            query = "SELECT * FROM c WHERE c.id = @record_id"
            parameters = [{"name": "@record_id", "value": record_id}]
            
            items = list(soap_container.query_items(
                query=query,
                parameters=parameters,
                enable_cross_partition_query=True
            ))
            
            if not items:
                raise Exception("SOAP record not found")
            
            record = items[0]
        
        # Update the record
        record['soap_sections'] = encrypt_json(soap_sections)
        record['updated_at'] = datetime.utcnow().isoformat()
        
        # Replace the item
        soap_container.replace_item(item=record['id'], body=record)
        logger.info(f"SOAP record updated: {record_id}")
        
        return True
        
    except Exception as e:
        logger.error(f"update_soap_record error: {e}")
        raise Exception(f"Failed to update SOAP record: {e}")


def get_voice_recordings(patient_id: str) -> List[Dict]:
    """Get all voice recordings for a patient."""
    check_db_available()
    
    try:
        # Fast query using partition key!
        query = "SELECT * FROM c WHERE c.patient_id = @patient_id ORDER BY c.created_at DESC"
        parameters = [{"name": "@patient_id", "value": patient_id}]
        
        recordings = list(voice_container.query_items(
            query=query,
            parameters=parameters,
            partition_key=patient_id
        ))
        
        return recordings
        
    except Exception as e:
        logger.error(f"get_voice_recordings error: {e}")
        raise Exception(f"Failed to get voice recordings: {e}")