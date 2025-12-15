"""
Patient database operations using pyodbc for Azure SQL.
Supports both Managed Identity and SQL Authentication.
"""
import os
import uuid
import json
import time
import logging
from datetime import datetime
from typing import List, Dict, Optional
import pyodbc
import io
import base64
import hashlib
from azure.storage.blob import BlobServiceClient

from database.azure_client import connection_string, blob_service_client, db_available, blob_available, use_managed_identity, get_db_connection
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
            "Azure SQL Database is not available. "
            "Check configuration and ensure Managed Identity or SQL credentials are set."
        )


def check_blob_available():
    """Check if blob storage is available, raise exception if not."""
    if not blob_available or not blob_service_client:
        raise RuntimeError(
            "Azure Blob Storage is not available. "
            "Please configure AZURE_STORAGE_CONNECTION_STRING or AZURE_STORAGE_ACCOUNT_URL."
        )


def row_to_dict(cursor, row):
    """Convert pyodbc row to dictionary"""
    if row is None:
        return None
    columns = [column[0] for column in cursor.description]
    return dict(zip(columns, row))


def generate_token_id() -> str:
    return str(uuid.uuid4())


def generate_user_id() -> str:
    return str(uuid.uuid4())


def convert_datetime_fields(data: Dict) -> Dict:
    """Convert datetime objects to ISO format strings"""
    if data is None:
        return data
    for key in data:
        if isinstance(data[key], datetime):
            data[key] = data[key].isoformat()
    return data


def create_patient(name: str, address: str = '', phone_number: str = '', problem: str = '', user_id: str = '') -> Dict:
    """Create a patient linked to a logged user."""
    check_db_available()
    
    if not user_id:
        raise ValueError('user_id is required to create a patient')

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        query = """
            INSERT INTO patients (user_id, name, address, phone_number, problem)
            OUTPUT inserted.id
            VALUES (?, ?, ?, ?, ?)
        """
        cursor.execute(query, (
            user_id,
            encrypt_text(name),
            encrypt_text(address),
            encrypt_text(phone_number),
            encrypt_text(problem)
        ))
        
        result = cursor.fetchone()
        if result is None:
            raise Exception("Failed to insert patient record")
        
        new_id = result[0]
        conn.commit()
        
        cursor.execute("SELECT * FROM patients WHERE id = ?", (new_id,))
        row = cursor.fetchone()
        patient = row_to_dict(cursor, row)
        
        logger.info(f"Patient created for user_id: {user_id}, patient_id: {new_id}")
        
        if patient is None:
            raise Exception(f"Patient record not found after insertion")
        
        # Decrypt sensitive fields
        patient['name'] = decrypt_text(patient['name'])
        patient['address'] = decrypt_text(patient['address'])
        patient['phone_number'] = decrypt_text(patient['phone_number'])
        patient['problem'] = decrypt_text(patient['problem'])
        
        convert_datetime_fields(patient)
        
        return patient
    except Exception as e:
        logger.error(f"create_patient error: {e}")
        if conn:
            conn.rollback()
        raise Exception(f"Failed to create patient: {e}")
    finally:
        if conn:
            conn.close()


def get_all_patients(user_id: str = '') -> List[Dict]:
    """Get all patients for a user."""
    check_db_available()
    
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        if user_id:
            query = "SELECT * FROM patients WHERE user_id = ? ORDER BY created_at DESC"
            cursor.execute(query, (user_id,))
        else:
            query = "SELECT * FROM patients ORDER BY created_at DESC"
            cursor.execute(query)
        
        rows = cursor.fetchall()
        patients = [row_to_dict(cursor, row) for row in rows]
        
        # Decrypt patient fields
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
                
                convert_datetime_fields(patient)
            except Exception:
                logger.exception('Failed to decrypt patient fields')
        
        return patients
    except Exception as e:
        logger.error(f"get_all_patients error: {e}")
        raise Exception(f"Failed to get patients: {e}")
    finally:
        if conn:
            conn.close()


def get_patient_by_id(patient_id: int, user_id: str = '') -> Optional[Dict]:
    """Get a patient by ID."""
    check_db_available()
    
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        query = "SELECT TOP 1 * FROM patients WHERE id = ?"
        cursor.execute(query, (patient_id,))
        row = cursor.fetchone()
        
        if not row:
            return None
        
        patient = row_to_dict(cursor, row)
        
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
            
            convert_datetime_fields(patient)
        except Exception:
            logger.exception('Failed to decrypt patient')
        
        return patient
    except Exception as e:
        logger.error(f"get_patient_by_id error: {e}")
        raise Exception(f"Failed to get patient: {e}")
    finally:
        if conn:
            conn.close()


def save_soap_record(patient_id: int, audio_file_name: str = None, audio_local_path: str = None,
                     transcript: str = '', original_transcript: Optional[str] = None,
                     soap_sections: Optional[Dict] = None) -> Dict:
    """Save a SOAP record and optionally upload audio to Azure Blob Storage."""
    check_db_available()
    
    storage_path = None
    conn = None
    
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
        
        # Save SOAP record to database
        conn = get_db_connection()
        cursor = conn.cursor()
        
        audio_file_to_store = storage_path if storage_path else audio_file_name
        
        query = """
            INSERT INTO soap_records (patient_id, audio_file_name, transcript, original_transcript, soap_sections)
            OUTPUT inserted.id
            VALUES (?, ?, ?, ?, ?)
        """
        cursor.execute(query, (
            patient_id,
            audio_file_to_store,
            encrypt_text(transcript),
            encrypt_text(original_transcript) if original_transcript is not None else None,
            encrypt_json(soap_sections or {})
        ))
        
        result = cursor.fetchone()
        if result is None:
            raise Exception('Failed to insert soap record')
        
        record_id = result[0]
        conn.commit()
        
        # Retrieve the inserted record
        cursor.execute("SELECT * FROM soap_records WHERE id = ?", (record_id,))
        row = cursor.fetchone()
        record = row_to_dict(cursor, row)
        
        if not record:
            raise Exception('Failed to retrieve inserted soap record')
        
        # Create voice_recordings entry if audio was uploaded
        if storage_path:
            voice_query = """
                INSERT INTO voice_recordings (patient_id, soap_record_id, storage_path, file_name, is_realtime)
                VALUES (?, ?, ?, ?, ?)
            """
            cursor.execute(voice_query, (
                patient_id,
                record_id,
                storage_path,
                audio_file_name or os.path.basename(audio_local_path),
                False
            ))
            conn.commit()
        
        record['storage_path'] = storage_path
        
        # Decrypt fields
        try:
            if record.get('transcript'):
                record['transcript'] = decrypt_text(record['transcript'])
            if record.get('original_transcript'):
                record['original_transcript'] = decrypt_text(record['original_transcript'])
            if record.get('soap_sections'):
                record['soap_sections'] = decrypt_json(record['soap_sections'])
        except Exception:
            logger.exception('Failed to decrypt soap record')
        
        convert_datetime_fields(record)
        
        return record
    except Exception as e:
        logger.error(f"Error saving SOAP record: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


def get_patient_soap_records(patient_id: int) -> List[Dict]:
    """Get all SOAP records for a patient."""
    check_db_available()
    
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        query = "SELECT * FROM soap_records WHERE patient_id = ? ORDER BY created_at DESC"
        cursor.execute(query, (patient_id,))
        
        rows = cursor.fetchall()
        records = [row_to_dict(cursor, row) for row in rows]
        
        # Decrypt fields
        for record in records:
            try:
                if record.get('transcript'):
                    record['transcript'] = decrypt_text(record['transcript'])
                if record.get('original_transcript'):
                    record['original_transcript'] = decrypt_text(record['original_transcript'])
                if record.get('soap_sections'):
                    record['soap_sections'] = decrypt_json(record['soap_sections'])
                
                convert_datetime_fields(record)
            except Exception:
                logger.exception('Failed to decrypt soap record')
        
        return records
    except Exception as e:
        logger.error(f"get_patient_soap_records error: {e}")
        raise Exception(f"Failed to get SOAP records: {e}")
    finally:
        if conn:
            conn.close()


def update_soap_record(record_id: int, soap_sections: Dict) -> bool:
    """Update SOAP sections for a record."""
    check_db_available()
    
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        enc = encrypt_json(soap_sections)
        query = "UPDATE soap_records SET soap_sections = ?, updated_at = GETDATE() WHERE id = ?"
        cursor.execute(query, (enc, record_id))
        conn.commit()
        
        return True
    except Exception as e:
        logger.error(f"update_soap_record error: {e}")
        if conn:
            conn.rollback()
        raise Exception(f"Failed to update SOAP record: {e}")
    finally:
        if conn:
            conn.close()


def get_voice_recordings(patient_id: int) -> List[Dict]:
    """Get all voice recordings for a patient."""
    check_db_available()
    
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        query = "SELECT * FROM voice_recordings WHERE patient_id = ? ORDER BY created_at DESC"
        cursor.execute(query, (patient_id,))
        
        rows = cursor.fetchall()
        recordings = [row_to_dict(cursor, row) for row in rows]
        
        for recording in recordings:
            convert_datetime_fields(recording)
        
        return recordings
    except Exception as e:
        logger.error(f"get_voice_recordings error: {e}")
        raise Exception(f"Failed to get voice recordings: {e}")
    finally:
        if conn:
            conn.close()


def create_logged_user(email: str) -> Dict:
    """Create a logged user record."""
    check_db_available()
    
    user_id = generate_user_id()
    email_norm = (email or '').strip().lower()
    email_hash = hashlib.sha256(email_norm.encode('utf-8')).hexdigest()
    
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        query = """
            INSERT INTO logged_users (id, email, email_hash)
            VALUES (?, ?, ?)
        """
        cursor.execute(query, (user_id, encrypt_text(email), email_hash))
        conn.commit()
        
        logger.info(f"Logged user created with id: {user_id}")
        
        cursor.execute("SELECT * FROM logged_users WHERE id = ?", (user_id,))
        row = cursor.fetchone()
        user = row_to_dict(cursor, row)
        
        if user and user.get('email'):
            user['email'] = decrypt_text(user['email'])
        
        return user
    except Exception as e:
        logger.error(f"create_logged_user error: {e}")
        if conn:
            conn.rollback()
        raise Exception(f"Failed to create logged user: {e}")
    finally:
        if conn:
            conn.close()


def get_logged_user_by_email(email: str) -> Optional[Dict]:
    """Lookup logged user by email hash."""
    check_db_available()
    
    email_norm = (email or '').strip().lower()
    email_hash = hashlib.sha256(email_norm.encode('utf-8')).hexdigest()
    
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        query = "SELECT TOP 1 * FROM logged_users WHERE email_hash = ?"
        cursor.execute(query, (email_hash,))
        row = cursor.fetchone()
        
        if not row:
            return None
        
        user = row_to_dict(cursor, row)
        
        try:
            if user.get('email'):
                user['email'] = decrypt_text(user['email'])
        except Exception:
            logger.exception('Failed to decrypt logged user email')
        
        return user
    except Exception as e:
        logger.error(f"get_logged_user_by_email error: {e}")
        raise Exception(f"Failed to get logged user: {e}")
    finally:
        if conn:
            conn.close()


def get_or_create_logged_user(email: str) -> Dict:
    """Return existing user or create new one."""
    existing = get_logged_user_by_email(email)
    if existing:
        return existing
    return create_logged_user(email)