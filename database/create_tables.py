import logging
import pyodbc
from typing import List
from database.azure_client import conn_str

logger = logging.getLogger("create_tables")


CREATE_TABLES_SQL: List[str] = [
    # logged_users
    """
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'logged_users')
BEGIN
    CREATE TABLE logged_users (
        id VARCHAR(36) PRIMARY KEY,
        email NVARCHAR(MAX),
        email_hash VARCHAR(128),
        created_at DATETIME2 DEFAULT SYSUTCDATETIME(),
        updated_at DATETIME2 DEFAULT SYSUTCDATETIME()
    )
END
""",

    # patients
    """
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'patients')
BEGIN
    CREATE TABLE patients (
        id INT IDENTITY(1,1) PRIMARY KEY,
        user_id VARCHAR(36) NOT NULL,
        name NVARCHAR(MAX),
        address NVARCHAR(MAX),
        phone_number NVARCHAR(MAX),
        problem NVARCHAR(MAX),
        created_at DATETIME2 DEFAULT SYSUTCDATETIME(),
        updated_at DATETIME2 DEFAULT SYSUTCDATETIME()
    )
END
""",

    # soap_records
    """
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'soap_records')
BEGIN
    CREATE TABLE soap_records (
        id INT IDENTITY(1,1) PRIMARY KEY,
        patient_id INT NOT NULL,
        audio_file_name NVARCHAR(512),
        transcript NVARCHAR(MAX),
        original_transcript NVARCHAR(MAX),
        soap_sections NVARCHAR(MAX),
        created_at DATETIME2 DEFAULT SYSUTCDATETIME(),
        updated_at DATETIME2 DEFAULT SYSUTCDATETIME()
    )
END
""",

    # voice_recordings
    """
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'voice_recordings')
BEGIN
    CREATE TABLE voice_recordings (
        id INT IDENTITY(1,1) PRIMARY KEY,
        patient_id INT,
        soap_record_id INT,
        storage_path NVARCHAR(1024),
        file_name NVARCHAR(512),
        is_realtime BIT DEFAULT 0,
        created_at DATETIME2 DEFAULT SYSUTCDATETIME(),
        updated_at DATETIME2 DEFAULT SYSUTCDATETIME()
    )
END
""",

    ]


def ensure_tables() -> None:
    """Connects to the database and creates required tables if they are missing.

    This function is idempotent and safe to call on every startup.
    It logs errors but lets the caller decide whether to raise.
    """
    conn = None
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        for stmt in CREATE_TABLES_SQL:
            try:
                cursor.execute(stmt)
                conn.commit()
            except Exception as e:
                logger.exception("Failed executing statement: %s", e)

        logger.info("Database ensure_tables completed")
    except Exception as e:
        logger.exception("Error connecting to database for ensure_tables: %s", e)
        raise
    finally:
        if conn:
            conn.close()


if __name__ == '__main__':
    # Run as script for manual creation
    logging.basicConfig(level=logging.INFO)
    try:
        ensure_tables()
        print("Tables ensured (created if missing)")
    except Exception as e:
        print(f"Error ensuring tables: {e}")