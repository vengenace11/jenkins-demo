import os
import sys
import zipfile
import subprocess
import requests
import psycopg2
from pathlib import Path
from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv()

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
DATABASE_NAME = os.getenv("DATABASE_NAME", "dvdrental")

DOWNLOAD_URL = os.getenv("DOWNLOAD_URL")
ZIP_PATH = Path(os.getenv("ZIP_PATH", "/tmp/dvdrental.zip"))
EXTRACT_DIR = Path(os.getenv("EXTRACT_DIR", "/tmp/dvdrental"))
TAR_PATH = Path(os.getenv("TAR_PATH", "/tmp/dvdrental/dvdrental.tar"))

PIPELINE_NAME = os.getenv("PIPELINE_NAME", "dvdrental-bootstrap-restore")
SOURCE_NAME = os.getenv("SOURCE_NAME", "dvdrental-sample-db")

ALLOW_RESTORE_OVER_EXISTING = os.getenv("ALLOW_RESTORE_OVER_EXISTING", "false").lower() == "true"


def require_env():
    required = [
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "DOWNLOAD_URL",
    ]

    missing = [var for var in required if not os.getenv(var)]

    if missing:
        raise RuntimeError(f"Missing required environment variables: {missing}")


def get_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=DATABASE_NAME,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def init_ingestion_tables():
    sql = """
    CREATE SCHEMA IF NOT EXISTS ingestion;

    CREATE TABLE IF NOT EXISTS ingestion.ingestion_run (
        ingestion_id BIGSERIAL PRIMARY KEY,
        pipeline_name TEXT NOT NULL,
        source_name TEXT NOT NULL,
        source_url TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED', 'SKIPPED')),
        started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        finished_at TIMESTAMPTZ,
        rows_fetched INTEGER DEFAULT 0,
        rows_inserted INTEGER DEFAULT 0,
        error_message TEXT
    );
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)


def start_ingestion_run():
    sql = """
    INSERT INTO ingestion.ingestion_run (
        pipeline_name,
        source_name,
        source_url,
        status
    )
    VALUES (%s, %s, %s, 'RUNNING')
    RETURNING ingestion_id;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (PIPELINE_NAME, SOURCE_NAME, DOWNLOAD_URL))
            ingestion_id = cur.fetchone()[0]

    return ingestion_id


def finish_ingestion_run(ingestion_id, status, error_message=None):
    sql = """
    UPDATE ingestion.ingestion_run
    SET status = %s,
        finished_at = now(),
        error_message = %s
    WHERE ingestion_id = %s;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (status, error_message, ingestion_id))


def acquire_lock():
    sql = "SELECT pg_try_advisory_lock(hashtext(%s));"

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(sql, (PIPELINE_NAME,))
    locked = cur.fetchone()[0]

    if not locked:
        cur.close()
        conn.close()
        raise RuntimeError("Another ingestion job is already running.")

    return conn, cur


def release_lock(conn, cur):
    try:
        cur.execute("SELECT pg_advisory_unlock(hashtext(%s));", (PIPELINE_NAME,))
        conn.commit()
    finally:
        cur.close()
        conn.close()


def database_has_user_tables():
    sql = """
    SELECT COUNT(*)
    FROM information_schema.tables
    WHERE table_schema = 'public'
      AND table_type = 'BASE TABLE';
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchone()[0] > 0


def download_file(url, output_path):
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_path.exists():
        print(f"[+] File already exists: {output_path}")
        return

    print(f"[+] Downloading {url}")

    response = requests.get(url, stream=True, timeout=60)
    response.raise_for_status()

    total_size = int(response.headers.get("content-length", 0))

    with open(output_path, "wb") as file, tqdm(
        desc=output_path.name,
        total=total_size,
        unit="iB",
        unit_scale=True,
        unit_divisor=1024,
    ) as bar:
        for chunk in response.iter_content(chunk_size=1024):
            if chunk:
                size = file.write(chunk)
                bar.update(size)

    print("[+] Download complete")


def unzip_file(zip_path, extract_to):
    extract_to.mkdir(parents=True, exist_ok=True)

    print(f"[+] Extracting {zip_path}")

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(extract_to)

    print("[+] Extraction complete")


def restore_database():
    if not TAR_PATH.exists():
        raise FileNotFoundError(f"Could not find restore file: {TAR_PATH}")

    print(f"[+] Restoring database from {TAR_PATH}")

    env = os.environ.copy()
    env["PGPASSWORD"] = POSTGRES_PASSWORD

    command = [
        "pg_restore",
        "-h", POSTGRES_HOST,
        "-p", POSTGRES_PORT,
        "-U", POSTGRES_USER,
        "-d", DATABASE_NAME,
        "--no-owner",
        "--no-privileges",
        "--verbose",
        str(TAR_PATH),
    ]

    subprocess.run(command, env=env, check=True)

    print("[+] Restore complete")


def main():
    require_env()

    print("[+] Initializing ingestion audit table")
    init_ingestion_tables()

    ingestion_id = start_ingestion_run()
    print(f"[+] Started ingestion_id={ingestion_id}")

    lock_conn = None
    lock_cur = None

    try:
        lock_conn, lock_cur = acquire_lock()

        if database_has_user_tables() and not ALLOW_RESTORE_OVER_EXISTING:
            raise RuntimeError(
                "Public schema already has tables. "
                "Refusing to restore over existing database. "
                "Set ALLOW_RESTORE_OVER_EXISTING=true only if you understand the risk."
            )

        download_file(DOWNLOAD_URL, ZIP_PATH)
        unzip_file(ZIP_PATH, EXTRACT_DIR)
        restore_database()

        finish_ingestion_run(ingestion_id, "SUCCESS")
        print(f"✅ dvdrental ingestion completed successfully. ingestion_id={ingestion_id}")

    except Exception as error:
        finish_ingestion_run(ingestion_id, "FAILED", str(error))
        print(f"❌ Ingestion failed. ingestion_id={ingestion_id}")
        print(f"Error: {error}")
        sys.exit(1)

    finally:
        if lock_conn and lock_cur:
            release_lock(lock_conn, lock_cur)


if __name__ == "__main__":
    main()
