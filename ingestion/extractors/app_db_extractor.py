"""
ingestion/extractors/app_db_extractor.py

PURPOSE: Extract data from production PostgreSQL app database
         and load into Snowflake RAW schema.

PATTERN: Incremental extraction — only pull records updated since last run.
         This is critical for large tables (events table can be 100M+ rows).

CONCEPTS demonstrated:
  - Incremental/watermark-based extraction
  - Connection pooling
  - Schema validation before load
  - Retry logic with exponential backoff
  - Structured logging
"""

import os
import logging
from datetime import datetime, timezone
from typing import Optional
import time

import psycopg2
import psycopg2.extras
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd

# ── Logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("app_db_extractor")


# ── Configuration ──────────────────────────────────────────────────────────────
# In production: use environment variables or a secrets manager (AWS Secrets Manager)
# NEVER hardcode credentials

POSTGRES_CONFIG = {
    "host":     os.environ["APP_DB_HOST"],
    "port":     int(os.environ.get("APP_DB_PORT", 5432)),
    "database": os.environ["APP_DB_NAME"],
    "user":     os.environ["APP_DB_USER"],
    "password": os.environ["APP_DB_PASSWORD"],
}

SNOWFLAKE_CONFIG = {
    "account":   os.environ["SNOWFLAKE_ACCOUNT"],
    "user":      os.environ["SNOWFLAKE_USER"],
    "password":  os.environ["SNOWFLAKE_PASSWORD"],
    "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    "database":  os.environ.get("SNOWFLAKE_DATABASE", "ANALYTICS_DEV"),
    "schema":    "APP_RAW",
    "role":      os.environ.get("SNOWFLAKE_ROLE", "TRANSFORMER"),
}

# Tables to extract — (table_name, watermark_column, batch_size)
TABLES_CONFIG = [
    ("users",         "updated_at", 50_000),
    ("subscriptions", "updated_at", 50_000),
    ("events",        "occurred_at", 100_000),   # High-volume table — larger batches
    ("sessions",      "ended_at",  50_000),
]


# ── Retry decorator ────────────────────────────────────────────────────────────
def retry(max_attempts: int = 3, backoff_factor: float = 2.0):
    """Simple retry decorator with exponential backoff."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_attempts - 1:
                        raise
                    wait = backoff_factor ** attempt
                    logger.warning(f"Attempt {attempt+1} failed: {e}. Retrying in {wait}s...")
                    time.sleep(wait)
        return wrapper
    return decorator


# ── Watermark Management ───────────────────────────────────────────────────────
class WatermarkManager:
    """
    Tracks the last successfully extracted timestamp per table.
    Stored in Snowflake itself — no external state store needed.
    """

    def __init__(self, snowflake_conn):
        self.conn = snowflake_conn
        self._ensure_watermark_table()

    def _ensure_watermark_table(self):
        """Create watermark table if it doesn't exist."""
        self.conn.cursor().execute("""
            create table if not exists APP_RAW._extraction_watermarks (
                table_name      varchar(255) primary key,
                last_extracted  timestamp_ntz,
                rows_extracted  integer,
                updated_at      timestamp_ntz default current_timestamp
            )
        """)

    def get_watermark(self, table_name: str) -> Optional[datetime]:
        """Get the last extraction timestamp for a table."""
        cursor = self.conn.cursor()
        cursor.execute(
            "select last_extracted from APP_RAW._extraction_watermarks where table_name = %s",
            (table_name,)
        )
        row = cursor.fetchone()
        return row[0] if row else None

    def set_watermark(self, table_name: str, timestamp: datetime, rows: int):
        """Update the watermark after a successful extraction."""
        self.conn.cursor().execute("""
            merge into APP_RAW._extraction_watermarks t
            using (select %s as table_name, %s as last_extracted, %s as rows_extracted) s
            on t.table_name = s.table_name
            when matched then
                update set last_extracted = s.last_extracted,
                           rows_extracted = s.rows_extracted,
                           updated_at = current_timestamp
            when not matched then
                insert (table_name, last_extracted, rows_extracted)
                values (s.table_name, s.last_extracted, s.rows_extracted)
        """, (table_name, timestamp, rows))


# ── Core Extraction Logic ──────────────────────────────────────────────────────
class AppDBExtractor:

    def __init__(self):
        self.pg_conn = None
        self.sf_conn = None
        self.watermark_manager = None

    def connect(self):
        """Establish connections to both Postgres and Snowflake."""
        logger.info("Connecting to PostgreSQL...")
        self.pg_conn = psycopg2.connect(**POSTGRES_CONFIG)

        logger.info("Connecting to Snowflake...")
        self.sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)

        self.watermark_manager = WatermarkManager(self.sf_conn)
        logger.info("✅ Connections established")

    def disconnect(self):
        if self.pg_conn:
            self.pg_conn.close()
        if self.sf_conn:
            self.sf_conn.close()

    @retry(max_attempts=3)
    def extract_table(
        self,
        table_name: str,
        watermark_col: str,
        batch_size: int
    ) -> int:
        """
        Extract one table incrementally.
        Returns total rows loaded.
        """
        # Get last extraction timestamp (None = full load)
        last_watermark = self.watermark_manager.get_watermark(table_name)
        extract_start = datetime.now(timezone.utc)

        if last_watermark:
            logger.info(f"📥 {table_name}: incremental load since {last_watermark}")
            where_clause = f"where {watermark_col} > %s"
            params = (last_watermark,)
        else:
            logger.info(f"📥 {table_name}: full load (no prior watermark)")
            where_clause = ""
            params = ()

        # Build query — use server-side cursor for large tables (memory efficient)
        query = f"""
            select *,
                   current_timestamp as _loaded_at
            from {table_name}
            {where_clause}
            order by {watermark_col}
        """

        total_rows = 0

        # Server-side cursor streams results in batches — critical for large tables
        with self.pg_conn.cursor(
            name=f"extract_{table_name}",
            cursor_factory=psycopg2.extras.DictCursor
        ) as cursor:

            cursor.execute(query, params)

            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break

                # Convert to DataFrame
                df = pd.DataFrame(rows, columns=[d[0] for d in cursor.description])

                # Validate schema before loading
                self._validate_schema(df, table_name)

                # Load batch to Snowflake
                self._load_to_snowflake(df, table_name)

                total_rows += len(df)
                logger.info(f"  → Loaded {total_rows:,} rows so far...")

        # Update watermark only after successful full extraction
        self.watermark_manager.set_watermark(table_name, extract_start, total_rows)
        logger.info(f"✅ {table_name}: {total_rows:,} rows loaded")

        return total_rows

    def _validate_schema(self, df: pd.DataFrame, table_name: str):
        """
        Basic schema validation before loading.
        Catches source schema changes that would break downstream models.
        """
        # Check for all-null columns (usually indicates a schema change)
        null_cols = [col for col in df.columns if df[col].isna().all()]
        if null_cols:
            logger.warning(f"⚠️ {table_name}: all-null columns detected: {null_cols}")

        # Check row count sanity (protect against extracting 0 rows due to bug)
        if len(df) == 0:
            logger.warning(f"⚠️ {table_name}: empty batch — check query")

    @retry(max_attempts=3)
    def _load_to_snowflake(self, df: pd.DataFrame, table_name: str):
        """
        Load a DataFrame to Snowflake using the write_pandas helper.
        Uses COPY INTO under the hood — much faster than INSERT.
        """
        # Uppercase column names (Snowflake convention)
        df.columns = [c.upper() for c in df.columns]

        success, num_chunks, num_rows, output = write_pandas(
            conn=self.sf_conn,
            df=df,
            table_name=table_name.upper(),
            schema="APP_RAW",
            overwrite=False,    # append mode for incremental
            auto_create_table=True,  # create table if first run
        )

        if not success:
            raise RuntimeError(f"Snowflake write_pandas failed for {table_name}: {output}")

    def run(self):
        """Run extraction for all configured tables."""
        self.connect()

        results = {}
        try:
            for table_name, watermark_col, batch_size in TABLES_CONFIG:
                try:
                    rows = self.extract_table(table_name, watermark_col, batch_size)
                    results[table_name] = {"status": "success", "rows": rows}
                except Exception as e:
                    logger.error(f"❌ Failed to extract {table_name}: {e}")
                    results[table_name] = {"status": "failed", "error": str(e)}

        finally:
            self.disconnect()

        # Summary
        logger.info("\n" + "="*50)
        logger.info("EXTRACTION SUMMARY")
        logger.info("="*50)
        for table, result in results.items():
            status = "✅" if result["status"] == "success" else "❌"
            rows = result.get("rows", "N/A")
            logger.info(f"  {status} {table}: {rows:,} rows" if isinstance(rows, int)
                       else f"  {status} {table}: {result.get('error')}")

        return results


# ── Entry Point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    extractor = AppDBExtractor()
    extractor.run()
