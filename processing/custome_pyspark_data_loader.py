
import logging
import time
from pathlib import Path

import pyarrow.dataset as ds
import pyodbc


# --------------------------------------------------
# 1. PROJECT PATHS AND CONFIGURATION
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent

PARQUET_PATH = PROJECT_ROOT / "datasets" / "silver" / "online_retail_cleaned"
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOG_DIR / "customer_data_loader.log"

BATCH_SIZE = 5000

CONNECTION_STRING = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=CustomerInsightsDW;"
    "Trusted_Connection=yes;"
    "Encrypt=no;"
)


# --------------------------------------------------
# 2. LOGGING
# --------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger(__name__)


# --------------------------------------------------
# 3. MAIN LOADING FUNCTION
# --------------------------------------------------

def main():
    start_time = time.time()
    conn = None
    total_inserted = 0

    try:
        logger.info("Starting PyArrow-to-SQL Server initial load.")

        # Check whether the saved Parquet dataset exists.
        if not PARQUET_PATH.exists():
            raise FileNotFoundError(
                f"Parquet dataset not found: {PARQUET_PATH}"
            )

        # Connect to SQL Server.
        conn = pyodbc.connect(CONNECTION_STRING, timeout=10)
        cursor = conn.cursor()

        logger.info("Connected to CustomerInsightsDW.")

        # Prevent accidental duplicate initial loading.
        cursor.execute(
            "SELECT COUNT_BIG(*) FROM staging.OnlineRetail"
        )
        existing_rows = int(cursor.fetchone()[0])

        if existing_rows > 0:
            raise RuntimeError(
                f"Initial load stopped: staging.OnlineRetail "
                f"already contains {existing_rows:,} rows. "
                "No new rows were inserted."
            )

        logger.info("Target table is empty. Initial load can proceed.")

        # Read the existing Parquet dataset without Spark.
        dataset = ds.dataset(
            str(PARQUET_PATH),
            format="parquet",
        )

        expected_rows = dataset.count_rows()
        logger.info("Source Parquet rows: %s", f"{expected_rows:,}")

        if expected_rows == 0:
            raise RuntimeError("The Parquet dataset contains no rows.")

        scanner = dataset.scanner(batch_size=BATCH_SIZE)

        insert_sql = """
            INSERT INTO staging.OnlineRetail (
                Invoice,
                StockCode,
                Description,
                Quantity,
                InvoiceDate,
                Price,
                [Customer ID],
                Country,
                is_cancelled_invoice,
                is_return,
                is_zero_price,
                is_negative_price,
                is_accounting_adjustment,
                transaction_type,
                customer_id_status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        # Stream the dataset in batches to avoid loading
        # all 1,033,036 rows into memory at once.
        for batch_number, record_batch in enumerate(
            scanner.to_batches(), start=1
        ):
            batch_data = record_batch.to_pydict()
            row_count = record_batch.num_rows

            rows = [
                (
                    batch_data["Invoice"][i],
                    batch_data["StockCode"][i],
                    batch_data["Description"][i],
                    batch_data["Quantity"][i],
                    batch_data["InvoiceDate"][i],
                    batch_data["Price"][i],
                    batch_data["Customer ID"][i],
                    batch_data["Country"][i],
                    batch_data["is_cancelled_invoice"][i],
                    batch_data["is_return"][i],
                    batch_data["is_zero_price"][i],
                    batch_data["is_negative_price"][i],
                    batch_data["is_accounting_adjustment"][i],
                    batch_data["transaction_type"][i],
                    batch_data["customer_id_status"][i],
                )
                for i in range(row_count)
            ]

            try:
                cursor.fast_executemany = True
                cursor.executemany(insert_sql, rows)
                conn.commit()

            except Exception:
                conn.rollback()
                raise

            total_inserted += row_count

            logger.info(
                "Batch %s loaded: %s rows | Total loaded: %s",
                batch_number,
                f"{row_count:,}",
                f"{total_inserted:,}",
            )

        # Verify the load against the target table.
        cursor.execute(
            "SELECT COUNT_BIG(*) FROM staging.OnlineRetail"
        )
        final_rows = int(cursor.fetchone()[0])

        if final_rows != total_inserted:
            raise RuntimeError(
                f"Row-count mismatch: inserted {total_inserted:,}, "
                f"but target table contains {final_rows:,} rows."
            )

        if total_inserted != expected_rows:
            raise RuntimeError(
                f"Source/target mismatch: source has {expected_rows:,} "
                f"rows, but this run inserted {total_inserted:,} rows."
            )

        elapsed = time.time() - start_time

        logger.info("INITIAL LOAD COMPLETED SUCCESSFULLY.")
        logger.info("Source rows: %s", f"{expected_rows:,}")
        logger.info("Rows inserted: %s", f"{total_inserted:,}")
        logger.info("Target table rows: %s", f"{final_rows:,}")
        logger.info("Elapsed time: %.2f seconds", elapsed)

    except Exception:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass

        logger.exception("DATA LOAD FAILED.")
        raise

    finally:
        if conn is not None:
            conn.close()
            logger.info("SQL Server connection closed.")


if __name__ == "__main__":
    main()

