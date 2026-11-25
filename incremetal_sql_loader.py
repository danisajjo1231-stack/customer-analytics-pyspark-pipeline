from pathlib import Path

import pyarrow.dataset as ds
import pyodbc


# Project paths
PROJECT_DIR = Path(__file__).resolve().parent
PARQUET_DIR = PROJECT_DIR / "datasets" / "silver" / "online_retail_cleaned"

# SQL Server configuration
CONNECTION_STRING = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=CustomerInsightsDW;"
    "Trusted_Connection=yes;"
    "Encrypt=no;"
)

TABLE_NAME = "staging.OnlineRetail"
BATCH_SIZE = 5000

# True  -> sirf InvoiceDate timezone check chalega, SQL mein kuch change nahi hoga
# False -> normal incremental load chalega
TIMEZONE_CHECK_ONLY = True

COLUMNS = [
    "Invoice",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "Price",
    "Customer ID",
    "Country",
    "is_cancelled_invoice",
    "is_return",
    "is_zero_price",
    "is_negative_price",
    "is_accounting_adjustment",
    "transaction_type",
    "customer_id_status",
]


def check_invoice_date_timezone(dataset):
    """Check InvoiceDate timezone without modifying any data."""
    print("\n===== INVOICE DATE TIMEZONE CHECK =====")

    invoice_date_field = dataset.schema.field("InvoiceDate")
    print("Parquet schema:", invoice_date_field)

    sample_batch = next(
        dataset.scanner(
            columns=["InvoiceDate"],
            batch_size=5,
        ).to_batches()
    )

    for row in sample_batch.to_pylist():
        value = row["InvoiceDate"]
        tz = value.tzinfo if value is not None else None
        print("Value:", value, "| Timezone:", tz)

    print("Diagnostic complete. No SQL data was changed.")


def main():
    if not PARQUET_DIR.exists():
        raise FileNotFoundError(
            f"Silver Parquet folder not found: {PARQUET_DIR}"
        )

    print("Reading Silver Parquet dataset...")

    dataset = ds.dataset(str(PARQUET_DIR), format="parquet")

    available_columns = set(dataset.schema.names)
    missing = set(COLUMNS) - available_columns
    if missing:
        raise ValueError(f"Missing Parquet columns: {sorted(missing)}")

    # Diagnostic mode: check timezone and stop here
    if TIMEZONE_CHECK_ONLY:
        check_invoice_date_timezone(dataset)
        return

    connection = pyodbc.connect(CONNECTION_STRING, timeout=30)
    cursor = connection.cursor()
    cursor.fast_executemany = True

    try:
        cursor.execute(f"SELECT COUNT_BIG(*) FROM {TABLE_NAME}")
        initial_count = cursor.fetchone()[0]
        print(f"Existing SQL rows: {initial_count:,}")

        # Load into a temporary table first.
        cursor.execute(
            f"SELECT TOP (0) * INTO #RetailStage FROM {TABLE_NAME}"
        )

        column_sql = ", ".join(f"[{c}]" for c in COLUMNS)
        placeholders = ", ".join("?" for _ in COLUMNS)

        insert_stage_sql = (
            f"INSERT INTO #RetailStage ({column_sql}) "
            f"VALUES ({placeholders})"
        )

        scanner = dataset.scanner(
            columns=COLUMNS,
            batch_size=BATCH_SIZE,
        )

        staged_count = 0

        for batch in scanner.to_batches():
            rows = batch.to_pylist()

            if not rows:
                continue

            values = [
                tuple(row[column] for column in COLUMNS)
                for row in rows
            ]

            cursor.executemany(insert_stage_sql, values)
            staged_count += len(values)

            if staged_count % 50000 == 0:
                print(f"Staged rows: {staged_count:,}")

        print(f"Total staged rows: {staged_count:,}")
        print("Finding rows that do not already exist...")

        # Compare every column, treating NULL values as equal.
        match_conditions = " AND ".join(
            (
                f"(t.[{c}] = s.[{c}] "
                f"OR (t.[{c}] IS NULL AND s.[{c}] IS NULL))"
            )
            for c in COLUMNS
        )

        # NOT EXISTS prevents inserting an exact row already in SQL.
        # DISTINCT prevents repeated rows within this staging batch.
        insert_new_sql = f"""
            INSERT INTO {TABLE_NAME} ({column_sql})
            SELECT DISTINCT {", ".join(f"s.[{c}]" for c in COLUMNS)}
            FROM #RetailStage AS s
            WHERE NOT EXISTS (
                SELECT 1
                FROM {TABLE_NAME} AS t
                WHERE {match_conditions}
            );
        """

        cursor.execute(insert_new_sql)
        inserted_count = cursor.rowcount  # rows inserted by the INSERT above

        connection.commit()

        cursor.execute(f"SELECT COUNT_BIG(*) FROM {TABLE_NAME}")
        final_count = cursor.fetchone()[0]

        print("\n===== INCREMENTAL LOAD SUMMARY =====")
        print(f"Rows before load: {initial_count:,}")
        print(f"Rows staged: {staged_count:,}")
        print(f"New rows inserted: {inserted_count:,}")
        print(f"Rows after load: {final_count:,}")
        print("Incremental load completed successfully.")

    except Exception:
        connection.rollback()
        raise

    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    main()