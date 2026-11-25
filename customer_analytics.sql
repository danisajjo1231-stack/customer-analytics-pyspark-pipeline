USE CustomerInsightsDW;
GO
----true tabel

EXEC sp_help 'staging.OnlineRetail';

-- Total records
SELECT COUNT_BIG(*) AS TotalRecords
FROM staging.OnlineRetail;

-- Check duplicate records
SELECT
    COUNT(*) AS TotalRows,
    COUNT(DISTINCT CHECKSUM(*)) AS DistinctChecksums
FROM staging.OnlineRetail;


USE CustomerInsightsDW;
GO

SELECT
    s.name AS SchemaName,
    t.name AS TableName,
    SUM(p.rows) AS TotalRows
FROM sys.tables t
JOIN sys.schemas s
    ON t.schema_id = s.schema_id
JOIN sys.partitions p
    ON t.object_id = p.object_id
WHERE t.name = 'OnlineRetail'
AND p.index_id IN (0, 1)
GROUP BY s.name, t.name;


USE CustomerInsightsDW;
GO

SELECT
    i.name AS IndexName,
    i.is_primary_key,
    i.is_unique,
    c.name AS ColumnName
FROM sys.indexes i
JOIN sys.index_columns ic
    ON i.object_id = ic.object_id
    AND i.index_id = ic.index_id
JOIN sys.columns c
    ON ic.object_id = c.object_id
    AND ic.column_id = c.column_id
WHERE i.object_id = OBJECT_ID('staging.OnlineRetail');

SELECT
    Invoice,
    StockCode,
    COUNT(*) AS DuplicateCount
FROM staging.OnlineRetail
GROUP BY Invoice, StockCode
HAVING COUNT(*) > 1
ORDER BY DuplicateCount DESC;

SELECT *
FROM staging.OnlineRetail
WHERE Invoice = 'C544580'
  AND StockCode = 'S'
ORDER BY RetailRowID;

SELECT
    COUNT(*) AS TotalRows,
    COUNT(DISTINCT RetailRowID) AS UniqueRowIDs
FROM staging.OnlineRetail;


SELECT
    transaction_type,
    COUNT_BIG(*) AS TotalRecords,
    SUM(CAST(is_cancelled_invoice AS BIGINT)) AS CancelledInvoices,
    SUM(CAST(is_return AS BIGINT)) AS Returns,
    SUM(CAST(is_zero_price AS BIGINT)) AS ZeroPrice,
    SUM(CAST(is_negative_price AS BIGINT)) AS NegativePrice,
    SUM(CAST(is_accounting_adjustment AS BIGINT)) AS AccountingAdjustments,
    SUM(CASE WHEN [Customer ID] IS NULL THEN 1 ELSE 0 END) AS MissingCustomerIDs
FROM staging.OnlineRetail
GROUP BY transaction_type
ORDER BY TotalRecords DESC;

SELECT TOP 10
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
FROM staging.OnlineRetail
ORDER BY RetailRowID;

SELECT TOP 10
    RetailRowID,
    Invoice,
    StockCode,
    Quantity,
    Price
FROM staging.OnlineRetail
ORDER BY RetailRowID;

SELECT
    Invoice,
    StockCode,
    Description,
    Quantity,
    InvoiceDate,
    Price,
    [Customer ID],
    Country
FROM staging.OnlineRetail
WHERE Invoice = '489536'
  AND StockCode = '22147';



  WITH SampleKeys AS
(
    SELECT *
    FROM (VALUES
        ('489436', '21756'),
        ('489445', '35951'),
        ('489531', '21034'),
        ('489532', '72008'),
        ('489533', '84352'),
        ('489536', '22147'),
        ('489552', '21479'),
        ('489561', '35051B'),
        ('489594', '20977'),
        ('489599', '22321')
    ) AS K(Invoice, StockCode)
)
SELECT
    T.Invoice,
    T.StockCode,
    T.Description,
    T.Quantity,
    T.InvoiceDate,
    T.Price,
    T.[Customer ID],
    T.Country
FROM staging.OnlineRetail AS T
INNER JOIN SampleKeys AS K
    ON T.Invoice = K.Invoice
    AND T.StockCode = K.StockCode
ORDER BY T.Invoice, T.Quantity;

SELECT
    DATA_TYPE,
    COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'staging'
AND TABLE_NAME = 'OnlineRetail'
AND COLUMN_NAME = 'InvoiceDate';

USE CustomerInsightsDW;

SELECT *
FROM staging.OnlineRetail;

SELECT
    DB_NAME() AS CurrentDatabase,
    local_net_address,
    local_tcp_port,
    net_transport
FROM sys.dm_exec_connections
WHERE session_id = @@SPID;

EXEC xp_readerrorlog 0, 1, N'Server is listening on';

--== real work

```sql
/*
=========================================================
 CUSTOMER ANALYTICS - SQL SERVER PROJECT
 Database: CustomerInsightsDW
 Table: staging.OnlineRetail
 Total Records: 1,033,036
=========================================================

 Technologies:
 SQL Server | PySpark | Python | PyArrow | PyODBC

*/


-- =======================================================
-- 1. DATABASE SELECTION
-- =======================================================

USE CustomerInsightsDW;
GO


-- =======================================================
-- 2. TABLE INFORMATION
-- =======================================================

-- Check total records

SELECT COUNT_BIG(*) AS TotalRecords
FROM staging.OnlineRetail;


-- View sample records

SELECT TOP 10 *
FROM staging.OnlineRetail;


-- Check table columns

SELECT
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'staging'
  AND TABLE_NAME = 'OnlineRetail'
ORDER BY ORDINAL_POSITION;


-- =======================================================
-- 3. DATA QUALITY ANALYSIS
-- =======================================================

-- Missing Customer IDs

SELECT
    COUNT_BIG(*) AS TotalRecords,
    SUM(CASE
        WHEN [Customer ID] IS NULL THEN 1
        ELSE 0
    END) AS MissingCustomerIDs,
    SUM(CASE
        WHEN [Customer ID] IS NOT NULL THEN 1
        ELSE 0
    END) AS KnownCustomerIDs
FROM staging.OnlineRetail;


-- Check missing descriptions

SELECT COUNT_BIG(*) AS MissingDescriptions
FROM staging.OnlineRetail
WHERE Description IS NULL;


-- Check negative quantities

SELECT COUNT_BIG(*) AS NegativeQuantityRecords
FROM staging.OnlineRetail
WHERE Quantity < 0;


-- Check zero and negative prices

SELECT
    SUM(CASE WHEN Price = 0 THEN 1 ELSE 0 END) AS ZeroPriceRecords,
    SUM(CASE WHEN Price < 0 THEN 1 ELSE 0 END) AS NegativePriceRecords
FROM staging.OnlineRetail;


-- =======================================================
-- 4. DUPLICATE RECORD ANALYSIS
-- =======================================================

-- Find repeated business records.
-- RetailRowID is excluded because it is an identity column.

SELECT
    Invoice,
    StockCode,
    Description,
    Quantity,
    InvoiceDate,
    Price,
    [Customer ID],
    Country,
    COUNT_BIG(*) AS DuplicateCount
FROM staging.OnlineRetail
GROUP BY
    Invoice,
    StockCode,
    Description,
    Quantity,
    InvoiceDate,
    Price,
    [Customer ID],
    Country
HAVING COUNT_BIG(*) > 1
ORDER BY DuplicateCount DESC;


-- =======================================================
-- 5. TRANSACTION TYPE ANALYSIS
-- =======================================================

SELECT
    transaction_type,
    COUNT_BIG(*) AS TotalRecords,
    SUM(CAST(is_cancelled_invoice AS BIGINT)) AS CancelledInvoices,
    SUM(CAST(is_return AS BIGINT)) AS Returns,
    SUM(CAST(is_zero_price AS BIGINT)) AS ZeroPrice,
    SUM(CAST(is_negative_price AS BIGINT)) AS NegativePrice,
    SUM(CAST(is_accounting_adjustment AS BIGINT))
        AS AccountingAdjustments,
    SUM(CASE
        WHEN [Customer ID] IS NULL THEN 1
        ELSE 0
    END) AS MissingCustomerIDs
FROM staging.OnlineRetail
GROUP BY transaction_type
ORDER BY TotalRecords DESC;


-- =======================================================
-- 6. CANCELLED INVOICES ANALYSIS
-- =======================================================

SELECT
    COUNT_BIG(*) AS CancelledRows,
    COUNT(DISTINCT Invoice) AS DistinctCancelledInvoices
FROM staging.OnlineRetail
WHERE is_cancelled_invoice = 1;


-- View cancelled invoice records

SELECT TOP 20
    Invoice,
    StockCode,
    Description,
    Quantity,
    Price,
    Country
FROM staging.OnlineRetail
WHERE is_cancelled_invoice = 1
ORDER BY Invoice;


-- =======================================================
-- 7. RETURNS AND REFUNDS ANALYSIS
-- =======================================================

SELECT
    COUNT_BIG(*) AS ReturnRecords,
    SUM(Quantity) AS TotalReturnedQuantity
FROM staging.OnlineRetail
WHERE is_return = 1;


-- =======================================================
-- 8. CUSTOMER-WISE SALES ANALYSIS
-- =======================================================

-- Only known customers and valid positive sales rows.

WITH CustomerSales AS
(
    SELECT
        [Customer ID],
        ROUND(SUM(Quantity * Price), 2) AS GrossSales,
        COUNT(DISTINCT Invoice) AS SalesInvoiceCount,
        SUM(Quantity) AS UnitsSold
    FROM staging.OnlineRetail
    WHERE [Customer ID] IS NOT NULL
      AND Quantity > 0
      AND Price > 0
      AND is_cancelled_invoice = 0
    GROUP BY [Customer ID]
)

SELECT *
FROM CustomerSales
ORDER BY GrossSales DESC;


-- =======================================================
-- 9. TOP 10 CUSTOMERS
-- =======================================================

WITH CustomerSales AS
(
    SELECT
        [Customer ID],
        ROUND(SUM(Quantity * Price), 2) AS GrossSales
    FROM staging.OnlineRetail
    WHERE [Customer ID] IS NOT NULL
      AND Quantity > 0
      AND Price > 0
      AND is_cancelled_invoice = 0
    GROUP BY [Customer ID]
)

SELECT TOP 10
    [Customer ID],
    GrossSales
FROM CustomerSales
ORDER BY GrossSales DESC;


-- =======================================================
-- 10. CUSTOMER RANKING USING WINDOW FUNCTIONS
-- =======================================================

WITH CustomerSales AS
(
    SELECT
        [Customer ID],
        ROUND(SUM(Quantity * Price), 2) AS GrossSales
    FROM staging.OnlineRetail
    WHERE [Customer ID] IS NOT NULL
      AND Quantity > 0
      AND Price > 0
      AND is_cancelled_invoice = 0
    GROUP BY [Customer ID]
),

CustomerRanking AS
(
    SELECT
        [Customer ID],
        GrossSales,
        DENSE_RANK() OVER (
            ORDER BY GrossSales DESC
        ) AS SalesRank
    FROM CustomerSales
)

SELECT *
FROM CustomerRanking
WHERE SalesRank <= 10
ORDER BY SalesRank;


-- =======================================================
-- 11. COUNTRY-WISE SALES ANALYSIS
-- =======================================================

SELECT
    Country,
    ROUND(SUM(Quantity * Price), 2) AS GrossSales,
    COUNT(DISTINCT Invoice) AS TotalInvoices,
    COUNT(DISTINCT [Customer ID]) AS UniqueCustomers
FROM staging.OnlineRetail
WHERE [Customer ID] IS NOT NULL
  AND Quantity > 0
  AND Price > 0
  AND is_cancelled_invoice = 0
GROUP BY Country
ORDER BY GrossSales DESC;


-- =======================================================
-- 12. OVERALL BUSINESS METRICS
-- =======================================================

SELECT
    COUNT_BIG(*) AS TotalRecords,

    SUM(CASE
        WHEN Quantity > 0
         AND Price > 0
         AND is_cancelled_invoice = 0
         AND [Customer ID] IS NOT NULL
        THEN Quantity * Price
        ELSE 0
    END) AS GrossSales,

    SUM(CASE
        WHEN Quantity > 0
         AND Price > 0
         AND is_cancelled_invoice = 0
         AND [Customer ID] IS NOT NULL
        THEN Quantity
        ELSE 0
    END) AS UnitsSold,

    COUNT(DISTINCT CASE
        WHEN Quantity > 0
         AND Price > 0
         AND is_cancelled_invoice = 0
         AND [Customer ID] IS NOT NULL
        THEN Invoice
    END) AS SalesInvoices,

    COUNT(DISTINCT [Customer ID]) AS UniqueCustomers

FROM staging.OnlineRetail;


-- =======================================================
-- 13. PYSPARK VS SQL SERVER VALIDATION
-- =======================================================

SELECT
    transaction_type,
    COUNT_BIG(*) AS TotalRecords,

    SUM(CAST(is_cancelled_invoice AS BIGINT))
        AS CancelledInvoices,

    SUM(CAST(is_return AS BIGINT))
        AS Returns,

    SUM(CAST(is_zero_price AS BIGINT))
        AS ZeroPrice,

    SUM(CAST(is_negative_price AS BIGINT))
        AS NegativePrice,

    SUM(CAST(is_accounting_adjustment AS BIGINT))
        AS AccountingAdjustments,

    SUM(CASE
        WHEN [Customer ID] IS NULL THEN 1
        ELSE 0
    END) AS MissingCustomerIDs

FROM staging.OnlineRetail
GROUP BY transaction_type
ORDER BY transaction_type;


-- =======================================================
-- 14. FINAL RECORD COUNT VALIDATION
-- =======================================================

SELECT
    COUNT_BIG(*) AS SQLServerTotalRecords,
    CASE
        WHEN COUNT_BIG(*) = 1033036
        THEN 'VALIDATION PASSED'
        ELSE 'VALIDATION FAILED'
    END AS ValidationStatus
FROM staging.OnlineRetail;


/*
=========================================================
 END OF CUSTOMER ANALYTICS SQL PROJECT
=========================================================
*/
```