-- Schema: InCommodities Trade Reconciliation
-- Purpose: Setup staging tables and seed exchange transactions
CREATE SCHEMA IF NOT EXISTS stg;

-- ============================================================================
-- 1. CLEARING TRADES TABLE
-- ============================================================================
DROP TABLE IF EXISTS stg.clearing_trades CASCADE;
CREATE TABLE stg.clearing_trades (
    trade_number    TEXT NOT NULL,
    fill_sequence   INT NOT NULL,
    product         TEXT NOT NULL,
    market          TEXT,
    direction       TEXT NOT NULL,
    quantity        NUMERIC(15,2) NOT NULL,
    price           NUMERIC(15,2),        -- Nullable for alerting
    counterparty    TEXT,
    fee             NUMERIC(15,2),
    trade_date_aest DATE,
    trade_date_utc  TIMESTAMP NOT NULL,   -- Normalized for joining
    is_complete     BOOLEAN DEFAULT TRUE,
    total_value     NUMERIC(15,2),
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW(),
    CONSTRAINT pk_clearing_trades PRIMARY KEY (trade_number, fill_sequence)
);

-- ============================================================================
-- 2. TRANSACTIONS TABLE
-- ============================================================================
DROP TABLE IF EXISTS stg.transactions CASCADE;
CREATE TABLE stg.transactions (
    unique_id       TEXT PRIMARY KEY,
    product         TEXT,
    trade_type      TEXT,
    direction       TEXT,
    quantity        NUMERIC,
    trade_price     NUMERIC,
    counterparty    TEXT,
    trade_date_utc  TIMESTAMP,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- ============================================================================
-- 3. INSERT EXCHANGE DATA
-- ============================================================================
INSERT INTO stg.transactions (unique_id, product, trade_type, direction, quantity, trade_price, counterparty, trade_date_utc)
VALUES
('af12e8', 'PWR-NORDIC', 'FUTURE', 'BUY', 5, 1.76, 'STATKRAFT', '2025-01-14 10:15:23'),
('af12e9', 'PWR-NORDIC', 'FUTURE', 'BUY', 3, 1.76, 'SHELL', '2025-01-14 10:15:24'),
('bb44c1', 'GAS-UK', 'FUTURE', 'SELL', -10, 1.16, 'BP', '2025-01-14 11:02:10'),
('bb44c2', 'GAS-UK', 'FUTURE', 'SELL', -10, 1.16, 'BP', '2025-01-14 11:02:12'),
('cc91d1', 'EUA', 'FUTURE', 'BUY', 4, 2.83, 'SHELL', '2025-01-14 12:30:11'),
('dd19e7', 'PWR-NORDIC', 'FUTURE', 'SELL', -7, 41.90, 'UNIPER', '2025-01-14 13:45:50'),
('ee55a9', 'GAS-UK', 'FUTURE', 'BUY', 6, 1.22, 'EQUINOR', '2025-01-14 14:10:07'),
('ff21b3', 'PWR-GERMANY', 'FUTURE', 'BUY', 8, 2.01, 'RWE', '2025-01-14 16:33:10'),
('ff21b4', 'PWR-GERMANY', 'FUTURE', 'BUY', 2, 2.01, 'RWE', '2025-01-14 16:33:11'),
('g712ac', 'EUA', 'FUTURE', 'SELL', -6, 2.80, 'SHELL', '2025-01-14 09:45:02'),
('h1k292', 'PWR-NORDIC', 'FUTURE', 'BUY', 8, 39.90, 'STATKRAFT', '2025-01-14 08:22:17'),
('i77m12', 'GAS-UK', 'FUTURE', 'SELL', -12, 26.80,'EQUINOR', '2025-01-14 07:10:20'),
('j9n812', 'PWR-GERMANY', 'FUTURE', 'SELL', -5, 2.04, 'RWE', '2025-01-14 06:55:40'),
('j9n813', 'PWR-GERMANY', 'FUTURE', 'SELL', -5, 2.04, 'RWE', '2025-01-14 06:55:41')
ON CONFLICT (unique_id) DO NOTHING;

-- ============================================================================
-- 4. RECONCILIATION VIEW
-- ============================================================================
CREATE OR REPLACE VIEW stg.reconciliation_report AS
WITH bank_agg AS (
    -- 1. Aggregate Bank Data by Key Attributes
    SELECT
        product,
        counterparty,
        trade_date_aest AS trade_date, -- Use the raw Date from CSV (fixes 13th vs 14th issue)
        direction,
        SUM(quantity) AS bank_qty,
        SUM(total_value) AS bank_value,
        COUNT(*) AS bank_record_count,
        STRING_AGG(trade_number || '-' || fill_sequence::text, ', ') AS bank_refs
    FROM stg.clearing_trades
    GROUP BY 1, 2, 3, 4
),
exch_agg AS (
    -- 2. Aggregate Exchange Data to handle fill splits
    SELECT
        product,
        counterparty,
        DATE(trade_date_utc) AS trade_date, -- Extract Business Date from UTC timestamp
        direction,
        SUM(ABS(quantity)) AS exch_qty, -- Normalize quantity to positive for comparison
        SUM(trade_price * ABS(quantity)) AS exch_value,
        COUNT(*) AS exch_record_count,
        STRING_AGG(unique_id, ', ') AS exch_refs
    FROM stg.transactions
    GROUP BY 1, 2, 3, 4
)
SELECT
    -- 3. Match Aggregated Datasets
    COALESCE(b.product, e.product) AS product,
    COALESCE(b.counterparty, e.counterparty) AS counterparty,
    COALESCE(b.trade_date, e.trade_date) AS trade_date,
    COALESCE(b.direction, e.direction) AS direction,
    
    -- Metrics
    COALESCE(b.bank_qty, 0) AS bank_quantity,
    COALESCE(e.exch_qty, 0) AS exchange_quantity,
    (COALESCE(b.bank_qty, 0) - COALESCE(e.exch_qty, 0)) AS quantity_diff,
    
    COALESCE(b.bank_value, 0) AS bank_value,
    COALESCE(e.exch_value, 0) AS exchange_value,
    (COALESCE(b.bank_value, 0) - COALESCE(e.exch_value, 0)) AS value_diff,
    
    -- Reference IDs (Helpful for drilling down)
    b.bank_refs,
    e.exch_refs,
    
    -- Status Logic
    CASE
        WHEN b.product IS NULL THEN 'MISSING IN BANK'
        WHEN e.product IS NULL THEN 'MISSING IN EXCHANGE'
        WHEN ABS(COALESCE(b.bank_qty, 0) - COALESCE(e.exch_qty, 0)) > 0.0001 THEN 'QTY MISMATCH'
        WHEN ABS(COALESCE(b.bank_value, 0) - COALESCE(e.exch_value, 0)) > 0.01 THEN 'VALUE MISMATCH'
        ELSE 'MATCHED'
    END AS recon_status
FROM bank_agg b
FULL OUTER JOIN exch_agg e
    ON b.product = e.product
    AND b.counterparty = e.counterparty
    AND b.trade_date = e.trade_date
    AND b.direction = e.direction;