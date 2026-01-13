-- Schema: InCommodities Trade Reconciliation
-- Purpose: Setup staging tables and seed exchange transactions
CREATE SCHEMA IF NOT EXISTS stg;

-- ============================================================================
-- 1. CLEARING TRADES TABLE (Bank Side)
-- ============================================================================
DROP TABLE IF EXISTS stg.clearing_trades CASCADE;
CREATE TABLE stg.clearing_trades (
    trade_number    TEXT NOT NULL,
    fill_sequence   INT NOT NULL,
    product         TEXT NOT NULL,
    market          TEXT,
    direction       TEXT NOT NULL,
    quantity        NUMERIC(15,4) NOT NULL, -- Precision adjusted for commodity decimals
    price           NUMERIC(15,4),
    counterparty    TEXT,
    fee             NUMERIC(15,4),
    trade_date_aest DATE NOT NULL,          -- Matches DD/MM/YYYY format in CSV
    trade_date_utc  TIMESTAMP NOT NULL,
    is_complete     BOOLEAN DEFAULT TRUE,
    total_value     NUMERIC(15,4),
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW(),
    CONSTRAINT pk_clearing_trades PRIMARY KEY (trade_number, fill_sequence)
);

-- Index for the Reconciliation View Grouping
-- Optimized for Index-Only Scans by INCLUDING the sum-targets
CREATE INDEX idx_clearing_trades_recon_lookup 
ON stg.clearing_trades (trade_date_aest, product, counterparty, direction)
INCLUDE (quantity, total_value);

-- ============================================================================
-- 2. TRANSACTIONS TABLE (Exchange Side)
-- ============================================================================
DROP TABLE IF EXISTS stg.transactions CASCADE;
CREATE TABLE stg.transactions (
    unique_id       TEXT PRIMARY KEY,
    product         TEXT NOT NULL,
    trade_type      TEXT,
    direction       TEXT NOT NULL,
    quantity        NUMERIC(15,4) NOT NULL,
    trade_price     NUMERIC(15,4) NOT NULL,
    counterparty    TEXT,
    trade_date_utc  TIMESTAMP NOT NULL,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- Functional Index: Matches the 'trade_date_utc::DATE' logic in the view
-- Essential because the Exchange uses Timestamps while the Bank CSV uses Dates
CREATE INDEX idx_transactions_recon_lookup 
ON stg.transactions ((trade_date_utc::DATE), product, counterparty, direction)
INCLUDE (quantity, trade_price);

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
    SELECT
        product,
        counterparty,
        trade_date_aest AS trade_date,
        direction,
        SUM(quantity) AS bank_qty,
        SUM(total_value) AS bank_value,
        COUNT(*) AS bank_record_count,
        STRING_AGG(trade_number || '-' || fill_sequence::text, ', ') AS bank_refs
    FROM stg.clearing_trades
    GROUP BY 1, 2, 3, 4
),
exch_agg AS (
    SELECT
        product,
        counterparty,
        trade_date_utc::DATE AS trade_date, -- Cast matches the functional index
        direction,
        SUM(ABS(quantity)) AS exch_qty,
        SUM(trade_price * ABS(quantity)) AS exch_value,
        COUNT(*) AS exch_record_count,
        STRING_AGG(unique_id, ', ') AS exch_refs
    FROM stg.transactions
    GROUP BY 1, 2, 3, 4
)
SELECT
    COALESCE(b.product, e.product) AS product,
    COALESCE(b.counterparty, e.counterparty) AS counterparty,
    COALESCE(b.trade_date, e.trade_date) AS trade_date,
    COALESCE(b.direction, e.direction) AS direction,
    
    COALESCE(b.bank_qty, 0) AS bank_quantity,
    COALESCE(e.exch_qty, 0) AS exchange_quantity,
    (COALESCE(b.bank_qty, 0) - COALESCE(e.exch_qty, 0)) AS quantity_diff,
    
    COALESCE(b.bank_value, 0) AS bank_value,
    COALESCE(e.exch_value, 0) AS exchange_value,
    (COALESCE(b.bank_value, 0) - COALESCE(e.exch_value, 0)) AS value_diff,
    
    b.bank_refs,
    e.exch_refs,
    
    CASE
        WHEN b.product IS NULL THEN 'MISSING IN BANK'
        WHEN e.product IS NULL THEN 'MISSING IN EXCHANGE'
        WHEN ABS(COALESCE(b.bank_qty, 0) - COALESCE(e.exch_qty, 0)) > 0.0001 THEN 'QTY MISMATCH'
        WHEN ABS(COALESCE(b.bank_value, 0) - COALESCE(e.exch_value, 0)) > 0.01 THEN 'VALUE MISMATCH'
        ELSE 'MATCHED'
    END AS recon_status
FROM bank_agg b
FULL OUTER JOIN exch_agg e
    USING (product, counterparty, trade_date, direction);

-- Update statistics for the planner
EXPLAIN ANALYZE stg.clearing_trades;
EXPLAIN ANALYZE stg.transactions;