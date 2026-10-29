-- Run once in MySQL Workbench as root.
-- 1. MySQL on UTC, so ON UPDATE CURRENT_TIMESTAMP matches the pipeline (all timestamps UTC).
-- 2. updated_at on raw_transactions, so status changes (Pending -> Completed) reach bronze.

SET PERSIST time_zone = '+00:00';
SET time_zone = '+00:00';

ALTER TABLE core_banking_prod.raw_transactions
  ADD COLUMN updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP AFTER created_at,
  ADD INDEX idx_transactions_updated_at (updated_at);

-- Existing rows: treat creation as the last change. Workbench blocks UPDATE without a key filter by default.
SET SQL_SAFE_UPDATES = 0;
UPDATE core_banking_prod.raw_transactions
SET updated_at = created_at;
SET SQL_SAFE_UPDATES = 1;

-- Check: time zone should be +00:00, and updated_at should equal created_at everywhere.
SELECT @@GLOBAL.time_zone AS GlobalTimeZone, @@SESSION.time_zone AS SessionTimeZone, UTC_TIMESTAMP() AS UtcNow, NOW() AS ServerNow;

SELECT
  COUNT(*) AS TransactionCount,
  SUM(updated_at = created_at) AS BackfilledCount
FROM
  core_banking_prod.raw_transactions;
