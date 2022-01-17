-- Events to remove accoumt expenses which are expired according to value configured
-- for retention_policies.retention_days
DROP EVENT IF EXISTS purge_expired_account_expenses_data;

DELIMITER $$
CREATE EVENT purge_expired_account_expenses_data
    ON SCHEDULE EVERY 1 DAY STARTS ADDTIME(UTC_DATE, '01:00:00')
DO BEGIN
    CALL purge_expired_data('account_expenses', 'expense_date', 'retention_days', 'day');
    CALL purge_expired_data('account_expenses_by_month', 'expense_date', 'retention_months', 'month');
END $$

DELIMITER ;