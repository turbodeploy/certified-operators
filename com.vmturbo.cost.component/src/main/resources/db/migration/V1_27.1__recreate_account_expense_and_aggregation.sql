/* Drop tables including aggregation tables */
DROP TABLE IF EXISTS account_expenses_by_hour;
DROP TABLE IF EXISTS account_expenses_by_day;

/* Drop views */
DROP VIEW IF EXISTS account_expenses_hourly_ins_vw;
DROP VIEW IF EXISTS account_expenses_daily_ins_vw;

-- The account_expenses table contains the latest record for each expense_date, associated_entity_id,
-- and associated_account_id. We assume that given two records that represent the spent
-- on the same cloud service/compute tier in a specific account on the same date, the recent record
-- holds the correct values.
-- For this table, insertion of duplicate keys is handled in SqlAccountExpensesStore#persistAccountExpense
DROP TABLE IF EXISTS account_expenses;
CREATE TABLE account_expenses (
    associated_account_id              BIGINT          NOT NULL,
    expense_date                       DATE            NOT NULL,
    associated_entity_id               BIGINT          NOT NULL,
    entity_type                        INT(11)         NOT NULL,
    currency                           INT(11)         NOT NULL,
    amount                             DECIMAL(20,7)   NOT NULL,
    aggregated                         tinyint(1)      NOT NULL  DEFAULT 0,
    PRIMARY KEY (expense_date, associated_account_id, associated_entity_id),
    INDEX ex_ai (associated_account_id)
);

-- The account_expenses_by_month table contains the average expenses on a cloud service/compute tier
-- in a specific account within a month.
DROP TABLE IF EXISTS account_expenses_by_month;
CREATE TABLE account_expenses_by_month (
    associated_account_id              BIGINT          NOT NULL,
    expense_date                       DATE            NOT NULL,
    associated_entity_id               BIGINT          NOT NULL,
    entity_type                        INT(11)         NOT NULL,
    currency                           INT(11)         NOT NULL,
    amount                             DECIMAL(20,7)   NOT NULL,
    samples                            INT(11)         NOT NULL,
    PRIMARY KEY (expense_date, associated_account_id, associated_entity_id),
    INDEX exm_ai (associated_account_id)
);

DROP PROCEDURE IF EXISTS aggregate_account_expenses;

-- Aggregation Procedure
-- Procedure:  aggregate_account_expenses
-- Rolls up account expenses that not yet aggregated to a monthly table
DELIMITER $$
CREATE PROCEDURE aggregate_account_expenses()
BEGIN
    DECLARE aggregation_date DATE;

    DECLARE EXIT HANDLER FOR sqlexception
    BEGIN
       ROLLBACK;
       RESIGNAL;
    END;

    SET @aggregation_date = UTC_DATE();

    /* Aggregation is done under a transaction to allow keeping the db in a consistent state in case
       of a failure in the aggregation process */
    START TRANSACTION;
    /* The monthly aggregation table stores the average expense on a service in a certain account
       during the month. We convert the expense date of each aggregated record to midnight on
       the last day of the expense date month to be able to group all the records of each month together */
    SET @sql=concat('CREATE OR REPLACE VIEW account_expenses_monthly_ins_vw AS
    SELECT
        associated_account_id,
        last_day(expense_date) as expense_date,
        associated_entity_id,
        entity_type,
        currency,
        avg(amount) as amount,
        count(*) as samples
    FROM account_expenses
    WHERE expense_date < \'', @aggregation_date ,'\' AND aggregated = 0
    GROUP BY last_day(expense_date), associated_account_id, associated_entity_id');

    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    INSERT INTO account_expenses_by_month(associated_account_id, expense_date, associated_entity_id, entity_type, currency, amount, samples)
    SELECT associated_account_id,
           expense_date,
           associated_entity_id,
           entity_type,
           currency,
           amount,
           samples
    FROM account_expenses_monthly_ins_vw a
    ON DUPLICATE KEY UPDATE
        account_expenses_by_month.amount =
        (account_expenses_by_month.amount * account_expenses_by_month.samples + a.amount * a.samples)/(account_expenses_by_month.samples + a.samples),
        account_expenses_by_month.samples = account_expenses_by_month.samples + a.samples;

    SET @sql=concat('UPDATE account_expenses SET aggregated = 1 WHERE expense_date < \'', @aggregation_date,'\' AND aggregated = 0');

    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    COMMIT;
END $$

DELIMITER ;

-- Recreate events to aggregate_expenses scheduled event
-- Executes once a day, aggregates only records up until yesterday including yesterday.
-- running the aggregate_account_expenses procedure

DROP EVENT IF EXISTS aggregate_expenses_evt;

DELIMITER $$
CREATE
EVENT aggregate_expenses_evt
  ON SCHEDULE EVERY 1 DAY STARTS ADDTIME(UTC_DATE, '01:00:00')
DO BEGIN
  CALL aggregate_account_expenses;
END $$

DELIMITER ;