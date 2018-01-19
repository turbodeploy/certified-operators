
-- XL MySQL Events to purge historical audit log entries in the audit_log table
-- Populate the audit log retention policies table with default out-of-box values.

-- Insert default retention period
-- 365 days

insert into audit_log_retention_policies (policy_name,retention_period) values ('retention_days', 365);

-- The following MySQL events require the MySQL event scheduler to be enabled to trigger.
-- Enable the MySQL event scheduler by adding event_scheduler=ON to your MySQL configuration file my.cnf in the [mysqld] section.
-- NOTE:  The event scheduler is enabled by default in the XL build.

DROP EVENT IF EXISTS purge_audit_log_expired_days;

DELIMITER //

CREATE
EVENT purge_audit_log_expired_days
  ON SCHEDULE EVERY 1 DAY
DO BEGIN
  DELETE FROM audit_log_entries where snapshot_time<(select date_sub(current_timestamp, interval retention_period day) from audit_log_retention_policies where policy_name='retention_days');
END //

DELIMITER ;

UPDATE version_info SET version=73.2 WHERE id=1;
