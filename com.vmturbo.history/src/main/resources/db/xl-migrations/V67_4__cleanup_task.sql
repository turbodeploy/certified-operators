
-- XL MySQL Events to implement out of retention period historical data purge
-- Retention periods are defined in the retention_policies table.
-- Create the retention policies table if it does not already exist.
-- Populate it with default out-of-box values.  Indexes not required.



DROP TABLE IF EXISTS retention_policies;
CREATE TABLE retention_policies
(
  policy_name varchar(50),
  retention_period integer
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


-- Insert default retention periods
-- 2 hours for 'latest'
-- 72 hours
-- 60 days
-- 24 months



insert into retention_policies (policy_name,retention_period) values ('retention_latest_hours',2);
insert into retention_policies (policy_name,retention_period) values ('retention_hours',72);
insert into retention_policies (policy_name,retention_period) values ('retention_days', 60);
insert into retention_policies (policy_name,retention_period) values ('retention_months', 24);



-- The following MySQL events require the MySQL event scheduler to be enabled to trigger.
-- Enable the MySQL event scheduler by adding event_scheduler=ON to your MySQL configuration file my.cnf in the [mysqld] section.

-- NOTE:  The event scheduler is enabled by default in the XL build.



DROP EVENT IF EXISTS purge_expired_latest;
DROP EVENT IF EXISTS purge_expired_hours;
DROP EVENT IF EXISTS purge_expired_days;
DROP EVENT IF EXISTS purge_expired_months;

DELIMITER //
CREATE
EVENT purge_expired_hours
  ON SCHEDULE EVERY 1 HOUR
DO BEGIN

  -- purge latest table records
  DELETE FROM app_stats_latest where snapshot_time<(select date_sub(current_timestamp, interval retention_period hour) from retention_policies where policy_name='retention_latest_hours');
  DELETE FROM ch_stats_latest where snapshot_time<(select date_sub(current_timestamp, interval retention_period hour) from retention_policies where policy_name='retention_latest_hours');
  DELETE FROM cnt_stats_latest where snapshot_time<(select date_sub(current_timestamp, interval retention_period hour) from retention_policies where policy_name='retention_latest_hours');
  DELETE FROM dpod_stats_latest where snapshot_time<(select date_sub(current_timestamp, interval retention_period hour) from retention_policies where policy_name='retention_latest_hours');
  DELETE FROM ds_stats_latest where snapshot_time<(select date_sub(current_timestamp, interval retention_period hour) from retention_policies where policy_name='retention_latest_hours');
  DELETE FROM iom_stats_latest where snapshot_time<(select date_sub(current_timestamp, interval retention_period hour) from retention_policies where policy_name='retention_latest_hours');
  DELETE FROM pm_stats_latest where snapshot_time<(select date_sub(current_timestamp, interval retention_period hour) from retention_policies where policy_name='retention_latest_hours');
  DELETE FROM sc_stats_latest where snapshot_time<(select date_sub(current_timestamp, interval retention_period hour) from retention_policies where policy_name='retention_latest_hours');
  DELETE FROM sw_stats_latest where snapshot_time<(select date_sub(current_timestamp, interval retention_period hour) from retention_policies where policy_name='retention_latest_hours');
  DELETE FROM vdc_stats_latest where snapshot_time<(select date_sub(current_timestamp, interval retention_period hour) from retention_policies where policy_name='retention_latest_hours');
  DELETE FROM vm_stats_latest where snapshot_time<(select date_sub(current_timestamp, interval retention_period hour) from retention_policies where policy_name='retention_latest_hours');
  DELETE FROM vpod_stats_latest where snapshot_time<(select date_sub(current_timestamp, interval retention_period hour) from retention_policies where policy_name='retention_latest_hours');

  -- purge _by_hour records
  DELETE FROM app_stats_by_hour where snapshot_time<(select date_sub(current_timestamp, interval retention_period hour) from retention_policies where policy_name='retention_hours');
  DELETE FROM ch_stats_by_hour where snapshot_time<(select date_sub(current_timestamp, interval retention_period hour) from retention_policies where policy_name='retention_hours');
  DELETE FROM cnt_stats_by_hour where snapshot_time<(select date_sub(current_timestamp, interval retention_period hour) from retention_policies where policy_name='retention_hours');
  DELETE FROM dpod_stats_by_hour where snapshot_time<(select date_sub(current_timestamp, interval retention_period hour) from retention_policies where policy_name='retention_hours');
  DELETE FROM ds_stats_by_hour where snapshot_time<(select date_sub(current_timestamp, interval retention_period hour) from retention_policies where policy_name='retention_hours');
  DELETE FROM iom_stats_by_hour where snapshot_time<(select date_sub(current_timestamp, interval retention_period hour) from retention_policies where policy_name='retention_hours');
  DELETE FROM pm_stats_by_hour where snapshot_time<(select date_sub(current_timestamp, interval retention_period hour) from retention_policies where policy_name='retention_hours');
  DELETE FROM sc_stats_by_hour where snapshot_time<(select date_sub(current_timestamp, interval retention_period hour) from retention_policies where policy_name='retention_hours');
  DELETE FROM sw_stats_by_hour where snapshot_time<(select date_sub(current_timestamp, interval retention_period hour) from retention_policies where policy_name='retention_hours');
  DELETE FROM vdc_stats_by_hour where snapshot_time<(select date_sub(current_timestamp, interval retention_period hour) from retention_policies where policy_name='retention_hours');
  DELETE FROM vm_stats_by_hour where snapshot_time<(select date_sub(current_timestamp, interval retention_period hour) from retention_policies where policy_name='retention_hours');
  DELETE FROM vpod_stats_by_hour where snapshot_time<(select date_sub(current_timestamp, interval retention_period hour) from retention_policies where policy_name='retention_hours');
END //

CREATE
EVENT purge_expired_days
  ON SCHEDULE EVERY 1 DAY
DO BEGIN
  DELETE FROM app_stats_by_day where snapshot_time<(select date_sub(current_timestamp, interval retention_period day) from retention_policies where policy_name='retention_days');
  DELETE FROM ch_stats_by_day where snapshot_time<(select date_sub(current_timestamp, interval retention_period day) from retention_policies where policy_name='retention_days');
  DELETE FROM cnt_stats_by_day where snapshot_time<(select date_sub(current_timestamp, interval retention_period day) from retention_policies where policy_name='retention_days');
  DELETE FROM dpod_stats_by_day where snapshot_time<(select date_sub(current_timestamp, interval retention_period day) from retention_policies where policy_name='retention_days');
  DELETE FROM ds_stats_by_day where snapshot_time<(select date_sub(current_timestamp, interval retention_period day) from retention_policies where policy_name='retention_days');
  DELETE FROM iom_stats_by_day where snapshot_time<(select date_sub(current_timestamp, interval retention_period day) from retention_policies where policy_name='retention_days');
  DELETE FROM pm_stats_by_day where snapshot_time<(select date_sub(current_timestamp, interval retention_period day) from retention_policies where policy_name='retention_days');
  DELETE FROM sc_stats_by_day where snapshot_time<(select date_sub(current_timestamp, interval retention_period day) from retention_policies where policy_name='retention_days');
  DELETE FROM sw_stats_by_day where snapshot_time<(select date_sub(current_timestamp, interval retention_period day) from retention_policies where policy_name='retention_days');
  DELETE FROM vdc_stats_by_day where snapshot_time<(select date_sub(current_timestamp, interval retention_period day) from retention_policies where policy_name='retention_days');
  DELETE FROM vm_stats_by_day where snapshot_time<(select date_sub(current_timestamp, interval retention_period day) from retention_policies where policy_name='retention_days');
  DELETE FROM vpod_stats_by_day where snapshot_time<(select date_sub(current_timestamp, interval retention_period day) from retention_policies where policy_name='retention_days');
END //

CREATE
EVENT purge_expired_months
  ON SCHEDULE EVERY 1 MONTH
DO BEGIN
  DELETE FROM app_stats_by_month where snapshot_time<(select date_sub(current_timestamp, interval retention_period month) from retention_policies where policy_name='retention_months');
  DELETE FROM ch_stats_by_month where snapshot_time<(select date_sub(current_timestamp, interval retention_period month) from retention_policies where policy_name='retention_months');
  DELETE FROM cnt_stats_by_month where snapshot_time<(select date_sub(current_timestamp, interval retention_period month) from retention_policies where policy_name='retention_months');
  DELETE FROM dpod_stats_by_month where snapshot_time<(select date_sub(current_timestamp, interval retention_period month) from retention_policies where policy_name='retention_months');
  DELETE FROM ds_stats_by_month where snapshot_time<(select date_sub(current_timestamp, interval retention_period month) from retention_policies where policy_name='retention_months');
  DELETE FROM iom_stats_by_month where snapshot_time<(select date_sub(current_timestamp, interval retention_period month) from retention_policies where policy_name='retention_months');
  DELETE FROM pm_stats_by_month where snapshot_time<(select date_sub(current_timestamp, interval retention_period month) from retention_policies where policy_name='retention_months');
  DELETE FROM sc_stats_by_month where snapshot_time<(select date_sub(current_timestamp, interval retention_period month) from retention_policies where policy_name='retention_months');
  DELETE FROM sw_stats_by_month where snapshot_time<(select date_sub(current_timestamp, interval retention_period month) from retention_policies where policy_name='retention_months');
  DELETE FROM vdc_stats_by_month where snapshot_time<(select date_sub(current_timestamp, interval retention_period month) from retention_policies where policy_name='retention_months');
  DELETE FROM vm_stats_by_month where snapshot_time<(select date_sub(current_timestamp, interval retention_period month) from retention_policies where policy_name='retention_months');
  DELETE FROM vpod_stats_by_month where snapshot_time<(select date_sub(current_timestamp, interval retention_period month) from retention_policies where policy_name='retention_months');
END //

DELIMITER ;


UPDATE version_info SET version=67.4 WHERE id=1;