-- track the number of "new" actions for each market analysis to use in roll-up processing
ALTER TABLE `action_stats_latest` ADD COLUMN `new_action_count` INT DEFAULT 0
    AFTER `total_action_count`;

-- track the number of actions from the very first market analysis of the hour
-- this is used in the roll-up from by_hour to by_day
ALTER TABLE `action_stats_by_hour` ADD COLUMN `prior_action_count` INT DEFAULT 0
    AFTER `avg_action_count`;
ALTER TABLE `action_stats_by_hour` ADD COLUMN `new_action_count` INT DEFAULT 0
    AFTER `avg_action_count`;

-- track the number of actions from the very first market analysis of the day
-- this is used in the roll-up from by_day to by_month
ALTER TABLE `action_stats_by_day` ADD COLUMN `prior_action_count` INT DEFAULT 0
    AFTER `avg_action_count`;
ALTER TABLE `action_stats_by_day` ADD COLUMN `new_action_count` INT DEFAULT 0
    AFTER `avg_action_count`;