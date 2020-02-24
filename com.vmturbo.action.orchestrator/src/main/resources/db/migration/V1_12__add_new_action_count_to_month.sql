-- track the number of actions from the very first market analysis of the month
-- this is used to return accurate action counts from monthly tables.
ALTER TABLE `action_stats_by_month` ADD COLUMN `prior_action_count` INT DEFAULT 0
    AFTER `avg_action_count`;
ALTER TABLE `action_stats_by_month` ADD COLUMN `new_action_count` INT DEFAULT 0
    AFTER `avg_action_count`;
