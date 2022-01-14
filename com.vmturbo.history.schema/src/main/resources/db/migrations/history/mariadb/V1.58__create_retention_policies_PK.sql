-- To resolve the conflict in duplicates,
-- the maximum retention period value is used to prevent the historical data purging.
CREATE TABLE V1_58__temp_retention_policies LIKE retention_policies;

-- The query selects max/any value for the unit column.
INSERT INTO V1_58__temp_retention_policies
    SELECT policy_name, MAX(retention_period), MAX(unit)
    FROM retention_policies
    WHERE policy_name IS NOT NULL
    GROUP BY policy_name;

DROP TABLE retention_policies;

ALTER TABLE V1_58__temp_retention_policies RENAME TO retention_policies;

ALTER TABLE retention_policies ADD PRIMARY KEY (policy_name);
