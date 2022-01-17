-- To resolve the conflict in duplicates, the policy of the maximum retention period value is
-- used to prevent the historical data purging.
CREATE TABLE V1_68__temp_account_expenses_retention_policies
    LIKE account_expenses_retention_policies;

INSERT INTO V1_68__temp_account_expenses_retention_policies
    SELECT policy_name, MAX(retention_period)
    FROM account_expenses_retention_policies
    WHERE policy_name IS NOT NULL
    GROUP BY policy_name;

DROP TABLE account_expenses_retention_policies;

ALTER TABLE V1_68__temp_account_expenses_retention_policies
    RENAME TO account_expenses_retention_policies;

ALTER TABLE account_expenses_retention_policies ADD PRIMARY KEY (policy_name);
