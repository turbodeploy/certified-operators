DELETE FROM setting_policy
WHERE id IN (SELECT * FROM (
    SELECT policy_id FROM setting_policy_setting
    WHERE policy_id IN (
        -- all the policies that have a collectionTimeUtilization setting
        SELECT policy_id FROM setting_policy_setting WHERE setting_name = "collectionTimeUtilization"
     )
     AND policy_id IN (
        -- all user policies
        SELECT id FROM setting_policy WHERE policy_type = "user"
     )
     -- policies where collectionTimeUtilization is the only setting
     GROUP BY policy_id HAVING count(*) = 1
-- temp table because mysql 5.5 doesn't like DELETE FROM the same table used in the WHERE clause
) temp_table);

DELETE FROM setting_policy_setting WHERE setting_name = "collectionTimeUtilization";