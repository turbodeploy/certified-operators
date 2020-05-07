DELETE FROM setting_policy
WHERE id IN (
    SELECT policy_id FROM setting_policy_setting WHERE policy_id in (
        -- all the policies that have a collectionTimeUtilization setting
        SELECT policy_id FROM setting_policy_setting WHERE setting_name = "collectionTimeUtilization"
        INTERSECT
        -- all user policies
        SELECT id FROM setting_policy WHERE policy_type = "user"
     )
     -- policies where collectionTimeUtilization is the only setting
     GROUP BY policy_id HAVING count(*) = 1
);

DELETE FROM setting_policy_setting WHERE setting_name = "collectionTimeUtilization";