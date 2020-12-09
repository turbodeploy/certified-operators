-- drop migrated table
DROP TABLE IF EXISTS recommendation_identity_hash;

-- create the migrated table
CREATE TABLE recommendation_identity_hash (
  id BIGINT(20) NOT NULL,
  action_hash BINARY(20) NOT NULL,
  PRIMARY KEY (id),
  UNIQUE INDEX action_hash_uniq_key (action_hash));

-- populate the the new table and drop the duplicates
INSERT IGNORE INTO recommendation_identity_hash
SELECT id, UNHEX(SHA1(CONCAT(action_type, ' ',
                   target_id, ' ',
                   IFNULL(action_details, ''), ' ',
	               IFNULL(concatentated_details.details, ''))))
  FROM recommendation_identity left join
    (SELECT recommendation_id, GROUP_CONCAT(detail ORDER BY detail SEPARATOR ' ') AS details
      FROM recommendation_identity_details GROUP BY recommendation_id) AS concatentated_details
	ON recommendation_identity.id = concatentated_details.recommendation_id;