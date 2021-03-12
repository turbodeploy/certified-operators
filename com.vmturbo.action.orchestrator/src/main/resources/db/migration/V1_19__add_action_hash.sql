-- this migratin consolidates the existing recommendation_identity and
-- recommendation_identity_details tables into a single new recommendation_identity_hash that uses
-- a hash computed from former action data to manage actoin identities

-- create the new table
DROP TABLE IF EXISTS recommendation_identity_hash;
CREATE TABLE recommendation_identity_hash (
  id BIGINT(20) NOT NULL,
  action_hash BINARY(20) NOT NULL,
  PRIMARY KEY (id));

-- now compute the new data one "shard" at a time, by including a WHERE clause to restrict
-- the action ids to be processed. This is to prevent a what was formerly a single operation
-- from running so long that the connection exceeds kube-proxy's 15-minute idle conneciton
-- timeout.
DROP PROCEDURE IF EXISTS _run_shard;
DELIMITER $$
CREATE PROCEDURE _run_shard(shard int, shard_count int, mult int)
BEGIN
  INSERT IGNORE INTO recommendation_identity_hash
    SELECT id, UNHEX(SHA1(CONCAT(
      action_type, ' ',
      target_id, ' ',
      IFNULL(action_details, ''), ' ',
      IFNULL(GROUP_CONCAT(detail ORDER BY detail SEPARATOR ' '), ''))))
    FROM  recommendation_identity
    LEFT JOIN recommendation_identity_details det ON id  = det.recommendation_id
    WHERE id*mult % shard_count = shard
    GROUP BY id ;
END; $$
DELIMITER ;

CALL _run_shard(0, 10, 23);
CALL _run_shard(1, 10, 23);
CALL _run_shard(2, 10, 23);
CALL _run_shard(3, 10, 23);
CALL _run_shard(4, 10, 23);
CALL _run_shard(5, 10, 23);
CALL _run_shard(6, 10, 23);
CALL _run_shard(7, 10, 23);
CALL _run_shard(8, 10, 23);
CALL _run_shard(9, 10, 23);

DROP procedure _run_shard;

-- finally create an index on the new hash table
CREATE UNIQUE INDEX action_hash_uniq_key ON recommendation_identity_hash(action_hash);
