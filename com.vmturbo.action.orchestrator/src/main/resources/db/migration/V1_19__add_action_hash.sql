-- this migratin consolidates the existing recommendation_identity and
-- recommendation_identity_details tables into a single new recommendation_identity_hash that uses
-- a hash computed from former action data to manage actoin identities


-- create the new table
DROP TABLE IF EXISTS recommendation_identity_hash;
CREATE TABLE recommendation_identity_hash (
  id BIGINT(20) NOT NULL,
  action_hash BINARY(20) NOT NULL,
  PRIMARY KEY (id));

-- create a means of splitting the overall job into shorter segments to avoid problems with
-- dropped connections. For this we create a table containing all the action ids, hash-partitioned
-- into 10 chunks
DROP TABLE IF EXISTS _ids;
CREATE TABLE _ids(id bigint NOT NULL)
PARTITION BY HASH(id) PARTITIONS 10;
INSERT INTO _ids SELECT id FROM recommendation_identity;

-- now compute the new data one partition at a time, by including a single partition of the
-- _ids table in a join against the existing tables.
INSERT IGNORE INTO recommendation_identity_hash
  SELECT _ids.id, UNHEX(SHA1(CONCAT(
    action_type, ' ',
    target_id, ' ',
   IFNULL(action_details, ''), ' ',
    IFNULL(GROUP_CONCAT(detail ORDER BY detail SEPARATOR ' '), ''))))
  FROM _ids PARTITION (p0) JOIN recommendation_identity USING(id)
  LEFT JOIN recommendation_identity_details det ON _ids.id  = det.recommendation_id
  GROUP BY _ids.id ;

INSERT IGNORE INTO recommendation_identity_hash
  SELECT _ids.id , UNHEX(SHA1(CONCAT(
    action_type, ' ',
    target_id, ' ',
    IFNULL(action_details, ''), ' ',
    IFNULL(GROUP_CONCAT(detail ORDER BY detail SEPARATOR ' '), ''))))
  FROM _ids PARTITION (p1) JOIN recommendation_identity USING(id)
  LEFT JOIN recommendation_identity_details det ON _ids.id  = det.recommendation_id
  GROUP BY _ids.id ;

INSERT IGNORE INTO recommendation_identity_hash
  SELECT _ids.id , UNHEX(SHA1(CONCAT(
    action_type, ' ',
    target_id, ' ',
    IFNULL(action_details, ''), ' ',
    IFNULL(GROUP_CONCAT(detail ORDER BY detail SEPARATOR ' '), ''))))
  FROM _ids PARTITION (p2) JOIN recommendation_identity USING(id)
  LEFT JOIN recommendation_identity_details det ON _ids.id  = det.recommendation_id
  GROUP BY _ids.id ;

INSERT IGNORE INTO recommendation_identity_hash
  SELECT _ids.id , UNHEX(SHA1(CONCAT(
    action_type, ' ',
    target_id, ' ',
    IFNULL(action_details, ''), ' ',
    IFNULL(GROUP_CONCAT(detail ORDER BY detail SEPARATOR ' '), ''))))
  FROM _ids PARTITION (p3) JOIN recommendation_identity USING(id)
  LEFT JOIN recommendation_identity_details det ON _ids.id  = det.recommendation_id
  GROUP BY _ids.id ;

INSERT IGNORE INTO recommendation_identity_hash
  SELECT _ids.id , UNHEX(SHA1(CONCAT(
    action_type, ' ',
    target_id, ' ',
    IFNULL(action_details, ''), ' ',
    IFNULL(GROUP_CONCAT(detail ORDER BY detail SEPARATOR ' '), ''))))
  FROM _ids PARTITION (p4) JOIN recommendation_identity USING(id)
  LEFT JOIN recommendation_identity_details det ON _ids.id  = det.recommendation_id
  GROUP BY _ids.id ;

INSERT IGNORE INTO recommendation_identity_hash
  SELECT _ids.id , UNHEX(SHA1(CONCAT(
    action_type, ' ',
    target_id, ' ',
    IFNULL(action_details, ''), ' ',
    IFNULL(GROUP_CONCAT(detail ORDER BY detail SEPARATOR ' '), ''))))
  FROM _ids PARTITION (p5) JOIN recommendation_identity USING(id)
  LEFT JOIN recommendation_identity_details det ON _ids.id  = det.recommendation_id
  GROUP BY _ids.id ;

INSERT IGNORE INTO recommendation_identity_hash
  SELECT _ids.id , UNHEX(SHA1(CONCAT(
    action_type, ' ',
    target_id, ' ',
    IFNULL(action_details, ''), ' ',
    IFNULL(GROUP_CONCAT(detail ORDER BY detail SEPARATOR ' '), ''))))
  FROM _ids PARTITION (p6) JOIN recommendation_identity USING(id)
  LEFT JOIN recommendation_identity_details det ON _ids.id  = det.recommendation_id
  GROUP BY _ids.id ;

INSERT IGNORE INTO recommendation_identity_hash
  SELECT _ids.id , UNHEX(SHA1(CONCAT(
    action_type, ' ',
    target_id, ' ',
    IFNULL(action_details, ''), ' ',
    IFNULL(GROUP_CONCAT(detail ORDER BY detail SEPARATOR ' '), ''))))
  FROM _ids PARTITION (p7) JOIN recommendation_identity USING(id)
  LEFT JOIN recommendation_identity_details det ON _ids.id  = det.recommendation_id
  GROUP BY _ids.id ;

INSERT IGNORE INTO recommendation_identity_hash
  SELECT _ids.id , UNHEX(SHA1(CONCAT(
    action_type, ' ',
    target_id, ' ',
    IFNULL(action_details, ''), ' ',
    IFNULL(GROUP_CONCAT(detail ORDER BY detail SEPARATOR ' '), ''))))
  FROM _ids PARTITION (p8) JOIN recommendation_identity USING(id)
  LEFT JOIN recommendation_identity_details det ON _ids.id  = det.recommendation_id
  GROUP BY _ids.id ;

INSERT IGNORE INTO recommendation_identity_hash
  SELECT _ids.id , UNHEX(SHA1(CONCAT(
    action_type, ' ',
    target_id, ' ',
    IFNULL(action_details, ''), ' ',
    IFNULL(GROUP_CONCAT(detail ORDER BY detail SEPARATOR ' '), ''))))
  FROM _ids PARTITION (p9) JOIN recommendation_identity USING(id)
  LEFT JOIN recommendation_identity_details det ON _ids.id  = det.recommendation_id
  GROUP BY _ids.id ;

-- drop the id table
DROP TABLE _ids;

-- finally create an index on the new hash table
CREATE UNIQUE INDEX action_hash_uniq_key ON recommendation_identity_hash(action_hash);
