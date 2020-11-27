
-- Remove the procedure if it already exists in the case the migration did not finish successfully last time
DROP PROCEDURE IF EXISTS ADD_HASH_COLUMN_AND_REMOVE_DETAILS_IDX_RECOMMENDATION_ID;

-- Define the idemponent procedure for adding the hash column and removing the index on action details
DELIMITER //
CREATE PROCEDURE ADD_HASH_COLUMN_AND_REMOVE_DETAILS_IDX_RECOMMENDATION_ID()
  BEGIN
      DECLARE CONTINUE HANDLER FOR SQLEXCEPTION BEGIN
          SELECT "Hash column is already in place or index already dropped in recommendation identity table " AS '';
      END;

      ALTER TABLE recommendation_identity
      DROP INDEX action_type,
      ADD COLUMN action_hash BINARY(20) NULL,
      ADD UNIQUE INDEX action_hash_uniq_key (action_hash);

  END//

DELIMITER ;

-- call the procedure to to add the the hash column
CALL ADD_HASH_COLUMN_AND_REMOVE_DETAILS_IDX_RECOMMENDATION_ID();

-- remove the procedure as we no longer need it
DROP PROCEDURE IF EXISTS ADD_HASH_COLUMN_AND_REMOVE_DETAILS_IDX_RECOMMENDATION_ID;

-- populate the the new field with the hash of action
UPDATE recommendation_identity
        LEFT JOIN
    (SELECT
        recommendation_id,
            GROUP_CONCAT(detail
                ORDER BY detail ASC
                SEPARATOR ' ') AS details
    FROM
        recommendation_identity_details
    GROUP BY recommendation_id) AS concatentated_details ON recommendation_identity.id = concatentated_details.recommendation_id
SET
    action_hash = UNHEX(SHA1(CONCAT(action_type,
                            ' ',
                            target_id,
                            ' ',
                            IFNULL(action_details, ''),
                            ' ',
                            IFNULL(concatentated_details.details, ''))));


-- change the column so it cannot be null
ALTER TABLE recommendation_identity CHANGE COLUMN action_hash action_hash BINARY(20) NOT NULL ;