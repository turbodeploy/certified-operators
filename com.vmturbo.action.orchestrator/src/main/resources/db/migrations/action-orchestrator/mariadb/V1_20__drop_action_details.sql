-- this script replaces the the new recommendation identity table with the old one

-- drop recommendation_identity_details table first at it has foreign key to recommendation_identity
DROP TABLE IF EXISTS recommendation_identity_details;
-- drop recommendation_identity table
DROP TABLE IF EXISTS recommendation_identity;
-- rename the recommendation_identity_hash to recommendation_identity
RENAME TABLE recommendation_identity_hash TO recommendation_identity;