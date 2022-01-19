-- This script adds an ability to recommendations to have tons of additional descriptions.
-- that could not be guaranteed to fit into a single column
CREATE TABLE recommendation_identity_details (
    recommendation_id BIGINT(20) NOT NULL,
    detail VARCHAR(200) NOT NULL,
    PRIMARY KEY (recommendation_id, detail),
    FOREIGN KEY fk_recommendation_id_details_recommendation_id (recommendation_id)
         REFERENCES recommendation_identity(id) ON DELETE CASCADE
);

ALTER TABLE recommendation_identity MODIFY COLUMN action_details VARCHAR(255) NULL;

