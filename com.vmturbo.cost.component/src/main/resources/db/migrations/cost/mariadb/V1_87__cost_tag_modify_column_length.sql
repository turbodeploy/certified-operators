-- Increase tag_key and tag_value column lengths to match max limits of all Cloud Providers:
--
-- | Cloud Provider | tag_key limit | tag_value limit |
-- |----------------|---------------|-----------------|
-- | Azure          | 512           | 256             |
-- | AWS            | 127           | 255             |
-- | GCP            | 63            | 63              |
--

DROP PROCEDURE IF EXISTS `WIDEN_COST_TAG`;
-- Carrying out the steps in a PROC since dropping index is non-idempotent.
DELIMITER $$
CREATE PROCEDURE WIDEN_COST_TAG()
BEGIN
    -- Dropping index on (tag_key, tag_value) to accommodate for MariaDB 5.5.X limit of 767 bytes, which gets violated
    -- on widening the cost_tag columns
    IF EXISTS ( (SELECT * FROM information_schema.statistics WHERE table_schema = DATABASE()
    AND table_name = 'cost_tag' AND index_name = 'unique_constraint_key_value') ) THEN
        ALTER TABLE cost_tag DROP INDEX unique_constraint_key_value;
    END IF;

    ALTER TABLE cost_tag MODIFY COLUMN tag_key varchar(512);
    ALTER TABLE cost_tag MODIFY COLUMN tag_value varchar(256);

END $$
DELIMITER ;

call WIDEN_COST_TAG();
DROP PROCEDURE IF EXISTS `WIDEN_COST_TAG`;