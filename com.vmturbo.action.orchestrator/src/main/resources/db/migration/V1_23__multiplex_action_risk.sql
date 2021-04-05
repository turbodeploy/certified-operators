
DROP TABLE IF EXISTS related_risk_description;

-- Create a new table to store risk descriptions associated with each risk.
-- There can be multiple rows with the same id and different description, because
-- a single action can have multiple risks.
CREATE TABLE related_risk_description (
    -- The id of the risk set.
    id INTEGER NOT NULL,

    -- The string describing the risk.
    risk_description VARCHAR(100) NOT NULL,

    FOREIGN KEY (id) REFERENCES related_risk_for_action(id) ON DELETE CASCADE,
    -- The risk_description comes first in the key because that's what we query for.
    PRIMARY KEY (risk_description, id)
);

-- Using procedure for an idempotent migration.
DROP PROCEDURE IF EXISTS `MULTIPLEX_RISK_23`;
DELIMITER $$
CREATE PROCEDURE MULTIPLEX_RISK_23()
BEGIN
    -- Add checksum column to related_risk_for_action if its not there.
    -- We use the checksum to quickly determine if a particular set of risks already has
    -- an associated risk_id in the database.
    IF NOT EXISTS( (SELECT * FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE()
            AND COLUMN_NAME='checksum' AND TABLE_NAME='related_risk_for_action') ) THEN
        ALTER TABLE related_risk_for_action ADD checksum CHAR(32) NOT NULL DEFAULT '';
    END IF;

    -- Set checksum from existing description.
    UPDATE related_risk_for_action SET checksum = MD5(risk_description) WHERE checksum = '';

    -- Make the checksum unique once it's initialized.
    -- This will ensure that attempts to insert the same row multiple times fail.
    ALTER TABLE related_risk_for_action ADD UNIQUE KEY (checksum);

    -- Transfer descriptions to the new related_risk_description table.
    -- We assume that all previously recorded risk_ids were only associated with a single
    -- description. This is a false assumption in general, but there is no way to reconstruct
    -- the correct risk sets and we don't want to just drop all the data.
    IF EXISTS( (SELECT * FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE()
            AND COLUMN_NAME='risk_description' AND TABLE_NAME='related_risk_for_action') ) THEN

        -- Transfer all descriptions to the other table.
        INSERT INTO related_risk_description (id, risk_description)
            SELECT r.id, r.risk_description FROM related_risk_for_action r
            LEFT OUTER JOIN related_risk_description d ON r.id = d.id
            -- Only attempt to transfer entries that don't exist.
            WHERE d.id IS NULL;

        -- Remove descriptions.
        ALTER TABLE related_risk_for_action DROP COLUMN risk_description;
    END IF;
END $$
DELIMITER ;

call MULTIPLEX_RISK_23();
DROP PROCEDURE IF EXISTS `MULTIPLEX_RISK_23`;
