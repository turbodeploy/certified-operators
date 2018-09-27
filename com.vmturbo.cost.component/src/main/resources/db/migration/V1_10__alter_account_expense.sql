-- Flatten the account_expenses table to support roll up

ALTER TABLE account_expenses DROP PRIMARY KEY;

ALTER TABLE account_expenses DROP account_expenses_info;

ALTER TABLE account_expenses DROP received_time;

-- The time of last update.
ALTER TABLE account_expenses ADD COLUMN snapshot_time TIMESTAMP NOT NULL;

 -- The associated entity id
ALTER TABLE account_expenses ADD COLUMN associated_entity_id BIGINT NOT NULL;

-- The entity type that this account expense applies to.
ALTER TABLE account_expenses ADD COLUMN entity_type INT NOT NULL;

 -- The currency code, default to USD
ALTER TABLE account_expenses ADD COLUMN currency INT NOT NULL;

-- The cost amount for a given cost_type.
-- DECIMAL(20, 7): 20 is the precision and 7 is the scale.
ALTER TABLE account_expenses ADD COLUMN amount DECIMAL(20, 7)   NOT NULL;

ALTER TABLE account_expenses ADD PRIMARY KEY (associated_account_id, snapshot_time, associated_entity_id);

