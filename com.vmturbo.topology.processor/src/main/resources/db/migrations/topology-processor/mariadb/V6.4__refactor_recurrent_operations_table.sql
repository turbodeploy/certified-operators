ALTER TABLE recurrent_operations ADD expiration_successful BOOLEAN NOT NULL DEFAULT 0;
ALTER TABLE recurrent_operations ADD last_seen_update_successful BOOLEAN NOT NULL DEFAULT 0;
ALTER TABLE recurrent_operations ADD expired_records INT NOT NULL DEFAULT 0;
ALTER TABLE recurrent_operations ADD updated_records INT NOT NULL DEFAULT 0;
ALTER TABLE recurrent_operations ADD errors TEXT DEFAULT NULL;
ALTER TABLE recurrent_operations DROP COLUMN summary;