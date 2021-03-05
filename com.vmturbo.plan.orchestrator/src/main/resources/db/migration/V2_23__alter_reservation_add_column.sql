-- Add column 'delayed_deletion' in reservation table to indicate the reservation is delayed deleted.
ALTER TABLE reservation ADD COLUMN deployed INT;
UPDATE reservation SET deployed = 0;
ALTER TABLE reservation MODIFY COLUMN deployed INT NOT NULL;