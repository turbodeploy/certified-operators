/*
Add Mode column to determine behavior type of buyers in reservation.
Add Grouping column to determine behavior type of reservation.
*/

ALTER TABLE reservation ADD COLUMN mode INT;
UPDATE reservation SET mode = 1;
ALTER TABLE reservation MODIFY COLUMN mode INT NOT NULL;

ALTER TABLE reservation ADD COLUMN grouping INT;
UPDATE reservation SET grouping = 1;
ALTER TABLE reservation MODIFY COLUMN grouping INT NOT NULL;