ALTER TABLE reservation ADD new_status int;
UPDATE reservation SET new_status=1 WHERE status='future';
UPDATE reservation SET new_status=2 WHERE status='reserved';
UPDATE reservation SET new_status=3 WHERE status='invalid';
ALTER TABLE reservation DROP status;
ALTER TABLE reservation CHANGE new_status status int;
