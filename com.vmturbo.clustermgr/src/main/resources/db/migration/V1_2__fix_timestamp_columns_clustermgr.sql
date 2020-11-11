-- If a column is created with data type timestamp, NOT NULL, with no default value and it is
-- the first timestamp column of the table, mysql will automatically assigned the
-- DEFAULT CURRENT_TIMESTAMP and ON UPDATE CURRENT_TIMESTAMP attributes for this column.
-- (https://dev.mysql.com/doc/refman/5.6/en/upgrading-from-previous-series.html)
-- The "on update" attribute can cause issues if the timestamp is updated unintentionally
-- when the record is updated. This upgrade step removed the "ON UPDATE CURRENT_TIMESTAMP" attribute
-- on all timestamp columns.

ALTER TABLE registered_component
CHANGE registration_time
registration_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP;
