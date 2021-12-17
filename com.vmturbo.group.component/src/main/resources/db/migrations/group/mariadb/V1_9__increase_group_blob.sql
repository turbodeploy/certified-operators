-- The "BLOB" datatype can fit objects up to 64kb.
-- Since each ID is a long (8 bytes), 64 kb will only accommodate static groups of up to ~8k
-- entities, which may not be big enough for large customer environments.
--
-- We will migrate to a "MEDIUMBLOB" which goes up to 16MB, which should be more than enough for
-- any non-malicious group.

ALTER TABLE grouping MODIFY group_data MEDIUMBLOB NOT NULL;
