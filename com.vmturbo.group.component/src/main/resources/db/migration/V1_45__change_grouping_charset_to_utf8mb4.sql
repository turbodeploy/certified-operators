-- Change the indexes on string fields to take into account only the first 191 characters.
-- In the (very rare) occasion that two entries have the same prefix, this should just make
-- the query a tiny bit slower since the resolution will have to happen on the fly.
-- This is needed in order to be backwards compatible with older innodb versions (<=5.6.x), since
-- on those versions the length of the index key has a limit of 767 bytes and innoDB assumes 4
-- bytes per character for utf8mb4 charset.
DROP INDEX idx_grouping_display_name ON grouping;
DROP INDEX idx_grouping_disc_src_id ON grouping;
-- Convert the character set from utf8 to utf8mb4
ALTER TABLE grouping CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE INDEX idx_grouping_display_name ON grouping (display_name(191));
CREATE INDEX idx_grouping_disc_src_id ON grouping (origin_discovered_src_id(191));
