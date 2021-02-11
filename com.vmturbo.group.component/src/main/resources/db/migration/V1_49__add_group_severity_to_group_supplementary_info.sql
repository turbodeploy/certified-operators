-- Table for storing group severity. It is stored in a different table than other group stats, to
-- able to update it independently.
ALTER TABLE group_supplementary_info ADD COLUMN severity INT(11) ;

