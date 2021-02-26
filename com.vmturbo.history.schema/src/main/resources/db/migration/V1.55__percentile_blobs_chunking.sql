-- Adding chunk_index column, so percentile blobs could be splitted into smaller chunks.

DROP PROCEDURE IF EXISTS _percentile_blobs_add_chunk_index;
DELIMITER //
CREATE PROCEDURE _percentile_blobs_add_chunk_index()
BEGIN
  IF (SELECT NOT EXISTS(SELECT * FROM information_schema.columns WHERE table_schema = 'vmtdb' AND table_name = 'percentile_blobs' AND column_name = 'chunk_index'))
  THEN
    ALTER TABLE `percentile_blobs` ADD `chunk_index` int not null default '0';
  END IF;
END;
//
DELIMITER ;
CALL _percentile_blobs_add_chunk_index();
DROP PROCEDURE _percentile_blobs_add_chunk_index;
ALTER TABLE `percentile_blobs` DROP PRIMARY KEY;
ALTER TABLE `percentile_blobs` ADD PRIMARY KEY(start_timestamp, chunk_index);
