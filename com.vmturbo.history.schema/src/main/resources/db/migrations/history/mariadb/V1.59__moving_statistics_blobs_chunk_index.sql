-- Adding chunk_index as primary key, so moving statistics blobs could be split into smaller chunks.

ALTER TABLE `moving_statistics_blobs` DROP PRIMARY KEY;
ALTER TABLE `moving_statistics_blobs` ADD PRIMARY KEY(start_timestamp, chunk_index);
