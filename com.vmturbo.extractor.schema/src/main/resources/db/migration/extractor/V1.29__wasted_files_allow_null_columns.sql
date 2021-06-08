/*
** Allow null in the last modification time and volume size columns for the wasted files.
*/

ALTER TABLE "wasted_file" ALTER COLUMN "modification_time" DROP NOT NULL;
ALTER TABLE "wasted_file" ALTER COLUMN "file_size_kb" DROP NOT NULL;