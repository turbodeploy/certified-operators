/*
** Initial schema for the wasted files.
*/

-- table for storing current wasted files in the system
DROP TABLE IF EXISTS "wasted_file";
CREATE TABLE "wasted_file" (
  -- path of the file
  "path" text NOT NULL,
  -- file size
  "file_size_kb" int8 NOT NULL,
  -- last time the file is modified
  "modification_time" timestamptz NOT NULL,
  -- id of storage which contains the file
  "storage_oid" int8 NOT NULL,
  -- name of the storage (denormalized for reporting)
  "storage_name" text NOT NULL
);