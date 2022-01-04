-- Prior to this change `wasted_file` table was populated with all files from most recent topology
-- that were listed for unattached volumes; files on attached volumes were not recorded.
-- In order to support repots concerning files in attached volumes, we here define a more general
-- `file` table to represent all files present in the most recent topology, and a `wasted_file`
-- view for backward compatibility.

-- enum to represent file types, synchronized with `VirtualMachineFileType` enum in
-- `CommonDTO.proto`
DROP TYPE IF EXISTS file_type;
CREATE TYPE file_type AS ENUM (
        'CONFIGURATION',
        'DISK',
        'SNAPSHOT',
        'MEMORY',
        'SWAP',
        'LOG',
        'ISO',
        'ESXCONSOLE'
);

-- table for storing file data from most recent topology (no historical data)
DROP TABLE IF EXISTS "file";
CREATE TABLE "file" (
  -- id of volume that contains the file
  "volume_oid" int8 NOT NULL,
  -- path of the file
  "path" text NOT NULL,
  -- file type
  "type" file_type NOT NULL,
  -- file size
  "file_size_kb" int8 NULL,
  -- last time the file is modified
  "modification_time" timestamptz NULL,
  -- id of storage which contains the file
  "storage_oid" int8 NOT NULL,
  -- name of the storage (denormalized for reporting)
  "is_attached" boolean NOT NULL,
  -- hash value to support quick removal of stale records
  "hash" int8 NOT NULL,
  PRIMARY KEY (volume_oid, path)
);
-- enable fast retrieval of all unattached files by indexing just those records
-- Note: index will only be used when query condition exactly matches condition shown here
-- ("NOT is_attached" will work; but "is_attached IS FALSE", "is_attached != TRUE", etc. will not)
CREATE INDEX file__unattached ON file(is_attached) WHERE NOT is_attached;
-- enable fast retrieval by file type for top storage reports
CREATE INDEX file__type ON file(type);
-- enable fast retrieval by hash value
CREATE INDEX file__hash ON file(hash);

-- replace current `wasted_file` table with an equivalent view for backward-compatibility
DROP TABLE IF EXISTS "wasted_file";
CREATE VIEW "wasted_file" AS
  SELECT f.path, f.file_size_kb, f.modification_time, f.storage_oid, e.name AS storage_name
  FROM file f, entity e
  WHERE e.oid = f.storage_oid
  AND NOT is_attached;
