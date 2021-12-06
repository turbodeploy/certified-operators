-- noinspection SqlNoDataSourceInspectionForFile

-- The storage keeps secure data.
-- The keys are unique, but don't enforce any semantics as to their construction.
CREATE TABLE storage (
  -- The key
  path  VARCHAR(255) NOT NULL,
  -- The owner
  owner VARCHAR(255) NOT NULL,
  -- We use BLOB to store custom data.
  -- We don't know how big that custom data will be.
  data  LONGTEXT     NOT NULL,

  PRIMARY KEY (path, owner)
) ENGINE=INNODB DEFAULT CHARSET=utf8;
