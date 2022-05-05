-- the target spec table depends on targetspec_oids, and so must be deleted first
DROP TABLE IF EXISTS targetspec_oid;

-- persist OIDs for TargetSpec objects here. The primary key is 'id', 'identity_matching_attributes'.
CREATE TABLE targetspec_oid (

  -- unique ID for this TargetSpec
  id BIGINT NOT NULL,

  -- identity matching attributes for the TargetSpec
  identity_matching_attributes text NOT NULL,

  -- the unique ID for this TargetSpec
  PRIMARY KEY (id)

) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;