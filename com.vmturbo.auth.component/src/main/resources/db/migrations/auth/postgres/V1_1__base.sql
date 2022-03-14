-- case insensitive collation
CREATE COLLATION IF NOT EXISTS ci (provider = 'icu', locale = 'und@colStrength=primary', deterministic = false);

-- The storage keeps secure data.
-- The keys are unique, but don't enforce any semantics as to their construction.
DROP TABLE IF EXISTS "storage";
CREATE TABLE "storage" (
  -- The key
  "path"  VARCHAR(255) COLLATE ci NOT NULL,
  -- The owner
  "owner" VARCHAR(255) COLLATE ci NOT NULL,
  -- We use BLOB to store custom data.
  -- We don't know how big that custom data will be.
  "data"  TEXT COLLATE ci NOT NULL,

  PRIMARY KEY ("path", "owner")
);

-- noinspection SqlNoDataSourceInspectionForFile

-- The storage keeps secure data.
-- The keys are unique, but don't enforce any semantics as to their construction.
DROP TABLE IF EXISTS "widgetset";
CREATE TABLE "widgetset" (

  -- the unique id for this widgetset
  "oid" INT8 NOT NULL,

  -- the user-facing name for the widgetset
  "display_name" VARCHAR(255) COLLATE ci,

  -- the user oid of the owner of this widgetset; limits access unless 'shared_with_all_users' is true
  "owner_oid" INT8 NOT NULL,

  -- defined by the UI, and available as a search filter for UI requests
  "category" VARCHAR(255) COLLATE ci,

  -- the scope attached to this widget
  "scope" VARCHAR(255) COLLATE ci,

  -- the type of the scope, used in search, e.g. Application, Cloud_Database, Cloud_Group_Storage,
  -- Group_PhysicalMachine, Group_ServiceEntity, Hybrid_Market, Market, Onprem_Application, etc.
  "scope_type" VARCHAR(255) COLLATE ci,

  -- should this widget be readable by all users
  "shared_with_all_users" BOOL NOT NULL DEFAULT false,

  -- Store the body of the widget definition as a String, for now. In legacy there are several
  -- further levels of sub-objects: WidgetApiDTO -> WidgetElementApiDTO -> StatApiInputDTO -> ...
  -- The server code needs no access to these inner objects, so a string will suffice.
  "widgets" TEXT COLLATE ci NOT NULL,


  PRIMARY KEY (oid)
);
