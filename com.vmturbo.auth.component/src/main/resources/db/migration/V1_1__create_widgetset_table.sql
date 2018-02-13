-- noinspection SqlNoDataSourceInspectionForFile

-- The storage keeps secure data.
-- The keys are unique, but don't enforce any semantics as to their construction.
DROP TABLE IF EXISTS `widgetset`;
CREATE TABLE widgetset (

  -- the unique id for this widgetset
  oid BIGINT UNIQUE NOT NULL,

  -- the user-facing name for the widgetset
  display_name VARCHAR(255),

  -- the user oid of the owner of this widgetset; limits access unless 'shared_with_all_users' is true
  owner_oid BIGINT NOT NULL,

  -- defined by the UI, and available as a search filter for UI requests
  category VARCHAR(255),

  -- the scope attached to this widget
  scope VARCHAR(255),

  -- the type of the scope, used in search, e.g. Application, Cloud_Database, Cloud_Group_Storage,
  -- Group_PhysicalMachine, Group_ServiceEntity, Hybrid_Market, Market, Onprem_Application, etc.
  scope_type VARCHAR(255),

  -- should this widget be readable by all users
  shared_with_all_users boolean NOT NULL DEFAULT false,

  -- Store the body of the widget definition as a String, for now. In legacy there are several
  -- further levels of sub-objects: WidgetApiDTO -> WidgetElementApiDTO -> StatApiInputDTO -> ...
  -- The server code needs no access to these inner objects, so a string will suffice.
  widgets MEDIUMTEXT NOT NULL,


  PRIMARY KEY (oid)
) ENGINE=INNODB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;