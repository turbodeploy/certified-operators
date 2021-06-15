-- This migration drops and recreates all stored executable objects appearing in the
-- group_component database in in conjunction with a change to database provisioning logic
-- so that initial migrations are no longer executed with root privileges. Recreation is
-- needed so that these objects, which may already be in the database with a DEFINER attribute
-- of 'root', will hereafter have a non-root definer and will no longer execute with root
-- privileges.

DROP VIEW IF EXISTS all_topo_data_defs;
CREATE VIEW all_topo_data_defs  AS
  SELECT id, name AS name_or_prefix, 'MANUAL' AS type
  FROM manual_topo_data_defs
UNION
  SELECT id, name_prefix AS name_or_prefix, 'AUTO' AS type
  FROM auto_topo_data_defs;
