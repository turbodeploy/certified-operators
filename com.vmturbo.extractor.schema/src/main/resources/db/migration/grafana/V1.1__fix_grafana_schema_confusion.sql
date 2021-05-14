-- This migration corrects an unintended change in the name of the schema used for Grafana's
-- internal data storage, introduced in release 8.1.4. The change was inconsequential until
-- 8.2.0 introduced re-running of provisioning on component restart. During testing we started
-- seeing instances that had been created prior 8.1.4 starting to fail after upgrade.
--
-- Along with some improvements to the provisioning logic and explicitly setting the preferred
-- schema name in the migration definition, in this migration we move tables from the incorrect
-- schema to the correct schema. Unfortunately the user under which this migration will run does
-- not have ownership of the wrong schema, so a simple schema rename is not possible, hence the
-- more complicated table move. We will be left with the wrong schema still present, but empty
-- and harmless.
--
-- This migration will only be operable if the wrong schema (`extractor`) is present and  the
-- correct schema (`grafana_writer`) is present and contains no tables. This is the expected state
-- in any installation that run afoul of this bug.
DO $$ BEGIN
  IF EXISTS(SELECT * FROM information_schema.schemata WHERE schema_name='extractor')
     AND NOT EXISTS(SELECT * FROM information_schema.tables WHERE table_schema='grafana_writer')
  THEN
    DECLARE t text;
    BEGIN
      FOR t IN SELECT table_name FROM information_schema.tables WHERE table_schema='extractor' LOOP
        EXECUTE concat('ALTER TABLE extractor.', t, ' SET SCHEMA grafana_writer');
      END LOOP;
    END
  END IF;
END; $$
