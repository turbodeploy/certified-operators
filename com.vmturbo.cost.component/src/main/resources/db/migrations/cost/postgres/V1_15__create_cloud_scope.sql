CREATE TABLE IF NOT EXISTS cloud_scope (
   scope_id               BIGINT           NOT NULL,
   -- provided as metadata, scope_id by itself is expected to be unique
   scope_type             SMALLINT         NOT NULL,
   resource_id            BIGINT           DEFAULT NULL,
   resource_type          SMALLINT         DEFAULT NULL,
   account_id             BIGINT           DEFAULT NULL,
   region_id              BIGINT           DEFAULT NULL,
   cloud_service_id       BIGINT           DEFAULT NULL,
   availability_zone_id   BIGINT           DEFAULT NULL,
   -- regardless of the scope_type, service provider must always be set
   service_provider_id    BIGINT           NOT NULL,
   resource_group_id      BIGINT           DEFAULT NULL,
   update_ts              TIMESTAMP        NOT NULL DEFAULT CURRENT_TIMESTAMP,

   PRIMARY KEY (scope_id))

PARTITION BY HASH(scope_id);

CREATE INDEX cloud_scope_account_id_idx ON cloud_scope USING btree (account_id);
CREATE INDEX cloud_scope_region_id_idx ON cloud_scope USING btree (region_id);
CREATE INDEX cloud_scope_zone_id_idx ON cloud_scope USING btree (availability_zone_id);
CREATE INDEX cloud_scope_rg_id_idx ON cloud_scope USING btree (resource_group_id);

CREATE FUNCTION update_update_ts_column() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
  BEGIN
    NEW.update_ts = NOW();
    RETURN NEW;
  END;
$$;

CREATE TRIGGER cloud_scope_update_ts BEFORE UPDATE ON cloud_scope FOR EACH ROW EXECUTE PROCEDURE update_update_ts_column();

DO $$ DECLARE
BEGIN
  FOR i IN 0..49 LOOP
     EXECUTE FORMAT('CREATE TABLE %I PARTITION OF cloud_scope FOR VALUES WITH (MODULUS 50, REMAINDER %s)', 'cloud_scope_'||TO_CHAR(i, 'FM00'), i);
  END LOOP;
END; $$;
