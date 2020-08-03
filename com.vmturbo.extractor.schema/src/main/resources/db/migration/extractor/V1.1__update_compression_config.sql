-- update to compression configuration of metric table
-- we only do this if there are not currently any compressed
-- chunks, since otherwise we'd have to decompress them all first.

DO $do$
BEGIN
  IF NOT EXISTS (
      SELECT * FROM timescaledb_information.compressed_chunk_stats
      WHERE hypertable_name::varchar = 'metric'
      AND compression_status = 'Compressed'
  ) THEN
      PERFORM remove_compress_chunks_policy('metric');
      ALTER TABLE metric SET (
          timescaledb.compress,
          timescaledb.compress_segmentby = 'entity_oid',
          timescaledb.compress_orderby = 'entity_hash, type'
      );
      PERFORM add_compress_chunks_policy('metric', INTERVAL '2 days');
  ELSE
      RAISE NOTICE 'Compression configuration not updated because there are compressed chunks';
  END IF;
END $do$;

