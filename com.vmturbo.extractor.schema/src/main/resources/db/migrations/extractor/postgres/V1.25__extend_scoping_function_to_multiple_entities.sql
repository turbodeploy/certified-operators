-- Very similar function as the one introduced on V1.20, with the difference that this one can
-- scope to multiple entity types and it includes them in the result

DROP FUNCTION IF exists scope_to_types;
CREATE FUNCTION scope_to_types(
  -- beginning of time range to scan
  t1 timestamptz,
  -- end of time range to scan
  t2 timestamptz,
  -- enitity type of desired metric records
  entity_type entity_type[],
  -- array of oids to constrain scope.seed_oid, in not-all queries, else ignored
  seeds bigint[],
  -- true for an all query, false for a not-all query
  all_p boolean
) RETURNS TABLE (
  -- an oid for which metric records should be scanned
  oid bigint,
  -- time range within which that oid should be scanned
  from_time timestamptz,
  to_time timestamptz,
  -- the seed that gave rise to this entry
  seed bigint,
  e_type entity_type
) AS $$
BEGIN
  IF all_p THEN
      RETURN QUERY
        -- all query - include all entities with lifetimes that intersect with the time range
        SELECT entity.oid, first_seen, last_seen, NULL::bigint as seed_oid, entity.type
        FROM entity
        WHERE type in (SELECT * FROM unnest(entity_type))
          AND (t1, t2) OVERLAPS (first_seen, last_seen);
    ELSE
      RETURN QUERY
        -- not-all query - use scope table to scope to given seeds
        SELECT DISTINCT scoped_oid, start, finish, seed_oid, scoped_type
        FROM scope
        WHERE scoped_type in (SELECT * FROM unnest(entity_type))
        AND seed_oid IN (SELECT * FROM unnest(seeds))
        AND (start, finish) OVERLAPS (t1, t2);
    END IF;
    RETURN;
  END;
$$ LANGUAGE plpgsql;
