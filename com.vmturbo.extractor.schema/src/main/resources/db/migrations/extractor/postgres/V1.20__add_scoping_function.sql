-- define a table function to apply scope constraints to a metric table query
-- this function handles "all" cases where scope is applied by joining with entity table rather
-- than scope table. It leads to much simpler dashboards than the previous approach that utilized
-- hidden variables to provide fragments of SQL that would differ between all and not-all queries

-- In a metric table join, the results should be joined as follows to ensure that the correct
-- metric records are scanned:
--    FROM metric m, scope_to(...) s
--    WHERE m.entity_oid = s.oid AND m.time BETWEEN s.from_time AND s.to_time
--    ... other conditions like constraining m.type and limiting m.time per overall time range
DROP FUNCTION IF EXISTS scope_to;
CREATE FUNCTION scope_to(
  -- beginning of time range to scan
  t1 timestamptz,
  -- end of time range to scan
  t2 timestamptz,
  -- enitity type of desired metric records
  entity_type entity_type,
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
  seed bigint
) AS $$
BEGIN
  IF all_p THEN
      RETURN QUERY
        -- all query - include all entities with lifetimes that intersect with the time range
        SELECT entity.oid, first_seen, last_seen, NULL::bigint as seed_oid
        FROM entity
        WHERE type=entity_type
          AND (t1, t2) OVERLAPS (first_seen, last_seen);
    ELSE
      RETURN QUERY
        -- not-all query - use scope table to scope to given seeds
        SELECT DISTINCT scoped_oid, start, finish, seed_oid
        FROM scope
        WHERE scoped_type=entity_type
          AND seed_oid IN (SELECT * FROM unnest(seeds))
          AND (start, finish) OVERLAPS (t1, t2);
    END IF;
    RETURN;
  END;
$$ LANGUAGE plpgsql;

