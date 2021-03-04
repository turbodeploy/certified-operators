-- function that perform health checks on the data schema, to make sure expected invariants
-- hold. The stored procedures are the same as the ones defined in V.21, with the exception of
-- hc_scope_oids_in_entity that now has an additional parameter to support the recovery mode

-- there's a single driver function and a number of procs for individual checks. Currently, the driver
-- just performs all the checks and returns results of individual checks as a result set


DROP function IF exists run_health_checks;
CREATE FUNCTION run_health_checks(recover_mode boolean)
RETURNS TABLE(check_name text, passed boolean, messages text[])
AS $$
  BEGIN
    CALL hc_scope_no_overlaps(passed, messages);
    check_name := 'No Overlapping Scopes';
    RETURN NEXT;
    CALL hc_scope_group_symmetry(passed, messages);
    check_name := 'Group Scopes Are Symmetric';
    RETURN NEXT;
    CALL hc_scope_is_reflexive(passed, messages);
    check_name := 'Scopes Are Reflexive';
    RETURN NEXT;
    CALL hc_scope_oids_in_entity(passed, messages, recover_mode);
    check_Name := 'Scope Entities Are In Entity Table';
    RETURN NEXT;
    RETURN;
  END;
$$ LANGUAGE plpgsql;

-- check that there are no seed/scoped oid pairs with records that have overlapping start/finish
-- time ranges
DROP PROCEDURE IF EXISTS hc_scope_no_overlaps;
CREATE PROCEDURE hc_scope_no_overlaps(INOUT passed boolean, INOUT messages text[])
AS $$
  DECLARE
    violations text[] = ARRAY[]::text[];
  BEGIN
    SELECT array(
      SELECT DISTINCT 'seed: ' || s1.seed_oid || '; scoped: ' || s1.scoped_oid
      FROM scope s1, scope s2
      WHERE s1.seed_oid = s2.seed_oid
        AND s1.scoped_oid = s2.scoped_oid
        -- make sure we're not comparing a record to itself
        AND s1.start <> s2.start
        -- DeMorgan's law negation of the more intuitive condition for lack of overlapping
        -- scopes, which would be s1.finish < s2.start || s2.finish < s1.start
        AND s1.finish >= s2.start AND s2.finish >= s1.start
    ) INTO violations;
    passed := array_length(violations, 1) = 0 OR array_length(violations, 1) IS NULL;
    messages := array(select 'violation: ' || x from unnest(violations) as x)::text[];
  END;
$$ LANGUAGE plpgsql;


-- check that scope table is symmetric with respect to groups
DROP PROCEDURE IF EXISTS hc_scope_group_symmetry;
CREATE PROCEDURE hc_scope_group_symmetry(INOUT passed boolean, INOUT messages text[])
AS $$
  DECLARE
    violations text[] = ARRAY[]::text[];
  BEGIN
    SELECT array(
      WITH groups AS (
        SELECT oid FROM entity
        WHERE type IN ('GROUP', 'COMPUTE_CLUSTER', 'STORAGE_CLUSTER', 'RESOURCE_GROUP',
          'K8S_CLUSTER', 'BILLING_FAMILY')
      ),
      group_seed AS (
        select seed_oid AS g, scoped_oid AS o, start, finish from scope WHERE seed_oid IN (SELECT * FROM groups)
      ),
      group_scoped AS (
        select scoped_oid AS g, seed_oid AS o, start, finish from scope where scoped_oid IN (SELECT * FROM groups)
      ),
      diff1 AS (SELECT * FROM group_seed EXCEPT SELECT * FROM group_scoped),
      diff2 AS (SELECT * FROM group_scoped EXCEPT SELECT * FROM group_seed)
      SELECT g from diff1
      UNION SELECT g from diff2
    ) INTO violations;
    passed := array_length(violations, 1) = 0 OR array_length(violations, 1) IS NULL;
    messages := array(select 'violation: ' || x from unnest(violations) as x)::text[];
  END;
$$ LANGUAGE plpgsql;

-- check that scope is reflexive - each entity is in its own scope
-- we check this for every time in which the entity appears at all in scope, by making sure that
-- each the start-finish range in a record involving the entity is wholly contained in the range
-- of one of the entity's reflexive ranges
DROP PROCEDURE IF EXISTS hc_scope_is_reflexive;
CREATE PROCEDURE hc_scope_is_reflexive(INOUT passed boolean, INOUT messages text[])
AS $$
  DECLARE
    violations text[] = ARRAY[]::text[];
    o bigint;
    s timestamptz;
    f timestamptz;
  BEGIN
    FOR o, s, f IN
      SELECT DISTINCT seed_oid AS o, start AS s, finish AS f FROM scope
      UNION SELECT DISTINCT scoped_oid AS o, start AS s, finish AS f FROM SCOPE
    LOOP
      PERFORM * FROM scope
        WHERE seed_oid = o AND scoped_oid = o AND start <= s AND finish >= f;
      IF NOT FOUND THEN
        violations := violations || o::text;
      END IF;
    END LOOP;
    passed := array_length(violations, 1) = 0 OR array_length(violations, 1) IS NULL;
    messages := array(select distinct 'violation: ' || x from unnest(violations) as x)::text[];
  END;
$$ LANGUAGE plpgsql;


-- check that all entity oids in scope are in entity table
DROP PROCEDURE IF EXISTS hc_scope_oids_in_entity;
CREATE PROCEDURE hc_scope_oids_in_entity(INOUT passed boolean, INOUT messages text[], recover_mode boolean)
AS $$
  DECLARE
    violations bigint[] = ARRAY[]::bigint[];
  DECLARE
    bad_records_count int;
  DECLARE
    deleted_records int;
  BEGIN
    SELECT array(
      WITH oids_in_scope AS (
        SELECT DISTINCT seed_oid AS oid FROM scope
        UNION SELECT DISTINCT scoped_oid AS oid FROM scope
      ), oids_in_entity AS (
        SELECT oid FROM entity
      ), missing AS (
        SELECT distinct oid FROM oids_in_scope EXCEPT SELECT oid FROM oids_in_entity
      )
      SELECT oid::text FROM missing WHERE oid <> 0
    ) INTO violations;
    passed := array_length(violations, 1) = 0 OR array_length(violations, 1) IS NULL;
    messages := array(select 'violation: ' || x from unnest(violations) as x)::text[];
    IF recover_mode THEN
        DELETE FROM scope WHERE scoped_oid = any(violations);
        GET DIAGNOSTICS deleted_records = ROW_COUNT;
        SELECT (SELECT count(*) FROM scope WHERE scoped_oid = any(violations)) INTO bad_records_count;
	    messages := array_append(messages,'attempted repair');
    	messages := array_append(messages, 'number of Deleted Records: ' || deleted_records);
    	messages := array_append(messages, 'number of bad records remaining: ' || bad_records_count);
    	passed := bad_records_count = 0;
    END IF;
  END;
$$ LANGUAGE plpgsql;

