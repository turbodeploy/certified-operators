-- `utilization` and `capacity` columns in `hist_utilization` table are defined as `decimal(15,3)`
-- type, which admits of numbers no longer than 999,999,999,999.999. This is causing errors at
-- at least one customer. There is no benefit to using decimal types, especially since on the Java
-- side, these are handled as `double` values (not initially on the read side, but eventually that's
-- where they land. So we'll change them both to `double` in the schema, which will provide an
-- essentially unlimited range.
ALTER TABLE hist_utilization
  ALTER COLUMN utilization TYPE double precision,
  ALTER COLUMN capacity TYPE double precision;