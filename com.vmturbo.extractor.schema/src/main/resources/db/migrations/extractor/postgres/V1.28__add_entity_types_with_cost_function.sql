-- This function can be used in dashboards that need to know which entity types may have associated
-- costs. This list should conform with the `ENITTY_TYPES_WITH_COST` constant in the
-- `CostCalculator` class in cost component. A unit test in `extractor` module ensures this.
DROP FUNCTION IF EXISTS entity_types_with_cost;
CREATE FUNCTION entity_types_with_cost() RETURNS TABLE(type entity_type) AS $$
BEGIN
  RETURN QUERY SELECT * FROM unnest(ARRAY[
    'DATABASE',
    'DATABASE_SERVER',
    'VIRTUAL_MACHINE',
    'VIRTUAL_VOLUME']::entity_type[]);
END; $$ LANGUAGE plpgsql;
