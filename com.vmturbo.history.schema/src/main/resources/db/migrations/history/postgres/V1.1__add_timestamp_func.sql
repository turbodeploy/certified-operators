
DROP FUNCTION IF EXISTS start_of_hour;
CREATE FUNCTION start_of_hour(ref_date timestamp)
  RETURNS timestamp AS
$$
BEGIN
   RETURN DATE_TRUNC('hour', ref_date);
END
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS start_of_day;
CREATE FUNCTION start_of_day(ref_date timestamp)
  RETURNS timestamp AS
$$
BEGIN
   RETURN DATE_TRUNC('day', ref_date);
END
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS start_of_month;
CREATE FUNCTION start_of_month(ref_date timestamp)
  RETURNS timestamp AS
$$
BEGIN
   RETURN DATE_TRUNC('month', ref_date);
END
$$ LANGUAGE plpgsql;
