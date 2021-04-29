-- We're storing topology creation times in `entity_cost.created_time` column, but that column
-- is currently defined as a `timestamp` column, with no provisional for fractional seconds. THat
-- means that the topology times are being truncated upon insertion, which means that requests
-- for records with a particular topology timestamp will almost always come back empty.

-- Here we alter the column type to `timeestamp(3)` to support the millisecond granularity
-- present in topology timestamps. This will rewrite existing records, but of course the existing
-- timestamps will remain truncated. New records will correctly represent millisecond-granularity
-- timestamps.
ALTER TABLE entity_cost CHANGE created_time created_time timestamp(3) NOT NULL DEFAULT current_time(3);

-- We'll also do the same for plan_entity_costs, for consistency more than for actual need. Plan
-- costs are currently always filtered by plan id not by plan topology timestamp, and that's not
-- likely to change.
ALTER TABLE plan_entity_cost CHANGE created_time created_time timestamp(3) NOT NULL DEFAULT current_time(3);
