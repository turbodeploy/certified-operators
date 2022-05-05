-- In this table we persist the information for historical utilization, i.e. the values for
-- used and peak for each commodity bought and sold in the previous cycle.
CREATE TABLE historical_utilization (

-- Only one record will be persisted, the id is dummy.
  id            BIGINT        NOT NULL,

-- All the info are persisted as 1 binary object.
  info          LONGBLOB          NOT NULL,

-- The dummy id.
  PRIMARY KEY (id)
);
