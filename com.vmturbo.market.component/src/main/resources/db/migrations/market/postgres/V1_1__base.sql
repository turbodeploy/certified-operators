-- In this table we persist the information for economy cache used for reservation.
CREATE TABLE IF NOT EXISTS economy_cache (

-- The ID assigned to a chunked sequence of economy cache byte stream.
-- Positive means historical economy, negative means real time economy.
  id           INT        NOT NULL,

-- All the data are persisted as 1 binary object. BYTEA size is up to 1GB.
  economy      BYTEA      NOT NULL,

  PRIMARY KEY (id)
);