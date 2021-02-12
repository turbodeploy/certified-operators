-- In this table we persist the information for economy cache used for reservation.
CREATE TABLE economy_cache (

-- The ID assigned to a chunked sequence of economy cache byte stream.
-- Positive means historical economy, negative means real time economy.
  id           INT        NOT NULL,

-- All the data are persisted as 1 binary object. LONGBLOB size is up to 4Gb.
  economy      LONGBLOB          NOT NULL,

  PRIMARY KEY (id)
);