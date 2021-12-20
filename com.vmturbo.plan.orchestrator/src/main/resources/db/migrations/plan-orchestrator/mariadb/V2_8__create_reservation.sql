-- Reservation table contains all user created reservations.
CREATE TABLE reservation (
  -- id of reservation, it should be primary key.
  id                                 BIGINT                      NOT NULL,
  -- name of reservation.
  name                               VARCHAR(255)                NOT NULL,
  -- start time of reservation.
  start_time                         TIMESTAMP                   NOT NULL,
  -- expire time of reservation.
  expire_time                        TIMESTAMP                   NOT NULL,
  -- status of reservation.
  status                             ENUM('reserved', 'future')  NOT NULL,
  -- contains all reservation instances by different templates.
  reservation_template_collection    BLOB                        NOT NULL,
  -- contains all specified constraints when creating reservation.
  constraint_info_collection         BLOB                        DEFAULT NULL,

  PRIMARY KEY (id)
);

CREATE INDEX status_index on reservation(status);