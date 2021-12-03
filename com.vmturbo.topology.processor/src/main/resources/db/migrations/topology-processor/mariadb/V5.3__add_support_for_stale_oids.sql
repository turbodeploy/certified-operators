ALTER TABLE assigned_identity ADD COLUMN expired BOOLEAN NOT NULL DEFAULT 0;
ALTER TABLE assigned_identity ADD COLUMN last_seen TIMESTAMP NOT NULL DEFAULT
CURRENT_TIMESTAMP;

CREATE TABLE recurrent_operations (
  execution_time TIMESTAMP NOT NULL,
  operation_name VARCHAR(255),
  summary VARCHAR(255),
PRIMARY KEY(execution_time, operation_name)
);

CREATE INDEX last_seen ON assigned_identity(last_seen);
