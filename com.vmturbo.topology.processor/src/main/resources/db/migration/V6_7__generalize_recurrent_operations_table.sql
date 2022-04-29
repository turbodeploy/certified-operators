DROP TABLE IF EXISTS recurrent_tasks;

CREATE TABLE recurrent_tasks (
  execution_time TIMESTAMP NOT NULL,
  operation_name VARCHAR(255),
  successful BOOLEAN NOT NULL DEFAULT 0,
  affected_rows  INT NOT NULL DEFAULT 0,
  summary VARCHAR(255),
  errors VARCHAR(255),
PRIMARY KEY(execution_time, operation_name)
);