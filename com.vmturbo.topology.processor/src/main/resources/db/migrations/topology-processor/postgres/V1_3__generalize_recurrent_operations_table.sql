DROP TABLE IF EXISTS recurrent_tasks;

CREATE TABLE recurrent_tasks (
  execution_time TIMESTAMP NOT NULL,
  operation_name TEXT COLLATE ci,
  successful BOOLEAN NOT NULL DEFAULT false,
  affected_rows  INT NOT NULL DEFAULT 0,
  summary TEXT COLLATE ci,
  errors TEXT COLLATE ci,
PRIMARY KEY(execution_time, operation_name)
);