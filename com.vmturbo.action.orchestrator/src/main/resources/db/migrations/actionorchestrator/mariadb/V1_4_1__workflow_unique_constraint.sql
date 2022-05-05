-- ensure that the workflow row has a unique ID column
ALTER TABLE workflow ADD UNIQUE (id);
