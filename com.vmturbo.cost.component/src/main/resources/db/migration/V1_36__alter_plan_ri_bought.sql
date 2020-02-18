-- Add plan_id to primary key for plan_reserved_instance_bought table

ALTER TABLE plan_reserved_instance_bought DROP PRIMARY KEY;

ALTER TABLE plan_reserved_instance_bought ADD PRIMARY KEY (id, plan_id);

