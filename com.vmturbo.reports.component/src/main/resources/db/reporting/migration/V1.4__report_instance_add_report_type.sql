-- Adds report type to the report instance records.
alter table report_instance add report_type SMALLINT NULL;
update report_instance set report_type = 1;
alter table report_instance modify report_type SMALLINT NOT NULL;
