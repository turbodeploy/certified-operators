-- Creates a separate table for storing schedules' parameters

create table schedule_parameters (
  schedule_id BIGINT NOT NULL,
  param_name VARCHAR(80) NOT NULL,
  param_value VARCHAR(250) NOT NULL,
  PRIMARY KEY(schedule_id, param_name),
  FOREIGN KEY (schedule_id) REFERENCES schedule(id) ON DELETE CASCADE
);

insert into schedule_parameters (schedule_id, param_name, param_value)
  select id, 'show_charts', show_charts from schedule where show_charts is not null;

insert into schedule_parameters (schedule_id, param_name, param_value)
  select id, 'selected_item_uuid', scope_oid from schedule where scope_oid is not null;

alter table schedule drop column scope_oid;
alter table schedule drop column show_charts;
