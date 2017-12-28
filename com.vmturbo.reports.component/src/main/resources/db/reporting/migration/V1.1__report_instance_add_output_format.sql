-- Adds report format to the report instance records.
alter table report_instance add output_format VARCHAR(10) NULL;
update report_instance set output_format='PDF';
alter table report_instance modify output_format VARCHAR(10) NOT NULL;
