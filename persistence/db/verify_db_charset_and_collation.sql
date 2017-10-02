select 'Start verifying db charset and collation...' as progress;
ALTER DATABASE vmtdb DEFAULT CHARACTER SET = UTF8 DEFAULT COLLATE = utf8_unicode_ci;

source functions.sql;
source views.sql;
source stored_procedures.sql;

select 'Done verifying charset and collation!' as progress;