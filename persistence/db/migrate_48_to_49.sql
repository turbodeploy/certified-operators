use vmtdb ;

start transaction ;

delimiter //
DROP FUNCTION IF EXISTS cluster_nm1_factor //
CREATE FUNCTION cluster_nm1_factor(arg_group_name varchar(255)) RETURNS float DETERMINISTIC
BEGIN
set @n_hosts := (select count(*) from pm_group_members where internal_name = arg_group_name COLLATE utf8_unicode_ci) ;
IF @n_hosts > 0 THEN
set @factor := (@n_hosts-1.0) / @n_hosts ;
ELSE
set @factor := 0 ;
END IF ;
return @factor ;
END //

DROP FUNCTION IF EXISTS cluster_nm2_factor //
CREATE FUNCTION cluster_nm2_factor(arg_group_name varchar(255)) RETURNS float DETERMINISTIC
BEGIN
set @n_hosts := (select count(*) from pm_group_members where internal_name = arg_group_name COLLATE utf8_unicode_ci) ;
IF @n_hosts > 1 THEN
  set @factor := (@n_hosts-2.0) / @n_hosts ;
ELSE
  set @factor := 0 ;
END IF ;
return @factor ;
END //
delimiter ;

delete from version_info where id=1;
insert into version_info values (1,49) ;

commit ;
