
/*

  This script creates function for vmtdb database.

*/
use vmtdb ;

/*
    Various utility functions for reports
*/

delimiter //

DROP FUNCTION IF EXISTS start_of_day //
CREATE FUNCTION start_of_day(ref_date date) RETURNS date DETERMINISTIC
BEGIN
    return date(date_format(ref_date, convert('%Y-%m-%d 00:00:00' using utf8))) ;
END //


DROP FUNCTION IF EXISTS end_of_day //
CREATE FUNCTION end_of_day(ref_date date) RETURNS date DETERMINISTIC
BEGIN
    return date(date_add(date_sub(date_format(ref_date, convert('%Y-%m-%d %H:00:00' using utf8)), interval 1 second), interval 1 day)) ;
END //




DROP FUNCTION IF EXISTS start_of_hour //
CREATE FUNCTION start_of_hour(ref_date timestamp) RETURNS timestamp DETERMINISTIC
BEGIN
    return timestamp(date_format(ref_date, convert('%Y-%m-%d %H:00:00' using utf8))) ;
END //


DROP FUNCTION IF EXISTS end_of_hour //
CREATE FUNCTION end_of_hour(ref_date timestamp) RETURNS timestamp DETERMINISTIC
BEGIN
    return timestamp(date_add(date_sub(date_format(ref_date, convert('%Y-%m-%d %H:00:00' using utf8)), interval 1 second), interval 1 hour)) ;
END //

delimiter ;





/*
 * FROM EACH DAY, move to genViews/genFunctions
 */
delimiter //

DROP FUNCTION IF EXISTS ftn_vm_count_for_month //
CREATE FUNCTION ftn_vm_count_for_month (month_day_1 date) RETURNS int DETERMINISTIC
BEGIN

    set @ms_day_1 := start_of_month(month_day_1) ;
    set @ms_day_n := end_of_month(month_day_1) ;

    set @count := (select count(uuid) as n_vms
                    from (select distinct uuid from vm_stats_by_day
                            where
                            property_type = 'priceIndex'
                            and snapshot_time between @ms_day_1 and @ms_day_n
                         ) as uuids
                  ) ;

    return @count ;

END //

DROP FUNCTION IF EXISTS ftn_pm_count_for_month //
CREATE FUNCTION ftn_pm_count_for_month (month_day_1 date) RETURNS int DETERMINISTIC
BEGIN

    set @ms_day_1 := start_of_month(month_day_1) ;
    set @ms_day_n := end_of_month(month_day_1) ;

    set @count := (select count(uuid) as n_pms
                    from (select distinct uuid from pm_stats_by_day
                            where
                            property_type = 'priceIndex'
                            and snapshot_time between @ms_day_1 and @ms_day_n
                         ) as uuids
                  ) ;

    return @count ;

END //


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



/*
    Date/Time utility functions for dates, months, milliseconds
*/


delimiter //

DROP FUNCTION IF EXISTS ms_from_date //
CREATE FUNCTION ms_from_date(the_date date) RETURNS bigint DETERMINISTIC
BEGIN
    return unix_timestamp(the_date)*1000 ;
END //


DROP FUNCTION IF EXISTS ms_from_datetime //
CREATE FUNCTION ms_from_datetime(the_datetime datetime) RETURNS bigint DETERMINISTIC
BEGIN
    return unix_timestamp(the_datetime)*1000 ;
END //


DROP FUNCTION IF EXISTS date_from_ms //
CREATE FUNCTION date_from_ms(ms_time bigint) RETURNS date DETERMINISTIC
BEGIN
    return date(from_unixtime(ms_time/1000)) ;
END //


DROP FUNCTION IF EXISTS datetime_from_ms //
CREATE FUNCTION datetime_from_ms(ms_time bigint) RETURNS datetime DETERMINISTIC
BEGIN
    return from_unixtime(ms_time/1000) ;
END //


DROP FUNCTION IF EXISTS days_ago //
CREATE FUNCTION days_ago(ndays int) RETURNS date DETERMINISTIC
BEGIN
    return date_sub(date(now()), interval ndays day) ;
END //


DROP FUNCTION IF EXISTS days_from_now //
CREATE FUNCTION days_from_now(ndays int) RETURNS date DETERMINISTIC
BEGIN
    return date_add(date(now()), interval ndays day) ;
END //


DROP FUNCTION IF EXISTS start_of_month //
CREATE FUNCTION start_of_month(ref_date date) RETURNS date DETERMINISTIC
BEGIN
    return date_sub(ref_date, interval day(ref_date)-1 day) ;
END //


DROP FUNCTION IF EXISTS end_of_month //
CREATE FUNCTION end_of_month(ref_date date) RETURNS date DETERMINISTIC
BEGIN
    return date_sub(date_add(start_of_month(ref_date), interval 1 month), interval 1 day) ;
END //


DROP FUNCTION IF EXISTS start_of_month_ms //
CREATE FUNCTION start_of_month_ms(ref_date date) RETURNS bigint DETERMINISTIC
BEGIN
    return ms_from_date(start_of_month(ref_date)) ;
END //


DROP FUNCTION IF EXISTS end_of_month_ms //
CREATE FUNCTION end_of_month_ms(ref_date date) RETURNS bigint DETERMINISTIC
BEGIN
    return ms_from_date(date_add(start_of_month(ref_date), interval 1 month))-1 ;
END //


delimiter ;