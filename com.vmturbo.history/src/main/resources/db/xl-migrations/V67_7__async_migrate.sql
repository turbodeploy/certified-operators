
DROP PROCEDURE IF EXISTS aggregate;

DELIMITER //

CREATE PROCEDURE aggregate(IN statspref CHAR(10))
  BEGIN
    DECLARE v_stats_table varchar(32);
    DECLARE v_snapshot_time datetime;
    DECLARE v_uuid varchar(80);
    DECLARE v_producer_uuid varchar(80);
    DECLARE v_property_type varchar(36);
    DECLARE v_property_subtype varchar(36);
    DECLARE v_capacity decimal(15,3);
    DECLARE v_avg_value decimal(15,3);
    DECLARE v_min_value decimal(15,3);
    DECLARE v_max_value decimal(15,3);
    DECLARE v_relation integer;
    DECLARE v_commodity_key varchar(80);
    DECLARE v_aggregated boolean;
    DECLARE done int default false;
    DECLARE cur1 CURSOR for select * from tmp_stats_vw;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done=TRUE;
    -- dynamic query to prepare view of rows not aggregated
    set v_stats_table=concat(statspref,'_stats_latest');
    DROP VIEW IF EXISTS tmp_stats_vw;
    SET @query = CONCAT('CREATE VIEW tmp_stats_vw as select * from ',statspref,'_stats_latest where aggregated=false');

    PREPARE stmt from @query;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;


    -- open cursor
    open cur1;

    read_loop: LOOP
      fetch cur1 into v_snapshot_time,v_uuid,v_producer_uuid,v_property_type,v_property_subtype,v_capacity,v_avg_value,v_min_value,v_max_value,v_relation,v_commodity_key,v_aggregated;
      if done THEN
        LEAVE read_loop;
      end if;


      -- HOURLY AGGREGATE
      -- Set stats table to process ie.  vm_stats_by_hour

      SET @stats_table = CONCAT(statspref,'_stats_by_hour');

      SET @checkexists_sql = CONCAT('select @checkexists := 1 from ',@stats_table,' where snapshot_time<=>?
                               and uuid<=>?
                               and producer_uuid<=>?
                               and property_type<=>?
                               and property_subtype<=>?
                               and relation<=>?
                               and commodity_key<=>?  limit 1 ');


      SET @checkexists=0;
      --  Build prepared statement to check if there is an existing row for the entity in for xx_stats_by_hour table, and set statement parameters.

      PREPARE stmt from @checkexists_sql;
      -- dataformat converts to string.  Converting back to datetime
      SET @p1=str_to_date(date_format(v_snapshot_time,"%Y-%m-%d %H:00:00"),'%Y-%m-%d %H:00:00');
      SET @p2=v_uuid;
      SET @p3=v_producer_uuid;
      SET @p4=v_property_type;
      SET @p5=v_property_subtype;
      SET @p6=v_relation;
      SET @p7=v_commodity_key;

      EXECUTE stmt USING @p1,@p2,@p3,@p4,@p5,@p6,@p7;
      DEALLOCATE PREPARE stmt;


      -- Build update sql statement
      set @update_hourly_sql=CONCAT('update ',@stats_table,'  set avg_value=if(? is null,avg_value,((avg_value*samples)+?)/(samples+1)),
                                                           samples=if(? is null, samples, samples+1),
                                                           capacity=?,
                                                           min_value=if(min_value < ?,min_value,?),
                                                           max_value=if(max_value > ?,max_value,?)
                                  where snapshot_time<=>?
                                  and uuid<=>?
                                  and producer_uuid<=>?
                                  and property_type<=>?
                                  and property_subtype<=>?
                                  and relation<=>?
                                  and commodity_key<=>?');


      -- Build insert sql statement
      set @insert_hourly_sql=CONCAT('insert into ',@stats_table,
                                    ' (snapshot_time,uuid,producer_uuid,property_type,property_subtype,capacity,relation,commodity_key,min_value,max_value,avg_value,samples)
                                    values (?,?,?,?,?,?,?,?,?,?,?,?)');



      -- Check if there is an existing entry in the xx_stats_hourly_table.   If exists then update and recalculate min,max,avg, otherwise insert new row.
      IF @checkexists=1  THEN

        PREPARE stmt FROM @update_hourly_sql;
        SET @p1=v_avg_value;
        SET @p3=v_capacity;
        SET @p4=v_min_value;
        SET @p5=v_max_value;
        SET @p6=str_to_date(date_format(v_snapshot_time,"%Y-%m-%d %H:00:00"),'%Y-%m-%d %H:00:00');
        SET @p7=v_uuid;
        SET @p8=v_producer_uuid;
        SET @p9=v_property_type;
        SET @p10=v_property_subtype;
        SET @p11=v_relation;
        SET @p12=v_commodity_key;

        EXECUTE stmt USING @p1,@p1,@p1,@p3,@p4,@p4,@p5,@p5,@p6,@p7,@p8,@p9,@p10,@p11,@p12;
        DEALLOCATE PREPARE stmt;



      ELSE

        PREPARE stmt from @insert_hourly_sql;
        SET @p1=str_to_date(date_format(v_snapshot_time,"%Y-%m-%d %H:00:00"),'%Y-%m-%d %H:00:00');
        SET @p2=v_uuid;
        SET @p3=v_producer_uuid;
        SET @p4=v_property_type;
        SET @p5=v_property_subtype;
        SET @p6=v_capacity;
        SET @p7=v_relation;
        SET @p8=v_commodity_key;
        SET @p9=v_min_value;
        SET @p10=v_max_value;
        SET @p11=v_avg_value;
        SET @p12=if(v_avg_value is null,0,1);

        EXECUTE stmt USING @p1,@p2,@p3,@p4,@p5,@p6,@p7,@p8,@p9,@p10,@p11,@p12;
        DEALLOCATE PREPARE stmt;


      END IF;



      -- DAILY AGGREGATE
      -- Set stats table to process ie.  vm_stats_by_day

      SET @stats_table = CONCAT(statspref,'_stats_by_day');

      SET @checkexists_sql = CONCAT('select @checkexists := 1 from ',@stats_table,' where snapshot_time<=>?
                               and uuid<=>?
                               and producer_uuid<=>?
                               and property_type<=>?
                               and property_subtype<=>?
                               and relation<=>?
                               and commodity_key<=>?  limit 1 ');



      SET @checkexists=0;

      --  Build prepared statement to check if there is an existing row for the entity in for xx_stats_by_day table, and set statement parameters.

      PREPARE stmt from @checkexists_sql;

      /* dataformat converts to string.  Converting back to datetime */

      SET @p1=str_to_date(date_format(v_snapshot_time,"%Y-%m-%d 00:00:00"),'%Y-%m-%d %H:00:00');
      SET @p2=v_uuid;
      SET @p3=v_producer_uuid;
      SET @p4=v_property_type;
      SET @p5=v_property_subtype;
      SET @p6=v_relation;
      SET @p7=v_commodity_key;

      EXECUTE stmt USING @p1,@p2,@p3,@p4,@p5,@p6,@p7;
      DEALLOCATE PREPARE stmt;


      -- Build update sql statement
      set @update_daily_sql=CONCAT('update ',@stats_table,'  set avg_value=if(? is null,avg_value,((avg_value*samples)+?)/(samples+1)),
                                                           samples=if(? is null, samples, samples+1),
                                                           capacity=?,
                                                           min_value=if(min_value < ?,min_value,?),
                                                           max_value=if(max_value > ?,max_value,?)
                                  where snapshot_time<=>?
                                  and uuid<=>?
                                  and producer_uuid<=>?
                                  and property_type<=>?
                                  and property_subtype<=>?
                                  and relation<=>?
                                  and commodity_key<=>?');


      -- Build insert sql statement
      set @insert_daily_sql=CONCAT('insert into ',@stats_table,
                                   ' (snapshot_time,uuid,producer_uuid,property_type,property_subtype,capacity,relation,commodity_key,min_value,max_value,avg_value,samples)
                                   values (?,?,?,?,?,?,?,?,?,?,?,?)');



      -- Check if there is an existing entry in the xx_stats_daily_table.   If exists then update and recalculate min,max,avg, otherwise insert new row
      IF @checkexists=1  THEN

        PREPARE stmt FROM @update_daily_sql;
        SET @p1=v_avg_value;
        SET @p3=v_capacity;
        SET @p4=v_min_value;
        SET @p5=v_max_value;
        SET @p6=str_to_date(date_format(v_snapshot_time,"%Y-%m-%d 00:00:00"),'%Y-%m-%d 00:00:00');
        SET @p7=v_uuid;
        SET @p8=v_producer_uuid;
        SET @p9=v_property_type;
        SET @p10=v_property_subtype;
        SET @p11=v_relation;
        SET @p12=v_commodity_key;

        EXECUTE stmt USING @p1,@p1,@p1,@p3,@p4,@p4,@p5,@p5,@p6,@p7,@p8,@p9,@p10,@p11,@p12;
        DEALLOCATE PREPARE stmt;



      ELSE

        PREPARE stmt from @insert_daily_sql;
        SET @p1=str_to_date(date_format(v_snapshot_time,"%Y-%m-%d 00:00:00"),'%Y-%m-%d 00:00:00');
        SET @p2=v_uuid;
        SET @p3=v_producer_uuid;
        SET @p4=v_property_type;
        SET @p5=v_property_subtype;
        SET @p6=v_capacity;
        SET @p7=v_relation;
        SET @p8=v_commodity_key;
        SET @p9=v_min_value;
        SET @p10=v_max_value;
        SET @p11=v_avg_value;
        SET @p12=if(v_avg_value is null,0,1);

        EXECUTE stmt USING @p1,@p2,@p3,@p4,@p5,@p6,@p7,@p8,@p9,@p10,@p11,@p12;
        DEALLOCATE PREPARE stmt;


      END IF;




      -- MONTHLY AGGREGATE
      -- Set stats table to process ie.  vm_stats_by_month

      SET @stats_table = CONCAT(statspref,'_stats_by_month');

      SET @checkexists_sql = CONCAT('select @checkexists := 1 from ',@stats_table,' where snapshot_time<=>?
                               and uuid<=>?
                               and producer_uuid<=>?
                               and property_type<=>?
                               and property_subtype<=>?
                               and relation<=>?
                               and commodity_key<=>?  limit 1 ');



      SET @checkexists=0;
      PREPARE stmt from @checkexists_sql;
      /* dataformat converts to string.  Converting back to datetime */
      SET @p1=str_to_date(date_format(last_day(v_snapshot_time),"%Y-%m-%d 00:00:00"),'%Y-%m-%d %H:00:00');
      SET @p2=v_uuid;
      SET @p3=v_producer_uuid;
      SET @p4=v_property_type;
      SET @p5=v_property_subtype;
      SET @p6=v_relation;
      SET @p7=v_commodity_key;

      EXECUTE stmt USING @p1,@p2,@p3,@p4,@p5,@p6,@p7;
      DEALLOCATE PREPARE stmt;
      -- Build update sql statement
      set @update_monthly_sql=CONCAT('update ',@stats_table,'  set avg_value=if(? is null,avg_value,((avg_value*samples)+?)/(samples+1)),
                                                           samples=if(? is null, samples, samples+1),
                                                           capacity=?,
                                                           min_value=if(min_value < ?,min_value,?),
                                                           max_value=if(max_value > ?,max_value,?)
                                  where snapshot_time<=>?
                                  and uuid<=>?
                                  and producer_uuid<=>?
                                  and property_type<=>?
                                  and property_subtype<=>?
                                  and relation<=>?
                                  and commodity_key<=>?');


      -- Build insert sql statemetn
      set @insert_monthly_sql=CONCAT('insert into ',@stats_table,
                                     ' (snapshot_time,uuid,producer_uuid,property_type,property_subtype,capacity,relation,commodity_key,min_value,max_value,avg_value,samples)
                                     values (?,?,?,?,?,?,?,?,?,?,?,?)');



      -- Check if there is an existing entry in the xx_stats_monthly.   If exists then update and recalculate min,max,avg, otherwise insert new row
      IF @checkexists=1  THEN

        PREPARE stmt FROM @update_monthly_sql;
        SET @p1=v_avg_value;
        SET @p3=v_capacity;
        SET @p4=v_min_value;
        SET @p5=v_max_value;
        SET @p6=str_to_date(date_format(last_day(v_snapshot_time),"%Y-%m-%d 00:00:00"),'%Y-%m-%d %H:00:00');
        SET @p7=v_uuid;
        SET @p8=v_producer_uuid;
        SET @p9=v_property_type;
        SET @p10=v_property_subtype;
        SET @p11=v_relation;
        SET @p12=v_commodity_key;

        EXECUTE stmt USING @p1,@p1,@p1,@p3,@p4,@p4,@p5,@p5,@p6,@p7,@p8,@p9,@p10,@p11,@p12;
        DEALLOCATE PREPARE stmt;



      ELSE

        PREPARE stmt from @insert_monthly_sql;
        SET @p1=str_to_date(date_format(last_day(v_snapshot_time),"%Y-%m-%d 00:00:00"),'%Y-%m-%d %H:00:00');
        SET @p2=v_uuid;
        SET @p3=v_producer_uuid;
        SET @p4=v_property_type;
        SET @p5=v_property_subtype;
        SET @p6=v_capacity;
        SET @p7=v_relation;
        SET @p8=v_commodity_key;
        SET @p9=v_min_value;
        SET @p10=v_max_value;
        SET @p11=v_avg_value;
        SET @p12=if(v_avg_value is null,0,1);

        EXECUTE stmt USING @p1,@p2,@p3,@p4,@p5,@p6,@p7,@p8,@p9,@p10,@p11,@p12;
        DEALLOCATE PREPARE stmt;


      END IF;






      /* Mark _latest row as aggregated */
      set @latest=concat(statspref,'_stats_latest');
      set @latest_sql=CONCAT('update ',@latest,' set aggregated=true where snapshot_time<=>? and uuid<=>?
        and producer_uuid<=>? and property_type<=>? and property_subtype<=>?  and relation<=>? and commodity_key<=>?');
      PREPARE stmt from @latest_sql;
      SET @p1=v_snapshot_time;
      SET @p2=v_uuid;
      SET @p3=v_producer_uuid;
      SET @p4=v_property_type;
      SET @p5=v_property_subtype;
      SET @p6=v_relation;
      SET @p7=v_commodity_key;

      EXECUTE stmt USING @p1,@p2,@p3,@p4,@p5,@p6,@p7;
      DEALLOCATE PREPARE stmt;

      -- loop until all rows aggregated
    END LOOP;
    close cur1;
    -- delete temporary view
    DROP VIEW tmp_stats_vw;
  END;
//
DELIMITER ;



/* Create aggregate_stats_event MySQL event                      */
/* Triggers every 5 minutes                                      */
/* Calls aggregate() procedure for each entity stats table group */

DROP EVENT IF EXISTS aggregate_stats_event;

DELIMITER //
CREATE
EVENT aggregate_stats_event
  ON SCHEDULE EVERY 5 MINUTE
DO BEGIN
  call aggregate('app');
  call aggregate('ch');
  call aggregate('cnt');
  call aggregate('dpod');
  call aggregate('ds');
  call aggregate('iom');
  call aggregate('pm');
  call aggregate('sc');
  call aggregate('sw');
  call aggregate('vdc');
  call aggregate('vm');
  call aggregate('vpod');
END //

DELIMITER ;

UPDATE version_info SET version=67.7 WHERE id=1;