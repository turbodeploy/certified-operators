
-- ADD new samples column to each roll-up table.  The samples column holds the number of samples used to calculate the average, and used to recalculate averages on the fly

alter table app_stats_by_hour add samples integer;
alter table app_stats_by_day add samples integer;
alter table app_stats_by_month add samples integer;

alter table app_stats_by_day modify snapshot_time datetime;
alter table app_stats_by_month modify snapshot_time datetime;

-- Create unique compound indexes for each rollup table.  Significantly improves performances of finding and updating stats

create index app_stats_by_hour_idx on app_stats_by_hour (snapshot_time,uuid,producer_uuid,property_type,property_subtype,relation);
create index app_stats_by_day_idx on app_stats_by_day (snapshot_time,uuid,producer_uuid,property_type,property_subtype,relation);
create index app_stats_by_mth_idx on app_stats_by_month (snapshot_time,uuid,producer_uuid,property_type,property_subtype,relation);

-- Create an AFTER INSERT trigger for each of the _latest tables.  The trigger fires on successful insert of new data into a particular _latest table.
-- With each new _latest row added, the hourly, daily and monthly rollup tables are updated at the same time.  Min, max and average is recalculated as required

drop trigger if exists app_stats_latest_after_insert;


DELIMITER //

CREATE TRIGGER app_stats_latest_after_insert AFTER INSERT ON app_stats_latest
FOR EACH ROW
  BEGIN


    -- With _HOURLY,_DAILY,_MONTHLY tables.  Check if there is an existing row for the rollup.  If the row exists, then update and recalculate.
    -- If the row does not exist, add a new row with the _latest data.


    -- HOURLY
    IF EXISTS (select 1 from app_stats_by_hour where snapshot_time<=>date_format(NEW.snapshot_time,"%Y-%m-%d %H:00:00")
                                                     and uuid<=>NEW.uuid
                                                     and producer_uuid<=>NEW.producer_uuid
                                                     and property_type<=>NEW.property_type
                                                     and property_subtype<=>NEW.property_subtype
                                                     and relation<=>NEW.relation
                                                     and commodity_key<=>NEW.commodity_key limit 1 ) THEN

      update app_stats_by_hour set avg_value=if(NEW.avg_value is null,avg_value,((avg_value*samples)+NEW.avg_value)/(samples+1)),
        samples=if(NEW.avg_value IS NULL,samples,samples+1),
        capacity=NEW.capacity,
        min_value=if((min_value < (NEW.min_value)),min_value,NEW.min_value),
        max_value=if((max_value > (NEW.max_value)),max_value,NEW.max_value)
      where snapshot_time<=>date_format(NEW.snapshot_time,"%Y-%m-%d %H:00:00")
            and uuid<=>NEW.uuid and producer_uuid<=>NEW.producer_uuid
            and property_type<=>NEW.property_type and property_subtype<=>NEW.property_subtype and relation<=>NEW.relation
            and commodity_key<=>NEW.commodity_key;

    ELSE

      insert into app_stats_by_hour (snapshot_time,uuid,producer_uuid,property_type,property_subtype,capacity,relation,commodity_key,min_value,max_value,avg_value,samples) values
        (date_format(NEW.snapshot_time,"%Y-%m-%d %H:00:00"),
          NEW.uuid,
          NEW.producer_uuid,
          NEW.property_type,
          NEW.property_subtype,
          NEW.capacity,
          NEW.relation,
          NEW.commodity_key,
          NEW.min_value,
          NEW.max_value,
          NEW.avg_value,
         if(NEW.avg_value IS NULL,0,1));

    END IF;


    -- DAILY
    IF EXISTS (select 1 from app_stats_by_day where snapshot_time<=>date_format(NEW.snapshot_time,"%Y-%m-%d 00:00:00")
                                                    and uuid<=>NEW.uuid
                                                    and producer_uuid<=>NEW.producer_uuid
                                                    and property_type<=>NEW.property_type
                                                    and property_subtype<=>NEW.property_subtype
                                                    and relation<=>NEW.relation
                                                    and commodity_key<=>NEW.commodity_key limit 1 ) THEN

      update app_stats_by_day set avg_value=if(NEW.avg_value is null,avg_value,((avg_value*samples)+NEW.avg_value)/(samples+1)),
        samples=if(NEW.avg_value IS NULL,samples,samples+1),
        capacity=NEW.capacity,
        min_value=if((min_value < (NEW.min_value)),min_value,NEW.min_value),
        max_value=if((max_value > (NEW.max_value)),max_value,NEW.max_value)
      where snapshot_time<=>date_format(NEW.snapshot_time,"%Y-%m-%d 00:00:00")
            and uuid<=>NEW.uuid and producer_uuid<=>NEW.producer_uuid
            and property_type<=>NEW.property_type and property_subtype<=>NEW.property_subtype and relation<=>NEW.relation
            and commodity_key<=>NEW.commodity_key;

    ELSE

      insert into app_stats_by_day (snapshot_time,uuid,producer_uuid,property_type,property_subtype,capacity,relation,commodity_key,min_value,max_value,avg_value,samples) values
        (date_format(NEW.snapshot_time,"%Y-%m-%d 00:00:00"),
          NEW.uuid,
          NEW.producer_uuid,
          NEW.property_type,
          NEW.property_subtype,
          NEW.capacity,
          NEW.relation,
          NEW.commodity_key,
          NEW.min_value,
          NEW.max_value,
          NEW.avg_value,
         if(NEW.avg_value IS NULL,0,1));

    END IF;


    -- MONTHLY
    IF EXISTS (select 1 from app_stats_by_month where snapshot_time<=>date_format(last_day(NEW.snapshot_time),"%Y-%m-%d 00:00:00")
                                                      and uuid<=>NEW.uuid
                                                      and producer_uuid<=>NEW.producer_uuid
                                                      and property_type<=>NEW.property_type
                                                      and property_subtype<=>NEW.property_subtype
                                                      and relation<=>NEW.relation
                                                      and commodity_key<=>NEW.commodity_key limit 1 ) THEN

      update app_stats_by_month set avg_value=if(NEW.avg_value is null,avg_value,((avg_value*samples)+NEW.avg_value)/(samples+1)),
        samples=if(NEW.avg_value IS NULL,samples,samples+1),
        capacity=NEW.capacity,
        min_value=if((min_value < (NEW.min_value)),min_value,NEW.min_value),
        max_value=if((max_value > (NEW.max_value)),max_value,NEW.max_value)
      where snapshot_time<=>date_format(last_day(NEW.snapshot_time),"%Y-%m-%d 00:00:00")
            and uuid<=>NEW.uuid and producer_uuid<=>NEW.producer_uuid
            and property_type<=>NEW.property_type and property_subtype<=>NEW.property_subtype and relation<=>NEW.relation
            and commodity_key<=>NEW.commodity_key;

    ELSE

      insert into app_stats_by_month (snapshot_time,uuid,producer_uuid,property_type,property_subtype,capacity,relation,commodity_key,min_value,max_value,avg_value,samples) values
        (date_format(last_day(NEW.snapshot_time),"%Y-%m-%d 00:00:00"),
          NEW.uuid,
          NEW.producer_uuid,
          NEW.property_type,
          NEW.property_subtype,
          NEW.capacity,
          NEW.relation,
          NEW.commodity_key,
          NEW.min_value,
          NEW.max_value,
          NEW.avg_value,
         if(NEW.avg_value IS NULL,0,1));

    END IF;

  END //
DELIMITER ;



-- ADD new samples column to each roll-up table.  The samples column holds the number of samples used to calculate the average, and used to recalculate averages on the fly

alter table ch_stats_by_hour add samples integer;
alter table ch_stats_by_day add samples integer;
alter table ch_stats_by_month add samples integer;

alter table ch_stats_by_day modify snapshot_time datetime;
alter table ch_stats_by_month modify snapshot_time datetime;

-- Create unique compound indexes for each rollup table.  Significantly improves performances of finding and updating stats

create index ch_stats_by_hour_idx on ch_stats_by_hour (snapshot_time,uuid,producer_uuid,property_type,property_subtype,relation);
create index ch_stats_by_day_idx on ch_stats_by_day (snapshot_time,uuid,producer_uuid,property_type,property_subtype,relation);
create index ch_stats_by_mth_idx on ch_stats_by_month (snapshot_time,uuid,producer_uuid,property_type,property_subtype,relation);

-- Create an AFTER INSERT trigger for each of the _latest tables.  The trigger fires on successful insert of new data into a particular _latest table.
-- With each new _latest row added, the hourly, daily and monthly rollup tables are updated at the same time.  Min, max and average is recalculated as required

drop trigger if exists ch_stats_latest_after_insert;


DELIMITER //

CREATE TRIGGER ch_stats_latest_after_insert AFTER INSERT ON ch_stats_latest
FOR EACH ROW
  BEGIN


    -- With _HOURLY,_DAILY,_MONTHLY tables.  Check if there is an existing row for the rollup.  If the row exists, then update and recalculate.
    -- If the row does not exist, add a new row with the _latest data.


    -- HOURLY
    IF EXISTS (select 1 from ch_stats_by_hour where snapshot_time<=>date_format(NEW.snapshot_time,"%Y-%m-%d %H:00:00")
                                                    and uuid<=>NEW.uuid
                                                    and producer_uuid<=>NEW.producer_uuid
                                                    and property_type<=>NEW.property_type
                                                    and property_subtype<=>NEW.property_subtype
                                                    and relation<=>NEW.relation
                                                    and commodity_key<=>NEW.commodity_key limit 1 ) THEN

      update ch_stats_by_hour set avg_value=if(NEW.avg_value is null,avg_value,((avg_value*samples)+NEW.avg_value)/(samples+1)),
        samples=if(NEW.avg_value IS NULL,samples,samples+1),
        capacity=NEW.capacity,
        min_value=if((min_value < (NEW.min_value)),min_value,NEW.min_value),
        max_value=if((max_value > (NEW.max_value)),max_value,NEW.max_value)
      where snapshot_time<=>date_format(NEW.snapshot_time,"%Y-%m-%d %H:00:00")
            and uuid<=>NEW.uuid and producer_uuid<=>NEW.producer_uuid
            and property_type<=>NEW.property_type and property_subtype<=>NEW.property_subtype and relation<=>NEW.relation
            and commodity_key<=>NEW.commodity_key;

    ELSE

      insert into ch_stats_by_hour (snapshot_time,uuid,producer_uuid,property_type,property_subtype,capacity,relation,commodity_key,min_value,max_value,avg_value,samples) values
        (date_format(NEW.snapshot_time,"%Y-%m-%d %H:00:00"),
          NEW.uuid,
          NEW.producer_uuid,
          NEW.property_type,
          NEW.property_subtype,
          NEW.capacity,
          NEW.relation,
          NEW.commodity_key,
          NEW.min_value,
          NEW.max_value,
          NEW.avg_value,
         if(NEW.avg_value IS NULL,0,1));

    END IF;


    -- DAILY
    IF EXISTS (select 1 from ch_stats_by_day where snapshot_time<=>date_format(NEW.snapshot_time,"%Y-%m-%d 00:00:00")
                                                   and uuid<=>NEW.uuid
                                                   and producer_uuid<=>NEW.producer_uuid
                                                   and property_type<=>NEW.property_type
                                                   and property_subtype<=>NEW.property_subtype
                                                   and relation<=>NEW.relation
                                                   and commodity_key<=>NEW.commodity_key limit 1 ) THEN

      update ch_stats_by_day set avg_value=if(NEW.avg_value is null,avg_value,((avg_value*samples)+NEW.avg_value)/(samples+1)),
        samples=if(NEW.avg_value IS NULL,samples,samples+1),
        capacity=NEW.capacity,
        min_value=if((min_value < (NEW.min_value)),min_value,NEW.min_value),
        max_value=if((max_value > (NEW.max_value)),max_value,NEW.max_value)
      where snapshot_time<=>date_format(NEW.snapshot_time,"%Y-%m-%d 00:00:00")
            and uuid<=>NEW.uuid and producer_uuid<=>NEW.producer_uuid
            and property_type<=>NEW.property_type and property_subtype<=>NEW.property_subtype and relation<=>NEW.relation
            and commodity_key<=>NEW.commodity_key;

    ELSE

      insert into ch_stats_by_day (snapshot_time,uuid,producer_uuid,property_type,property_subtype,capacity,relation,commodity_key,min_value,max_value,avg_value,samples) values
        (date_format(NEW.snapshot_time,"%Y-%m-%d 00:00:00"),
          NEW.uuid,
          NEW.producer_uuid,
          NEW.property_type,
          NEW.property_subtype,
          NEW.capacity,
          NEW.relation,
          NEW.commodity_key,
          NEW.min_value,
          NEW.max_value,
          NEW.avg_value,
         if(NEW.avg_value IS NULL,0,1));

    END IF;


    -- MONTHLY
    IF EXISTS (select 1 from ch_stats_by_month where snapshot_time<=>date_format(last_day(NEW.snapshot_time),"%Y-%m-%d 00:00:00")
                                                     and uuid<=>NEW.uuid
                                                     and producer_uuid<=>NEW.producer_uuid
                                                     and property_type<=>NEW.property_type
                                                     and property_subtype<=>NEW.property_subtype
                                                     and relation<=>NEW.relation
                                                     and commodity_key<=>NEW.commodity_key limit 1 ) THEN

      update ch_stats_by_month set avg_value=if(NEW.avg_value is null,avg_value,((avg_value*samples)+NEW.avg_value)/(samples+1)),
        samples=if(NEW.avg_value IS NULL,samples,samples+1),
        capacity=NEW.capacity,
        min_value=if((min_value < (NEW.min_value)),min_value,NEW.min_value),
        max_value=if((max_value > (NEW.max_value)),max_value,NEW.max_value)
      where snapshot_time<=>date_format(last_day(NEW.snapshot_time),"%Y-%m-%d 00:00:00")
            and uuid<=>NEW.uuid and producer_uuid<=>NEW.producer_uuid
            and property_type<=>NEW.property_type and property_subtype<=>NEW.property_subtype and relation<=>NEW.relation
            and commodity_key<=>NEW.commodity_key;

    ELSE

      insert into ch_stats_by_month (snapshot_time,uuid,producer_uuid,property_type,property_subtype,capacity,relation,commodity_key,min_value,max_value,avg_value,samples) values
        (date_format(last_day(NEW.snapshot_time),"%Y-%m-%d 00:00:00"),
          NEW.uuid,
          NEW.producer_uuid,
          NEW.property_type,
          NEW.property_subtype,
          NEW.capacity,
          NEW.relation,
          NEW.commodity_key,
          NEW.min_value,
          NEW.max_value,
          NEW.avg_value,
         if(NEW.avg_value IS NULL,0,1));

    END IF;

  END //
DELIMITER ;



-- ADD new samples column to each roll-up table.  The samples column holds the number of samples used to calculate the average, and used to recalculate averages on the fly

alter table cnt_stats_by_hour add samples integer;
alter table cnt_stats_by_day add samples integer;
alter table cnt_stats_by_month add samples integer;

alter table cnt_stats_by_day modify snapshot_time datetime;
alter table cnt_stats_by_month modify snapshot_time datetime;

-- Create unique compound indexes for each rollup table.  Significantly improves performances of finding and updating stats

create index cnt_stats_by_hour_idx on cnt_stats_by_hour (snapshot_time,uuid,producer_uuid,property_type,property_subtype,relation);
create index cnt_stats_by_day_idx on cnt_stats_by_day (snapshot_time,uuid,producer_uuid,property_type,property_subtype,relation);
create index cnt_stats_by_mth_idx on cnt_stats_by_month (snapshot_time,uuid,producer_uuid,property_type,property_subtype,relation);

-- Create an AFTER INSERT trigger for each of the _latest tables.  The trigger fires on successful insert of new data into a particular _latest table.
-- With each new _latest row added, the hourly, daily and monthly rollup tables are updated at the same time.  Min, max and average is recalculated as required

drop trigger if exists cnt_stats_latest_after_insert;


DELIMITER //

CREATE TRIGGER cnt_stats_latest_after_insert AFTER INSERT ON cnt_stats_latest
FOR EACH ROW
  BEGIN


    -- With _HOURLY,_DAILY,_MONTHLY tables.  Check if there is an existing row for the rollup.  If the row exists, then update and recalculate.
    -- If the row does not exist, add a new row with the _latest data.


    -- HOURLY
    IF EXISTS (select 1 from cnt_stats_by_hour where snapshot_time<=>date_format(NEW.snapshot_time,"%Y-%m-%d %H:00:00")
                                                     and uuid<=>NEW.uuid
                                                     and producer_uuid<=>NEW.producer_uuid
                                                     and property_type<=>NEW.property_type
                                                     and property_subtype<=>NEW.property_subtype
                                                     and relation<=>NEW.relation
                                                     and commodity_key<=>NEW.commodity_key limit 1 ) THEN

      update cnt_stats_by_hour set avg_value=if(NEW.avg_value is null,avg_value,((avg_value*samples)+NEW.avg_value)/(samples+1)),
        samples=if(NEW.avg_value IS NULL,samples,samples+1),
        capacity=NEW.capacity,
        min_value=if((min_value < (NEW.min_value)),min_value,NEW.min_value),
        max_value=if((max_value > (NEW.max_value)),max_value,NEW.max_value)
      where snapshot_time<=>date_format(NEW.snapshot_time,"%Y-%m-%d %H:00:00")
            and uuid<=>NEW.uuid and producer_uuid<=>NEW.producer_uuid
            and property_type<=>NEW.property_type and property_subtype<=>NEW.property_subtype and relation<=>NEW.relation
            and commodity_key<=>NEW.commodity_key;

    ELSE

      insert into cnt_stats_by_hour (snapshot_time,uuid,producer_uuid,property_type,property_subtype,capacity,relation,commodity_key,min_value,max_value,avg_value,samples) values
        (date_format(NEW.snapshot_time,"%Y-%m-%d %H:00:00"),
          NEW.uuid,
          NEW.producer_uuid,
          NEW.property_type,
          NEW.property_subtype,
          NEW.capacity,
          NEW.relation,
          NEW.commodity_key,
          NEW.min_value,
          NEW.max_value,
          NEW.avg_value,
         if(NEW.avg_value IS NULL,0,1));

    END IF;


    -- DAILY
    IF EXISTS (select 1 from cnt_stats_by_day where snapshot_time<=>date_format(NEW.snapshot_time,"%Y-%m-%d 00:00:00")
                                                    and uuid<=>NEW.uuid
                                                    and producer_uuid<=>NEW.producer_uuid
                                                    and property_type<=>NEW.property_type
                                                    and property_subtype<=>NEW.property_subtype
                                                    and relation<=>NEW.relation
                                                    and commodity_key<=>NEW.commodity_key limit 1 ) THEN

      update cnt_stats_by_day set avg_value=if(NEW.avg_value is null,avg_value,((avg_value*samples)+NEW.avg_value)/(samples+1)),
        samples=if(NEW.avg_value IS NULL,samples,samples+1),
        capacity=NEW.capacity,
        min_value=if((min_value < (NEW.min_value)),min_value,NEW.min_value),
        max_value=if((max_value > (NEW.max_value)),max_value,NEW.max_value)
      where snapshot_time<=>date_format(NEW.snapshot_time,"%Y-%m-%d 00:00:00")
            and uuid<=>NEW.uuid and producer_uuid<=>NEW.producer_uuid
            and property_type<=>NEW.property_type and property_subtype<=>NEW.property_subtype and relation<=>NEW.relation
            and commodity_key<=>NEW.commodity_key;

    ELSE

      insert into cnt_stats_by_day (snapshot_time,uuid,producer_uuid,property_type,property_subtype,capacity,relation,commodity_key,min_value,max_value,avg_value,samples) values
        (date_format(NEW.snapshot_time,"%Y-%m-%d 00:00:00"),
          NEW.uuid,
          NEW.producer_uuid,
          NEW.property_type,
          NEW.property_subtype,
          NEW.capacity,
          NEW.relation,
          NEW.commodity_key,
          NEW.min_value,
          NEW.max_value,
          NEW.avg_value,
         if(NEW.avg_value IS NULL,0,1));

    END IF;


    -- MONTHLY
    IF EXISTS (select 1 from cnt_stats_by_month where snapshot_time<=>date_format(last_day(NEW.snapshot_time),"%Y-%m-%d 00:00:00")
                                                      and uuid<=>NEW.uuid
                                                      and producer_uuid<=>NEW.producer_uuid
                                                      and property_type<=>NEW.property_type
                                                      and property_subtype<=>NEW.property_subtype
                                                      and relation<=>NEW.relation
                                                      and commodity_key<=>NEW.commodity_key limit 1 ) THEN

      update cnt_stats_by_month set avg_value=if(NEW.avg_value is null,avg_value,((avg_value*samples)+NEW.avg_value)/(samples+1)),
        samples=if(NEW.avg_value IS NULL,samples,samples+1),
        capacity=NEW.capacity,
        min_value=if((min_value < (NEW.min_value)),min_value,NEW.min_value),
        max_value=if((max_value > (NEW.max_value)),max_value,NEW.max_value)
      where snapshot_time<=>date_format(last_day(NEW.snapshot_time),"%Y-%m-%d 00:00:00")
            and uuid<=>NEW.uuid and producer_uuid<=>NEW.producer_uuid
            and property_type<=>NEW.property_type and property_subtype<=>NEW.property_subtype and relation<=>NEW.relation
            and commodity_key<=>NEW.commodity_key;

    ELSE

      insert into cnt_stats_by_month (snapshot_time,uuid,producer_uuid,property_type,property_subtype,capacity,relation,commodity_key,min_value,max_value,avg_value,samples) values
        (date_format(last_day(NEW.snapshot_time),"%Y-%m-%d 00:00:00"),
          NEW.uuid,
          NEW.producer_uuid,
          NEW.property_type,
          NEW.property_subtype,
          NEW.capacity,
          NEW.relation,
          NEW.commodity_key,
          NEW.min_value,
          NEW.max_value,
          NEW.avg_value,
         if(NEW.avg_value IS NULL,0,1));

    END IF;

  END //
DELIMITER ;



-- ADD new samples column to each roll-up table.  The samples column holds the number of samples used to calculate the average, and used to recalculate averages on the fly

alter table dpod_stats_by_hour add samples integer;
alter table dpod_stats_by_day add samples integer;
alter table dpod_stats_by_month add samples integer;

alter table dpod_stats_by_day modify snapshot_time datetime;
alter table dpod_stats_by_month modify snapshot_time datetime;

-- Create unique compound indexes for each rollup table.  Significantly improves performances of finding and updating stats

create index dpod_stats_by_hour_idx on dpod_stats_by_hour (snapshot_time,uuid,producer_uuid,property_type,property_subtype,relation);
create index dpod_stats_by_day_idx on dpod_stats_by_day (snapshot_time,uuid,producer_uuid,property_type,property_subtype,relation);
create index dpod_stats_by_mth_idx on dpod_stats_by_month (snapshot_time,uuid,producer_uuid,property_type,property_subtype,relation);

-- Create an AFTER INSERT trigger for each of the _latest tables.  The trigger fires on successful insert of new data into a particular _latest table.
-- With each new _latest row added, the hourly, daily and monthly rollup tables are updated at the same time.  Min, max and average is recalculated as required

drop trigger if exists dpod_stats_latest_after_insert;


DELIMITER //

CREATE TRIGGER dpod_stats_latest_after_insert AFTER INSERT ON dpod_stats_latest
FOR EACH ROW
  BEGIN


    -- With _HOURLY,_DAILY,_MONTHLY tables.  Check if there is an existing row for the rollup.  If the row exists, then update and recalculate.
    -- If the row does not exist, add a new row with the _latest data.


    -- HOURLY
    IF EXISTS (select 1 from dpod_stats_by_hour where snapshot_time<=>date_format(NEW.snapshot_time,"%Y-%m-%d %H:00:00")
                                                      and uuid<=>NEW.uuid
                                                      and producer_uuid<=>NEW.producer_uuid
                                                      and property_type<=>NEW.property_type
                                                      and property_subtype<=>NEW.property_subtype
                                                      and relation<=>NEW.relation
                                                      and commodity_key<=>NEW.commodity_key limit 1 ) THEN

      update dpod_stats_by_hour set avg_value=if(NEW.avg_value is null,avg_value,((avg_value*samples)+NEW.avg_value)/(samples+1)),
        samples=if(NEW.avg_value IS NULL,samples,samples+1),
        capacity=NEW.capacity,
        min_value=if((min_value < (NEW.min_value)),min_value,NEW.min_value),
        max_value=if((max_value > (NEW.max_value)),max_value,NEW.max_value)
      where snapshot_time<=>date_format(NEW.snapshot_time,"%Y-%m-%d %H:00:00")
            and uuid<=>NEW.uuid and producer_uuid<=>NEW.producer_uuid
            and property_type<=>NEW.property_type and property_subtype<=>NEW.property_subtype and relation<=>NEW.relation
            and commodity_key<=>NEW.commodity_key;

    ELSE

      insert into dpod_stats_by_hour (snapshot_time,uuid,producer_uuid,property_type,property_subtype,capacity,relation,commodity_key,min_value,max_value,avg_value,samples) values
        (date_format(NEW.snapshot_time,"%Y-%m-%d %H:00:00"),
          NEW.uuid,
          NEW.producer_uuid,
          NEW.property_type,
          NEW.property_subtype,
          NEW.capacity,
          NEW.relation,
          NEW.commodity_key,
          NEW.min_value,
          NEW.max_value,
          NEW.avg_value,
         if(NEW.avg_value IS NULL,0,1));

    END IF;


    -- DAILY
    IF EXISTS (select 1 from dpod_stats_by_day where snapshot_time<=>date_format(NEW.snapshot_time,"%Y-%m-%d 00:00:00")
                                                     and uuid<=>NEW.uuid
                                                     and producer_uuid<=>NEW.producer_uuid
                                                     and property_type<=>NEW.property_type
                                                     and property_subtype<=>NEW.property_subtype
                                                     and relation<=>NEW.relation
                                                     and commodity_key<=>NEW.commodity_key limit 1 ) THEN

      update dpod_stats_by_day set avg_value=if(NEW.avg_value is null,avg_value,((avg_value*samples)+NEW.avg_value)/(samples+1)),
        samples=if(NEW.avg_value IS NULL,samples,samples+1),
        capacity=NEW.capacity,
        min_value=if((min_value < (NEW.min_value)),min_value,NEW.min_value),
        max_value=if((max_value > (NEW.max_value)),max_value,NEW.max_value)
      where snapshot_time<=>date_format(NEW.snapshot_time,"%Y-%m-%d 00:00:00")
            and uuid<=>NEW.uuid and producer_uuid<=>NEW.producer_uuid
            and property_type<=>NEW.property_type and property_subtype<=>NEW.property_subtype and relation<=>NEW.relation
            and commodity_key<=>NEW.commodity_key;

    ELSE

      insert into dpod_stats_by_day (snapshot_time,uuid,producer_uuid,property_type,property_subtype,capacity,relation,commodity_key,min_value,max_value,avg_value,samples) values
        (date_format(NEW.snapshot_time,"%Y-%m-%d 00:00:00"),
          NEW.uuid,
          NEW.producer_uuid,
          NEW.property_type,
          NEW.property_subtype,
          NEW.capacity,
          NEW.relation,
          NEW.commodity_key,
          NEW.min_value,
          NEW.max_value,
          NEW.avg_value,
         if(NEW.avg_value IS NULL,0,1));

    END IF;


    -- MONTHLY
    IF EXISTS (select 1 from dpod_stats_by_month where snapshot_time<=>date_format(last_day(NEW.snapshot_time),"%Y-%m-%d 00:00:00")
                                                       and uuid<=>NEW.uuid
                                                       and producer_uuid<=>NEW.producer_uuid
                                                       and property_type<=>NEW.property_type
                                                       and property_subtype<=>NEW.property_subtype
                                                       and relation<=>NEW.relation
                                                       and commodity_key<=>NEW.commodity_key limit 1 ) THEN

      update dpod_stats_by_month set avg_value=if(NEW.avg_value is null,avg_value,((avg_value*samples)+NEW.avg_value)/(samples+1)),
        samples=if(NEW.avg_value IS NULL,samples,samples+1),
        capacity=NEW.capacity,
        min_value=if((min_value < (NEW.min_value)),min_value,NEW.min_value),
        max_value=if((max_value > (NEW.max_value)),max_value,NEW.max_value)
      where snapshot_time<=>date_format(last_day(NEW.snapshot_time),"%Y-%m-%d 00:00:00")
            and uuid<=>NEW.uuid and producer_uuid<=>NEW.producer_uuid
            and property_type<=>NEW.property_type and property_subtype<=>NEW.property_subtype and relation<=>NEW.relation
            and commodity_key<=>NEW.commodity_key;

    ELSE

      insert into dpod_stats_by_month (snapshot_time,uuid,producer_uuid,property_type,property_subtype,capacity,relation,commodity_key,min_value,max_value,avg_value,samples) values
        (date_format(last_day(NEW.snapshot_time),"%Y-%m-%d 00:00:00"),
          NEW.uuid,
          NEW.producer_uuid,
          NEW.property_type,
          NEW.property_subtype,
          NEW.capacity,
          NEW.relation,
          NEW.commodity_key,
          NEW.min_value,
          NEW.max_value,
          NEW.avg_value,
         if(NEW.avg_value IS NULL,0,1));

    END IF;

  END //
DELIMITER ;



-- ADD new samples column to each roll-up table.  The samples column holds the number of samples used to calculate the average, and used to recalculate averages on the fly

alter table ds_stats_by_hour add samples integer;
alter table ds_stats_by_day add samples integer;
alter table ds_stats_by_month add samples integer;

alter table ds_stats_by_day modify snapshot_time datetime;
alter table ds_stats_by_month modify snapshot_time datetime;

-- Create unique compound indexes for each rollup table.  Significantly improves performances of finding and updating stats

create index ds_stats_by_hour_idx on ds_stats_by_hour (snapshot_time,uuid,producer_uuid,property_type,property_subtype,relation);
create index ds_stats_by_day_idx on ds_stats_by_day (snapshot_time,uuid,producer_uuid,property_type,property_subtype,relation);
create index ds_stats_by_mth_idx on ds_stats_by_month (snapshot_time,uuid,producer_uuid,property_type,property_subtype,relation);

-- Create an AFTER INSERT trigger for each of the _latest tables.  The trigger fires on successful insert of new data into a particular _latest table.
-- With each new _latest row added, the hourly, daily and monthly rollup tables are updated at the same time.  Min, max and average is recalculated as required

drop trigger if exists ds_stats_latest_after_insert;


DELIMITER //

CREATE TRIGGER ds_stats_latest_after_insert AFTER INSERT ON ds_stats_latest
FOR EACH ROW
  BEGIN


    -- With _HOURLY,_DAILY,_MONTHLY tables.  Check if there is an existing row for the rollup.  If the row exists, then update and recalculate.
    -- If the row does not exist, add a new row with the _latest data.


    -- HOURLY
    IF EXISTS (select 1 from ds_stats_by_hour where snapshot_time<=>date_format(NEW.snapshot_time,"%Y-%m-%d %H:00:00")
                                                    and uuid<=>NEW.uuid
                                                    and producer_uuid<=>NEW.producer_uuid
                                                    and property_type<=>NEW.property_type
                                                    and property_subtype<=>NEW.property_subtype
                                                    and relation<=>NEW.relation
                                                    and commodity_key<=>NEW.commodity_key limit 1 ) THEN

      update ds_stats_by_hour set avg_value=if(NEW.avg_value is null,avg_value,((avg_value*samples)+NEW.avg_value)/(samples+1)),
        samples=if(NEW.avg_value IS NULL,samples,samples+1),
        capacity=NEW.capacity,
        min_value=if((min_value < (NEW.min_value)),min_value,NEW.min_value),
        max_value=if((max_value > (NEW.max_value)),max_value,NEW.max_value)
      where snapshot_time<=>date_format(NEW.snapshot_time,"%Y-%m-%d %H:00:00")
            and uuid<=>NEW.uuid and producer_uuid<=>NEW.producer_uuid
            and property_type<=>NEW.property_type and property_subtype<=>NEW.property_subtype and relation<=>NEW.relation
            and commodity_key<=>NEW.commodity_key;

    ELSE

      insert into ds_stats_by_hour (snapshot_time,uuid,producer_uuid,property_type,property_subtype,capacity,relation,commodity_key,min_value,max_value,avg_value,samples) values
        (date_format(NEW.snapshot_time,"%Y-%m-%d %H:00:00"),
          NEW.uuid,
          NEW.producer_uuid,
          NEW.property_type,
          NEW.property_subtype,
          NEW.capacity,
          NEW.relation,
          NEW.commodity_key,
          NEW.min_value,
          NEW.max_value,
          NEW.avg_value,
         if(NEW.avg_value IS NULL,0,1));

    END IF;


    -- DAILY
    IF EXISTS (select 1 from ds_stats_by_day where snapshot_time<=>date_format(NEW.snapshot_time,"%Y-%m-%d 00:00:00")
                                                   and uuid<=>NEW.uuid
                                                   and producer_uuid<=>NEW.producer_uuid
                                                   and property_type<=>NEW.property_type
                                                   and property_subtype<=>NEW.property_subtype
                                                   and relation<=>NEW.relation
                                                   and commodity_key<=>NEW.commodity_key limit 1 ) THEN

      update ds_stats_by_day set avg_value=if(NEW.avg_value is null,avg_value,((avg_value*samples)+NEW.avg_value)/(samples+1)),
        samples=if(NEW.avg_value IS NULL,samples,samples+1),
        capacity=NEW.capacity,
        min_value=if((min_value < (NEW.min_value)),min_value,NEW.min_value),
        max_value=if((max_value > (NEW.max_value)),max_value,NEW.max_value)
      where snapshot_time<=>date_format(NEW.snapshot_time,"%Y-%m-%d 00:00:00")
            and uuid<=>NEW.uuid and producer_uuid<=>NEW.producer_uuid
            and property_type<=>NEW.property_type and property_subtype<=>NEW.property_subtype and relation<=>NEW.relation
            and commodity_key<=>NEW.commodity_key;

    ELSE

      insert into ds_stats_by_day (snapshot_time,uuid,producer_uuid,property_type,property_subtype,capacity,relation,commodity_key,min_value,max_value,avg_value,samples) values
        (date_format(NEW.snapshot_time,"%Y-%m-%d 00:00:00"),
          NEW.uuid,
          NEW.producer_uuid,
          NEW.property_type,
          NEW.property_subtype,
          NEW.capacity,
          NEW.relation,
          NEW.commodity_key,
          NEW.min_value,
          NEW.max_value,
          NEW.avg_value,
         if(NEW.avg_value IS NULL,0,1));

    END IF;


    -- MONTHLY
    IF EXISTS (select 1 from ds_stats_by_month where snapshot_time<=>date_format(last_day(NEW.snapshot_time),"%Y-%m-%d 00:00:00")
                                                     and uuid<=>NEW.uuid
                                                     and producer_uuid<=>NEW.producer_uuid
                                                     and property_type<=>NEW.property_type
                                                     and property_subtype<=>NEW.property_subtype
                                                     and relation<=>NEW.relation
                                                     and commodity_key<=>NEW.commodity_key limit 1 ) THEN

      update ds_stats_by_month set avg_value=if(NEW.avg_value is null,avg_value,((avg_value*samples)+NEW.avg_value)/(samples+1)),
        samples=if(NEW.avg_value IS NULL,samples,samples+1),
        capacity=NEW.capacity,
        min_value=if((min_value < (NEW.min_value)),min_value,NEW.min_value),
        max_value=if((max_value > (NEW.max_value)),max_value,NEW.max_value)
      where snapshot_time<=>date_format(last_day(NEW.snapshot_time),"%Y-%m-%d 00:00:00")
            and uuid<=>NEW.uuid and producer_uuid<=>NEW.producer_uuid
            and property_type<=>NEW.property_type and property_subtype<=>NEW.property_subtype and relation<=>NEW.relation
            and commodity_key<=>NEW.commodity_key;

    ELSE

      insert into ds_stats_by_month (snapshot_time,uuid,producer_uuid,property_type,property_subtype,capacity,relation,commodity_key,min_value,max_value,avg_value,samples) values
        (date_format(last_day(NEW.snapshot_time),"%Y-%m-%d 00:00:00"),
          NEW.uuid,
          NEW.producer_uuid,
          NEW.property_type,
          NEW.property_subtype,
          NEW.capacity,
          NEW.relation,
          NEW.commodity_key,
          NEW.min_value,
          NEW.max_value,
          NEW.avg_value,
         if(NEW.avg_value IS NULL,0,1));

    END IF;

  END //
DELIMITER ;



-- ADD new samples column to each roll-up table.  The samples column holds the number of samples used to calculate the average, and used to recalculate averages on the fly

alter table iom_stats_by_hour add samples integer;
alter table iom_stats_by_day add samples integer;
alter table iom_stats_by_month add samples integer;

alter table iom_stats_by_day modify snapshot_time datetime;
alter table iom_stats_by_month modify snapshot_time datetime;

-- Create unique compound indexes for each rollup table.  Significantly improves performances of finding and updating stats

create index iom_stats_by_hour_idx on iom_stats_by_hour (snapshot_time,uuid,producer_uuid,property_type,property_subtype,relation);
create index iom_stats_by_day_idx on iom_stats_by_day (snapshot_time,uuid,producer_uuid,property_type,property_subtype,relation);
create index iom_stats_by_mth_idx on iom_stats_by_month (snapshot_time,uuid,producer_uuid,property_type,property_subtype,relation);

-- Create an AFTER INSERT trigger for each of the _latest tables.  The trigger fires on successful insert of new data into a particular _latest table.
-- With each new _latest row added, the hourly, daily and monthly rollup tables are updated at the same time.  Min, max and average is recalculated as required

drop trigger if exists iom_stats_latest_after_insert;


DELIMITER //

CREATE TRIGGER iom_stats_latest_after_insert AFTER INSERT ON iom_stats_latest
FOR EACH ROW
  BEGIN


    -- With _HOURLY,_DAILY,_MONTHLY tables.  Check if there is an existing row for the rollup.  If the row exists, then update and recalculate.
    -- If the row does not exist, add a new row with the _latest data.


    -- HOURLY
    IF EXISTS (select 1 from iom_stats_by_hour where snapshot_time<=>date_format(NEW.snapshot_time,"%Y-%m-%d %H:00:00")
                                                     and uuid<=>NEW.uuid
                                                     and producer_uuid<=>NEW.producer_uuid
                                                     and property_type<=>NEW.property_type
                                                     and property_subtype<=>NEW.property_subtype
                                                     and relation<=>NEW.relation
                                                     and commodity_key<=>NEW.commodity_key limit 1 ) THEN

      update iom_stats_by_hour set avg_value=if(NEW.avg_value is null,avg_value,((avg_value*samples)+NEW.avg_value)/(samples+1)),
        samples=if(NEW.avg_value IS NULL,samples,samples+1),
        capacity=NEW.capacity,
        min_value=if((min_value < (NEW.min_value)),min_value,NEW.min_value),
        max_value=if((max_value > (NEW.max_value)),max_value,NEW.max_value)
      where snapshot_time<=>date_format(NEW.snapshot_time,"%Y-%m-%d %H:00:00")
            and uuid<=>NEW.uuid and producer_uuid<=>NEW.producer_uuid
            and property_type<=>NEW.property_type and property_subtype<=>NEW.property_subtype and relation<=>NEW.relation
            and commodity_key<=>NEW.commodity_key;

    ELSE

      insert into iom_stats_by_hour (snapshot_time,uuid,producer_uuid,property_type,property_subtype,capacity,relation,commodity_key,min_value,max_value,avg_value,samples) values
        (date_format(NEW.snapshot_time,"%Y-%m-%d %H:00:00"),
          NEW.uuid,
          NEW.producer_uuid,
          NEW.property_type,
          NEW.property_subtype,
          NEW.capacity,
          NEW.relation,
          NEW.commodity_key,
          NEW.min_value,
          NEW.max_value,
          NEW.avg_value,
         if(NEW.avg_value IS NULL,0,1));

    END IF;


    -- DAILY
    IF EXISTS (select 1 from iom_stats_by_day where snapshot_time<=>date_format(NEW.snapshot_time,"%Y-%m-%d 00:00:00")
                                                    and uuid<=>NEW.uuid
                                                    and producer_uuid<=>NEW.producer_uuid
                                                    and property_type<=>NEW.property_type
                                                    and property_subtype<=>NEW.property_subtype
                                                    and relation<=>NEW.relation
                                                    and commodity_key<=>NEW.commodity_key limit 1 ) THEN

      update iom_stats_by_day set avg_value=if(NEW.avg_value is null,avg_value,((avg_value*samples)+NEW.avg_value)/(samples+1)),
        samples=if(NEW.avg_value IS NULL,samples,samples+1),
        capacity=NEW.capacity,
        min_value=if((min_value < (NEW.min_value)),min_value,NEW.min_value),
        max_value=if((max_value > (NEW.max_value)),max_value,NEW.max_value)
      where snapshot_time<=>date_format(NEW.snapshot_time,"%Y-%m-%d 00:00:00")
            and uuid<=>NEW.uuid and producer_uuid<=>NEW.producer_uuid
            and property_type<=>NEW.property_type and property_subtype<=>NEW.property_subtype and relation<=>NEW.relation
            and commodity_key<=>NEW.commodity_key;

    ELSE

      insert into iom_stats_by_day (snapshot_time,uuid,producer_uuid,property_type,property_subtype,capacity,relation,commodity_key,min_value,max_value,avg_value,samples) values
        (date_format(NEW.snapshot_time,"%Y-%m-%d 00:00:00"),
          NEW.uuid,
          NEW.producer_uuid,
          NEW.property_type,
          NEW.property_subtype,
          NEW.capacity,
          NEW.relation,
          NEW.commodity_key,
          NEW.min_value,
          NEW.max_value,
          NEW.avg_value,
         if(NEW.avg_value IS NULL,0,1));

    END IF;


    -- MONTHLY
    IF EXISTS (select 1 from iom_stats_by_month where snapshot_time<=>date_format(last_day(NEW.snapshot_time),"%Y-%m-%d 00:00:00")
                                                      and uuid<=>NEW.uuid
                                                      and producer_uuid<=>NEW.producer_uuid
                                                      and property_type<=>NEW.property_type
                                                      and property_subtype<=>NEW.property_subtype
                                                      and relation<=>NEW.relation
                                                      and commodity_key<=>NEW.commodity_key limit 1 ) THEN

      update iom_stats_by_month set avg_value=if(NEW.avg_value is null,avg_value,((avg_value*samples)+NEW.avg_value)/(samples+1)),
        samples=if(NEW.avg_value IS NULL,samples,samples+1),
        capacity=NEW.capacity,
        min_value=if((min_value < (NEW.min_value)),min_value,NEW.min_value),
        max_value=if((max_value > (NEW.max_value)),max_value,NEW.max_value)
      where snapshot_time<=>date_format(last_day(NEW.snapshot_time),"%Y-%m-%d 00:00:00")
            and uuid<=>NEW.uuid and producer_uuid<=>NEW.producer_uuid
            and property_type<=>NEW.property_type and property_subtype<=>NEW.property_subtype and relation<=>NEW.relation
            and commodity_key<=>NEW.commodity_key;

    ELSE

      insert into iom_stats_by_month (snapshot_time,uuid,producer_uuid,property_type,property_subtype,capacity,relation,commodity_key,min_value,max_value,avg_value,samples) values
        (date_format(last_day(NEW.snapshot_time),"%Y-%m-%d 00:00:00"),
          NEW.uuid,
          NEW.producer_uuid,
          NEW.property_type,
          NEW.property_subtype,
          NEW.capacity,
          NEW.relation,
          NEW.commodity_key,
          NEW.min_value,
          NEW.max_value,
          NEW.avg_value,
         if(NEW.avg_value IS NULL,0,1));

    END IF;

  END //
DELIMITER ;



-- ADD new samples column to each roll-up table.  The samples column holds the number of samples used to calculate the average, and used to recalculate averages on the fly

alter table pm_stats_by_hour add samples integer;
alter table pm_stats_by_day add samples integer;
alter table pm_stats_by_month add samples integer;

alter table pm_stats_by_day modify snapshot_time datetime;
alter table pm_stats_by_month modify snapshot_time datetime;

-- Create unique compound indexes for each rollup table.  Significantly improves performances of finding and updating stats

create index pm_stats_by_hour_idx on pm_stats_by_hour (snapshot_time,uuid,producer_uuid,property_type,property_subtype,relation);
create index pm_stats_by_day_idx on pm_stats_by_day (snapshot_time,uuid,producer_uuid,property_type,property_subtype,relation);
create index pm_stats_by_mth_idx on pm_stats_by_month (snapshot_time,uuid,producer_uuid,property_type,property_subtype,relation);

-- Create an AFTER INSERT trigger for each of the _latest tables.  The trigger fires on successful insert of new data into a particular _latest table.
-- With each new _latest row added, the hourly, daily and monthly rollup tables are updated at the same time.  Min, max and average is recalculated as required

drop trigger if exists pm_stats_latest_after_insert;


DELIMITER //

CREATE TRIGGER pm_stats_latest_after_insert AFTER INSERT ON pm_stats_latest
FOR EACH ROW
  BEGIN


    -- With _HOURLY,_DAILY,_MONTHLY tables.  Check if there is an existing row for the rollup.  If the row exists, then update and recalculate.
    -- If the row does not exist, add a new row with the _latest data.


    -- HOURLY
    IF EXISTS (select 1 from pm_stats_by_hour where snapshot_time<=>date_format(NEW.snapshot_time,"%Y-%m-%d %H:00:00")
                                                    and uuid<=>NEW.uuid
                                                    and producer_uuid<=>NEW.producer_uuid
                                                    and property_type<=>NEW.property_type
                                                    and property_subtype<=>NEW.property_subtype
                                                    and relation<=>NEW.relation
                                                    and commodity_key<=>NEW.commodity_key limit 1 ) THEN

      update pm_stats_by_hour set avg_value=if(NEW.avg_value is null,avg_value,((avg_value*samples)+NEW.avg_value)/(samples+1)),
        samples=if(NEW.avg_value IS NULL,samples,samples+1),
        capacity=NEW.capacity,
        min_value=if((min_value < (NEW.min_value)),min_value,NEW.min_value),
        max_value=if((max_value > (NEW.max_value)),max_value,NEW.max_value)
      where snapshot_time<=>date_format(NEW.snapshot_time,"%Y-%m-%d %H:00:00")
            and uuid<=>NEW.uuid and producer_uuid<=>NEW.producer_uuid
            and property_type<=>NEW.property_type and property_subtype<=>NEW.property_subtype and relation<=>NEW.relation
            and commodity_key<=>NEW.commodity_key;

    ELSE

      insert into pm_stats_by_hour (snapshot_time,uuid,producer_uuid,property_type,property_subtype,capacity,relation,commodity_key,min_value,max_value,avg_value,samples) values
        (date_format(NEW.snapshot_time,"%Y-%m-%d %H:00:00"),
          NEW.uuid,
          NEW.producer_uuid,
          NEW.property_type,
          NEW.property_subtype,
          NEW.capacity,
          NEW.relation,
          NEW.commodity_key,
          NEW.min_value,
          NEW.max_value,
          NEW.avg_value,
         if(NEW.avg_value IS NULL,0,1));

    END IF;


    -- DAILY
    IF EXISTS (select 1 from pm_stats_by_day where snapshot_time<=>date_format(NEW.snapshot_time,"%Y-%m-%d 00:00:00")
                                                   and uuid<=>NEW.uuid
                                                   and producer_uuid<=>NEW.producer_uuid
                                                   and property_type<=>NEW.property_type
                                                   and property_subtype<=>NEW.property_subtype
                                                   and relation<=>NEW.relation
                                                   and commodity_key<=>NEW.commodity_key limit 1 ) THEN

      update pm_stats_by_day set avg_value=if(NEW.avg_value is null,avg_value,((avg_value*samples)+NEW.avg_value)/(samples+1)),
        samples=if(NEW.avg_value IS NULL,samples,samples+1),
        capacity=NEW.capacity,
        min_value=if((min_value < (NEW.min_value)),min_value,NEW.min_value),
        max_value=if((max_value > (NEW.max_value)),max_value,NEW.max_value)
      where snapshot_time<=>date_format(NEW.snapshot_time,"%Y-%m-%d 00:00:00")
            and uuid<=>NEW.uuid and producer_uuid<=>NEW.producer_uuid
            and property_type<=>NEW.property_type and property_subtype<=>NEW.property_subtype and relation<=>NEW.relation
            and commodity_key<=>NEW.commodity_key;

    ELSE

      insert into pm_stats_by_day (snapshot_time,uuid,producer_uuid,property_type,property_subtype,capacity,relation,commodity_key,min_value,max_value,avg_value,samples) values
        (date_format(NEW.snapshot_time,"%Y-%m-%d 00:00:00"),
          NEW.uuid,
          NEW.producer_uuid,
          NEW.property_type,
          NEW.property_subtype,
          NEW.capacity,
          NEW.relation,
          NEW.commodity_key,
          NEW.min_value,
          NEW.max_value,
          NEW.avg_value,
         if(NEW.avg_value IS NULL,0,1));

    END IF;


    -- MONTHLY
    IF EXISTS (select 1 from pm_stats_by_month where snapshot_time<=>date_format(last_day(NEW.snapshot_time),"%Y-%m-%d 00:00:00")
                                                     and uuid<=>NEW.uuid
                                                     and producer_uuid<=>NEW.producer_uuid
                                                     and property_type<=>NEW.property_type
                                                     and property_subtype<=>NEW.property_subtype
                                                     and relation<=>NEW.relation
                                                     and commodity_key<=>NEW.commodity_key limit 1 ) THEN

      update pm_stats_by_month set avg_value=if(NEW.avg_value is null,avg_value,((avg_value*samples)+NEW.avg_value)/(samples+1)),
        samples=if(NEW.avg_value IS NULL,samples,samples+1),
        capacity=NEW.capacity,
        min_value=if((min_value < (NEW.min_value)),min_value,NEW.min_value),
        max_value=if((max_value > (NEW.max_value)),max_value,NEW.max_value)
      where snapshot_time<=>date_format(last_day(NEW.snapshot_time),"%Y-%m-%d 00:00:00")
            and uuid<=>NEW.uuid and producer_uuid<=>NEW.producer_uuid
            and property_type<=>NEW.property_type and property_subtype<=>NEW.property_subtype and relation<=>NEW.relation
            and commodity_key<=>NEW.commodity_key;

    ELSE

      insert into pm_stats_by_month (snapshot_time,uuid,producer_uuid,property_type,property_subtype,capacity,relation,commodity_key,min_value,max_value,avg_value,samples) values
        (date_format(last_day(NEW.snapshot_time),"%Y-%m-%d 00:00:00"),
          NEW.uuid,
          NEW.producer_uuid,
          NEW.property_type,
          NEW.property_subtype,
          NEW.capacity,
          NEW.relation,
          NEW.commodity_key,
          NEW.min_value,
          NEW.max_value,
          NEW.avg_value,
         if(NEW.avg_value IS NULL,0,1));

    END IF;

  END //
DELIMITER ;



-- ADD new samples column to each roll-up table.  The samples column holds the number of samples used to calculate the average, and used to recalculate averages on the fly

alter table sc_stats_by_hour add samples integer;
alter table sc_stats_by_day add samples integer;
alter table sc_stats_by_month add samples integer;

alter table sc_stats_by_day modify snapshot_time datetime;
alter table sc_stats_by_month modify snapshot_time datetime;

-- Create unique compound indexes for each rollup table.  Significantly improves performances of finding and updating stats

create index sc_stats_by_hour_idx on sc_stats_by_hour (snapshot_time,uuid,producer_uuid,property_type,property_subtype,relation);
create index sc_stats_by_day_idx on sc_stats_by_day (snapshot_time,uuid,producer_uuid,property_type,property_subtype,relation);
create index sc_stats_by_mth_idx on sc_stats_by_month (snapshot_time,uuid,producer_uuid,property_type,property_subtype,relation);

-- Create an AFTER INSERT trigger for each of the _latest tables.  The trigger fires on successful insert of new data into a particular _latest table.
-- With each new _latest row added, the hourly, daily and monthly rollup tables are updated at the same time.  Min, max and average is recalculated as required

drop trigger if exists sc_stats_latest_after_insert;


DELIMITER //

CREATE TRIGGER sc_stats_latest_after_insert AFTER INSERT ON sc_stats_latest
FOR EACH ROW
  BEGIN


    -- With _HOURLY,_DAILY,_MONTHLY tables.  Check if there is an existing row for the rollup.  If the row exists, then update and recalculate.
    -- If the row does not exist, add a new row with the _latest data.


    -- HOURLY
    IF EXISTS (select 1 from sc_stats_by_hour where snapshot_time<=>date_format(NEW.snapshot_time,"%Y-%m-%d %H:00:00")
                                                    and uuid<=>NEW.uuid
                                                    and producer_uuid<=>NEW.producer_uuid
                                                    and property_type<=>NEW.property_type
                                                    and property_subtype<=>NEW.property_subtype
                                                    and relation<=>NEW.relation
                                                    and commodity_key<=>NEW.commodity_key limit 1 ) THEN

      update sc_stats_by_hour set avg_value=if(NEW.avg_value is null,avg_value,((avg_value*samples)+NEW.avg_value)/(samples+1)),
        samples=if(NEW.avg_value IS NULL,samples,samples+1),
        capacity=NEW.capacity,
        min_value=if((min_value < (NEW.min_value)),min_value,NEW.min_value),
        max_value=if((max_value > (NEW.max_value)),max_value,NEW.max_value)
      where snapshot_time<=>date_format(NEW.snapshot_time,"%Y-%m-%d %H:00:00")
            and uuid<=>NEW.uuid and producer_uuid<=>NEW.producer_uuid
            and property_type<=>NEW.property_type and property_subtype<=>NEW.property_subtype and relation<=>NEW.relation
            and commodity_key<=>NEW.commodity_key;

    ELSE

      insert into sc_stats_by_hour (snapshot_time,uuid,producer_uuid,property_type,property_subtype,capacity,relation,commodity_key,min_value,max_value,avg_value,samples) values
        (date_format(NEW.snapshot_time,"%Y-%m-%d %H:00:00"),
          NEW.uuid,
          NEW.producer_uuid,
          NEW.property_type,
          NEW.property_subtype,
          NEW.capacity,
          NEW.relation,
          NEW.commodity_key,
          NEW.min_value,
          NEW.max_value,
          NEW.avg_value,
         if(NEW.avg_value IS NULL,0,1));

    END IF;


    -- DAILY
    IF EXISTS (select 1 from sc_stats_by_day where snapshot_time<=>date_format(NEW.snapshot_time,"%Y-%m-%d 00:00:00")
                                                   and uuid<=>NEW.uuid
                                                   and producer_uuid<=>NEW.producer_uuid
                                                   and property_type<=>NEW.property_type
                                                   and property_subtype<=>NEW.property_subtype
                                                   and relation<=>NEW.relation
                                                   and commodity_key<=>NEW.commodity_key limit 1 ) THEN

      update sc_stats_by_day set avg_value=if(NEW.avg_value is null,avg_value,((avg_value*samples)+NEW.avg_value)/(samples+1)),
        samples=if(NEW.avg_value IS NULL,samples,samples+1),
        capacity=NEW.capacity,
        min_value=if((min_value < (NEW.min_value)),min_value,NEW.min_value),
        max_value=if((max_value > (NEW.max_value)),max_value,NEW.max_value)
      where snapshot_time<=>date_format(NEW.snapshot_time,"%Y-%m-%d 00:00:00")
            and uuid<=>NEW.uuid and producer_uuid<=>NEW.producer_uuid
            and property_type<=>NEW.property_type and property_subtype<=>NEW.property_subtype and relation<=>NEW.relation
            and commodity_key<=>NEW.commodity_key;

    ELSE

      insert into sc_stats_by_day (snapshot_time,uuid,producer_uuid,property_type,property_subtype,capacity,relation,commodity_key,min_value,max_value,avg_value,samples) values
        (date_format(NEW.snapshot_time,"%Y-%m-%d 00:00:00"),
          NEW.uuid,
          NEW.producer_uuid,
          NEW.property_type,
          NEW.property_subtype,
          NEW.capacity,
          NEW.relation,
          NEW.commodity_key,
          NEW.min_value,
          NEW.max_value,
          NEW.avg_value,
         if(NEW.avg_value IS NULL,0,1));

    END IF;


    -- MONTHLY
    IF EXISTS (select 1 from sc_stats_by_month where snapshot_time<=>date_format(last_day(NEW.snapshot_time),"%Y-%m-%d 00:00:00")
                                                     and uuid<=>NEW.uuid
                                                     and producer_uuid<=>NEW.producer_uuid
                                                     and property_type<=>NEW.property_type
                                                     and property_subtype<=>NEW.property_subtype
                                                     and relation<=>NEW.relation
                                                     and commodity_key<=>NEW.commodity_key limit 1 ) THEN

      update sc_stats_by_month set avg_value=if(NEW.avg_value is null,avg_value,((avg_value*samples)+NEW.avg_value)/(samples+1)),
        samples=if(NEW.avg_value IS NULL,samples,samples+1),
        capacity=NEW.capacity,
        min_value=if((min_value < (NEW.min_value)),min_value,NEW.min_value),
        max_value=if((max_value > (NEW.max_value)),max_value,NEW.max_value)
      where snapshot_time<=>date_format(last_day(NEW.snapshot_time),"%Y-%m-%d 00:00:00")
            and uuid<=>NEW.uuid and producer_uuid<=>NEW.producer_uuid
            and property_type<=>NEW.property_type and property_subtype<=>NEW.property_subtype and relation<=>NEW.relation
            and commodity_key<=>NEW.commodity_key;

    ELSE

      insert into sc_stats_by_month (snapshot_time,uuid,producer_uuid,property_type,property_subtype,capacity,relation,commodity_key,min_value,max_value,avg_value,samples) values
        (date_format(last_day(NEW.snapshot_time),"%Y-%m-%d 00:00:00"),
          NEW.uuid,
          NEW.producer_uuid,
          NEW.property_type,
          NEW.property_subtype,
          NEW.capacity,
          NEW.relation,
          NEW.commodity_key,
          NEW.min_value,
          NEW.max_value,
          NEW.avg_value,
         if(NEW.avg_value IS NULL,0,1));

    END IF;

  END //
DELIMITER ;



-- ADD new samples column to each roll-up table.  The samples column holds the number of samples used to calculate the average, and used to recalculate averages on the fly

alter table sw_stats_by_hour add samples integer;
alter table sw_stats_by_day add samples integer;
alter table sw_stats_by_month add samples integer;

alter table sw_stats_by_day modify snapshot_time datetime;
alter table sw_stats_by_month modify snapshot_time datetime;

-- Create unique compound indexes for each rollup table.  Significantly improves performances of finding and updating stats

create index sw_stats_by_hour_idx on sw_stats_by_hour (snapshot_time,uuid,producer_uuid,property_type,property_subtype,relation);
create index sw_stats_by_day_idx on sw_stats_by_day (snapshot_time,uuid,producer_uuid,property_type,property_subtype,relation);
create index sw_stats_by_mth_idx on sw_stats_by_month (snapshot_time,uuid,producer_uuid,property_type,property_subtype,relation);

-- Create an AFTER INSERT trigger for each of the _latest tables.  The trigger fires on successful insert of new data into a particular _latest table.
-- With each new _latest row added, the hourly, daily and monthly rollup tables are updated at the same time.  Min, max and average is recalculated as required

drop trigger if exists sw_stats_latest_after_insert;


DELIMITER //

CREATE TRIGGER sw_stats_latest_after_insert AFTER INSERT ON sw_stats_latest
FOR EACH ROW
  BEGIN


    -- With _HOURLY,_DAILY,_MONTHLY tables.  Check if there is an existing row for the rollup.  If the row exists, then update and recalculate.
    -- If the row does not exist, add a new row with the _latest data.


    -- HOURLY
    IF EXISTS (select 1 from sw_stats_by_hour where snapshot_time<=>date_format(NEW.snapshot_time,"%Y-%m-%d %H:00:00")
                                                    and uuid<=>NEW.uuid
                                                    and producer_uuid<=>NEW.producer_uuid
                                                    and property_type<=>NEW.property_type
                                                    and property_subtype<=>NEW.property_subtype
                                                    and relation<=>NEW.relation
                                                    and commodity_key<=>NEW.commodity_key limit 1 ) THEN

      update sw_stats_by_hour set avg_value=if(NEW.avg_value is null,avg_value,((avg_value*samples)+NEW.avg_value)/(samples+1)),
        samples=if(NEW.avg_value IS NULL,samples,samples+1),
        capacity=NEW.capacity,
        min_value=if((min_value < (NEW.min_value)),min_value,NEW.min_value),
        max_value=if((max_value > (NEW.max_value)),max_value,NEW.max_value)
      where snapshot_time<=>date_format(NEW.snapshot_time,"%Y-%m-%d %H:00:00")
            and uuid<=>NEW.uuid and producer_uuid<=>NEW.producer_uuid
            and property_type<=>NEW.property_type and property_subtype<=>NEW.property_subtype and relation<=>NEW.relation
            and commodity_key<=>NEW.commodity_key;

    ELSE

      insert into sw_stats_by_hour (snapshot_time,uuid,producer_uuid,property_type,property_subtype,capacity,relation,commodity_key,min_value,max_value,avg_value,samples) values
        (date_format(NEW.snapshot_time,"%Y-%m-%d %H:00:00"),
          NEW.uuid,
          NEW.producer_uuid,
          NEW.property_type,
          NEW.property_subtype,
          NEW.capacity,
          NEW.relation,
          NEW.commodity_key,
          NEW.min_value,
          NEW.max_value,
          NEW.avg_value,
         if(NEW.avg_value IS NULL,0,1));

    END IF;


    -- DAILY
    IF EXISTS (select 1 from sw_stats_by_day where snapshot_time<=>date_format(NEW.snapshot_time,"%Y-%m-%d 00:00:00")
                                                   and uuid<=>NEW.uuid
                                                   and producer_uuid<=>NEW.producer_uuid
                                                   and property_type<=>NEW.property_type
                                                   and property_subtype<=>NEW.property_subtype
                                                   and relation<=>NEW.relation
                                                   and commodity_key<=>NEW.commodity_key limit 1 ) THEN

      update sw_stats_by_day set avg_value=if(NEW.avg_value is null,avg_value,((avg_value*samples)+NEW.avg_value)/(samples+1)),
        samples=if(NEW.avg_value IS NULL,samples,samples+1),
        capacity=NEW.capacity,
        min_value=if((min_value < (NEW.min_value)),min_value,NEW.min_value),
        max_value=if((max_value > (NEW.max_value)),max_value,NEW.max_value)
      where snapshot_time<=>date_format(NEW.snapshot_time,"%Y-%m-%d 00:00:00")
            and uuid<=>NEW.uuid and producer_uuid<=>NEW.producer_uuid
            and property_type<=>NEW.property_type and property_subtype<=>NEW.property_subtype and relation<=>NEW.relation
            and commodity_key<=>NEW.commodity_key;

    ELSE

      insert into sw_stats_by_day (snapshot_time,uuid,producer_uuid,property_type,property_subtype,capacity,relation,commodity_key,min_value,max_value,avg_value,samples) values
        (date_format(NEW.snapshot_time,"%Y-%m-%d 00:00:00"),
          NEW.uuid,
          NEW.producer_uuid,
          NEW.property_type,
          NEW.property_subtype,
          NEW.capacity,
          NEW.relation,
          NEW.commodity_key,
          NEW.min_value,
          NEW.max_value,
          NEW.avg_value,
         if(NEW.avg_value IS NULL,0,1));

    END IF;


    -- MONTHLY
    IF EXISTS (select 1 from sw_stats_by_month where snapshot_time<=>date_format(last_day(NEW.snapshot_time),"%Y-%m-%d 00:00:00")
                                                     and uuid<=>NEW.uuid
                                                     and producer_uuid<=>NEW.producer_uuid
                                                     and property_type<=>NEW.property_type
                                                     and property_subtype<=>NEW.property_subtype
                                                     and relation<=>NEW.relation
                                                     and commodity_key<=>NEW.commodity_key limit 1 ) THEN

      update sw_stats_by_month set avg_value=if(NEW.avg_value is null,avg_value,((avg_value*samples)+NEW.avg_value)/(samples+1)),
        samples=if(NEW.avg_value IS NULL,samples,samples+1),
        capacity=NEW.capacity,
        min_value=if((min_value < (NEW.min_value)),min_value,NEW.min_value),
        max_value=if((max_value > (NEW.max_value)),max_value,NEW.max_value)
      where snapshot_time<=>date_format(last_day(NEW.snapshot_time),"%Y-%m-%d 00:00:00")
            and uuid<=>NEW.uuid and producer_uuid<=>NEW.producer_uuid
            and property_type<=>NEW.property_type and property_subtype<=>NEW.property_subtype and relation<=>NEW.relation
            and commodity_key<=>NEW.commodity_key;

    ELSE

      insert into sw_stats_by_month (snapshot_time,uuid,producer_uuid,property_type,property_subtype,capacity,relation,commodity_key,min_value,max_value,avg_value,samples) values
        (date_format(last_day(NEW.snapshot_time),"%Y-%m-%d 00:00:00"),
          NEW.uuid,
          NEW.producer_uuid,
          NEW.property_type,
          NEW.property_subtype,
          NEW.capacity,
          NEW.relation,
          NEW.commodity_key,
          NEW.min_value,
          NEW.max_value,
          NEW.avg_value,
         if(NEW.avg_value IS NULL,0,1));

    END IF;

  END //
DELIMITER ;



-- ADD new samples column to each roll-up table.  The samples column holds the number of samples used to calculate the average, and used to recalculate averages on the fly

alter table vdc_stats_by_hour add samples integer;
alter table vdc_stats_by_day add samples integer;
alter table vdc_stats_by_month add samples integer;

alter table vdc_stats_by_day modify snapshot_time datetime;
alter table vdc_stats_by_month modify snapshot_time datetime;

-- Create unique compound indexes for each rollup table.  Significantly improves performances of finding and updating stats

create index vdc_stats_by_hour_idx on vdc_stats_by_hour (snapshot_time,uuid,producer_uuid,property_type,property_subtype,relation);
create index vdc_stats_by_day_idx on vdc_stats_by_day (snapshot_time,uuid,producer_uuid,property_type,property_subtype,relation);
create index vdc_stats_by_mth_idx on vdc_stats_by_month (snapshot_time,uuid,producer_uuid,property_type,property_subtype,relation);

-- Create an AFTER INSERT trigger for each of the _latest tables.  The trigger fires on successful insert of new data into a particular _latest table.
-- With each new _latest row added, the hourly, daily and monthly rollup tables are updated at the same time.  Min, max and average is recalculated as required

drop trigger if exists vdc_stats_latest_after_insert;


DELIMITER //

CREATE TRIGGER vdc_stats_latest_after_insert AFTER INSERT ON vdc_stats_latest
FOR EACH ROW
  BEGIN


    -- With _HOURLY,_DAILY,_MONTHLY tables.  Check if there is an existing row for the rollup.  If the row exists, then update and recalculate.
    -- If the row does not exist, add a new row with the _latest data.


    -- HOURLY
    IF EXISTS (select 1 from vdc_stats_by_hour where snapshot_time<=>date_format(NEW.snapshot_time,"%Y-%m-%d %H:00:00")
                                                     and uuid<=>NEW.uuid
                                                     and producer_uuid<=>NEW.producer_uuid
                                                     and property_type<=>NEW.property_type
                                                     and property_subtype<=>NEW.property_subtype
                                                     and relation<=>NEW.relation
                                                     and commodity_key<=>NEW.commodity_key limit 1 ) THEN

      update vdc_stats_by_hour set avg_value=if(NEW.avg_value is null,avg_value,((avg_value*samples)+NEW.avg_value)/(samples+1)),
        samples=if(NEW.avg_value IS NULL,samples,samples+1),
        capacity=NEW.capacity,
        min_value=if((min_value < (NEW.min_value)),min_value,NEW.min_value),
        max_value=if((max_value > (NEW.max_value)),max_value,NEW.max_value)
      where snapshot_time<=>date_format(NEW.snapshot_time,"%Y-%m-%d %H:00:00")
            and uuid<=>NEW.uuid and producer_uuid<=>NEW.producer_uuid
            and property_type<=>NEW.property_type and property_subtype<=>NEW.property_subtype and relation<=>NEW.relation
            and commodity_key<=>NEW.commodity_key;

    ELSE

      insert into vdc_stats_by_hour (snapshot_time,uuid,producer_uuid,property_type,property_subtype,capacity,relation,commodity_key,min_value,max_value,avg_value,samples) values
        (date_format(NEW.snapshot_time,"%Y-%m-%d %H:00:00"),
          NEW.uuid,
          NEW.producer_uuid,
          NEW.property_type,
          NEW.property_subtype,
          NEW.capacity,
          NEW.relation,
          NEW.commodity_key,
          NEW.min_value,
          NEW.max_value,
          NEW.avg_value,
         if(NEW.avg_value IS NULL,0,1));

    END IF;


    -- DAILY
    IF EXISTS (select 1 from vdc_stats_by_day where snapshot_time<=>date_format(NEW.snapshot_time,"%Y-%m-%d 00:00:00")
                                                    and uuid<=>NEW.uuid
                                                    and producer_uuid<=>NEW.producer_uuid
                                                    and property_type<=>NEW.property_type
                                                    and property_subtype<=>NEW.property_subtype
                                                    and relation<=>NEW.relation
                                                    and commodity_key<=>NEW.commodity_key limit 1 ) THEN

      update vdc_stats_by_day set avg_value=if(NEW.avg_value is null,avg_value,((avg_value*samples)+NEW.avg_value)/(samples+1)),
        samples=if(NEW.avg_value IS NULL,samples,samples+1),
        capacity=NEW.capacity,
        min_value=if((min_value < (NEW.min_value)),min_value,NEW.min_value),
        max_value=if((max_value > (NEW.max_value)),max_value,NEW.max_value)
      where snapshot_time<=>date_format(NEW.snapshot_time,"%Y-%m-%d 00:00:00")
            and uuid<=>NEW.uuid and producer_uuid<=>NEW.producer_uuid
            and property_type<=>NEW.property_type and property_subtype<=>NEW.property_subtype and relation<=>NEW.relation
            and commodity_key<=>NEW.commodity_key;

    ELSE

      insert into vdc_stats_by_day (snapshot_time,uuid,producer_uuid,property_type,property_subtype,capacity,relation,commodity_key,min_value,max_value,avg_value,samples) values
        (date_format(NEW.snapshot_time,"%Y-%m-%d 00:00:00"),
          NEW.uuid,
          NEW.producer_uuid,
          NEW.property_type,
          NEW.property_subtype,
          NEW.capacity,
          NEW.relation,
          NEW.commodity_key,
          NEW.min_value,
          NEW.max_value,
          NEW.avg_value,
         if(NEW.avg_value IS NULL,0,1));

    END IF;


    -- MONTHLY
    IF EXISTS (select 1 from vdc_stats_by_month where snapshot_time<=>date_format(last_day(NEW.snapshot_time),"%Y-%m-%d 00:00:00")
                                                      and uuid<=>NEW.uuid
                                                      and producer_uuid<=>NEW.producer_uuid
                                                      and property_type<=>NEW.property_type
                                                      and property_subtype<=>NEW.property_subtype
                                                      and relation<=>NEW.relation
                                                      and commodity_key<=>NEW.commodity_key limit 1 ) THEN

      update vdc_stats_by_month set avg_value=if(NEW.avg_value is null,avg_value,((avg_value*samples)+NEW.avg_value)/(samples+1)),
        samples=if(NEW.avg_value IS NULL,samples,samples+1),
        capacity=NEW.capacity,
        min_value=if((min_value < (NEW.min_value)),min_value,NEW.min_value),
        max_value=if((max_value > (NEW.max_value)),max_value,NEW.max_value)
      where snapshot_time<=>date_format(last_day(NEW.snapshot_time),"%Y-%m-%d 00:00:00")
            and uuid<=>NEW.uuid and producer_uuid<=>NEW.producer_uuid
            and property_type<=>NEW.property_type and property_subtype<=>NEW.property_subtype and relation<=>NEW.relation
            and commodity_key<=>NEW.commodity_key;

    ELSE

      insert into vdc_stats_by_month (snapshot_time,uuid,producer_uuid,property_type,property_subtype,capacity,relation,commodity_key,min_value,max_value,avg_value,samples) values
        (date_format(last_day(NEW.snapshot_time),"%Y-%m-%d 00:00:00"),
          NEW.uuid,
          NEW.producer_uuid,
          NEW.property_type,
          NEW.property_subtype,
          NEW.capacity,
          NEW.relation,
          NEW.commodity_key,
          NEW.min_value,
          NEW.max_value,
          NEW.avg_value,
         if(NEW.avg_value IS NULL,0,1));

    END IF;

  END //
DELIMITER ;



-- ADD new samples column to each roll-up table.  The samples column holds the number of samples used to calculate the average, and used to recalculate averages on the fly

alter table vm_stats_by_hour add samples integer;
alter table vm_stats_by_day add samples integer;
alter table vm_stats_by_month add samples integer;

alter table vm_stats_by_day modify snapshot_time datetime;
alter table vm_stats_by_month modify snapshot_time datetime;

-- Create unique compound indexes for each rollup table.  Significantly improves performances of finding and updating stats

create index vm_stats_by_hour_idx on vm_stats_by_hour (snapshot_time,uuid,producer_uuid,property_type,property_subtype,relation);
create index vm_stats_by_day_idx on vm_stats_by_day (snapshot_time,uuid,producer_uuid,property_type,property_subtype,relation);
create index vm_stats_by_mth_idx on vm_stats_by_month (snapshot_time,uuid,producer_uuid,property_type,property_subtype,relation);

-- Create an AFTER INSERT trigger for each of the _latest tables.  The trigger fires on successful insert of new data into a particular _latest table.
-- With each new _latest row added, the hourly, daily and monthly rollup tables are updated at the same time.  Min, max and average is recalculated as required

drop trigger if exists vm_stats_latest_after_insert;


DELIMITER //

CREATE TRIGGER vm_stats_latest_after_insert AFTER INSERT ON vm_stats_latest
FOR EACH ROW
  BEGIN


    -- With _HOURLY,_DAILY,_MONTHLY tables.  Check if there is an existing row for the rollup.  If the row exists, then update and recalculate.
    -- If the row does not exist, add a new row with the _latest data.


    -- HOURLY
    IF EXISTS (select 1 from vm_stats_by_hour where snapshot_time<=>date_format(NEW.snapshot_time,"%Y-%m-%d %H:00:00")
                                                    and uuid<=>NEW.uuid
                                                    and producer_uuid<=>NEW.producer_uuid
                                                    and property_type<=>NEW.property_type
                                                    and property_subtype<=>NEW.property_subtype
                                                    and relation<=>NEW.relation
                                                    and commodity_key<=>NEW.commodity_key limit 1 ) THEN

      update vm_stats_by_hour set avg_value=if(NEW.avg_value is null,avg_value,((avg_value*samples)+NEW.avg_value)/(samples+1)),
        samples=if(NEW.avg_value IS NULL,samples,samples+1),
        capacity=NEW.capacity,
        min_value=if((min_value < (NEW.min_value)),min_value,NEW.min_value),
        max_value=if((max_value > (NEW.max_value)),max_value,NEW.max_value)
      where snapshot_time<=>date_format(NEW.snapshot_time,"%Y-%m-%d %H:00:00")
            and uuid<=>NEW.uuid and producer_uuid<=>NEW.producer_uuid
            and property_type<=>NEW.property_type and property_subtype<=>NEW.property_subtype and relation<=>NEW.relation
            and commodity_key<=>NEW.commodity_key;

    ELSE

      insert into vm_stats_by_hour (snapshot_time,uuid,producer_uuid,property_type,property_subtype,capacity,relation,commodity_key,min_value,max_value,avg_value,samples) values
        (date_format(NEW.snapshot_time,"%Y-%m-%d %H:00:00"),
          NEW.uuid,
          NEW.producer_uuid,
          NEW.property_type,
          NEW.property_subtype,
          NEW.capacity,
          NEW.relation,
          NEW.commodity_key,
          NEW.min_value,
          NEW.max_value,
          NEW.avg_value,
         if(NEW.avg_value IS NULL,0,1));

    END IF;


    -- DAILY
    IF EXISTS (select 1 from vm_stats_by_day where snapshot_time<=>date_format(NEW.snapshot_time,"%Y-%m-%d 00:00:00")
                                                   and uuid<=>NEW.uuid
                                                   and producer_uuid<=>NEW.producer_uuid
                                                   and property_type<=>NEW.property_type
                                                   and property_subtype<=>NEW.property_subtype
                                                   and relation<=>NEW.relation
                                                   and commodity_key<=>NEW.commodity_key limit 1 ) THEN

      update vm_stats_by_day set avg_value=if(NEW.avg_value is null,avg_value,((avg_value*samples)+NEW.avg_value)/(samples+1)),
        samples=if(NEW.avg_value IS NULL,samples,samples+1),
        capacity=NEW.capacity,
        min_value=if((min_value < (NEW.min_value)),min_value,NEW.min_value),
        max_value=if((max_value > (NEW.max_value)),max_value,NEW.max_value)
      where snapshot_time<=>date_format(NEW.snapshot_time,"%Y-%m-%d 00:00:00")
            and uuid<=>NEW.uuid and producer_uuid<=>NEW.producer_uuid
            and property_type<=>NEW.property_type and property_subtype<=>NEW.property_subtype and relation<=>NEW.relation
            and commodity_key<=>NEW.commodity_key;

    ELSE

      insert into vm_stats_by_day (snapshot_time,uuid,producer_uuid,property_type,property_subtype,capacity,relation,commodity_key,min_value,max_value,avg_value,samples) values
        (date_format(NEW.snapshot_time,"%Y-%m-%d 00:00:00"),
          NEW.uuid,
          NEW.producer_uuid,
          NEW.property_type,
          NEW.property_subtype,
          NEW.capacity,
          NEW.relation,
          NEW.commodity_key,
          NEW.min_value,
          NEW.max_value,
          NEW.avg_value,
         if(NEW.avg_value IS NULL,0,1));

    END IF;


    -- MONTHLY
    IF EXISTS (select 1 from vm_stats_by_month where snapshot_time<=>date_format(last_day(NEW.snapshot_time),"%Y-%m-%d 00:00:00")
                                                     and uuid<=>NEW.uuid
                                                     and producer_uuid<=>NEW.producer_uuid
                                                     and property_type<=>NEW.property_type
                                                     and property_subtype<=>NEW.property_subtype
                                                     and relation<=>NEW.relation
                                                     and commodity_key<=>NEW.commodity_key limit 1 ) THEN

      update vm_stats_by_month set avg_value=if(NEW.avg_value is null,avg_value,((avg_value*samples)+NEW.avg_value)/(samples+1)),
        samples=if(NEW.avg_value IS NULL,samples,samples+1),
        capacity=NEW.capacity,
        min_value=if((min_value < (NEW.min_value)),min_value,NEW.min_value),
        max_value=if((max_value > (NEW.max_value)),max_value,NEW.max_value)
      where snapshot_time<=>date_format(last_day(NEW.snapshot_time),"%Y-%m-%d 00:00:00")
            and uuid<=>NEW.uuid and producer_uuid<=>NEW.producer_uuid
            and property_type<=>NEW.property_type and property_subtype<=>NEW.property_subtype and relation<=>NEW.relation
            and commodity_key<=>NEW.commodity_key;

    ELSE

      insert into vm_stats_by_month (snapshot_time,uuid,producer_uuid,property_type,property_subtype,capacity,relation,commodity_key,min_value,max_value,avg_value,samples) values
        (date_format(last_day(NEW.snapshot_time),"%Y-%m-%d 00:00:00"),
          NEW.uuid,
          NEW.producer_uuid,
          NEW.property_type,
          NEW.property_subtype,
          NEW.capacity,
          NEW.relation,
          NEW.commodity_key,
          NEW.min_value,
          NEW.max_value,
          NEW.avg_value,
         if(NEW.avg_value IS NULL,0,1));

    END IF;

  END //
DELIMITER ;



-- ADD new samples column to each roll-up table.  The samples column holds the number of samples used to calculate the average, and used to recalculate averages on the fly

alter table vpod_stats_by_hour add samples integer;
alter table vpod_stats_by_day add samples integer;
alter table vpod_stats_by_month add samples integer;

alter table vpod_stats_by_day modify snapshot_time datetime;
alter table vpod_stats_by_month modify snapshot_time datetime;

-- Create unique compound indexes for each rollup table.  Significantly improves performances of finding and updating stats

create index vpod_stats_by_hour_idx on vpod_stats_by_hour (snapshot_time,uuid,producer_uuid,property_type,property_subtype,relation);
create index vpod_stats_by_day_idx on vpod_stats_by_day (snapshot_time,uuid,producer_uuid,property_type,property_subtype,relation);
create index vpod_stats_by_mth_idx on vpod_stats_by_month (snapshot_time,uuid,producer_uuid,property_type,property_subtype,relation);

-- Create an AFTER INSERT trigger for each of the _latest tables.  The trigger fires on successful insert of new data into a particular _latest table.
-- With each new _latest row added, the hourly, daily and monthly rollup tables are updated at the same time.  Min, max and average is recalculated as required

drop trigger if exists vpod_stats_latest_after_insert;


DELIMITER //

CREATE TRIGGER vpod_stats_latest_after_insert AFTER INSERT ON vpod_stats_latest
FOR EACH ROW
  BEGIN


    -- With _HOURLY,_DAILY,_MONTHLY tables.  Check if there is an existing row for the rollup.  If the row exists, then update and recalculate.
    -- If the row does not exist, add a new row with the _latest data.


    -- HOURLY
    IF EXISTS (select 1 from vpod_stats_by_hour where snapshot_time<=>date_format(NEW.snapshot_time,"%Y-%m-%d %H:00:00")
                                                      and uuid<=>NEW.uuid
                                                      and producer_uuid<=>NEW.producer_uuid
                                                      and property_type<=>NEW.property_type
                                                      and property_subtype<=>NEW.property_subtype
                                                      and relation<=>NEW.relation
                                                      and commodity_key<=>NEW.commodity_key limit 1 ) THEN

      update vpod_stats_by_hour set avg_value=if(NEW.avg_value is null,avg_value,((avg_value*samples)+NEW.avg_value)/(samples+1)),
        samples=if(NEW.avg_value IS NULL,samples,samples+1),
        capacity=NEW.capacity,
        min_value=if((min_value < (NEW.min_value)),min_value,NEW.min_value),
        max_value=if((max_value > (NEW.max_value)),max_value,NEW.max_value)
      where snapshot_time<=>date_format(NEW.snapshot_time,"%Y-%m-%d %H:00:00")
            and uuid<=>NEW.uuid and producer_uuid<=>NEW.producer_uuid
            and property_type<=>NEW.property_type and property_subtype<=>NEW.property_subtype and relation<=>NEW.relation
            and commodity_key<=>NEW.commodity_key;

    ELSE

      insert into vpod_stats_by_hour (snapshot_time,uuid,producer_uuid,property_type,property_subtype,capacity,relation,commodity_key,min_value,max_value,avg_value,samples) values
        (date_format(NEW.snapshot_time,"%Y-%m-%d %H:00:00"),
          NEW.uuid,
          NEW.producer_uuid,
          NEW.property_type,
          NEW.property_subtype,
          NEW.capacity,
          NEW.relation,
          NEW.commodity_key,
          NEW.min_value,
          NEW.max_value,
          NEW.avg_value,
         if(NEW.avg_value IS NULL,0,1));

    END IF;


    -- DAILY
    IF EXISTS (select 1 from vpod_stats_by_day where snapshot_time<=>date_format(NEW.snapshot_time,"%Y-%m-%d 00:00:00")
                                                     and uuid<=>NEW.uuid
                                                     and producer_uuid<=>NEW.producer_uuid
                                                     and property_type<=>NEW.property_type
                                                     and property_subtype<=>NEW.property_subtype
                                                     and relation<=>NEW.relation
                                                     and commodity_key<=>NEW.commodity_key limit 1 ) THEN

      update vpod_stats_by_day set avg_value=if(NEW.avg_value is null,avg_value,((avg_value*samples)+NEW.avg_value)/(samples+1)),
        samples=if(NEW.avg_value IS NULL,samples,samples+1),
        capacity=NEW.capacity,
        min_value=if((min_value < (NEW.min_value)),min_value,NEW.min_value),
        max_value=if((max_value > (NEW.max_value)),max_value,NEW.max_value)
      where snapshot_time<=>date_format(NEW.snapshot_time,"%Y-%m-%d 00:00:00")
            and uuid<=>NEW.uuid and producer_uuid<=>NEW.producer_uuid
            and property_type<=>NEW.property_type and property_subtype<=>NEW.property_subtype and relation<=>NEW.relation
            and commodity_key<=>NEW.commodity_key;

    ELSE

      insert into vpod_stats_by_day (snapshot_time,uuid,producer_uuid,property_type,property_subtype,capacity,relation,commodity_key,min_value,max_value,avg_value,samples) values
        (date_format(NEW.snapshot_time,"%Y-%m-%d 00:00:00"),
          NEW.uuid,
          NEW.producer_uuid,
          NEW.property_type,
          NEW.property_subtype,
          NEW.capacity,
          NEW.relation,
          NEW.commodity_key,
          NEW.min_value,
          NEW.max_value,
          NEW.avg_value,
         if(NEW.avg_value IS NULL,0,1));

    END IF;


    -- MONTHLY
    IF EXISTS (select 1 from vpod_stats_by_month where snapshot_time<=>date_format(last_day(NEW.snapshot_time),"%Y-%m-%d 00:00:00")
                                                       and uuid<=>NEW.uuid
                                                       and producer_uuid<=>NEW.producer_uuid
                                                       and property_type<=>NEW.property_type
                                                       and property_subtype<=>NEW.property_subtype
                                                       and relation<=>NEW.relation
                                                       and commodity_key<=>NEW.commodity_key limit 1 ) THEN

      update vpod_stats_by_month set avg_value=if(NEW.avg_value is null,avg_value,((avg_value*samples)+NEW.avg_value)/(samples+1)),
        samples=if(NEW.avg_value IS NULL,samples,samples+1),
        capacity=NEW.capacity,
        min_value=if((min_value < (NEW.min_value)),min_value,NEW.min_value),
        max_value=if((max_value > (NEW.max_value)),max_value,NEW.max_value)
      where snapshot_time<=>date_format(last_day(NEW.snapshot_time),"%Y-%m-%d 00:00:00")
            and uuid<=>NEW.uuid and producer_uuid<=>NEW.producer_uuid
            and property_type<=>NEW.property_type and property_subtype<=>NEW.property_subtype and relation<=>NEW.relation
            and commodity_key<=>NEW.commodity_key;

    ELSE

      insert into vpod_stats_by_month (snapshot_time,uuid,producer_uuid,property_type,property_subtype,capacity,relation,commodity_key,min_value,max_value,avg_value,samples) values
        (date_format(last_day(NEW.snapshot_time),"%Y-%m-%d 00:00:00"),
          NEW.uuid,
          NEW.producer_uuid,
          NEW.property_type,
          NEW.property_subtype,
          NEW.capacity,
          NEW.relation,
          NEW.commodity_key,
          NEW.min_value,
          NEW.max_value,
          NEW.avg_value,
         if(NEW.avg_value IS NULL,0,1));

    END IF;

  END //
DELIMITER ;


