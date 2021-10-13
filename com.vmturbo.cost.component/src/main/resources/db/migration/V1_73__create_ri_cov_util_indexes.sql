
DELIMITER //

DROP PROCEDURE IF EXISTS add_ri_cov_util_indexes //

CREATE PROCEDURE add_ri_cov_util_indexes()
  BEGIN

    DECLARE CONTINUE HANDLER FOR 1061 BEGIN END;

    create index ricl_st on reserved_instance_coverage_latest(snapshot_time);
    create index ricbh_st on reserved_instance_coverage_by_hour(snapshot_time);
    create index ricbd_st on reserved_instance_coverage_by_day(snapshot_time);
    create index ricbm_st on reserved_instance_coverage_by_month(snapshot_time);

    create index riul_st on reserved_instance_utilization_latest(snapshot_time);
    create index riubh_st on reserved_instance_utilization_by_hour(snapshot_time);
    create index riubd_st on reserved_instance_utilization_by_day(snapshot_time);
    create index riubm_st on reserved_instance_utilization_by_month(snapshot_time);
END //

DELIMITER ;

CALL add_ri_cov_util_indexes;
DROP PROCEDURE IF EXISTS add_ri_cov_util_indexes;



