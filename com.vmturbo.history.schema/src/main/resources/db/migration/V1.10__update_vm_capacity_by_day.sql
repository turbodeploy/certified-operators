/*
  Modify the "vm_capacity_by_day" view to support report like "VM 15 Top Bottom Capacity".
  XL doesn't persist "utilization" property_subtype in vm_stats_by_* table. But this view requires
  this subtype. The changes will replace subtype "utilization" as "used", and related value (e.g. avg_value)
  to round(value/capacity, 3).
*/
 DROP TABLE IF EXISTS vm_capacity_by_day;
 DROP VIEW IF EXISTS vm_capacity_by_day;
 CREATE ALGORITHM=UNDEFINED
 VIEW vm_capacity_by_day AS
     select 'VirtualMachine' AS class_name,
         vm_stats_by_day.uuid,
         vm_stats_by_day.producer_uuid,
         vm_stats_by_day.property_type,
         round((vm_stats_by_day.avg_value / vm_stats_by_day.capacity),3) AS utilization,
         vm_stats_by_day.capacity,
         vm_stats_by_day.avg_value AS used_capacity,
         round((1.0 - vm_stats_by_day.avg_value),0) AS available_capacity,
         cast(vm_stats_by_day.snapshot_time as date) AS recorded_on
    from (vm_stats_by_day join vm_instances)
     where ((vm_stats_by_day.uuid = vm_instances.uuid)
         and (vm_stats_by_day.property_subtype = 'used')
         and (vm_stats_by_day.capacity > 0.00));
