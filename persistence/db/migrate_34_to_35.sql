

use vmtdb ;

start transaction ;

/* add new on-demand reports */
INSERT INTO `standard_reports` VALUES (185,'Storage Access IO Latency - 30 Days','This report presents two independent views of information related to Latency in accessing storage. The first highlights data stores having the largest amounts of storage access latency.  The second lists Virtual Machines organized most to least in terms of latency in accessing storage. ','Digest Storage Top Latency','Performance Management/Storage','daily_digest_storage_top_latency_grid_30_days','Daily',NULL);
INSERT INTO `standard_reports` VALUES (186,'Storage Access IOPS - 30 Days','This report presents two independent views of information related to IOPS for storage. The first highlights data stores that possess the largest usage of IOPS.  The second lists Virtual Machines organized most to least in terms of their use of IOPS related to storage. ','Digest Storage Top Latency','Performance Management/Storage','daily_digest_storage_top_iops_grid_30_days','Daily',NULL);
INSERT INTO `standard_reports` VALUES (187,'Storage Access IO Latency - 7 Days','This report presents two independent views of information related to Latency in accessing storage. The first highlights data stores having the largest amounts of storage access latency.  The second lists Virtual Machines organized most to least in terms of latency in accessing storage. ','Digest Storage Top Latency','Performance Management/Storage','daily_digest_storage_top_latency_grid_7_days','Daily',NULL);
INSERT INTO `standard_reports` VALUES (188,'Storage Access IOPS - 7 Days','This report presents two independent views of information related to IOPS for storage. The first highlights data stores that possess the largest usage of IOPS.  The second lists Virtual Machines organized most to least in terms of their use of IOPS related to storage. ','Digest Storage Top Latency','Performance Management/Storage','daily_digest_storage_top_iops_grid_7_days','Daily',NULL);

delete from version_info ;
insert into version_info values (1,35) ;


commit ;
