

use vmtdb ;

start transaction ;




delete from version_info ;
insert into version_info values (1,39) ;
INSERT INTO `standard_reports` VALUES (189,'Socket Audit Report','This report gives a socket count per PM for licensing purposes ','Socket Audit Report','Capacity Management/Hosts','monthly_socket_audit_report','Monthly',NULL);

commit ;
