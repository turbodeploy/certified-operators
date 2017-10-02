

use vmtdb ;

start transaction ;


ALTER TABLE notifications CHANGE COLUMN snapshot_time clear_time bigint ;
ALTER TABLE notifications ADD COLUMN last_notify_time bigint ;


delete from version_info ;
insert into version_info values (1,29) ;


commit ;
