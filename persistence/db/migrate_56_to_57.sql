use vmtdb ;

start transaction ;

/*
 * THIS IS A MARKER MIGRATION TO KEEP THE DB VERSION 
 * IN SYNC WITH THE BRANCH  
 */

delete from version_info where id=1;
insert into version_info values (1,57) ;

commit ;