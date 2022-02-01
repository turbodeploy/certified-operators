Placeholder for the V1_77__drop_stored_procedures_and_events_converted_to_java.sql in 
db/migrations/cost/mariadb. We don't need to do the same in mariadb legacy (db/migration) since
it's still using the stored procedures in mariadb, and we don't want to change the behavior. But
when DbEndpoint is enabled, we will use the java version of the procedure and need to drop the
ones in mariadb.