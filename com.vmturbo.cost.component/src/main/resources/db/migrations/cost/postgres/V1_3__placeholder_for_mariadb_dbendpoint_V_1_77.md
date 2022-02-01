Placeholder for the V1_77__drop_stored_procedures_and_events_converted_to_java.sql in 
db/migrations/cost/mariadb. We don't need to do the same in postgres (db/migrations/cost/postgres) 
since we didn't create any stored procedures in postgres, thus no need to drop them.