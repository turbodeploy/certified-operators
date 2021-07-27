# Extractor Data Extension Tool

This tool quickly (well, compared to ingestion!) creates a large extractor data set
for performance benchmarking or other purposes.

The tool begins with a representative data set that has already been created in a "sample data"
schema. It then replicates that data set some number of times to create an extended data set in a
"replica" schema.

Replication can occur in two dimensions: space and time

Space replication extends the size of the topology, by making copies of entities appearing
in the sample data. New records are created with new entity oids, so that the resulting topology
resembles a topology that really consisted of some number of copies of the sample topology.

Time replication refers to creating copies of the sample data with times adjusted into the past,
without overlap. The result is sort of a groundhog-day data set in which exactly the same metric
values, topology changes, etc. take place over and over again. The `--replica-gap` command line
parameter can be used to specify the separation between the first timestamp appearing in one
copy and the last timestamp appearing in the next (earlier) copy. By default, a 10-minute gap
is used.

Some tables have special extension algorithms. For example, the `entity` table is extended merely
by adjusting `first_seen` values to be relative to the time range of the final (earliest) replica,
rather than to the original sample data. In this case, only one copy of the sample records is
created in the replica table. The algorithm for the `scope` table is the most complicated.

## Preparation of Replica Tables

Before copying commences for a given table, the replica tables are "prepared," which means one of
two things:

* If a table already exists in the replica schema, it is merely truncated.

* Otherwise, it is created so as to be just like the table in the sample data schema, including
  provisioning of indexes. (Postgres' `CREATE TABLE (LIKE ... INCLUDING ALL)` is used.)
  
In the latter case, if the table is a hypertable, the following hypertable configuration is
copied from the sample data table to the replica table:

* Primary time-dimension column
* Chunk interval
* Compression enabled or not
* Segment-by columns for compression
* Order-by columns for compression

Note that compression policy and retention policy are _not_ set in the replica table. The main
reason is that, once we've finished creating the replica, we want it to be stable for subsequent
analysis. Having chunks compressed or dropped while that is ongoing would be problematic.

In addition, if the timescale operates on chunks while we're building a replica table, certain
operations could fail, like the script compressing a chunk that was already compressed by policy.

The following features of hypertables are specifically not implemented, and if we start using them 
this script should be extended as needed:

* Additional dimensions beyond the primary time dimension
* Tablespace attachments
* Data node attachments
* Other job configurations beyond compression and retention policies

### Pre-Creating Replica Tables

One benefit of the fact that preparation only truncates - but does not recreate - replica tables
when they already exist is that alternative configurations of a given table can be benchmarked quite
easily.

For example, if one wished to see whether an alternate configuration of `segmentby` and/or
`orderby` compression settings would impact size or performance, one can simply pre-create the table
to be tested in the replica schema, with the desired configuration. In that case, this script will
leave it as-is, only truncating it prior to copying in data.

# Command Line Arguments

## Database Connection

* `--db-host` or `-H`: Host name or IP address of DB server, defaults to `localhost`
* `--db-port` or `-p`: DB server port, defaults to `5432`
* `--db-user` or `-U:` Username for login to DB server, default is `extractor`
* `--db-password` or `-P`: Password for login user, defaults to standard passwd used in test
  environments
* `--db-database` or `-D`: Name of database to connect to, defaults to `extractor`
* `--db-schema` or `-S`: Schema that holds sample data tables, defaults to `extractor`

## Data Extension

* `--extend-tables`: multi-valued argument listing tables to be extended, defaults to all tables 
  currently supported for extension by this script. Follow by one or more table names you wish to 
  extend, as separate command line arguments . You can prefix any name with `!` to exclude that 
  table. If you only specify excluded tables, it will be as if you had listed all extendable tables, 
  and any exclusions are applied to that list.
* `--sample-start` or `--start`: specify inclusive beginning of range of timestamps to be included
  in sample data, defaults to min timestamp in `time` column of `metric` table, not counting
  records for cluster stats (since those are always stored with a time of midnight on their
  day of observation, which could cause the sample range to have an undesired period of no
  apparent activity spanning much or most of the first day).
* `--sample-end` or `--end`: specify exclusive end of range of timestamps to be included in sample
  data, defaults to one millisecond beyond max timestamp in `time` column of `metric` table.
* `--replica-schema`: schema where replica tables will be created, defaults to `extractor_extend`.
    * This schema must be on the same database as the sample data schema.
* `--init-replica-schema`: causes the script to drop (if present) and (re-)create the schema 
  specified with `--db-replica-schema`. The user specified with `--db_user` must have `CREATE`
  privileges in the database specified by `--db-database`. Also, the replica schema must not
  already exist, or the `--db-user` user must be the owner of that schema or otherwise be allowed
  to drop it.
    * Grant `CREATE` privileges with `GRANT CREATE ON DATABASE <database> TO <user>` to grant the
      needed privilege. You must be logged in to the database as the database owner or as a super
      user in order to do this.
* `--replica-count` or `-n`: specify the number of copies of the sample data to copy into the
  replica schema for time replication, defaults, to 1.
* `--scale-factor` or `-s`: specify the number of copies of the sample data to copy into the
  replica schema for space replication, defaults to 1.
* `--replica-gap`: specify an additional time gap to be used between consecutive replicas, e.g., to
  simulate a particular topology cycle time. Default is 10 minutes. Specify like `10m` or `1h 30m`
  etc.

## Miscellaneous

* `--log-level` or `-l`: Set desired logging level, defaults to `INFO`
* `--no-log-sizes`: suppress logging summaries of table sizes when the replication is complete,
which happens if this option is not used


Also:

* `mvn test -o` - to run unit tests (though they don't really exist yet)

# How to Run

## From the git repo
* `cd com.vmturbo.extractor.pyutil` - from your XL working tree root
* `../build/turbo-pipenv install` - to create a python virtual environment for this script
* `../build/turbo-pipenv run extend-data [args...]` - to execute the script

## Packaged as a docker image
This is convenient because there is no dependence on a local python installation - everything
is packaged in the image!

### Create the image
* `cd com.vmturbo.extractor.pyutil` - from your XL working tree root
* `docker build -t turbonomic/data-extender .` - or substitute our desired docker tag(s). 
Don't forget that final period - it's easy to miss!

### Zip up the image if you think you'll want to use it elsewhere
* `docker save -o /tmp/data-extender.zip turbonomic/data-extender` - substitute your desired zip 
file path

### Load the image from the zip file on some other machine  
* `docker load -i data-extender.zip`

### Run the tool from the container
* `docker run --rm turbonomic/data-extender extend-data -h` - or whatever options/args you want
