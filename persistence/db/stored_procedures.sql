/*
 * This file contains stored procedures used by vmtdb
 * @ariel_tal
 */

use vmtdb ;

/*
 * CLUSTER_STATS PROCEDURES
 */

/**
 * Used to insert a single clusters' aggregated info of the previous day
 * into the cluster_stats_by_day table
 */

source /srv/rails/webapps/persistence/db/clusterAggPreviousDay.sql;
source /srv/rails/webapps/persistence/db/populate_AllClusters_PreviousDayAggStats.sql;
