package com.vmturbo.history.stats;

import static com.vmturbo.reports.db.abstraction.Tables.CLUSTER_STATS_BY_DAY;
import static com.vmturbo.reports.db.abstraction.Tables.PM_STATS_BY_DAY;
import static org.jooq.impl.DSL.sum;

import java.math.BigDecimal;
import java.sql.Date;
import java.time.Clock;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.InsertSetMoreStep;
import org.jooq.Query;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.Select;
import org.jooq.Table;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.reports.db.BasedbIO;
import com.vmturbo.reports.db.VmtDbException;

/**
 * DB Methods to write Cluster Stats.
 *
 * Cluster Stats are maintained by_day and rolled up by_month.
 *
 **/
class ClusterStatsWriter {

    // these are the two column names in the aggregated PM_STATS DB rows used to name the
    // calculated sum and average over the desired stats rows.
    private static final String SUM_OF_CAPACITY = "sum_cap";
    private static final String SUM_OF_AVERAGE = "sum_avg";


    private final HistorydbIO historydbIO;

    /**
     *  for each type of Cluster, specify which DB table to gather from to calculate CLUSTER stats
     */
    private static Map<ClusterInfo.Type, Table> dbTablesToQuery =
            ImmutableMap.<ClusterInfo.Type, Table>builder()
                    .put(ClusterInfo.Type.COMPUTE, PM_STATS_BY_DAY)
//                    .put(ClusterInfo.Type.STORAGE, DS_STATS_BY_DAY) TODO: handle DS stats in OM-23047
                    .build();

    /**
     * For a subset of PROPERTY_TYPEs from the source table set, specify one or more
     * Cluster Stats Value writer initialized with the desired PROPERTY_TYPE and
     * PROPERTY_SUBTYPE values to use in writing the CLUSTER_STATS_BY_DAY row.
     */
    private static Map<String, List<PMClusterStatsValue>> computeClusterStatTransforms = ImmutableMap.of(
            "CPU", ImmutableList.of(
                    new PMClusterStatsValue("CPU", "currentUtilization", SUM_OF_CAPACITY)),
            "numCPUs", ImmutableList.of(
                    new PMClusterStatsValue("CPU", "numCPUs", SUM_OF_AVERAGE)),
            "numSockets", ImmutableList.of(
                    new PMClusterStatsValue("CPU", "numSockets", SUM_OF_AVERAGE)),
            "Mem", ImmutableList.of(
                    new PMClusterStatsValue("Mem", "capacity", SUM_OF_CAPACITY),
                    new PMClusterStatsValue("Mem", "currentUtilization", SUM_OF_AVERAGE))
    );

    private static final Logger logger = LogManager.getLogger();

    ClusterStatsWriter(HistorydbIO historydbIO) {
        this.historydbIO = historydbIO;
    }

    /**
     * Calculate and write the stats for each cluster given.
     *
     * For each cluster, read from the PM_STATS_BY_DAY table and aggregate over PMs in the cluster.
     *
     * We will "upsert" the data, using "ON DUPLICATE KEY" to either add a new row or
     * update an existing row.
     *
     * @param clusters a list of Cluster objects, each of which will have stats rollup performed.
     */
    void rollupClusterStats(@Nonnull final Map<Long, ClusterInfo> clusters) {

        // Calculate the date for "today" and convert to a SQL DB-style date
        LocalDate today = LocalDate.now(Clock.systemUTC());
        Date dbToday = Date.valueOf(today);
        Date dbTomorrow = Date.valueOf(today.plus(1, ChronoUnit.DAYS));

        // roll up each cluster, one at a time
        clusters.forEach((clusterOID, clusterInfo) -> {
            // note that we're using the OID for the unique name for clusters - the display_name
            // is not currently unique
            final String clusterName = clusterInfo.getName();

            Table dbTableToQuery = dbTablesToQuery.get(clusterInfo.getClusterType());
            if (dbTableToQuery == null) {
                logger.debug("Unhandled Cluster Type: {}", clusterInfo.getClusterType());
                // "return" in forEach == "continue" in a regular "for" loop.
                return;
            }

            // determine the list of elements in the Cluster to aggregate stats over
            List<String> memberOidStrings = clusterInfo.getMembers().getStaticMemberOidsList()
                    .stream()
                    .map(l -> Long.toString(l))
                    .collect(Collectors.toList());

            // we handle COMPUTE or STORAGE clusters - the tables have identical columns
            try {
                Select<?> clusterStatsSelect = getClusterStatsSelect(dbTableToQuery, dbToday,
                        dbTomorrow, memberOidStrings);
                Result<? extends Record> pmStatsInCluster = historydbIO.execute(BasedbIO.Style.FORCED,
                        clusterStatsSelect);
                logger.debug("cluster: {} {}, row count: {}", clusterName, clusterOID,
                        pmStatsInCluster.size());
                if (pmStatsInCluster.isNotEmpty()) {
                    // aggregate stats for this cluster
                    List<Query> clusterStatsInserts = new ArrayList<>();

                    for (Record pmStatRecord : pmStatsInCluster) {

                        String statKey = pmStatRecord.getValue(PM_STATS_BY_DAY.PROPERTY_TYPE);

                        if (computeClusterStatTransforms.containsKey(statKey)) {
                            for (PMClusterStatsValue t : computeClusterStatTransforms.get(statKey)) {
                                InsertSetMoreStep<?> insertStmt = getBaseClusterStatInsert(dbToday,
                                        clusterOID);
                                clusterStatsInserts.add(t.populateClusterStatsInsert(pmStatRecord,
                                        insertStmt));
                            }
                        }
                    }
                    // calculate the number of hosts in the cluster
                    final int numHostsInCluster = clusterInfo.getMembers()
                            .getStaticMemberOidsCount();

                    // add a statement to the list to persist it to the CLUSTER_STATS table
                    InsertSetMoreStep<?> insertStmt = getBaseClusterStatInsert(dbToday, clusterOID);
                    clusterStatsInserts.add(new NumHostsTransform()
                            .populateClusterStatsInsert(numHostsInCluster, insertStmt));

                    // do the inserts in the list
                    logger.debug("cluster {} stats insert stmt count: {}", clusterOID,
                            clusterStatsInserts.size());
                    historydbIO.execute(BasedbIO.Style.FORCED, clusterStatsInserts);

                }
            } catch (VmtDbException e) {
                // error checking stats for this cluster; log the error and keep trying.
                logger.error("Error persisting stats for cluster " + clusterOID +
                        ";  continuing", e);
            }
        });
    }

    /**
     * Create a DB insert statement for the CLUSTER_STATS_BY_DAY table, starting with the date
     * and cluster oid. Return the DB insert statement so other fields may be added to the
     * row to be inserted.
     *
     * Note that in Classic OpsManager the key is "internal_name", but in XL we are switching
     * to uniform usage of OIDs.
     *
     * @param dbToday the {@link java.sql.Date} for this rollup
     * @param clusterOid the unique OID of the cluster to use as the key
     * @return an SQL statement to insert a row in CLUSTER_STATS_BY_DAY with the RECORDED_ON
     * and INTERNAL_NAME values provided
     */
    private InsertSetMoreStep<?> getBaseClusterStatInsert(Date dbToday, long clusterOid) {
        return historydbIO.
                getCommodityInsertStatement(CLUSTER_STATS_BY_DAY)
                .set(CLUSTER_STATS_BY_DAY.RECORDED_ON, dbToday)
                .set(CLUSTER_STATS_BY_DAY.INTERNAL_NAME, Long.toString(clusterOid));
    }

    /**
     * Request stats used in ClusterRollup. These stats come from the PM_STATS_BY_DAY table and
     * are averaged over the given day.
     *
     * @param dbTableToQuery - database table to query for the base stats - PM_STATS_BY_DAY
     * @param today today at 0:00 AM
     * @param tomorrow tomorrw at 0:00 AM
     * @param pmOidStrings list of OIDs of PMs in the desired cluster
     * @return a Select statement to query and average / sum the desired values.
     */
    @SuppressWarnings("unchecked")
    private Select<?> getClusterStatsSelect(@Nonnull Table dbTableToQuery,
                                           @Nonnull Date today,
                                           @Nonnull Date tomorrow,
                                           @Nonnull List<String> pmOidStrings) {
        return historydbIO.JooqBuilder()
                .select(dbTableToQuery.field(PM_STATS_BY_DAY.PROPERTY_TYPE),
                        dbTableToQuery.field(PM_STATS_BY_DAY.PROPERTY_SUBTYPE),
                        sum(dbTableToQuery.field(PM_STATS_BY_DAY.CAPACITY)).as(SUM_OF_CAPACITY),
                        sum(dbTableToQuery.field(PM_STATS_BY_DAY.AVG_VALUE)).as(SUM_OF_AVERAGE))
                .from(dbTableToQuery)
                .where(dbTableToQuery.field(PM_STATS_BY_DAY.UUID).in(pmOidStrings))
                .and(dbTableToQuery.field(PM_STATS_BY_DAY.SNAPSHOT_TIME).between(today, tomorrow))
                .groupBy(dbTableToQuery.field(PM_STATS_BY_DAY.PROPERTY_TYPE),
                        dbTableToQuery.field(PM_STATS_BY_DAY.PROPERTY_SUBTYPE));
    }


    /**
     * Class to help with the mapping from the value type in a PM Stats DB aggregation record to a
     * desired PROPERTY_TYPE, PROPERTY_SUBTYPE. Note that the "sourceField" is the name of a
     * field in the DB row returned from PM_STATS_BY_DAY table.
     */
    private static class PMClusterStatsValue {

        private final String propertyType;
        private final String propertySubtype;
        private final String sourceField;

        PMClusterStatsValue(@Nonnull String propertyType,
                            @Nonnull String propertySubtype,
                            @Nonnull String sourceField) {

            this.propertyType = propertyType;
            this.propertySubtype = propertySubtype;
            this.sourceField = sourceField;
        }

        /**
         * Given a DB record, extract the desire value from the input aggregation record
         * and add clauses to the given DB insert statement to save that value to
         * the CLUSTER_STATS_BY_DAY table.
         *
         * Note that the returned DB insert statement will either add the value to a new row for this
         * combination of Date, Cluster Name, Property Type, Property Subtype; If there is already
         * a row with this composite key, then the value will be replaced in that row; no new
         * row will be inserted.
         *
         * @param pmStatRecord the aggreggate query record containing the value to extract
         * @param insertStmt the DB insert statement to add the "upsert" values to
         * @return the given DB insert statement with additional ".set" clauses to populate the
         * remainder of the DB row.
         */
        Query populateClusterStatsInsert(@Nonnull Record pmStatRecord,
                                         @Nonnull InsertSetMoreStep<?> insertStmt) {
            BigDecimal value = (BigDecimal) pmStatRecord.getValue(sourceField);
            return insertStmt
                    .set(CLUSTER_STATS_BY_DAY.PROPERTY_TYPE, propertyType)
                    .set(CLUSTER_STATS_BY_DAY.PROPERTY_SUBTYPE, propertySubtype)
                    .set(CLUSTER_STATS_BY_DAY.VALUE, value)
                    .onDuplicateKeyUpdate()
                    .set(CLUSTER_STATS_BY_DAY.VALUE, value);
        }
    }

    /**
     * Class to help saving a given number-of-hosts to CLUSTER_STATS_BY_DAY
     */
    private static class NumHostsTransform {

        /**
         * Given an int Number of Hosts, add clauses to the given insert statement to save
         * the numHostsInCluster to the CLUSTER_STATS_BY_DAY table. These clauses will
         * set the desired PROPERTY_TYPE, PROPERTY_SUBTYPE and VALUE in the DB row.
         *
         * @param numHostsInCluster the number to record in the CLUSTER_STATS table
         * @param insertStmt the DB insert statement to add the new
         * @return the given DB insert statement with additional ".set" clauses to
         * write the CLUSTER_STATS_BY_DAY row.
         */
        Query populateClusterStatsInsert(int numHostsInCluster,
                                         @Nonnull InsertSetMoreStep<?> insertStmt) {
            BigDecimal numHostsDbValue = BigDecimal.valueOf(numHostsInCluster);
            return insertStmt
                    .set(CLUSTER_STATS_BY_DAY.PROPERTY_TYPE, "Host")
                    .set(CLUSTER_STATS_BY_DAY.PROPERTY_SUBTYPE, "currentNumHosts")
                    .set(CLUSTER_STATS_BY_DAY.VALUE, numHostsDbValue)
                    .onDuplicateKeyUpdate()
                    .set(CLUSTER_STATS_BY_DAY.VALUE, numHostsDbValue);


        }
    }

}
