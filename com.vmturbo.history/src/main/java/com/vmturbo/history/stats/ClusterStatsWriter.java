package com.vmturbo.history.stats;

import static com.vmturbo.history.schema.abstraction.Tables.CLUSTER_STATS_BY_DAY;
import static com.vmturbo.history.schema.abstraction.Tables.PM_STATS_BY_DAY;
import static com.vmturbo.history.schema.abstraction.Tables.VM_STATS_BY_DAY;
import static org.jooq.impl.DSL.sum;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.InsertSetMoreStep;
import org.jooq.InsertValuesStep5;
import org.jooq.Query;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.Select;
import org.jooq.Table;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.history.db.BasedbIO;
import com.vmturbo.history.db.BasedbIO.Style;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;

import com.vmturbo.history.schema.abstraction.tables.records.ClusterStatsByDayRecord;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

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
    private static Map<GroupType, Table> dbTablesToQuery =
            ImmutableMap.<GroupType, Table>builder()
                    .put(GroupType.COMPUTE_HOST_CLUSTER, PM_STATS_BY_DAY)
//                    .put(ClusterInfo.Type.STORAGE, DS_STATS_BY_DAY) TODO: handle DS stats in OM-23047
                    .build();

    /**
     * For a subset of PROPERTY_TYPEs from the source table set, specify one or more
     * Cluster Stats Value writer initialized with the desired PROPERTY_TYPE and
     * PROPERTY_SUBTYPE values to use in writing the CLUSTER_STATS_BY_DAY row.
     */
    private static Map<String, List<PMClusterStatsValue>> computeClusterStatTransforms = ImmutableMap.of(
            StringConstants.CPU, ImmutableList.of(
                    new PMClusterStatsValue(StringConstants.CPU, StringConstants.USED, SUM_OF_AVERAGE),
                    new PMClusterStatsValue(StringConstants.CPU, StringConstants.CAPACITY, SUM_OF_CAPACITY)),
            StringConstants.NUM_CPUS, ImmutableList.of(
                    new PMClusterStatsValue(StringConstants.NUM_CPUS, StringConstants.NUM_CPUS, SUM_OF_AVERAGE)),
            StringConstants.NUM_SOCKETS, ImmutableList.of(
                    new PMClusterStatsValue(StringConstants.NUM_SOCKETS, StringConstants.NUM_SOCKETS, SUM_OF_AVERAGE)),
            StringConstants.MEM, ImmutableList.of(
                    new PMClusterStatsValue(StringConstants.MEM, StringConstants.CAPACITY, SUM_OF_CAPACITY),
                    new PMClusterStatsValue(StringConstants.MEM, StringConstants.USED, SUM_OF_AVERAGE))
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
    void rollupClusterStats(@Nonnull final Map<Long, Grouping> clusters) {

        // Calculate the date for "today" and convert to a SQL DB-style date
        LocalDate today = LocalDate.now(Clock.systemUTC());
        Date dbToday = Date.valueOf(today);
        Date dbTomorrow = Date.valueOf(today.plus(1, ChronoUnit.DAYS));

        // roll up each cluster, one at a time
        clusters.forEach((clusterOID, group) -> {
            final String clusterName = group.getDefinition().getDisplayName();

            GroupType clusterType = group.getDefinition().getType();
            Table dbTableToQuery = dbTablesToQuery.get(clusterType);
            if (dbTableToQuery == null) {
                logger.debug("Unhandled Cluster Type: {}", clusterType);
                // "return" in forEach == "continue" in a regular "for" loop.
                return;
            }

            // determine the list of elements in the Cluster to aggregate stats over
            List<String> memberOidStrings = GroupProtoUtil.getAllStaticMembers(group.getDefinition())
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
                    final int numHostsInCluster = GroupProtoUtil.getAllStaticMembers(group.getDefinition()).size();
                    // add a statement to the list to persist it to the CLUSTER_STATS table
                    InsertSetMoreStep<?> insertStmt = getBaseClusterStatInsert(dbToday, clusterOID);
                    clusterStatsInserts.add(new NumEntityTransform()
                            .populateClusterStatsInsert(numHostsInCluster, insertStmt, StringConstants.HOST, "currentNumHosts"));

                    // calculate the number of VMs in the cluster
                    final int numVMsInCluster = calculateNumberOfVMs(memberOidStrings, dbToday);
                    if (numVMsInCluster == 0) {
                        logger.info("No VMs found for cluster : {} with id {}", clusterName, clusterOID);
                    }
                    InsertSetMoreStep<?> insertStmtForVM = getBaseClusterStatInsert(dbToday, clusterOID);
                    clusterStatsInserts.add(new NumEntityTransform()
                        .populateClusterStatsInsert(numVMsInCluster, insertStmtForVM, StringConstants.VM_NUM_VMS, StringConstants.VM_NUM_VMS));

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

    private int calculateNumberOfVMs(final List<String> memberOidStrings, final Date today) throws VmtDbException {
        final Result<? extends Record> res = historydbIO.execute(Style.FORCED, HistorydbIO.getJooqBuilder()
             .selectCount()
             .from(HistorydbIO.getJooqBuilder().selectDistinct(VM_STATS_BY_DAY.UUID)
                 .from(VM_STATS_BY_DAY)
                 .where(VM_STATS_BY_DAY.PRODUCER_UUID.in(memberOidStrings))
                 .and(VM_STATS_BY_DAY.SNAPSHOT_TIME.equal(new Timestamp(today.getTime())))));
         return res.isEmpty() ? 0 : (int) res.get(0).getValue(0);
    }

    /**
     * Insert multiple records into the cluster_stats_by_day table.
     * Records are provided in table with <PropertyType, SubPropertyType, Value>
     * Current date will be used for the record.
     * If a record with the same primary key already exists, the value of the existing record.
     * will be updated with the new value.
     * @param clusterOID cluster OID.
     * @param clusterData with <PropertyType, SubPropertyType, Value>.
     * @throws VmtDbException if there is a problem writing to the DB.
     */
    public void batchInsertClusterStatsByDayRecord(final long clusterOID,
                    final com.google.common.collect.Table<String, String, BigDecimal> clusterData)
                    throws VmtDbException {
        LocalDate today = LocalDate.now(Clock.systemUTC());
        String clusterOid = Long.toString(clusterOID);
        Date dbToday = Date.valueOf(today);

        // Note : If we don't expect to run headroom twice in a day in any circumstances,
        // code to delete rows can be removed.
        // Delete old headroom values for same date for this cluster.
        historydbIO.execute(BasedbIO.Style.FORCED,
            HistorydbIO.getJooqBuilder()
            .delete(CLUSTER_STATS_BY_DAY)
            .where(CLUSTER_STATS_BY_DAY.RECORDED_ON.equal(dbToday)
                .and(CLUSTER_STATS_BY_DAY.INTERNAL_NAME.equal(clusterOid)))
                .and(CLUSTER_STATS_BY_DAY.PROPERTY_TYPE.in(StatsHistoryRpcService.HEADROOM_STATS))
                .and(CLUSTER_STATS_BY_DAY.PROPERTY_SUBTYPE.in(clusterData.columnKeySet())));

        // Insert new values with current date for this cluster.
        final InsertValuesStep5<ClusterStatsByDayRecord, Date, String, String, String, BigDecimal> insertStmt =
            HistorydbIO.getJooqBuilder().insertInto(CLUSTER_STATS_BY_DAY,
                CLUSTER_STATS_BY_DAY.RECORDED_ON,
                CLUSTER_STATS_BY_DAY.INTERNAL_NAME,
                CLUSTER_STATS_BY_DAY.PROPERTY_TYPE,
                CLUSTER_STATS_BY_DAY.PROPERTY_SUBTYPE,
                CLUSTER_STATS_BY_DAY.VALUE);

        // Expected size of batch is ~10 rows with 5 columns.
        clusterData.cellSet().forEach(cell -> {
            insertStmt.values(dbToday, clusterOid, cell.getRowKey(), cell.getColumnKey(), cell.getValue());
        });

        historydbIO.execute(BasedbIO.Style.FORCED, insertStmt);
    }

    /**
     * Insert a record into the cluster_stats_by_day table. Current date will be used for the record.
     * If a record with the same primary key already exists, the value of the existing record
     * will be updated with the new value.
     *
     * @param clusterOID cluster OID
     * @param propertyType property Type
     * @param propertySubtype property subtype
     * @param value value
     * @throws VmtDbException
     */
    public void insertClusterStatsByDayRecord(final long clusterOID,
                                              final String propertyType,
                                              final String propertySubtype,
                                              final BigDecimal value)
            throws VmtDbException {
        LocalDate today = LocalDate.now(Clock.systemUTC());
        Date dbToday = Date.valueOf(today);

        // create insert statement persist it to the CLUSTER_STATS table
        InsertSetMoreStep<?> insertStmt = getBaseClusterStatInsert(dbToday, clusterOID);
        insertStmt
                .set(CLUSTER_STATS_BY_DAY.PROPERTY_TYPE, propertyType)
                .set(CLUSTER_STATS_BY_DAY.PROPERTY_SUBTYPE, propertySubtype)
                .set(CLUSTER_STATS_BY_DAY.VALUE, value)
                .onDuplicateKeyUpdate()
                .set(CLUSTER_STATS_BY_DAY.VALUE, value);

        historydbIO.execute(BasedbIO.Style.FORCED, insertStmt);
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
     * Class to help saving a given number of hosts/VMs to CLUSTER_STATS_BY_DAY
     */
    private static class NumEntityTransform {

        /**
         * Given an int Number of Hosts/VM, add clauses to the given insert statement to save
         * the number of Hosts/VM in Cluster to the CLUSTER_STATS_BY_DAY table. These clauses will
         * set the desired PROPERTY_TYPE, PROPERTY_SUBTYPE and VALUE in the DB row.
         *
         * @param numEntityInCluster the number to record in the CLUSTER_STATS table
         * @param insertStmt the DB insert statement to add the new
         * @param propertyType property type for the column
         * @param propertySubtype property subtype for the column
         * @return the given DB insert statement with additional ".set" clauses to
         * write the CLUSTER_STATS_BY_DAY row.
         */
        Query populateClusterStatsInsert(int numEntityInCluster,
                                         @Nonnull InsertSetMoreStep<?> insertStmt,
                                         @Nonnull String propertyType,
                                         @Nonnull String propertySubtype) {
            BigDecimal numEntitiesDbValue = BigDecimal.valueOf(numEntityInCluster);
            return insertStmt
                    .set(CLUSTER_STATS_BY_DAY.PROPERTY_TYPE, propertyType )
                    .set(CLUSTER_STATS_BY_DAY.PROPERTY_SUBTYPE, propertySubtype)
                    .set(CLUSTER_STATS_BY_DAY.VALUE, numEntitiesDbValue)
                    .onDuplicateKeyUpdate()
                    .set(CLUSTER_STATS_BY_DAY.VALUE, numEntitiesDbValue);
        }
    }
}
