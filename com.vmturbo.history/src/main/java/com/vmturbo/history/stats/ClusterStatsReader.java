package com.vmturbo.history.stats;

import static com.vmturbo.components.common.utils.StringConstants.CPU_HEADROOM;
import static com.vmturbo.components.common.utils.StringConstants.MEM_HEADROOM;
import static com.vmturbo.components.common.utils.StringConstants.PROPERTY_TYPE;
import static com.vmturbo.components.common.utils.StringConstants.STORAGE_HEADROOM;
import static com.vmturbo.components.common.utils.StringConstants.TOTAL_HEADROOM;
import static com.vmturbo.history.db.jooq.JooqUtils.getDateField;
import static com.vmturbo.history.db.jooq.JooqUtils.getStringField;
import static com.vmturbo.history.schema.abstraction.tables.ClusterStatsByDay.CLUSTER_STATS_BY_DAY;
import static com.vmturbo.history.schema.abstraction.tables.ClusterStatsByMonth.CLUSTER_STATS_BY_MONTH;
import static org.jooq.impl.DSL.min;

import java.math.BigDecimal;
import java.sql.Date;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.jooq.Condition;
import org.jooq.Record;
import org.jooq.Record5;
import org.jooq.Select;
import org.jooq.Table;
import org.jooq.impl.DSL;

import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.history.db.BasedbIO.Style;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.db.jooq.JooqUtils;
import com.vmturbo.history.schema.abstraction.tables.records.ClusterStatsByDayRecord;
import com.vmturbo.history.schema.abstraction.tables.records.ClusterStatsByMonthRecord;

/**
 * This class is responsible for reading data from the cluster_stats tables.
 */
class ClusterStatsReader {
    private final HistorydbIO historydbIO;
    private static final Set<String> TOTAL_HEADROOM_STATS = ImmutableSet.of(
        CPU_HEADROOM,
        MEM_HEADROOM,
        STORAGE_HEADROOM);

    ClusterStatsReader(HistorydbIO historydbIO) {
        this.historydbIO = historydbIO;
    }

    /**
     * Gets stats records from CLUSTER_STATS_BY_DAY table based on an optional date range and a list
     * of commodity names.
     *
     * @param clusterUuid Cluster ID
     * @param startDate The start date of the date range
     * @param endDate The end date of the date range
     * @param commodityNames Names in the property_type of each record
     * @return A list of statistics records within the date range and with record property_types
     *         that match commodity names.
     * @throws VmtDbException vmtdb exception
     */
    @Nonnull List<ClusterStatsByDayRecord> getStatsRecordsByDay(
            @Nonnull Long clusterUuid,
            @Nonnull Long startDate,
            @Nonnull Long endDate,
            @Nonnull Set<String> commodityNames)
            throws VmtDbException {
        Objects.requireNonNull(clusterUuid);
        Objects.requireNonNull(startDate);
        Objects.requireNonNull(endDate);
        Objects.requireNonNull(commodityNames);

        return historydbIO.execute(Style.FORCED,
            getClusterStatsQuery(CLUSTER_STATS_BY_DAY, clusterUuid, startDate, endDate, commodityNames))
            .into(ClusterStatsByDayRecord.class);
    }

    /**
     * Gets stats records from CLUSTER_STATS_BY_MONTH table based on an date range and a
     * list of commodity names.
     *
     * @param clusterUuid Cluster ID
     * @param startDate The start date of the date range
     * @param endDate The end date of the date range
     * @param commodityNames Names in the property_type of each record
     * @return A list of statistics records within the date range and with record property_types
     *         that match commodity names.
     * @throws VmtDbException vmtdb exception
     */
    @Nonnull List<ClusterStatsByMonthRecord> getStatsRecordsByMonth(
            @Nonnull Long clusterUuid,
            @Nonnull Long startDate,
            @Nonnull Long endDate,
            @Nonnull Set<String> commodityNames)
            throws VmtDbException {
        Objects.requireNonNull(clusterUuid);
        Objects.requireNonNull(startDate);
        Objects.requireNonNull(endDate);
        Objects.requireNonNull(commodityNames);

        return historydbIO.execute(Style.FORCED,
            getClusterStatsQuery(CLUSTER_STATS_BY_MONTH, clusterUuid, startDate, endDate, commodityNames))
            .into(ClusterStatsByMonthRecord.class);
    }

    /**
     * A utility class for reading {@link ClusterStatsByDayRecord} and {@link ClusterStatsByMonthRecord}
     * records using a common interface, since the schemas are very similar (except for the
     * "aggregated" and "# samples" fields).
     */
    public interface ClusterStatsRecordReader {
        /**
         * Get the recorded-on date from the cluster stat record.
         *
         * @return the recorded-on date
         */
        Date getRecordedOn();

        /**
         * get the internal name from the cluster stat record.
         *
         * @return the internal name
         */
        String getInternalName();

        /**
         * Get the property type from the cluster stat record.
         *
         * @return the property type
         */
        String getPropertyType();

        /**
         * Get the property subtype from the cluster stat record.
         *
         * @return the property subtype
         */
        String getPropertySubType();

        /**
         * Get the property value from the cluster stat record.
         *
         * @return value
         */
        float getValue();
    }

    /**
     * A {@link ClusterStatsRecordReader} that wraps access to a {@link ClusterStatsByDayRecord}.
     */
    public static class ClusterStatsByDayRecordReader implements ClusterStatsRecordReader {
        @Nonnull final ClusterStatsByDayRecord record;

        /**
         * Create an instance that wraps the specified record.
         *
         * @param record the record to read from
         */
        ClusterStatsByDayRecordReader(@Nonnull ClusterStatsByDayRecord record) {
            Objects.requireNonNull(record);
            this.record = record;
        }

        @Override
        public Date getRecordedOn() {
            return record.getRecordedOn();
        }

        @Override
        public String getInternalName() {
            return record.getInternalName();
        }

        @Override
        public String getPropertyType() {
            return record.getPropertyType();
        }

        @Override
        public String getPropertySubType() {
            return record.getPropertySubtype();
        }

        @Override
        public float getValue() {
            return record.getValue().floatValue();
        }
    }

    /**
     * A {@link ClusterStatsRecordReader} that wraps access to a {@link ClusterStatsByMonthRecord}.
     */
    public static class ClusterStatsByMonthRecordReader implements ClusterStatsRecordReader {
        @Nonnull final ClusterStatsByMonthRecord record;

        /**
         * Create an instance that provides reads from the specified record.
         *
         * @param record the record to read from
         */
        ClusterStatsByMonthRecordReader(@Nonnull ClusterStatsByMonthRecord record) {
            Objects.requireNonNull(record);
            this.record = record;
        }

        @Override
        public Date getRecordedOn() {
            return record.getRecordedOn();
        }

        @Override
        public String getInternalName() {
            return record.getInternalName();
        }

        @Override
        public String getPropertyType() {
            return record.getPropertyType();
        }

        @Override
        public String getPropertySubType() {
            return record.getPropertySubtype();
        }

        @Override
        public float getValue() {
            return record.getValue().floatValue();
        }
    }

    /**
     * Get the full table Select query from the provided table.
     * @param table The table to create the select for.
     * @param clusterUuid The UUID of the Cluster to select stats for.
     * @param startDate The start date of the range to build the select statement for.
     * @param endDate The end date of the range to build the select statement for.
     * @param commodityNames Names in the property_type of each record.
     * @return a Select statement that will get the TotalHeadroom statistics from the provided table.
     */
    private Select<?> getClusterStatsQuery(@Nonnull Table<? extends Record> table,
                                                @Nonnull Long clusterUuid,
                                                @Nonnull Long startDate,
                                                @Nonnull Long endDate,
                                                @Nonnull Set<String> commodityNames
    ) {
        final boolean shouldQueryTotalHeadroom = commodityNames.contains(TOTAL_HEADROOM);

        //Remove TOTAL_HEADROOM stat since we will collect that separately
        final Set<String> filteredCommodityNames = Sets.difference(commodityNames, ImmutableSet.of(TOTAL_HEADROOM));

        if (shouldQueryTotalHeadroom && filteredCommodityNames.isEmpty()) {
            //If only requesting TotalHeadroom, simply query that then return with query results.
            return getTotalClusterHeadroomSelect(table, true, clusterUuid, startDate, endDate);
        }

        //Retrieve the conditions to be used in querying the the cluster_stats_by_day table
        final List<Condition> conditions =
            getConditionsForClusterStatsQuery(table, true, clusterUuid, startDate, endDate, filteredCommodityNames);

        if (shouldQueryTotalHeadroom) {
            //When we get here, we should UNION ALL the Total Headroom stats and any other stats requested.
            final Select<Record5<Date, String, String, String, BigDecimal>> totalHeadroomStatSelect =
                getTotalClusterHeadroomSelect(table, true, clusterUuid, startDate, endDate);
            final Select<Record5<Date, String, String, String, BigDecimal>> otherHeadroomStatsSelect =
                getClusterStatsSelect(table, conditions);

            return totalHeadroomStatSelect.unionAll(otherHeadroomStatsSelect);
        }

        return historydbIO.JooqBuilder().selectFrom(table).where(conditions).getQuery();
    }

    /**
     * Get the Cluster Headroom Stats Select statement when also requesting Total Headroom stats.
     *
     * @param table the table to request from.
     * @param conditions the where conditions for the query.
     * @return the Select query for gathering the Headroom stats.
     */
    private Select<Record5<Date, String, String, String, BigDecimal>> getClusterStatsSelect(@Nonnull Table table,
                                                                                            @Nonnull List<Condition> conditions) {
        return historydbIO.JooqBuilder()
            .select(
                getDateField(table, StringConstants.RECORDED_ON),
                getStringField(table, StringConstants.INTERNAL_NAME),
                getStringField(table, StringConstants.PROPERTY_TYPE),
                getStringField(table, StringConstants.PROPERTY_SUBTYPE),
                JooqUtils.getBigDecimalField(table, StringConstants.VALUE)
            ).from(table)
            .where(conditions).getQuery();
    }

    /**
     * To gather requestable statistic: TotalHeadroom which is an aggregation of 3 other Headroom
     *  statistics: CPU_HEADROOM, MEM_HEADROOM, and STORAGE HEADROOM.
     *
     * Total Headroom is the following:
     *  - Total Headroom Capacity: MIN(CPU_HEADROOM, MEM_HEADROOM, STORAGE_HEADROOM) capacities.
     *  - Total Headroom Values: MIN(CPU_HEADROOM, MEM_HEADROOM, STORAGE_HEADROOM) values.
     *
     *  EXAMPLE:
     *  - CPU Headroom: capacity = 100, value (headroom VMs) = 20  => used VMs (UI calculated): 80
     *  - MEM Headroom: capacity = 80, value (headroom VMs) = 30   => used VMs (UI calculated): 50
     *  - STG Headroom: capacity = 60, value (headroom VMs) = 40   => used VMs (UI calculated): 20
     *
     *  - Total Headroom: capacity = 60, value (headroom VMs) = 20    => used VMs (UI calculated): 40
     *
     * @param table The table to create the select for.
     * @param isDayTable whether or not we are requesting stats from the DAY table.
     * @param clusterUuid The UUID of the Cluster to select stats for.
     * @param startDate The start date of the range to build the select statement for.
     * @param endDate The end date of the range to build the select statement for.
     * @return a Select statement that will get the TotalHeadroom statistics from the provided table.
     */
    private Select<Record5<Date, String, String, String, BigDecimal>> getTotalClusterHeadroomSelect(
                                                                        @Nonnull Table table,
                                                                        boolean isDayTable,
                                                                        @Nonnull Long clusterUuid,
                                                                        @Nonnull Long startDate,
                                                                        @Nonnull Long endDate) {
        final List<Condition> conditions =
            getConditionsForClusterStatsQuery(table, isDayTable, clusterUuid, startDate, endDate, TOTAL_HEADROOM_STATS);

        return historydbIO.JooqBuilder()
            .select(getDateField(table, StringConstants.RECORDED_ON),
                getStringField(table, StringConstants.INTERNAL_NAME),
                DSL.inline(TOTAL_HEADROOM).as(getStringField(table, StringConstants.PROPERTY_TYPE)),
                getStringField(table, StringConstants.PROPERTY_SUBTYPE),
                min(JooqUtils.getBigDecimalField(table, StringConstants.VALUE))
                    .as(JooqUtils.getBigDecimalField(table, StringConstants.VALUE)))
            .from(table)
            .where(conditions)
            .groupBy(
                getDateField(table, StringConstants.RECORDED_ON),
                getStringField(table, StringConstants.INTERNAL_NAME),
                getStringField(table, StringConstants.PROPERTY_SUBTYPE)
            );
    }

    /**
     * Get the conditions necessary for making the select queries to retrieve Headroom statistics.
     *
     * @param table The table to create the conditions for.
     * @param isDayTable whether or not we are requesting stats from the DAY table.
     * @param clusterUuid The UUID of the Cluster to select stats for.
     * @param startDate The start date of the range to build the select statement for.
     * @param endDate The end date of the range to build the select statement for.
     * @param commodityNames Names in the property_type of each record.
     * @return The List of Condition for the headroom query.
     */
    private List<Condition> getConditionsForClusterStatsQuery(@Nonnull Table table,
                                                              boolean isDayTable,
                                                              @Nonnull Long clusterUuid,
                                                              @Nonnull Long startDate,
                                                              @Nonnull Long endDate,
                                                              @Nonnull Set<String> commodityNames) {
        final List<Condition> conditions = new ArrayList<>();
        conditions.add(getStringField(table, StringConstants.INTERNAL_NAME)
            .eq(Long.toString(clusterUuid)));

        // Don't add inCommodityNames condition if commodityNames is empty.
        if (!commodityNames.isEmpty()) {
            conditions.add(getStringField(table, PROPERTY_TYPE).in(commodityNames));
        }

        conditions.add(getDateConditionForClusterStatsQuery(table, isDayTable, clusterUuid, startDate, endDate, commodityNames));

        return conditions;
    }

    /**
     * Build and return the appropriate Condition for the date range when querying the
     * cluster_stats_by_day table.
     *
     * @param table The table to create the conditions for.
     * @param isDayTable whether or not we are requesting stats from the DAY table.
     * @param clusterUuid The UUID of the Cluster to select stats for.
     * @param startDate The start date of the range to build the select statement for.
     * @param endDate The end date of the range to build the select statement for.
     * @param commodityNames Names in the property_type of each record.
     * @return The Condition for the date range.
     */
    private Condition getDateConditionForClusterStatsQuery(@Nonnull Table table,
                                                           boolean isDayTable,
                                                           @Nonnull Long clusterUuid,
                                                           @Nonnull Long startDate,
                                                           @Nonnull Long endDate,
                                                           @Nonnull Set<String> commodityNames) {
        if (isDayTable && startDate.equals(endDate)) {
            //When querying the day table, if the startDate=endDate, create a subquery condition
            //to retrieve the most recent records
            final List<Condition> conditions = new ArrayList<>();
            conditions.add(getStringField(table, StringConstants.INTERNAL_NAME).eq(Long.toString(clusterUuid)));

            // Don't add inCommodityNames condition if commodityNames is empty.
            if (!commodityNames.isEmpty()) {
                conditions.add(getStringField(table, PROPERTY_TYPE).in(commodityNames));
            }
            conditions.add(getDateField(table, StringConstants.RECORDED_ON).lessOrEqual(new java.sql.Date(startDate)));

            // Fetch the most recent records for this cluster
            return getDateField(table, StringConstants.RECORDED_ON).eq(
                historydbIO.JooqBuilder()
                    .select(CLUSTER_STATS_BY_DAY.RECORDED_ON.max())
                    .from(CLUSTER_STATS_BY_DAY)
                    .where(conditions));
        }
        return getDateField(table, StringConstants.RECORDED_ON)
            .between(new java.sql.Date(startDate), new java.sql.Date(endDate));
    }
}
