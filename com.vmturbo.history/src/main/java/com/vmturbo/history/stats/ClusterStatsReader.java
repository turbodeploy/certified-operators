package com.vmturbo.history.stats;

import static com.vmturbo.history.db.jooq.JooqUtils.dField;
import static com.vmturbo.history.db.jooq.JooqUtils.date;
import static com.vmturbo.history.db.jooq.JooqUtils.str;
import static com.vmturbo.components.common.utils.StringConstants.PROPERTY_TYPE;
import static com.vmturbo.history.schema.abstraction.tables.ClusterStatsByDay.CLUSTER_STATS_BY_DAY;
import static com.vmturbo.history.schema.abstraction.tables.ClusterStatsByMonth.CLUSTER_STATS_BY_MONTH;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import org.jooq.Condition;

import com.vmturbo.history.db.BasedbIO.Style;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.schema.abstraction.tables.records.ClusterStatsByDayRecord;
import com.vmturbo.history.schema.abstraction.tables.records.ClusterStatsByMonthRecord;

/**
 * This class is responsible for reading data from the cluster_stats tables.
 */
class ClusterStatsReader {
    private final HistorydbIO historydbIO;

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

        List<Condition> conditions = new ArrayList<>();
        conditions.add(CLUSTER_STATS_BY_DAY.INTERNAL_NAME.eq(Long.toString(clusterUuid)));
        // Don't add inCommodityNames condition if commodityNames is empty.
        if (!commodityNames.isEmpty()) {
            conditions.add(str(dField(CLUSTER_STATS_BY_DAY, PROPERTY_TYPE)).in(commodityNames));
        }

        final Condition dateCondition;
        if (startDate.equals(endDate)) {
            // Fetch the most recent records for this cluster
            dateCondition = CLUSTER_STATS_BY_DAY.RECORDED_ON.eq(
                historydbIO.JooqBuilder()
                    .select(CLUSTER_STATS_BY_DAY.RECORDED_ON.max())
                    .from(CLUSTER_STATS_BY_DAY)
                    .where(conditions)
                    .and(CLUSTER_STATS_BY_DAY.RECORDED_ON.lessOrEqual(new java.sql.Date(startDate))));
        } else {
            dateCondition = date(CLUSTER_STATS_BY_DAY.RECORDED_ON)
                .between(new java.sql.Date(startDate), new java.sql.Date(endDate));
        }
        conditions.add(dateCondition);

        return historydbIO.execute(Style.FORCED,
            historydbIO.JooqBuilder().selectFrom(CLUSTER_STATS_BY_DAY)
                .where(conditions).getQuery()).into(ClusterStatsByDayRecord.class);
    }

    /**
     * Gets stats records from CLUSTER_STATS_BY_MONTH table based on an optional date range and a
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
        List<Condition> conditions = new ArrayList<>();
        conditions.add(CLUSTER_STATS_BY_MONTH.INTERNAL_NAME.eq(Long.toString(clusterUuid)));
        // Don't add inCommodityNames condition if commodityNames is empty.
        if (!commodityNames.isEmpty()) {
            conditions.add(str(dField(CLUSTER_STATS_BY_MONTH, PROPERTY_TYPE)).in(commodityNames));
        }
        conditions.add(date(CLUSTER_STATS_BY_MONTH.RECORDED_ON)
            .between(new java.sql.Date(startDate), new java.sql.Date(endDate)));

        return historydbIO.execute(Style.FORCED,
            historydbIO.JooqBuilder().selectFrom(CLUSTER_STATS_BY_MONTH)
                .where(conditions).getQuery()).into(ClusterStatsByMonthRecord.class);
    }
}
