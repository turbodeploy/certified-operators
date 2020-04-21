package com.vmturbo.history.stats;

import static com.vmturbo.history.schema.abstraction.Tables.MKT_SNAPSHOTS_STATS;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;
import org.jooq.Result;
import org.jooq.SelectConditionStep;

import com.vmturbo.common.protobuf.stats.Stats.GlobalFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.schema.abstraction.tables.records.MktSnapshotsStatsRecord;

/**
 * Read records from the PLAN topologies stats table, mkt_snapshots_stats.
 **/
public class PlanStatsReader {

    private final HistorydbIO historydbIO;

    public PlanStatsReader(HistorydbIO historydbIO) {
        this.historydbIO = historydbIO;
    }

    /**
     * Retrieve stats records for the specified plan, based on the provided filters.
     *
     * @param topologyContextId the plan ID
     * @param commodityRequests a list of named commodities that have been requested
     * @param globalFilter the global filter to apply to all returned stats
     * @return a list of of Market Snapshot Stats Records containing the result
     * @throws VmtDbException when an error occurs while executing the query
     */
    public @Nonnull List<MktSnapshotsStatsRecord> getStatsRecords(
            long topologyContextId,
            @Nonnull List<CommodityRequest> commodityRequests,
            @Nonnull GlobalFilter globalFilter) throws VmtDbException {
        SelectConditionStep<MktSnapshotsStatsRecord> queryBuilder = historydbIO.JooqBuilder()
            .selectFrom(MKT_SNAPSHOTS_STATS)
            .where(MKT_SNAPSHOTS_STATS.MKT_SNAPSHOT_ID.equal(topologyContextId));

        final Set<Short> relatedEntityTypes = new HashSet<>();
        // An empty list of commodity names means we're looking for all commodities.
        // A non-empty list means we're looking for specific ones.
        if (!commodityRequests.isEmpty()) {
            final List<String> commodityNames = commodityRequests.stream()
                .map(CommodityRequest::getCommodityName)
                .filter(StringUtils::isNotEmpty)
                .collect(Collectors.toList());

            if (!commodityNames.isEmpty()) {
                queryBuilder = queryBuilder.and(MKT_SNAPSHOTS_STATS.PROPERTY_TYPE.in(commodityNames));
            }

            //TODO: Review this when we analyze the STATS API endpoint. An argument can be made
            // that we should be doing a separate query for each commodity request, so that each
            // can have its own relatedEntityTypes defined and they won't interact with each other.
            relatedEntityTypes.addAll(commodityRequests.stream()
                .filter(CommodityRequest::hasRelatedEntityType)
                .map(CommodityRequest::getRelatedEntityType)
                .map(ApiEntityType::fromString)
                .map(ApiEntityType::typeNumber)
                .map(Integer::shortValue)
                .collect(Collectors.toSet()));
        }

        // Respect any relatedEntityTypes defined globally for this request
        relatedEntityTypes.addAll(
            globalFilter.getRelatedEntityTypeList().stream()
                .map(ApiEntityType::fromString)
                .map(ApiEntityType::typeNumber)
                .map(Integer::shortValue)
                .collect(Collectors.toSet()));

        if (!relatedEntityTypes.isEmpty()) {
            queryBuilder = queryBuilder.and(MKT_SNAPSHOTS_STATS.ENTITY_TYPE.in(relatedEntityTypes));
        }

        return (Result<MktSnapshotsStatsRecord>) historydbIO.execute(queryBuilder);
    }
}
