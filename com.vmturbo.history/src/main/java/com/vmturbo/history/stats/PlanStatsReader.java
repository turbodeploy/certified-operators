package com.vmturbo.history.stats;

import static com.vmturbo.history.schema.abstraction.Tables.MKT_SNAPSHOTS_STATS;

import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.jooq.Result;
import org.jooq.SelectConditionStep;

import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
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

    public @Nonnull List<MktSnapshotsStatsRecord> getStatsRecords(
            long topologyContextId,
            @Nonnull List<CommodityRequest> commodityRequests) throws VmtDbException {
            SelectConditionStep<MktSnapshotsStatsRecord> queryBuilder = historydbIO.JooqBuilder()
                    .selectFrom(MKT_SNAPSHOTS_STATS)
                    .where(MKT_SNAPSHOTS_STATS.MKT_SNAPSHOT_ID.equal(topologyContextId));

            // An empty list of commodity names means we're looking for all commodities.
            // A non-empty list means we're looking for specific ones.
            if (!commodityRequests.isEmpty()) {
                final List<String> commodityNames = commodityRequests.stream()
                        .map(CommodityRequest::getCommodityName)
                        .collect(Collectors.toList());

                queryBuilder = queryBuilder.and(MKT_SNAPSHOTS_STATS.PROPERTY_TYPE.in(commodityNames));
            }

            return (Result<MktSnapshotsStatsRecord>) historydbIO.execute(queryBuilder);
    }
}
