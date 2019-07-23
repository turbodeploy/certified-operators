package com.vmturbo.api.component.external.api.util.stats.query.impl;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.api.component.external.api.mapper.StatsMapper;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext.TimeWindow;
import com.vmturbo.api.component.external.api.util.stats.query.StatsSubQuery;
import com.vmturbo.api.component.external.api.util.stats.query.SubQuerySupportedStats;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedStatsResponse;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;

/**
 * Sub-query responsible for getting projected commodity stats from the history component.
 */
public class ProjectedCommodityStatsSubQuery implements StatsSubQuery {
    private final Duration liveStatsRetrievalWindow;
    private final StatsMapper statsMapper;
    private final StatsHistoryServiceBlockingStub statsServiceRpc;

    public ProjectedCommodityStatsSubQuery(@Nonnull final Duration liveStatsRetrievalWindow,
                                           @Nonnull final StatsMapper statsMapper,
                                           @Nonnull final StatsHistoryServiceBlockingStub statsServiceRpc) {
        this.liveStatsRetrievalWindow = liveStatsRetrievalWindow;
        this.statsMapper = statsMapper;
        this.statsServiceRpc = statsServiceRpc;
    }

    @Override
    public boolean applicableInContext(@Nonnull final StatsQueryContext context) {
        // OM-37484: give the startTime a +/- 60 second window for delineating between "current"
        // and "projected" stats requests. Without this window, the stats retrieval is too
        // sensitive to clock skew issues between the browser and the server, leading to incorrect
        // results in the UI.
        final boolean requestProjected = context.getTimeWindow()
            .map(TimeWindow::endTime)
            .map(endTime -> endTime > (context.getCurTime() + liveStatsRetrievalWindow.toMillis()))
            .orElse(false);
        return requestProjected && !context.getScope().isPlan();
    }

    @Override
    public SubQuerySupportedStats getHandledStats(@Nonnull final StatsQueryContext context) {
        return SubQuerySupportedStats.leftovers();
    }

    @Nonnull
    @Override
    public Map<Long, List<StatApiDTO>> getAggregateStats(@Nonnull final Set<StatApiInputDTO> stats,
                                                         @Nonnull final StatsQueryContext context) throws OperationFailedException {
        final ProjectedStatsRequest.Builder builder = ProjectedStatsRequest.newBuilder()
            .addAllEntities(context.getScopeEntities());
        stats.forEach(statApiInputDTO -> {
            // If necessary we can add support for other parts of the StatPeriodApiInputDTO,
            // and extend the Projected Stats API to serve the additional functionality.
            if (statApiInputDTO.getName() != null) {
                builder.addCommodityName(statApiInputDTO.getName());
            }
        });

        final ProjectedStatsResponse response = statsServiceRpc.getProjectedStats(builder.build());

        // create a StatSnapshotApiDTO from the ProjectedStatsResponse
        final StatSnapshotApiDTO projectedStatSnapshot = statsMapper.toStatSnapshotApiDTO(
            response.getSnapshot());

        // set the time of the snapshot to "future" using the "endDate" of the request
        //
        // The "get()" should be safe, because we only run this query if there is an explicit
        // end time.
        return ImmutableMap.of(context.getTimeWindow().get().endTime(), projectedStatSnapshot.getStatistics());
    }
}
