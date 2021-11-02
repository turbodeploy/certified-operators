package com.vmturbo.api.component.external.api.util.stats.query.impl;

import static com.vmturbo.api.component.external.api.util.StatsUtils.getProjectedStatSnapshotDate;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.api.component.external.api.mapper.StatsMapper;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext.TimeWindow;
import com.vmturbo.api.component.external.api.util.stats.query.StatsSubQuery;
import com.vmturbo.api.component.external.api.util.stats.query.SubQuerySupportedStats;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatFilterApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.enums.Epoch;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.components.common.stats.StatsUtils;

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
        return context.requestProjected() && !context.getInputScope().isPlan();
    }

    @Override
    public SubQuerySupportedStats getHandledStats(@Nonnull final StatsQueryContext context) {
        return SubQuerySupportedStats.leftovers();
    }

    @Nonnull
    @Override
    public List<StatSnapshotApiDTO> getAggregateStats(@Nonnull final Set<StatApiInputDTO> stats,
                                                      @Nonnull final StatsQueryContext context) throws OperationFailedException {
        final Set<Long> providerOids = getProviderOids(stats);
        final ProjectedStatsRequest.Builder builder = ProjectedStatsRequest.newBuilder()
            .addAllEntities(context.getQueryScope().getExpandedOids()).addAllProviders(providerOids);
        stats.forEach(statApiInputDTO -> {
                CommodityRequest.Builder commodityRequestBuilder = CommodityRequest.newBuilder();
                if (statApiInputDTO.getName() != null) {
                    commodityRequestBuilder.setCommodityName(statApiInputDTO.getName());
                }
                if (statApiInputDTO.getGroupBy() != null) {
                    commodityRequestBuilder.addAllGroupBy(statApiInputDTO.getGroupBy());
                }
                builder.addFilter(commodityRequestBuilder);
            }
        );

        final ProjectedStatsResponse response = statsServiceRpc.getProjectedStats(builder.build());

        // create a StatSnapshotApiDTO from the ProjectedStatsResponse
        final StatSnapshotApiDTO projectedStatSnapshot = statsMapper.toStatSnapshotApiDTO(
            response.getSnapshot());

        // set the time of the snapshot to "future" using the "endDate" of the request
        projectedStatSnapshot.setDate(getProjectedStatSnapshotDate(context.getTimeWindow(),
                context.getCurTime(), liveStatsRetrievalWindow));
        projectedStatSnapshot.setEpoch(Epoch.PROJECTED);
        return Collections.singletonList(projectedStatSnapshot);
    }

    private Set<Long> getProviderOids(@Nonnull final Set<StatApiInputDTO> statInputs) {
        return statInputs.stream()
            .filter(stat -> stat.getFilters() != null)
            .flatMap(stat -> stat.getFilters().stream())
            .filter(filter -> StatsUtils.PROJECTED_PROVIDER_STAT_FILTER.equals(filter.getType()))
            .map(StatFilterApiDTO::getValue)
            .map(Long::valueOf)
            .collect(Collectors.toSet());
    }
}
