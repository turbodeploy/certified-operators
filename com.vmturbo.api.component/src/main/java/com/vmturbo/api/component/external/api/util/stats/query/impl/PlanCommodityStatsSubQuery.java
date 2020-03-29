package com.vmturbo.api.component.external.api.util.stats.query.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Streams;

import com.vmturbo.api.component.external.api.mapper.StatsMapper;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.component.external.api.util.stats.query.StatsSubQuery;
import com.vmturbo.api.component.external.api.util.stats.query.SubQuerySupportedStats;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.stats.Stats.GetAveragedEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.utils.StringConstants;

/**
 * Handles commodity stats (i.e. entity-related stats saved in the history component) for plans.
 * <p>
 * This is split from {@link HistoricalCommodityStatsSubQuery} because the way the history component
 * stores plan stats is different, and the way we obtain plan source/projected stats is also
 * different in plan vs. realtime.
 */
public class PlanCommodityStatsSubQuery implements StatsSubQuery {

    private final StatsMapper statsMapper;

    private final StatsHistoryServiceBlockingStub statsServiceRpc;

    private final RequestMapper requestMapper;

    /**
     * @param statsMapper {@link StatsMapper} to convert history component results to API-tongue.
     * @param statsServiceRpc RPC stub for calls to the history component.
     */
    public PlanCommodityStatsSubQuery(@Nonnull final StatsMapper statsMapper,
                                      @Nonnull final StatsHistoryServiceBlockingStub statsServiceRpc) {
        this(statsMapper, statsServiceRpc, new RequestMapper(statsMapper));
    }

    @VisibleForTesting
    PlanCommodityStatsSubQuery(@Nonnull final StatsMapper statsMapper,
                               @Nonnull final StatsHistoryServiceBlockingStub statsServiceRpc,
                               @Nonnull final RequestMapper requestMapper) {
        this.statsMapper = statsMapper;
        this.statsServiceRpc = statsServiceRpc;
        this.requestMapper = requestMapper;
    }

    @Override
    public boolean applicableInContext(@Nonnull final StatsQueryContext context) {
        return context.getInputScope().isPlan();
    }

    @Override
    public SubQuerySupportedStats getHandledStats(@Nonnull final StatsQueryContext context) {
        return SubQuerySupportedStats.leftovers();
    }

    @Nonnull
    @Override
    public List<StatSnapshotApiDTO> getAggregateStats(@Nonnull final Set<StatApiInputDTO> stats,
                                                      @Nonnull final StatsQueryContext context)
            throws OperationFailedException {
        final GetAveragedEntityStatsRequest request =
            requestMapper.toAveragedEntityStatsRequest(stats, context);

        return Streams.stream(statsServiceRpc.getAveragedEntityStats(request))
            .map(snapshot -> statsMapper.toStatSnapshotApiDTO(snapshot))
            .collect(Collectors.toList());
    }

    /**
     * Utility class to help isolate testing.
     */
    static class RequestMapper {
        private final StatsMapper statsMapper;

        RequestMapper(@Nonnull final StatsMapper statsMapper) {
            this.statsMapper = statsMapper;
        }

        @Nonnull
        GetAveragedEntityStatsRequest toAveragedEntityStatsRequest(@Nonnull final Set<StatApiInputDTO> requestedStats,
                                                                   @Nonnull final StatsQueryContext context) {
            final Set<StatApiInputDTO> finalRequestedStats = new HashSet<>();
            if (context.includeCurrent()) {
                // For plans, the "source" stats are available with the "current" prefix.
                // For each requested stat, add an equivalent StatApiInputDTO with the prefix.
                requestedStats.stream()
                    .map(stat -> {
                        if (stat.getName() == null) {
                            return stat;
                        } else {
                            final String newName = StringConstants.STAT_PREFIX_CURRENT + StringUtils.capitalize(stat.getName());
                            return new StatApiInputDTO(newName,
                                stat.getRelatedEntityType(), stat.getFilters(), stat.getGroupBy());
                        }
                    })
                    .forEach(finalRequestedStats::add);
            }

            if (context.requestProjected()) {
                // If we want the projected stats, also add the original StatApiInputDTOs without
                // the "current" prefix.
                finalRequestedStats.addAll(requestedStats);
            }

            final GetAveragedEntityStatsRequest.Builder entityStatsRequest =
                GetAveragedEntityStatsRequest.newBuilder()
                    .setFilter(statsMapper.newPeriodStatsFilter(context.newPeriodInputDto(finalRequestedStats)));

            // Note - we do not support sub-scopes within the plan.
            entityStatsRequest.addEntities(context.getInputScope().oid());

            return entityStatsRequest.build();
        }
    }
}
