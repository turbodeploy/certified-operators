package com.vmturbo.api.component.external.api.util.stats.query.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.api.component.external.api.mapper.StatsMapper;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryScopeExpander.GlobalScope;
import com.vmturbo.api.component.external.api.util.stats.query.StatsSubQuery;
import com.vmturbo.api.component.external.api.util.stats.query.SubQuerySupportedStats;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.enums.Epoch;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.stats.Stats.GetAveragedEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.GlobalFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;

/**
 * Sub-query responsible for getting historical entity commodity stats. This is the most common
 * type of stats query.
 */
public class HistoricalCommodityStatsSubQuery implements StatsSubQuery {
    private final StatsMapper statsMapper;
    private final StatsHistoryServiceBlockingStub statsServiceRpc;
    private final UserSessionContext userSessionContext;

    public HistoricalCommodityStatsSubQuery(@Nonnull final StatsMapper statsMapper,
                                            @Nonnull final StatsHistoryServiceBlockingStub statsServiceRpc,
                                            @Nonnull final UserSessionContext userSessionContext) {
        this.statsMapper = statsMapper;
        this.statsServiceRpc = statsServiceRpc;
        this.userSessionContext = userSessionContext;
    }

    @Override
    public boolean applicableInContext(@Nonnull final StatsQueryContext context) {
        return !context.getInputScope().isPlan();
    }

    @Override
    public SubQuerySupportedStats getHandledStats(@Nonnull final StatsQueryContext context) {
        return SubQuerySupportedStats.leftovers();
    }

    @Nonnull
    @Override
    public List<StatSnapshotApiDTO> getAggregateStats(@Nonnull final Set<StatApiInputDTO> requestedStats,
                                                      @Nonnull final StatsQueryContext context) throws OperationFailedException {
        final GetAveragedEntityStatsRequest request =
            toAveragedEntityStatsRequest(requestedStats, context);

        final Iterable<StatSnapshot> statsIterator = () -> statsServiceRpc.getAveragedEntityStats(request);
        final List<StatSnapshotApiDTO> statsList = new ArrayList<>();
        statsIterator.forEach(snapshot -> statsList.add(statsMapper.toStatSnapshotApiDTO(snapshot)));

        // If the request is:
        //    1) In the realtime topology.
        //    2) Looking for historical points in a time range, and not a single snapshot.
        // Then copy the most recent snapshot and pretend it's also the "current" snapshot.
        // The reason to do it is that DB may not necessarily have records that
        // matches with the time point when API queries stats data, therefore we decide to use the
        // value from the latest record in history to represent it.
        // TODO: Why do we apply this rule only to this subquery and not others?
        final boolean copyLast = context.includeCurrent() && context.getTimeWindow()
            .map(timeWindow -> timeWindow.startTime() != timeWindow.endTime())
            .orElse(false);
        if (copyLast) {
            StatSnapshotApiDTO latestSnapshot = getLatestSnapShotInPast(statsList, context.getCurTime());
            if (latestSnapshot != null &&  context.getCurTime() != DateTimeUtil.parseTime(latestSnapshot.getDate())) {
                final StatSnapshotApiDTO clone = new StatSnapshotApiDTO();
                clone.setDate(DateTimeUtil.toString(context.getCurTime()));
                clone.setEpoch(Epoch.CURRENT);
                clone.setStatistics(latestSnapshot.getStatistics());
                statsList.add(clone);
            }
        }

        return statsList;
    }

    /**
     * A helper method to find the stats snapshot with the latest time stamp in history.
     *
     * @param statsSnapshots a list of snapshot builders
     * @param currentTimeStamp the current time stamp which is used to decide snapshots in history
     * @return a Stats.StatSnapshot.Builder
     */
    private StatSnapshotApiDTO getLatestSnapShotInPast(Iterable<StatSnapshotApiDTO> statsSnapshots, long currentTimeStamp) {
        StatSnapshotApiDTO latestRecordInPast = null;
        long latestTimeStamp = 0;
        for (StatSnapshotApiDTO snapshot : statsSnapshots) {
            long snapShotTimeStamp = DateTimeUtil.parseTime(snapshot.getDate());
            if (snapShotTimeStamp > latestTimeStamp && snapShotTimeStamp <= currentTimeStamp) {
                latestTimeStamp = snapShotTimeStamp;
                latestRecordInPast = snapshot;
            }
        }
        return latestRecordInPast;
    }

    @Nonnull
    private GetAveragedEntityStatsRequest toAveragedEntityStatsRequest(@Nonnull final Set<StatApiInputDTO> requestedStats,
                                                               @Nonnull final StatsQueryContext context) {
        final GetAveragedEntityStatsRequest.Builder entityStatsRequest =
            GetAveragedEntityStatsRequest.newBuilder()
                .setFilter(statsMapper.newPeriodStatsFilter(context.newPeriodInputDto(requestedStats)));

        final Optional<GlobalScope> queryGlobalScope = context.getQueryScope().getGlobalScope()
            .filter(globalScope -> !userSessionContext.isUserScoped());
        if (queryGlobalScope.isPresent()) {
            GlobalFilter.Builder globalFilter = GlobalFilter.newBuilder();
            queryGlobalScope.get().environmentType().ifPresent(globalFilter::setEnvironmentType);
            // since we've expanded DC to PMs, we should also set related entity type to
            // PhysicalMachine, otherwise history component will not return required data
            queryGlobalScope.get().entityTypes().forEach(type -> globalFilter.addRelatedEntityType(
                statsMapper.normalizeRelatedType(type.apiStr())));
            entityStatsRequest.setGlobalFilter(globalFilter);
        } else {
            entityStatsRequest.addAllEntities(context.getQueryScope().getEntities());
        }

        return entityStatsRequest.build();
    }
}
