package com.vmturbo.api.component.external.api.util.stats.query.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.StatsMapper;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryScopeExpander.GlobalScope;
import com.vmturbo.api.component.external.api.util.stats.query.StatsSubQuery;
import com.vmturbo.api.component.external.api.util.stats.query.SubQuerySupportedStats;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatFilterApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.enums.Epoch;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.stats.Stats.GetAveragedEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity.RelatedEntity;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.components.common.stats.StatsUtils;

/**
 * Sub-query responsible for getting historical entity commodity stats. This is the most common
 * type of stats query.
 */
public class HistoricalCommodityStatsSubQuery implements StatsSubQuery {

    private final StatsMapper statsMapper;
    private final StatsHistoryServiceBlockingStub statsServiceRpc;
    private final UserSessionContext userSessionContext;
    private final RepositoryApi repositoryApi;

    public HistoricalCommodityStatsSubQuery(@Nonnull final StatsMapper statsMapper,
                                            @Nonnull final StatsHistoryServiceBlockingStub statsServiceRpc,
                                            @Nonnull final UserSessionContext userSessionContext,
                                            @Nonnull final RepositoryApi repositoryApi) {
        this.statsMapper = statsMapper;
        this.statsServiceRpc = statsServiceRpc;
        this.userSessionContext = userSessionContext;
        this.repositoryApi = repositoryApi;
    }

    @Override
    public boolean applicableInContext(@Nonnull final StatsQueryContext context) {
        return !context.getInputScope().isPlan();
    }

    @Override
    public SubQuerySupportedStats getHandledStats(@Nonnull final StatsQueryContext context) {
        return SubQuerySupportedStats.leftovers();
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public List<StatSnapshotApiDTO> getAggregateStats(@Nonnull final Set<StatApiInputDTO> requestedStats,
                                                      @Nonnull final StatsQueryContext context) {
        final List<StatSnapshotApiDTO> statsList = new ArrayList<>();
        toAveragedEntityStatsRequest(requestedStats, context).ifPresent(request -> {
            final Iterable<StatSnapshot> statsIterator = () -> statsServiceRpc.getAveragedEntityStats(request);
            statsIterator.forEach(snapshot -> statsList.add(statsMapper.toStatSnapshotApiDTO(snapshot)));
        });

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
            if (latestSnapshot != null && context.getCurTime() != DateTimeUtil.parseTime(latestSnapshot.getDate())) {
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
    private Optional<GetAveragedEntityStatsRequest> toAveragedEntityStatsRequest(
            @Nonnull final Set<StatApiInputDTO> requestedStats,
            @Nonnull final StatsQueryContext context) {
        final Set<Long> expandedOids = context.getQueryScope().getExpandedOids();
        final Set<StatApiInputDTO> convertedStats = convertProviderType(requestedStats, expandedOids);
        final GetAveragedEntityStatsRequest.Builder entityStatsRequest =
            GetAveragedEntityStatsRequest.newBuilder()
                .setFilter(statsMapper.newPeriodStatsFilter(context.newPeriodInputDto(convertedStats)));

        final Optional<GlobalScope> queryGlobalScope = context.getQueryScope().getGlobalScope()
            .filter(globalScope -> !userSessionContext.isUserScoped());
        // Only create a request if :
        // 1. context is globalScope OR
        // 2. Context is not global Scoped but expandedOids has atleast 1 oid in scope.
        if (!queryGlobalScope.isPresent() && expandedOids.isEmpty()) {
            return Optional.empty();
        }
        if (queryGlobalScope.isPresent()) {
            entityStatsRequest.setGlobalFilter(statsMapper.newGlobalFilter(queryGlobalScope.get()));
        } else {
            entityStatsRequest.addAllEntities(expandedOids);
        }
        return Optional.of(entityStatsRequest.build());
    }

    private Set<StatApiInputDTO> convertProviderType(@Nonnull final Set<StatApiInputDTO> rawStats,
            @Nonnull final Set<Long> scopeOids) {
        final Set<StatApiInputDTO> result = new HashSet<>();
        final Set<StatApiInputDTO> statsToAdjust = new HashSet<>();
        final Map<String, Set<String>> filterValuesRawToAdjusted = new HashMap<>();
        // first, sort stats. stats with no statfilters, or with no "providerType" statfilter, can be returned
        // as-is. set aside stats with "providerType" statfilter, and set aside all values of the statfilter.
        for (final StatApiInputDTO rawStat : rawStats) {
            if (rawStat.getFilters() != null && !rawStat.getFilters().isEmpty()) {
                final Set<String> filtersPerStatToAdjust = rawStat.getFilters().stream()
                    .filter(f -> StatsUtils.PROVIDER_TYPE_STAT_FILTER.equals(f.getType()))
                    .map(StatFilterApiDTO::getValue)
                    .collect(Collectors.toSet());
                if (filtersPerStatToAdjust.isEmpty()) {
                    result.add(rawStat);
                } else {
                    filtersPerStatToAdjust.forEach(toAdjust ->
                        filterValuesRawToAdjusted.putIfAbsent(toAdjust, new HashSet<>()));
                    statsToAdjust.add(rawStat);
                }
            } else {
                result.add(rawStat);
            }
        }

        // bail early if there's no need for further processing
        if (statsToAdjust.isEmpty()) {
            return result;
        }

        // collect all providers of entities in scope, ready to be filtered by entity type.
        final Set<RelatedEntity> providers =
            repositoryApi.entitiesRequest(scopeOids)
                .getEntities()
                .flatMap(e -> e.getProvidersList().stream())
                .filter(RelatedEntity::hasEntityType)
                .filter(RelatedEntity::hasOid)
                .collect(Collectors.toSet());

        // collect the adjusted statfilter values (provider oids of desired type) by filtering
        // providers by desired type.
        filterValuesRawToAdjusted.replaceAll((rawValue, set) -> {
            final int entityTypeValue = ApiEntityType.fromString(rawValue).typeNumber();
            return providers.stream()
                    .filter(provider -> provider.getEntityType() == entityTypeValue)
                    .map(RelatedEntity::getOid)
                    .map(id -> Long.toString(id))
                    .collect(Collectors.toSet());
        });

        for (final StatApiInputDTO rawStat : statsToAdjust) {
            // collect non-provider statfilters to be added as-is to adjusted stats
            final Set<StatFilterApiDTO> nonProviderFilters = rawStat.getFilters().stream()
                .filter(rawFilter -> !StatsUtils.PROVIDER_TYPE_STAT_FILTER.equals(rawFilter.getType()) ||
                    !filterValuesRawToAdjusted.containsKey(rawFilter.getValue()))
                .collect(Collectors.toSet());

            // retrieve the set of provider ids values associated with the stat's original
            // provider type statfilter
            final Set<String> providerIds = rawStat.getFilters().stream()
                .filter(rawFilter -> StatsUtils.PROVIDER_TYPE_STAT_FILTER.equals(rawFilter.getType()) &&
                    filterValuesRawToAdjusted.containsKey(rawFilter.getValue()))
                .findAny()
                .map(StatFilterApiDTO::getValue)
                .map(filterValuesRawToAdjusted::get)
                .orElse(Collections.emptySet());

            // separate each provider id into its own statfilter on its own stat, copied from
            // the original. use PRODUCER_UUID as new statfilter's type.
            providerIds.forEach(id -> {
                final StatApiInputDTO clone = cloneStat(rawStat);
                final List<StatFilterApiDTO> adjustedFilters = new ArrayList<>(nonProviderFilters);
                final StatFilterApiDTO newFilter = new StatFilterApiDTO();
                newFilter.setType(StringConstants.PRODUCER_UUID);
                newFilter.setValue(id);
                adjustedFilters.add(newFilter);
                clone.setFilters(adjustedFilters);
                result.add(clone);
            });
        }
        return result;
    }

    private StatApiInputDTO cloneStat(@Nonnull final StatApiInputDTO originalStat) {
        final StatApiInputDTO result = new StatApiInputDTO();
        result.setName(originalStat.getName());
        result.setRelatedEntityType(originalStat.getRelatedEntityType());
        result.setGroupBy(originalStat.getGroupBy());
        result.setFilters(originalStat.getFilters());
        return result;
    }
}
