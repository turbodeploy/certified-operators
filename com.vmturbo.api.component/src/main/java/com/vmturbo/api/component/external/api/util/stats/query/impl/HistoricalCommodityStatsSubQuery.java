package com.vmturbo.api.component.external.api.util.stats.query.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.StatsMapper;
import com.vmturbo.api.component.external.api.util.StatsUtils;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryScopeExpander.GlobalScope;
import com.vmturbo.api.component.external.api.util.stats.query.StatsSubQuery;
import com.vmturbo.api.component.external.api.util.stats.query.SubQuerySupportedStats;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.enums.Epoch;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.stats.Stats.GetAveragedEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.GlobalFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;

/**
 * Sub-query responsible for getting historical entity commodity stats. This is the most common
 * type of stats query.
 */
public class HistoricalCommodityStatsSubQuery implements StatsSubQuery {

    private static final Logger logger = LogManager.getLogger();

    /**
     * List of storage related commodities.
     * TODO OM-56401 update list of storage commodities.
     */
    @VisibleForTesting
    static final ImmutableList<String> storageCommodities = ImmutableList.<String>builder()
        .add(UICommodityType.STORAGE_AMOUNT.apiStr(),
             UICommodityType.STORAGE_ACCESS.apiStr(),
             UICommodityType.STORAGE_LATENCY.apiStr(),
             UICommodityType.STORAGE_PROVISIONED.apiStr())
        .build();

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
                                                      @Nonnull final StatsQueryContext context) throws OperationFailedException {
        // TODO OM-56401 this will be removed once new cloud storage model is in place
        // Append group by [key] if request is single volume scope
        final Set<Long> scopedOids = context.getQueryScope().getScopeOids();
        final Set<Long> expandedOids = context.getQueryScope().getExpandedOids();
        if (scopedOids != null && scopedOids.size() == 1 && expandedOids != null && expandedOids.size() == 2) {
            logger.debug("scopedOids = {} and expandedOids = {}", scopedOids, expandedOids);
            // TODO OM-56401 This is extra call to repository but will be removed once move to new storage model
            final PropertyFilter startFilter = SearchProtoUtil.idFilter(scopedOids);
            final SearchParameters searchParams = SearchProtoUtil.makeSearchParameters(startFilter)
                .build();
            final boolean isVolume = repositoryApi.newSearchRequest(searchParams).getEntities()
                .allMatch(entity -> entity.getEntityType() == EntityType.VIRTUAL_VOLUME.getValue());

            if (isVolume) {
                requestedStats.forEach(statApiInputDTO -> {
                    String relatedEntityType = statApiInputDTO.getRelatedEntityType();
                    if (relatedEntityType != null && ApiEntityType.VIRTUAL_MACHINE.apiStr().equals(relatedEntityType)) {
                        final String groupByKey = "key";
                        if (statApiInputDTO.getGroupBy() == null || !statApiInputDTO.getGroupBy().contains(groupByKey)) {
                            statApiInputDTO.setGroupBy(Collections.singletonList(groupByKey));
                        }
                    }
                });
            }
        }

        final Optional<GetAveragedEntityStatsRequest> requestOptional =
                toAveragedEntityStatsRequest(requestedStats, context);

        final List<StatSnapshotApiDTO> statsList = new ArrayList<>();
        final Set<Long> commodityOids = new HashSet<>();
        requestOptional.ifPresent(request -> {
            final Iterable<StatSnapshot> statsIterator = () -> statsServiceRpc.getAveragedEntityStats(request);
            statsIterator.forEach(snapshot -> {
                // record any volume commodity keys
                snapshot.getStatRecordsList().stream().forEach(statRecord -> {
                    if (storageCommodities.contains(statRecord.getName()) &&
                        statRecord.getStatKey() != null &&
                        statRecord.getStatKey().length() > 0) {
                        String commodityKey = statRecord.getStatKey();
                        try {
                            Long oid = Long.parseLong(commodityKey);
                            commodityOids.add(oid);
                        } catch (NumberFormatException e) {
                            logger.error("Unable to get oid from statKey " + commodityKey + ".");
                        }
                    }
                });
                statsList.add(statsMapper.toStatSnapshotApiDTO(snapshot));
            });
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

        // replace commodity key oid in displayName with displayName of the entity
        updateStorageCommodityName(statsList, scopedOids, commodityOids);

        return statsList;
    }

    @VisibleForTesting
    void updateStorageCommodityName(@Nonnull final List<StatSnapshotApiDTO> statsList,
                                    @Nonnull final Set<Long> scopedOid,
                                    @Nonnull final Set<Long> commodityOids) {
        if (!commodityOids.isEmpty()) {
            final SearchParameters searchVvInScope = SearchProtoUtil
                .makeSearchParameters(SearchProtoUtil.idFilter(commodityOids))
                .build();
            RepositoryApi.SearchRequest commodityEntitiesRequest = repositoryApi.newSearchRequest(searchVvInScope);
            Set<Long> vvOids = new HashSet(); // TODO OM-56401 this will be removed once new cloud storage model is in place
            Map<Long, String> oidToDisplayNames = new HashMap<>();
            commodityEntitiesRequest.getFullEntities()
                .forEach(topologyEntityDTO -> {
                    oidToDisplayNames.put(topologyEntityDTO.getOid(), topologyEntityDTO.getDisplayName());
                    // TODO OM-56401 this will be removed once new cloud storage model is in place
                    if (topologyEntityDTO.getEntityType() == EntityType.VIRTUAL_VOLUME.getValue()) {
                        vvOids.add(topologyEntityDTO.getOid());
                    }
                });

            // TODO OM-56401 this will be removed once new cloud storage model is in place

            final boolean singleVolumeScope = scopedOid != null && scopedOid.size() == 1 &&
                vvOids.contains(scopedOid.iterator().next());

            statsList.forEach(statSnapshotApiDTO -> {
                final List<StatApiDTO> statistics = statSnapshotApiDTO.getStatistics();
                final List<StatApiDTO> processedStatistics = new ArrayList<>();
                for (StatApiDTO statApiDTO : statistics) {
                    if (storageCommodities.contains(statApiDTO.getName())) {
                        final String displayName = statApiDTO.getDisplayName();
                        // displayName for storageCommodities is in the form of [FROM: <ST Display Name> KEY: <VV Oid>]
                        final String[] displayNameParsed = displayName.split(StatsUtils.COMMODITY_KEY_PREFIX);
                        if (displayNameParsed.length > 0) {
                            final int last = displayNameParsed.length - 1;
                            try {
                                Long oid = Long.parseLong(displayNameParsed[last]);
                                if ((singleVolumeScope && scopedOid.iterator().next().equals(oid)) || !singleVolumeScope) {
                                    String commodityDisplayName = oidToDisplayNames.get(oid);
                                    if (commodityDisplayName != null) {
                                        String newDisplayName = displayName.replace(displayNameParsed[last], commodityDisplayName);
                                        statApiDTO.setDisplayName(newDisplayName);
                                    }
                                    processedStatistics.add(statApiDTO);
                                }
                            } catch (NumberFormatException e) {
                                logger.error("Unable to get oid from displayName " + displayName + ".");
                                processedStatistics.add(statApiDTO);
                            }
                        } else {
                            logger.warn("storage commodities key does not have the format as expected.  displayName: " + displayName);
                            processedStatistics.add(statApiDTO);
                        }
                    } else {
                        if (!singleVolumeScope) {
                            processedStatistics.add(statApiDTO);
                        }
                    }
                }
                statSnapshotApiDTO.setStatistics(processedStatistics);

            });
        }
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
        final GetAveragedEntityStatsRequest.Builder entityStatsRequest =
            GetAveragedEntityStatsRequest.newBuilder()
                .setFilter(statsMapper.newPeriodStatsFilter(context.newPeriodInputDto(requestedStats)));

        final Optional<GlobalScope> queryGlobalScope = context.getQueryScope().getGlobalScope()
            .filter(globalScope -> !userSessionContext.isUserScoped());
        final Set<Long> expandedOids = context.getQueryScope().getExpandedOids();
        // Only create a request if :
        // 1. context is globalScope OR
        // 2. Context is not global Scoped but expandedOids has atleast 1 oid in scope.
        if (!queryGlobalScope.isPresent() && expandedOids.isEmpty()) {
            return Optional.empty();
        }
        if (queryGlobalScope.isPresent()) {
            GlobalFilter.Builder globalFilter = GlobalFilter.newBuilder();
            queryGlobalScope.get().environmentType().ifPresent(globalFilter::setEnvironmentType);
            // since we've expanded DC to PMs, we should also set related entity type to
            // PhysicalMachine, otherwise history component will not return required data
            queryGlobalScope.get().entityTypes().forEach(type -> globalFilter.addRelatedEntityType(
                statsMapper.normalizeRelatedType(type.apiStr())));
            entityStatsRequest.setGlobalFilter(globalFilter);
        } else {
            entityStatsRequest.addAllEntities(expandedOids);
        }
        return Optional.of(entityStatsRequest.build());
    }
}
