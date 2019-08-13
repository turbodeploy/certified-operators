package com.vmturbo.api.component.external.api.util.stats.query.impl;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext.TimeWindow;
import com.vmturbo.api.component.external.api.util.stats.query.StatsSubQuery;
import com.vmturbo.api.component.external.api.util.stats.query.SubQuerySupportedStats;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.stats.StatsUtils;

/**
 * Sub-query responsible for getting the counts of entities for a scoped user.
 */
public class ScopedUserCountStatsSubQuery implements StatsSubQuery {

    private final Duration liveStatsRetrievalWindow;
    private final UserSessionContext userSessionContext;
    private final RepositoryApi repositoryApi;


    public ScopedUserCountStatsSubQuery(@Nonnull final Duration liveStatsRetrievalWindow,
                                            @Nonnull final UserSessionContext userSessionContext,
                                            @Nonnull final RepositoryApi repositoryApi) {
        this.liveStatsRetrievalWindow = liveStatsRetrievalWindow;
        this.userSessionContext = userSessionContext;
        this.repositoryApi = repositoryApi;
    }

    @Override
    public boolean applicableInContext(@Nonnull final StatsQueryContext context) {
        return userSessionContext.isUserScoped() && !context.getScope().isPlan();
    }

    @Override
    public SubQuerySupportedStats getHandledStats(@Nonnull final StatsQueryContext context) {
        return SubQuerySupportedStats.some(context.findStats(StatsUtils.COUNT_ENTITY_METRIC_NAMES.keySet()));
    }

    @Nonnull
    @Override
    public Map<Long, List<StatApiDTO>> getAggregateStats(@Nonnull final Set<StatApiInputDTO> requestedStats,
                                                         @Nonnull final StatsQueryContext context) throws OperationFailedException {
        Map<Long, List<StatApiDTO>> statSnapShots = new HashMap<>();
        List <StatApiDTO> statList = new ArrayList<>();
        requestedStats.stream()
            .map(StatApiInputDTO::getName)
            .forEach(statName -> {
                StatApiDTO statApi = new StatApiDTO();
                statApi.setName(statName);
                statList.add(statApi);
            });
        if (context.requestProjected()) {
            StatSnapshotApiDTO projectedStatSnapshot = new StatSnapshotApiDTO();
            projectedStatSnapshot.setDate(context.getTimeWindow()
                .map(TimeWindow::endTime)
                // If the request didn't have an explicit end time, set the time the future (and beyond).
                // We want it to be out of the "live stats retrieval window" (to keep the semantics
                // that anything within the live stats retrieval window = current stats), so we add
                // a minute.
                .orElseGet(() -> context.getCurTime() + liveStatsRetrievalWindow.plusMinutes(1).toMillis()).toString());
            projectedStatSnapshot.setStatistics(statList);
            statSnapShots.put(DateTimeUtil.parseTime(projectedStatSnapshot.getDate()),
                projectedStatSnapshot.getStatistics());
        }
        StatSnapshotApiDTO currentSnapshot = new StatSnapshotApiDTO();
        currentSnapshot.setDate(Long.toString(context.getCurTime()));
        currentSnapshot.setStatistics(statList);
        statSnapShots.put(DateTimeUtil.parseTime(currentSnapshot.getDate()), currentSnapshot.getStatistics());

        setCurrentEntityCount(statSnapShots, context);

        return statSnapShots;
    }

    @Nonnull
    private void setCurrentEntityCount (@Nonnull Map<Long, List<StatApiDTO>> statSnapShots, @Nonnull final StatsQueryContext context){

        final List<UIEntityType> entityTypes = statSnapShots.values().stream()
            .flatMap(List::stream)
            .map(dto -> StatsUtils.COUNT_ENTITY_METRIC_NAMES.get(dto.getName()))
            .collect(Collectors.toList());

        final Map<Integer, Long> entityTypeCount =
            repositoryApi.entitiesRequest(context.getScopeEntities())
                .restrictTypes(entityTypes)
                .getMinimalEntities()
                .map(MinimalEntity::getEntityType)
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        statSnapShots.values().stream()
            .flatMap(List::stream)
            .filter(dto -> StatsUtils.COUNT_ENTITY_METRIC_NAMES.containsKey(dto.getName()))
            .forEach(statApiDTO -> {
                float statValue =
                    entityTypeCount.get(StatsUtils.COUNT_ENTITY_METRIC_NAMES.get(statApiDTO.getName()).typeNumber());
                final StatValueApiDTO valueDto = new StatValueApiDTO();
                valueDto.setAvg(statValue);
                valueDto.setMax(statValue);
                valueDto.setMin(statValue);
                valueDto.setTotal(statValue);
                statApiDTO.setValues(valueDto);
                statApiDTO.setValue(statValue);
            });
    }
}
