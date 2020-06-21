package com.vmturbo.api.component.external.api.util.action;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.BuyRiScopeHandler;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.enums.ActionCostType;
import com.vmturbo.api.enums.CloudType;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.utils.CompositeEntityTypesSpec;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.common.api.mappers.EnvironmentTypeMapper;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery;
import com.vmturbo.common.protobuf.action.ActionDTO.GetCurrentActionStatsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetCurrentActionStatsResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.GetHistoricalActionStatsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetHistoricalActionStatsResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionStatsQuery;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;

/**
 * A shared utility class to execute action stats queries, meant to be used by whichever
 * service needs to do so.
 */
public class ActionStatsQueryExecutor {

    private static final Logger logger = LogManager.getLogger();

    private final ActionsServiceBlockingStub actionsServiceBlockingStub;

    private final UserSessionContext userSessionContext;

    private final UuidMapper uuidMapper;

    private final HistoricalQueryMapper historicalQueryMapper;

    private final CurrentQueryMapper currentQueryMapper;

    private final ActionStatsMapper actionStatsMapper;

    private final RepositoryApi repositoryApi;

    private final Clock clock;

    private final ThinTargetCache thinTargetCache;

    /**
     * Constructor for the executor.
     *
     * @param clock {@link Clock} used to determine the current time.
     * @param actionsServiceBlockingStub Stub to the action orchestrator's actions service.
     * @param actionSpecMapper {@link ActionSpecMapper}.
     * @param uuidMapper {@link UuidMapper} to map UI string IDs to IDs in the platform.
     * @param groupExpander {@link GroupExpander} to help expand group members.
     * @param supplyChainFetcherFactory {@link SupplyChainFetcherFactory} to expand supply chain.
     * @param userSessionContext {@link UserSessionContext} to enforce user scope.
     * @param repositoryApi {@link RepositoryApi} for repository access.
     * @param buyRiScopeHandler {@link BuyRiScopeHandler}.
     * @param thinTargetCache {@link ThinTargetCache}
     */
    public ActionStatsQueryExecutor(@Nonnull final Clock clock,
                                    @Nonnull final ActionsServiceBlockingStub actionsServiceBlockingStub,
                                    @Nonnull final ActionSpecMapper actionSpecMapper,
                                    @Nonnull final UuidMapper uuidMapper,
                                    @Nonnull final GroupExpander groupExpander,
                                    @Nonnull final SupplyChainFetcherFactory supplyChainFetcherFactory,
                                    @Nonnull final UserSessionContext userSessionContext,
                                    @Nonnull final RepositoryApi repositoryApi,
                                    @Nonnull final BuyRiScopeHandler buyRiScopeHandler,
                                    @Nonnull final ThinTargetCache thinTargetCache) {
        this(clock, actionsServiceBlockingStub,
            userSessionContext,
            uuidMapper,
            new HistoricalQueryMapper(actionSpecMapper, buyRiScopeHandler, clock),
            new CurrentQueryMapper(actionSpecMapper, groupExpander, supplyChainFetcherFactory,
                    userSessionContext, repositoryApi, buyRiScopeHandler),
            new ActionStatsMapper(clock, actionSpecMapper),
            repositoryApi,
            thinTargetCache);
    }

    @VisibleForTesting
    ActionStatsQueryExecutor(@Nonnull final Clock clock,
                             @Nonnull final ActionsServiceBlockingStub actionsServiceBlockingStub,
                             @Nonnull final UserSessionContext userSessionContext,
                             @Nonnull final UuidMapper uuidMapper,
                             @Nonnull final HistoricalQueryMapper historicalQueryMapper,
                             @Nonnull final CurrentQueryMapper currentQueryMapper,
                             @Nonnull final ActionStatsMapper actionStatsMapper,
                             @Nonnull final RepositoryApi repositoryApi,
                             @Nonnull final ThinTargetCache thinTargetCache) {
        this.clock = clock;
        this.actionsServiceBlockingStub = Objects.requireNonNull(actionsServiceBlockingStub);
        this.userSessionContext = Objects.requireNonNull(userSessionContext);
        this.uuidMapper = Objects.requireNonNull(uuidMapper);
        this.historicalQueryMapper = Objects.requireNonNull(historicalQueryMapper);
        this.currentQueryMapper = Objects.requireNonNull(currentQueryMapper);
        this.actionStatsMapper = Objects.requireNonNull(actionStatsMapper);
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.thinTargetCache = Objects.requireNonNull(thinTargetCache);
    }

    /**
     * Retrieve the action stats targeted by an {@link ActionStatsQuery}.
     *
     * @param query The query (build from {@link ImmutableActionStatsQuery}).
     * @return (scope id) -> (list of {@link StatSnapshotApiDTO}s for the scope) for each scope
     *         specified in the query. Note - the last snapshot in each list will be the
     *         "current" action stats for the scope.
     * @throws OperationFailedException If one of the necessary steps/RPCs failed.
     */
    @Nonnull
    public Map<ApiId, List<StatSnapshotApiDTO>> retrieveActionStats(@Nonnull final ActionStatsQuery query)
            throws OperationFailedException {
        final EntityAccessScope userScope = userSessionContext.getUserAccessScope();
        final Map<ApiId, List<StatSnapshotApiDTO>> retStats = new HashMap<>(query.scopes().size());

        // We issue a historical OR a current query. Combining historical and current results can
        // lead to confusion. There are delays between when actions disappear from "current" results
        // (as they're cleared when processing action plans from the market)
        // and when they appear in the historical data. If we co-display historical and current
        // results it can be confusing to the user, because actions will seem to disappear
        // entirely during those delays.
        if (query.isHistorical(clock)) {
            if (!userScope.containsAll()) {
                logger.warn("Scoped user (scope: {}) requested historical action stats." +
                    "Will not return any.", userScope.toString());
            } else {
                final Map<ApiId, HistoricalActionStatsQuery> grpcQuery =
                    historicalQueryMapper.mapToHistoricalQueries(query);
                final GetHistoricalActionStatsRequest.Builder historicalReqBuilder =
                    GetHistoricalActionStatsRequest.newBuilder();
                grpcQuery.forEach((scopeId, scopeQuery) -> {
                    historicalReqBuilder.addQueries(GetHistoricalActionStatsRequest.SingleQuery.newBuilder()
                        .setQueryId(scopeId.oid())
                        .setQuery(scopeQuery));
                });
                final GetHistoricalActionStatsResponse actionStatsResponse =
                    actionsServiceBlockingStub.getHistoricalActionStats(historicalReqBuilder.build());
                actionStatsResponse.getResponsesList().forEach(singleResponse -> {
                    final List<StatSnapshotApiDTO> ret = retStats.computeIfAbsent(
                        // Note - here we are re-creating an input api ID instead of reusing one of
                        // the IDs from the input scope.
                        uuidMapper.fromOid(singleResponse.getQueryId()),
                        k -> new ArrayList<>());
                    ret.addAll(actionStatsMapper.historicalActionStatsToApiSnapshots(
                        singleResponse.getActionStats(), query));
                });
            }
        } else {
            // Not a historical query, so we get the "current" action stats.
            final Map<ApiId, CurrentActionStatsQuery> curQueries = currentQueryMapper.mapToCurrentQueries(query);
            final GetCurrentActionStatsRequest.Builder curReqBldr = GetCurrentActionStatsRequest.newBuilder();
            curQueries.forEach((scopeId, scopeQuery) -> curReqBldr.addQueries(
                GetCurrentActionStatsRequest.SingleQuery.newBuilder()
                    .setQueryId(scopeId.oid())
                    .setQuery(scopeQuery)
                    .build()));
            final GetCurrentActionStatsResponse curResponse =
                actionsServiceBlockingStub.getCurrentActionStats(curReqBldr.build());

            final Map<Long, MinimalEntity> entityLookup;
            final Map<Long, String> cspLookup;
            if (query.actionInput().getGroupBy() != null) {
                // If the request was to group by templates, then we group by target id. For these
                // requests, we need to get the names of the target ids from the search service
                if (query.actionInput().getGroupBy().contains(StringConstants.TEMPLATE)) {
                    Set<Long> templatesToLookup = curResponse.getResponsesList().stream()
                            .flatMap(singleResponse -> singleResponse.getActionStatsList().stream())
                            .filter(stat -> stat.getStatGroup().hasTargetEntityId())
                            .map(stat -> stat.getStatGroup().getTargetEntityId())
                            .collect(Collectors.toSet());
                    entityLookup = repositoryApi.entitiesRequest(templatesToLookup).getMinimalEntities()
                            .collect(Collectors.toMap(MinimalEntity::getOid, Function.identity()));
                } else {
                    entityLookup = Collections.emptyMap();
                }

                if (query.actionInput().getGroupBy().contains(StringConstants.CSP)) {
                    final Set<Long> cspsToLookup = curResponse.getResponsesList().stream()
                            .flatMap(singleResponse -> singleResponse.getActionStatsList().stream())
                            .filter(stat -> stat.getStatGroup().hasCsp())
                            .map(stat -> Long.parseLong(stat.getStatGroup().getCsp()))
                            .collect(Collectors.toSet());

                    final Map<Long, String> tempCspLookup = new HashMap<>();
                    repositoryApi.entitiesRequest(cspsToLookup).getEntities().forEach(apiEntity -> {
                        tempCspLookup.put(apiEntity.getOid(), getCloudTypeFromProbeType(apiEntity::getDiscoveredTargetDataMap));
                    });
                    cspLookup = ImmutableMap.copyOf(tempCspLookup);
                } else {
                    cspLookup = Collections.emptyMap();
                }
            } else {
                entityLookup = Collections.emptyMap();
                cspLookup = Collections.emptyMap();
            }

            curResponse.getResponsesList().forEach(singleResponse -> {
                final List<StatSnapshotApiDTO> snapshots = retStats.computeIfAbsent(
                    uuidMapper.fromOid(singleResponse.getQueryId()),
                    k -> new ArrayList<>(1));
                snapshots.add(actionStatsMapper.currentActionStatsToApiSnapshot(
                    singleResponse.getActionStatsList(), query, entityLookup, cspLookup));
            });
        }
        return retStats;
    }

    private String getCloudTypeFromProbeType(Supplier<Map<Long, PerTargetEntityInformation>> idMapGetter) {
        final Set<CloudType> cloudTypeSet = new HashSet<>();
        Map<Long, PerTargetEntityInformation> target2data = idMapGetter.get();
        if (!target2data.isEmpty()) {
            for (Map.Entry<Long, PerTargetEntityInformation> info : target2data.entrySet()) {
                thinTargetCache.getTargetInfo(info.getKey()).ifPresent(thinTargetInfo ->
                        cloudTypeSet.add(CloudType.fromProbeType(thinTargetInfo.probeInfo().type())));
            }
        }
        if (cloudTypeSet.size() == 1) {
            final Optional<CloudType> matchedCloudType = cloudTypeSet.stream().findFirst();
            return matchedCloudType.get().toString();
        }
        return CloudType.UNKNOWN.toString();
    }

    /**
     * A query for actions.
     */
    @Value.Immutable
    public interface ActionStatsQuery {

        /**
         * The scope for the query - either an OID or the entire market. Note that historical
         * action stats are only available for certain types of objects (e.g. global environment,
         * clusters). Querying for "invalid" types of objects (e.g. individual entities) will
         * return no results.
         *
         * @return The scopes.
         */
        Set<ApiId> scopes();

        /**
         * The entity type to restrict the query to. Note: {@link ActionStatsQuery#actionInput()}
         * also contains a field to restrict entity types
         * ({@link ActionApiInputDTO#getRelatedEntityTypes()}) but some REST API calls accept
         * a separate entity type, so we add it here instead of forcing them to change the
         * input DTO.
         *
         * @return The entity type, if any.
         */
        Optional<Integer> entityType();

        /**
         * The {@link ActionApiInputDTO} that specifies the kinds of stats to retrieve.
         * Note that we don't support all the possible options and groupings.
         *
         * @return The {@link ActionApiInputDTO} input from the API.
         */
        ActionApiInputDTO actionInput();

        /**
         * The time stamp when the query was constructed.
         *
         * @return The time of the query.
         */
        Optional<String> currentTimeStamp();

        /**
         * Return whether or not the query should be considered historical.
         *
         * @param clock The clock to use to determine current time.
         * @return True if the query is a historical query.
         */
        default boolean isHistorical(@Nonnull final Clock clock) {
            // A query is historical if it has a start time in the past.
            return actionInput().getStartTime() != null &&
                DateTimeUtil.parseTime(actionInput().getStartTime()) < clock.millis();
        }

        /**
         * Get the related entity types specified in the {@link ActionApiInputDTO}.
         *
         * @return The set of related entity types extracted from the API input.
         */
        @Nonnull
        default Set<Integer> getRelatedEntityTypes() {
            final Set<Integer> types = new HashSet<>();
            CollectionUtils.emptyIfNull(actionInput().getRelatedEntityTypes()).stream()
                .flatMap(relatedEntityType ->
                    CompositeEntityTypesSpec.WORKLOAD_ENTITYTYPE.equals(relatedEntityType)
                        ? CompositeEntityTypesSpec.WORKLOAD_TYPE_PRIMITIVES.stream()
                        : Stream.of(relatedEntityType))
                .map(ApiEntityType::fromString)
                .map(ApiEntityType::typeNumber)
                .forEach(types::add);
            entityType().ifPresent(types::add);
            return types;
        }

        @Nonnull
        default Optional<EnvironmentTypeEnum.EnvironmentType> getEnvironmentType() {
            return
                Optional.ofNullable(actionInput().getEnvironmentType())
                    .map(EnvironmentTypeMapper::fromApiToXL);
        }

        @Nonnull
        default Optional<ActionCostType> getCostType() {
            return Optional.ofNullable(actionInput().getCostType());
        }
    }

}
