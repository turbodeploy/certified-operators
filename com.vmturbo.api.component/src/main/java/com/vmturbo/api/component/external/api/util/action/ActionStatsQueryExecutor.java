package com.vmturbo.api.component.external.api.util.action;

import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.enums.ActionCostType;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.common.protobuf.action.ActionDTO.GetHistoricalActionStatsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetHistoricalActionStatsResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.GetCurrentActionStatsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetCurrentActionStatsResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionStatsQuery;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.components.common.mapping.UIEnvironmentType;

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

    public ActionStatsQueryExecutor(@Nonnull final Clock clock,
                                    @Nonnull final ActionsServiceBlockingStub actionsServiceBlockingStub,
                                    @Nonnull final ActionSpecMapper actionSpecMapper,
                                    @Nonnull final UuidMapper uuidMapper,
                                    @Nonnull final GroupExpander groupExpander,
                                    @Nonnull final SupplyChainFetcherFactory supplyChainFetcherFactory,
                                    @Nonnull final UserSessionContext userSessionContext) {
        this(actionsServiceBlockingStub,
            userSessionContext,
            uuidMapper,
            new HistoricalQueryMapper(actionSpecMapper),
            new CurrentQueryMapper(actionSpecMapper, groupExpander, supplyChainFetcherFactory, userSessionContext),
            new ActionStatsMapper(clock, actionSpecMapper));
    }

    /**
     * Constructor for unit testing purposes.
     */
    @VisibleForTesting
    ActionStatsQueryExecutor(@Nonnull final ActionsServiceBlockingStub actionsServiceBlockingStub,
                             @Nonnull final UserSessionContext userSessionContext,
                             @Nonnull final UuidMapper uuidMapper,
                             @Nonnull final HistoricalQueryMapper historicalQueryMapper,
                             @Nonnull final CurrentQueryMapper currentQueryMapper,
                             @Nonnull final ActionStatsMapper actionStatsMapper) {
        this.actionsServiceBlockingStub = Objects.requireNonNull(actionsServiceBlockingStub);
        this.userSessionContext = Objects.requireNonNull(userSessionContext);
        this.uuidMapper = Objects.requireNonNull(uuidMapper);
        this.historicalQueryMapper = Objects.requireNonNull(historicalQueryMapper);
        this.currentQueryMapper = Objects.requireNonNull(currentQueryMapper);
        this.actionStatsMapper = Objects.requireNonNull(actionStatsMapper);
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
        if (query.isHistorical()) {
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
        }

        // Now get the current stats.

        final Map<ApiId, CurrentActionStatsQuery> curQueries = currentQueryMapper.mapToCurrentQueries(query);
        final GetCurrentActionStatsRequest.Builder curReqBldr = GetCurrentActionStatsRequest.newBuilder();
        curQueries.forEach((scopeId, scopeQuery) -> curReqBldr.addQueries(
            GetCurrentActionStatsRequest.SingleQuery.newBuilder()
                .setQueryId(scopeId.oid())
                .setQuery(scopeQuery)
                .build()));
        final GetCurrentActionStatsResponse curResponse =
            actionsServiceBlockingStub.getCurrentActionStats(curReqBldr.build());
        curResponse.getResponsesList().forEach(singleResponse -> {
            final List<StatSnapshotApiDTO> snapshots = retStats.computeIfAbsent(
                uuidMapper.fromOid(singleResponse.getQueryId()),
                k -> new ArrayList<>(1));
            snapshots.add(actionStatsMapper.currentActionStatsToApiSnapshot(
                singleResponse.getActionStatsList(), query));
        });
        return retStats;
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
         */
        Set<ApiId> scopes();

        /**
         * The entity type to restrict the query to. Note: {@link ActionStatsQuery#actionInput()}
         * also contains a field to restrict entity types
         * ({@link ActionApiInputDTO#getRelatedEntityTypes()}) but some REST API calls accept
         * a separate entity type, so we add it here instead of forcing them to change the
         * input DTO.
         */
        Optional<Integer> entityType();

        /**
         * The {@link ActionApiInputDTO} that specifies the kinds of stats to retrieve.
         * Note that we don't support all the possible options and groupings.
         */
        ActionApiInputDTO actionInput();

        default boolean isHistorical() {
            return actionInput().getStartTime() != null && actionInput().getEndTime() != null;
        }

        @Nonnull
        default Set<Integer> getRelatedEntityTypes() {
            final Set<Integer> types = new HashSet<>();
            CollectionUtils.emptyIfNull(actionInput().getRelatedEntityTypes()).stream()
                .map(ServiceEntityMapper::fromUIEntityType)
                .forEach(types::add);
            entityType().ifPresent(types::add);
            return types;
        }

        @Nonnull
        default Optional<EnvironmentTypeEnum.EnvironmentType> getEnvironmentType() {
            return UIEnvironmentType.fromString(actionInput().getEnvironmentType().name()).toEnvType();
        }

        @Nonnull
        default Optional<ActionCostType> getCostType() {
            return Optional.ofNullable(actionInput().getCostType());
        }
    }

}
