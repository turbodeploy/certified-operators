package com.vmturbo.api.component.external.api.util.action;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.BuyRiScopeHandler;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory.SupplyChainNodeFetcherBuilder;
import com.vmturbo.api.component.external.api.util.action.ActionStatsQueryExecutor.ActionStatsQuery;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery.GroupBy;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery.ScopeFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery.ScopeFilter.EntityScope;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery.ScopeFilter.GlobalScope;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.utils.StringConstants;

/**
 * Utility class responsible for mapping {@link ActionStatsQuery}s to
 * {@link CurrentActionStatsQuery}s.
 */
class CurrentQueryMapper {

    private static final Logger logger = LogManager.getLogger();

    private final ActionGroupFilterExtractor actionGroupFilterExtractor;

    private final GroupByExtractor groupByExtractor;

    private final ScopeFilterExtractor scopeFilterExtractor;

    CurrentQueryMapper(@Nonnull final ActionSpecMapper actionSpecMapper,
                       @Nonnull final GroupExpander groupExpander,
                       @Nonnull final SupplyChainFetcherFactory supplyChainFetcherFactory,
                       @Nonnull final UserSessionContext userSessionContext,
                       @Nonnull final RepositoryApi repositoryApi,
                       @Nonnull final BuyRiScopeHandler buyRiScopeHandler,
                       @Nonnull final UuidMapper uuidMapper) {
        this(new ActionGroupFilterExtractor(actionSpecMapper, buyRiScopeHandler),
            new GroupByExtractor(),
            new ScopeFilterExtractor(userSessionContext,
                new EntityScopeFactory(groupExpander, supplyChainFetcherFactory, repositoryApi, buyRiScopeHandler, uuidMapper),
                uuidMapper));
    }

    @VisibleForTesting
    CurrentQueryMapper(@Nonnull final ActionGroupFilterExtractor actionGroupFilterExtractor,
                       @Nonnull final GroupByExtractor groupByExtractor,
                       @Nonnull final ScopeFilterExtractor scopeFilterExtractor) {
        this.actionGroupFilterExtractor = actionGroupFilterExtractor;
        this.groupByExtractor = groupByExtractor;
        this.scopeFilterExtractor = scopeFilterExtractor;
    }

    /**
     * Map the input {@link ActionStatsQuery} to a set of {@link CurrentActionStatsQuery}s that can
     * be sent to the action orchestrator.
     *
     * @param query The input {@link ActionStatsQuery}.
     * @return (scope id) -> {@link CurrentActionStatsQuery} for that scope, for each scope in
     *         {@link ActionStatsQuery#scopes()}.
     * @throws OperationFailedException If any underlying RPC blows up.
     */
    @Nonnull
    public Map<ApiId, CurrentActionStatsQuery> mapToCurrentQueries(@Nonnull final ActionStatsQuery query)
            throws OperationFailedException {

        final List<GroupBy> groupBy = groupByExtractor.extractGroupByCriteria(query);

        final Map<ApiId, ScopeFilter> filtersByScope = scopeFilterExtractor.extractScopeFilters(query);

        return filtersByScope.entrySet().stream()
            .collect(Collectors.toMap(Entry::getKey, e -> CurrentActionStatsQuery.newBuilder()
                .setActionGroupFilter(actionGroupFilterExtractor.extractActionGroupFilter(
                        query, e.getKey()))
                .setScopeFilter(e.getValue())
                .addAllGroupBy(groupBy)
                .build()));
    }

    /**
     * Extracts the {@link CurrentActionStatsQuery.ActionGroupFilter} from an {@link ActionStatsQuery}.
     * In a separate class for better testability/mock-ability.
     */
    @VisibleForTesting
    static class ActionGroupFilterExtractor {
        private final ActionSpecMapper actionSpecMapper;
        private final BuyRiScopeHandler buyRiScopeHandler;

        ActionGroupFilterExtractor(@Nonnull final ActionSpecMapper actionSpecMapper,
                                   @Nonnull final BuyRiScopeHandler buyRiScopeHandler) {
            this.actionSpecMapper = Objects.requireNonNull(actionSpecMapper);
            this.buyRiScopeHandler = Objects.requireNonNull(buyRiScopeHandler);
        }

        @Nonnull
        CurrentActionStatsQuery.ActionGroupFilter extractActionGroupFilter(
                @Nonnull final ActionStatsQuery query,
                @Nonnull final ApiId scope) {
            final CurrentActionStatsQuery.ActionGroupFilter.Builder agFilterBldr =
                CurrentActionStatsQuery.ActionGroupFilter.newBuilder()
                    .setVisible(true);

            CollectionUtils.emptyIfNull(query.actionInput().getActionModeList()).stream()
                .map(ActionSpecMapper::mapApiModeToXl)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .forEach(agFilterBldr::addActionMode);

            if (query.actionInput().getActionStateList() == null) {
                // if there is no filter from the UI query, filter out just ready, queued and in_progress actions
                Stream.of(ActionSpecMapper.OPERATIONAL_ACTION_STATES).forEach(agFilterBldr::addActionState);
            } else {
                query.actionInput().getActionStateList().stream()
                        .map(ActionSpecMapper::mapApiStateToXl)
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .forEach(agFilterBldr::addActionState);
            }

            CollectionUtils.emptyIfNull(query.actionInput().getRiskSubCategoryList()).stream()
                .map(ActionSpecMapper::mapApiActionCategoryToXl)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .forEach(agFilterBldr::addActionCategory);

            agFilterBldr.addAllActionType(buyRiScopeHandler.extractActionTypes(
                    query.actionInput(), scope));

            return agFilterBldr.build();
        }

    }

    /**
     * Creates an {@link EntityScope} - including the necessary group expansion, supply chain
     * lookups, and user-scope filtering.
     */
    @VisibleForTesting
    static class EntityScopeFactory {

        private final BuyRiScopeHandler buyRiScopeHandler;

        private final GroupExpander groupExpander;

        private final SupplyChainFetcherFactory supplyChainFetcherFactory;

        private final RepositoryApi repositoryApi;

        private final UuidMapper uuidMapper;

        EntityScopeFactory(@Nonnull final GroupExpander groupExpander,
                           @Nonnull final SupplyChainFetcherFactory supplyChainFetcherFactory,
                           @Nonnull final RepositoryApi repositoryApi,
                           @Nonnull final BuyRiScopeHandler buyRiScopeHandler,
                           @Nonnull final UuidMapper uuidMapper) {
            this.groupExpander = groupExpander;
            this.supplyChainFetcherFactory = supplyChainFetcherFactory;
            this.repositoryApi = repositoryApi;
            this.buyRiScopeHandler = buyRiScopeHandler;
            this.uuidMapper = uuidMapper;
        }

        Map<ApiId, Set<Long>> bulkExpandScopes(@Nonnull final Map<ApiId, Set<ApiId>> oidGroupList,
                @Nonnull final Set<Integer> relatedEntityTypes,
                @Nonnull final Optional<EnvironmentType> environmentType,
                @Nonnull final EntityAccessScope userScope) throws OperationFailedException {

            Map<Long, Set<Long>> responseNoAggregatedExpansion = new HashMap<>(oidGroupList.size());

            for (Entry<ApiId, Set<ApiId>> oidGroup : oidGroupList.entrySet()) {
                Set<ApiId> oids = oidGroup.getValue();
                final Set<Long> allEntitiesInScope;
                if (relatedEntityTypes.isEmpty()) {
                    // Expand groups that are in scope
                    // And then filter entities by environment type
                    Set<Long> unFilteredEntities = groupExpander.expandOids(oids);
                    if (environmentType.isPresent() && environmentType.get() != EnvironmentType.HYBRID) {
                        // Need to get repos data to get the environment type to allow filtering
                        List<ApiId> idList = unFilteredEntities.stream()
                                .map(uuidMapper::fromOid)
                                .collect(Collectors.toList());
                        uuidMapper.bulkResolveEntities(idList);
                        allEntitiesInScope = idList.stream()
                            .filter(id -> id.getCachedEntityInfo()
                                .map(e -> e.getEnvironmentType() == environmentType.get()).orElse(false))
                            .map(ApiId::oid)
                            .collect(Collectors.toSet());
                    } else {
                        allEntitiesInScope = unFilteredEntities;
                    }
                } else {
                    // TODO (roman, Feb 14 2019): The scope object is being expanded to allow
                    // looking up in-scope entities by type, so we can avoid the supply
                    // chain query here in the future if userScope is not "global."

                    // We need to get entities related to the scope, so we use the supply chain
                    // fetcher.
                    final SupplyChainNodeFetcherBuilder builder =
                            supplyChainFetcherFactory.newNodeFetcher().entityTypes(
                                    relatedEntityTypes.stream()
                                            .map(ApiEntityType::fromType)
                                            .map(ApiEntityType::apiStr)
                                            .collect(Collectors.toList()));
                    oids.stream().map(ApiId::uuid).forEach(builder::addSeedUuid);

                    environmentType.ifPresent(builder::environmentType);

                    allEntitiesInScope = builder.fetch().values().stream().flatMap(node -> node.getMembersByStateMap().values().stream()).flatMap(
                            memberList -> memberList.getMemberOidsList().stream()).collect(
                            Collectors.toSet());
                }
                allEntitiesInScope.addAll(buyRiScopeHandler.extractBuyRiEntities(oids, relatedEntityTypes));
                responseNoAggregatedExpansion.put(oidGroup.getKey().oid(), allEntitiesInScope);
            }


            final Map<Long, Set<Long>> withAggregatedExpansion =
                    supplyChainFetcherFactory.bulkExpandAggregatedEntities(responseNoAggregatedExpansion);
            final Map<ApiId, Set<Long>> finalResponse = new HashMap<>(withAggregatedExpansion.size());
            withAggregatedExpansion.forEach((id, vals) -> {
                finalResponse.put(uuidMapper.fromOid(id), userScope.filter(vals));
            });
            return finalResponse;
        }

        /**
         * Create an {@link EntityScope} to use in a current action stats query.
         *
         * @param oids The OIDs of entities in the scope.
         * @param relatedEntityTypes The entity types related to the oids that should be in the
         *                           scope. If this is not empty, we need to do a supply chain
         *                           query to figure out what's ACTUALLY in the scope.
         * @param environmentType The environment type to restrict the scope to.
         * @param userScope The {@link EntityAccessScope} object for the current user.
         * @param buyRiEntities Additional entities that must be added to the request to return
         * Buy RI actions in the scope.
         * @return The {@link EntityScope} to use for the query.
         * @throws OperationFailedException If one of the underlying RPC calls goes wrong.
         */
        @Nonnull
        EntityScope createEntityScope(@Nonnull final Set<ApiId> oids,
                  @Nonnull final Set<Integer> relatedEntityTypes,
                  @Nonnull final Optional<EnvironmentType> environmentType,
                  @Nonnull final EntityAccessScope userScope,
                  @Nonnull final Set<Long> buyRiEntities) throws OperationFailedException {
            final Set<Long> allEntitiesInScope;
            if (relatedEntityTypes.isEmpty()) {
                // Expand groups that are in scope
                // And then filter entities by environment type
                Set<Long> unFilteredEntities = groupExpander.expandOids(oids);
                if (environmentType.isPresent() && environmentType.get() != EnvironmentType.HYBRID) {
                    // Need to get repos data to get the environment type to allow filtering
                    allEntitiesInScope = repositoryApi.entitiesRequest(unFilteredEntities)
                            .getMinimalEntities()
                            .filter(minimalEntity -> minimalEntity.getEnvironmentType() == environmentType.get())
                            .map(minimalEntity -> minimalEntity.getOid())
                            .collect(Collectors.toSet());
                } else {
                    allEntitiesInScope = unFilteredEntities;
                }
            } else {
                // TODO (roman, Feb 14 2019): The scope object is being expanded to allow
                // looking up in-scope entities by type, so we can avoid the supply
                // chain query here in the future if userScope is not "global."

                // We need to get entities related to the scope, so we use the supply chain
                // fetcher.
                final SupplyChainNodeFetcherBuilder builder =
                    supplyChainFetcherFactory.newNodeFetcher()
                        .entityTypes(relatedEntityTypes.stream()
                            .map(ApiEntityType::fromType)
                            .map(ApiEntityType::apiStr)
                            .collect(Collectors.toList()));
                oids.stream()
                    .map(ApiId::uuid)
                    .forEach(builder::addSeedUuid);

                environmentType.ifPresent(builder::environmentType);

                allEntitiesInScope = builder.fetch().values().stream()
                    .flatMap(node -> node.getMembersByStateMap().values().stream())
                    .flatMap(memberList -> memberList.getMemberOidsList().stream())
                    .collect(Collectors.toSet());
            }

            // if there are grouping entity like DataCenter, we should expand it to PMs to show
            // all actions for PMs in this DataCenter
            final Set<Long> entitiesInUserScope = userScope.filter(
                supplyChainFetcherFactory.expandAggregatedEntities(allEntitiesInScope));
            return EntityScope.newBuilder()
                .addAllOids(Sets.union(entitiesInUserScope, buyRiEntities))
                .build();
        }
    }

    /**
     * Extracts {@link ScopeFilter}s from an {@link ActionStatsQuery}.
     * Lives in its own class for easier testability/mocking.
     */
    @VisibleForTesting
    static class ScopeFilterExtractor {
        private final UserSessionContext userSessionContext;

        private final EntityScopeFactory entityScopeFactory;

        private final UuidMapper uuidMapper;

        ScopeFilterExtractor(@Nonnull final UserSessionContext userSessionContext,
                             @Nonnull final EntityScopeFactory entityScopeFactory,
                             @Nonnull final UuidMapper uuidMapper) {
            this.userSessionContext = Objects.requireNonNull(userSessionContext);
            this.entityScopeFactory = Objects.requireNonNull(entityScopeFactory);
            this.uuidMapper = Objects.requireNonNull(uuidMapper);
        }

        /**
         * Extract the {@link ScopeFilter}s contained in an {@link ActionStatsQuery}.
         * This method may make RPC calls - e.g. calls to expand groups, fetch related entity types,
         * and so on.
         *
         * @param query The query.
         * @return (scope id) -> ({@link ScopeFilter}) for every scope in {@link ActionStatsQuery#scopes()}
         * @throws OperationFailedException If there is an issue with any of the underlying RPC
         *         calls.
         */
        @Nonnull
        public Map<ApiId, ScopeFilter> extractScopeFilters(@Nonnull final ActionStatsQuery query)
                throws OperationFailedException {
            final Set<Integer> relatedEntityTypes = query.getRelatedEntityTypes();

            final EntityAccessScope userScope = userSessionContext.getUserAccessScope();

            final Map<ApiId, ScopeFilter> filtersByScope =
                new HashMap<>(query.scopes().size());
            Map<ApiId, Set<ApiId>> normalExpansionScopes = new HashMap<>();
            for (final ApiId scope : query.scopes()) {
                final ScopeFilter.Builder scopeBuilder = ScopeFilter.newBuilder();
                if (scope.isRealtimeMarket() || scope.isPlan()) {
                    scopeBuilder.setTopologyContextId(scope.oid());
                    if (userScope.containsAll() || scope.isPlan()) {
                        final GlobalScope.Builder globalScopeBuilder = GlobalScope.newBuilder();
                        query.getEnvironmentType().ifPresent(globalScopeBuilder::setEnvironmentType);
                        globalScopeBuilder.addAllEntityType(relatedEntityTypes);
                        scopeBuilder.setGlobal(globalScopeBuilder);
                        filtersByScope.put(scope, scopeBuilder.build());
                    } else {
                        // Take only the entities in the scope.
                        Set<ApiId> ids = new HashSet<>(userScope.accessibleOids().size());
                        for (Long oid : userScope.accessibleOids()) {
                            ids.add(uuidMapper.fromOid(oid));
                        }
                        normalExpansionScopes.put(scope, ids);
                    }
                } else if (scope.isGlobalTempGroup()) {
                    // this is an optimization because evaluating the entities in the global scope,
                    // especially for for a temp global group containing all regions are zones
                    // is EXTREMELY EXPENSIVE.
                    // taken from HistoricalQueryMapper.extractMgmtUnitSubgroupFilter
                    // If it's a global-scope temporary group, we treat it as a case of the market.
                    final GlobalScope.Builder globalScope = GlobalScope.newBuilder();
                    if (query.getEnvironmentType().isPresent()) {
                        globalScope.setEnvironmentType(query.getEnvironmentType().get());
                    }
                    // If the query doesn't specify explicit related entity types, use the type
                    // of the group as the entity type.
                    //
                    // If the query DOES specify explicit related entity types, ignore the group
                    // entity types. i.e. saying "get me stats for all PMs related to all VMs in
                    // the system" is pretty much the same - or close enough - to "get me stats for
                    // all PMs in the system".
                    if (query.getRelatedEntityTypes().isEmpty()) {
                        // The .get() is safe because we know it's a group (or else we wouldn't be
                        // in this block.
                        scope.getScopeTypes().get().stream()
                            .map(ApiEntityType::typeNumber)
                            .forEach(globalScope::addEntityType);
                    } else {
                        globalScope.addAllEntityType(relatedEntityTypes);
                    }

                    scopeBuilder.setGlobal(globalScope.build());
                    filtersByScope.put(scope, scopeBuilder.build());
                } else {
                    // Right now there is no way to specify an entity scope within a plan,
                    // so we leave the context ID unset (default = realtime).
                    normalExpansionScopes.put(scope, Collections.singleton(scope));
                }

            }

            Map<ApiId, Set<Long>> expandedScopes =
                entityScopeFactory.bulkExpandScopes(normalExpansionScopes,
                        relatedEntityTypes, query.getEnvironmentType(), userScope);
            expandedScopes.forEach((scope, entities) -> {
                final ScopeFilter.Builder scopeFilterBldr = ScopeFilter.newBuilder()
                    .setEntityList(EntityScope.newBuilder()
                        .addAllOids(entities));
                if (scope.isRealtimeMarket() || scope.isPlan()) {
                    scopeFilterBldr.setTopologyContextId(scope.oid());
                }
                filtersByScope.put(scope, scopeFilterBldr.build());
            });
            return filtersByScope;
        }

    }

    /**
     * Extracts XL-supported group-by criteria from an {@link ActionStatsQuery}.
     */
    @VisibleForTesting
    static class GroupByExtractor {

        @Nonnull
        List<GroupBy> extractGroupByCriteria(@Nonnull final ActionStatsQuery query) {
            return CollectionUtils.emptyIfNull(query.actionInput().getGroupBy()).stream()
                .map(groupByStr -> {
                    switch (groupByStr) {
                        case StringConstants.RISK_SUB_CATEGORY:
                            return Optional.of(GroupBy.ACTION_CATEGORY);
                        case StringConstants.RISK:
                            return Optional.of(GroupBy.ACTION_RELATED_RISK);
                        case StringConstants.SEVERITY:
                            return Optional.of(GroupBy.SEVERITY);
                        case StringConstants.ACTION_STATES:
                            return Optional.of(GroupBy.ACTION_STATE);
                        case StringConstants.ACTION_TYPE:
                        case StringConstants.ACTION_TYPES:
                            return Optional.of(GroupBy.ACTION_TYPE);
                        case StringConstants.ACTION_COST_TYPE:
                            return Optional.of(GroupBy.COST_TYPE);
                        case StringConstants.TARGET_TYPE:
                            return Optional.of(GroupBy.TARGET_ENTITY_TYPE);
                        case StringConstants.TEMPLATE:
                            // If the request is to group by template, we fall through to group by target entity id.
                        case StringConstants.TARGET_UUID_CC:
                            return Optional.of(GroupBy.TARGET_ENTITY_ID);
                        case StringConstants.REASON_COMMODITY:
                            return Optional.of(GroupBy.REASON_COMMODITY);
                        case StringConstants.BUSINESS_UNIT:
                            return Optional.of(GroupBy.BUSINESS_ACCOUNT_ID);
                        case StringConstants.RESOURCE_GROUP:
                            return Optional.of(GroupBy.RESOURCE_GROUP_ID);
                        case StringConstants.CSP:
                            return Optional.of(GroupBy.CSP);
                        default:
                            logger.error("Unhandled action stats group-by criteria: {}", groupByStr);
                            return Optional.<GroupBy>empty();
                    }
                })
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
        }
    }

}
