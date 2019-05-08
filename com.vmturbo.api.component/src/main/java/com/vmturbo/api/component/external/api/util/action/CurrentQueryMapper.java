package com.vmturbo.api.component.external.api.util.action;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.ActionTypeMapper;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
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
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.components.common.utils.StringConstants;

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
                       @Nonnull final UserSessionContext userSessionContext) {
        this(new ActionGroupFilterExtractor(actionSpecMapper),
            new GroupByExtractor(),
            new ScopeFilterExtractor(userSessionContext,
                new EntityScopeFactory(groupExpander, supplyChainFetcherFactory)));
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

        final CurrentActionStatsQuery.ActionGroupFilter actionGroupFilter =
            actionGroupFilterExtractor.extractActionGroupFilter(query);

        final List<GroupBy> groupBy = groupByExtractor.extractGroupByCriteria(query);

        final Map<ApiId, ScopeFilter> filtersByScope = scopeFilterExtractor.extractScopeFilters(query);

        return filtersByScope.entrySet().stream()
            .collect(Collectors.toMap(Entry::getKey, e -> CurrentActionStatsQuery.newBuilder()
                .setActionGroupFilter(actionGroupFilter)
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

        ActionGroupFilterExtractor(@Nonnull final ActionSpecMapper actionSpecMapper) {
            this.actionSpecMapper = Objects.requireNonNull(actionSpecMapper);
        }

        @Nonnull
        CurrentActionStatsQuery.ActionGroupFilter extractActionGroupFilter(@Nonnull final ActionStatsQuery query) {
            final CurrentActionStatsQuery.ActionGroupFilter.Builder agFilterBldr =
                CurrentActionStatsQuery.ActionGroupFilter.newBuilder();

            CollectionUtils.emptyIfNull(query.actionInput().getActionModeList()).stream()
                .map(actionSpecMapper::mapApiModeToXl)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .forEach(agFilterBldr::addActionMode);

            CollectionUtils.emptyIfNull(query.actionInput().getActionStateList()).stream()
                .map(actionSpecMapper::mapApiStateToXl)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .forEach(agFilterBldr::addActionState);

            CollectionUtils.emptyIfNull(query.actionInput().getRiskSubCategoryList()).stream()
                .map(actionSpecMapper::mapApiActionCategoryToXl)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .forEach(agFilterBldr::addActionCategory);

            CollectionUtils.emptyIfNull(query.actionInput().getActionTypeList()).stream()
                .map(ActionTypeMapper::fromApi)
                .flatMap(Collection::stream)
                .forEach(agFilterBldr::addActionType);

            return agFilterBldr.build();
        }

    }

    /**
     * Creates an {@link EntityScope} - including the necessary group expansion, supply chain
     * lookups, and user-scope filtering.
     */
    @VisibleForTesting
    static class EntityScopeFactory {
        private final GroupExpander groupExpander;

        private final SupplyChainFetcherFactory supplyChainFetcherFactory;

        EntityScopeFactory(@Nonnull final GroupExpander groupExpander,
                           @Nonnull final SupplyChainFetcherFactory supplyChainFetcherFactory) {
            this.groupExpander = groupExpander;
            this.supplyChainFetcherFactory = supplyChainFetcherFactory;
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
         * @return The {@link EntityScope} to use for the query.
         * @throws OperationFailedException If one of the underlying RPC calls goes wrong.
         */
        @Nonnull
        EntityScope createEntityScope(@Nonnull final Set<Long> oids,
                  @Nonnull final Set<Integer> relatedEntityTypes,
                  @Nonnull final Optional<EnvironmentTypeEnum.EnvironmentType> environmentType,
                  @Nonnull final EntityAccessScope userScope) throws OperationFailedException {
            final Set<Long> allEntitiesInScope;
            if (relatedEntityTypes.isEmpty()) {
                // The scope may still be a group, in which case we need to expand the group.
                //
                // TODO (roman, Feb 8 2019): GroupExpander should accept an environment type,
                // so that we can filter the members of a group by environment type.
                allEntitiesInScope = groupExpander.expandOids(oids);
            } else {
                // TODO (roman, Feb 14 2019): The scope object is being expanded to allow
                // looking up in-scope entities by type, so we can avoid the supply
                // chain query here in the future if userScope is not "global."

                // We need to get entities related to the scope, so we use the supply chain
                // fetcher.
                final SupplyChainNodeFetcherBuilder builder =
                    supplyChainFetcherFactory.newNodeFetcher()
                        .entityTypes(relatedEntityTypes.stream()
                            .map(ServiceEntityMapper::toUIEntityType)
                            .collect(Collectors.toList()));
                oids.stream()
                    .map(oid -> oid.toString())
                    .forEach(builder::addSeedUuid);

                environmentType.ifPresent(builder::environmentType);

                allEntitiesInScope = builder.fetch().values().stream()
                    .flatMap(node -> node.getMembersByStateMap().values().stream())
                    .flatMap(memberList -> memberList.getMemberOidsList().stream())
                    .collect(Collectors.toSet());
            }

            final Set<Long> entitiesInUserScope = userScope.filter(allEntitiesInScope);
            return EntityScope.newBuilder()
                .addAllOids(entitiesInUserScope)
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

        ScopeFilterExtractor(@Nonnull final UserSessionContext userSessionContext,
                             @Nonnull final EntityScopeFactory entityScopeFactory) {
            this.userSessionContext = Objects.requireNonNull(userSessionContext);
            this.entityScopeFactory = Objects.requireNonNull(entityScopeFactory);
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
            final ScopeFilter.Builder scopeBuilder = ScopeFilter.newBuilder();

            final EntityAccessScope userScope = userSessionContext.getUserAccessScope();

            final Map<ApiId, ScopeFilter> filtersByScope =
                new HashMap<>(query.scopes().size());
            for (final ApiId scope : query.scopes()) {
                if (scope.isRealtimeMarket() || scope.isPlan()) {
                    scopeBuilder.setTopologyContextId(scope.oid());
                    if (userScope.containsAll() || scope.isPlan()) {
                        final GlobalScope.Builder globalScopeBuilder = GlobalScope.newBuilder();
                        query.getEnvironmentType().ifPresent(globalScopeBuilder::setEnvironmentType);
                        globalScopeBuilder.addAllEntityType(relatedEntityTypes);
                        scopeBuilder.setGlobal(globalScopeBuilder);
                    } else {
                        // Take only the entities in the scope.
                        scopeBuilder.setEntityList(entityScopeFactory.createEntityScope(
                            Sets.newHashSet(userScope.getScopeGroupIds()),
                            relatedEntityTypes,
                            query.getEnvironmentType(),
                            userScope));
                    }
                } else {
                    // Right now there is no way to specify an entity scope within a plan,
                    // so we leave the context ID unset (default = realtime).

                    scopeBuilder.setEntityList(entityScopeFactory.createEntityScope(
                        Collections.singleton(scope.oid()),
                        relatedEntityTypes,
                        query.getEnvironmentType(),
                        userScope));
                }

                filtersByScope.put(scope, scopeBuilder.build());
            }
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
                        case StringConstants.RISK_DESCRIPTION:
                            return Optional.of(GroupBy.ACTION_EXPLANATION);
                        case StringConstants.ACTION_STATES:
                            return Optional.of(GroupBy.ACTION_STATE);
                        case StringConstants.ACTION_TYPE:
                        case StringConstants.ACTION_TYPES:
                            return Optional.of(GroupBy.ACTION_TYPE);
                        case StringConstants.TARGET_TYPE:
                            return Optional.of(GroupBy.TARGET_ENTITY_TYPE);
                        case StringConstants.TEMPLATE:
                            // If the request is to group by template, we fall through to group by target entity id.
                        case StringConstants.TARGET_UUID_CC:
                            return Optional.of(GroupBy.TARGET_ENTITY_ID);
                        case StringConstants.REASON_COMMODITY:
                            return Optional.of(GroupBy.REASON_COMMODITY);
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
