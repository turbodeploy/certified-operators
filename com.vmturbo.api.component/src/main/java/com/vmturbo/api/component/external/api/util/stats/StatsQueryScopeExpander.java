package com.vmturbo.api.component.external.api.util.stats;

import static java.util.stream.Collectors.toSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import com.google.common.collect.Sets;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.CachedPlanInfo;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.UserScopeUtils;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.topology.ApiEntityType;

/**
 * Responsible for expanding the {@link ApiId} that a stats query is scoped to into the list
 * of entities in the scope.
 */
public class StatsQueryScopeExpander {

    private static final Logger logger = LogManager.getLogger();

    private final SupplyChainFetcherFactory supplyChainFetcherFactory;

    private final UserSessionContext userSessionContext;

    public StatsQueryScopeExpander(@Nonnull final SupplyChainFetcherFactory supplyChainFetcherFactory,
                                   @Nonnull final UserSessionContext userSessionContext) {
        this.supplyChainFetcherFactory = supplyChainFetcherFactory;
        this.userSessionContext = userSessionContext;
    }

    /**
     * If a stats query applies to a "global" scope, this object contains restrictions on the
     * entities that are included in the query.
     */
    @Value.Immutable
    public interface GlobalScope {

        /**
         * If non-empty, restrict the query only to the specified entity types.
         */
        Set<ApiEntityType> entityTypes();

        /**
         * If set, restrict the query only to the specified environment type.
         */
        Optional<EnvironmentType> environmentType();
    }

    /**
     * The scope of a stats query.
     */
    public interface StatsQueryScope {
        /**
         * If the scope is global, returns a {@link GlobalScope} describing the global scope.
         *
         * @return the global scope.
         */
        Optional<GlobalScope> getGlobalScope();

        /**
         * Returns the specific entities in the expanded scope.
         * Note - if {@link StatsQueryScope#getGlobalScope()} is non-empty, this will be empty. But it may
         * also be empty if the expanded scope contains nothing.
         *
         * @return the expanded entities
         */
        @Nonnull
        Set<Long> getExpandedOids();

        /**
         * Returns the entities  in the scope. This will be a single entity if the scope is
         * single entity or members of group if the scope is group. This is versus
         * {@link StatsQueryScope#getExpandedOids()} where some entity types (e.g., accounts) are
         * replaced with entities they contain.
         *
         * @return the immediate entities.
         */
        @Nonnull
        Set<Long> getScopeOids();

    }

    /**
     * This class represents an implementation of {@link StatsQueryScope} which
     * populates the different fields as needed rather than populate them initially.
     */
    private static class StatQueryScopeLazyLoader implements StatsQueryScope {
        private final GlobalScope globalScope;
        private final Set<Long> scopeOids;
        @GuardedBy("lock")
        private Set<Long> expandedOids = null;
        private final Object lock = new Object();

        private final StatsQueryScopeExpander expander;
        private final ApiId scope;
        private final Set<ApiEntityType> relatedTypes;

        StatQueryScopeLazyLoader(@Nonnull final StatsQueryScopeExpander expander,
                                 @Nonnull final ApiId scope,
                                 @Nullable final GlobalScope globalScope,
                                 @Nullable final Set<Long> scopeOids,
                                 @Nonnull final Set<ApiEntityType> relatedTypes,
                                 @Nonnull final UserSessionContext userSessionContext) {
            this.expander = expander;
            this.scope = scope;
            this.relatedTypes = relatedTypes;
            this.globalScope = globalScope;
            if ((globalScope != null || CollectionUtils.isEmpty(scopeOids))
                && !expander.supplyChainFetcherFactory.containsApplicationType(relatedTypes)) {
                this.scopeOids = fetchScopedOids(userSessionContext, Collections.emptySet());
                this.expandedOids = fetchExpandedOids(userSessionContext, relatedTypes);
            } else {
                Set<Long> oids = fetchScopedOids(userSessionContext, scopeOids);
                oids.retainAll(scopeOids);
                this.scopeOids = oids;
                if (scope.isRealtimeMarket()) {
                    this.expandedOids = this.scopeOids;
                }
            }
        }

        private static Set<Long> fetchExpandedOids(
                @Nonnull final UserSessionContext userSessionContext,
                @Nonnull final Set<ApiEntityType> relatedTypes) {
            return (userSessionContext.isUserObserver() && userSessionContext.isUserScoped()) ?
                    userSessionContext.getUserAccessScope().getAccessibleOidsByEntityTypes(
                            relatedTypes.stream().map(ApiEntityType::apiStr).collect(toSet())).toSet() :
                    Collections.emptySet();
        }

        private static Set<Long> fetchScopedOids(
                @Nonnull final UserSessionContext userSessionContext,
                @Nonnull final Set<Long> scopeOids) {
            return (userSessionContext.isUserObserver() && userSessionContext.isUserScoped()) ?
                    userSessionContext.getUserAccessScope().accessibleOids().toSet() :
                    scopeOids;
        }

        @Override
        public Optional<GlobalScope> getGlobalScope() {
            return Optional.ofNullable(globalScope);
        }

        @Nonnull
        @Override
        public Set<Long> getScopeOids() {
            return Collections.unmodifiableSet(scopeOids);
        }

        @Nonnull
        @Override
        public Set<Long> getExpandedOids() {
            synchronized (lock) {
                if (expandedOids == null) {
                    expandedOids = expander.findExpandedOids(scopeOids, scope, relatedTypes);
                }
                return Collections.unmodifiableSet(expandedOids);
            }
        }
    }

    /**
     * Returns a {@link GlobalScope} object if the scope is an instance of global scope. A global
     * scope is
     * a scope that can potentially have all the entities there where those got filtered by type or
     * other things.
     *
     * @param scope the input scope.
     * @param relatedTypes the types that should be filter.
     * @return if the scope is a global scope the {@link GlobalScope} object otherwise null.
     */
    @Nullable
    private Optional<GlobalScope> findGlobalScope(ApiId scope, Set<ApiEntityType> relatedTypes) {
        // Full market.
        if (scope.isRealtimeMarket()) {
            if (!userSessionContext.isUserScoped()) {
                return Optional.of(ImmutableGlobalScope.builder()
                    .entityTypes(relatedTypes)
                    .build());
            } else {
                return Optional.empty();
            }
        }

        if (scope.isGlobalTempGroup()) {
            // If it's a global temp group group, we don't worry about fully expanding it.
            List<ApiEntityType> entityTypes = relatedTypes.isEmpty() ?
                new ArrayList<>(scope.getCachedGroupInfo().get().getEntityTypes()) :
                new ArrayList<>(relatedTypes);
            return Optional.of(ImmutableGlobalScope.builder()
                .entityTypes(entityTypes)
                .environmentType(scope.getCachedGroupInfo().get().getGlobalEnvType())
                .build());
        }

        if (scope.isPlan()) {
            final Set<Long>  explicitPlanScope = scope.getCachedPlanInfo()
                .map(CachedPlanInfo::getPlanScopeIds)
                .orElse(Collections.emptySet());

            // If the plan is not scoped, it must be defined on the entire market, so
            // the expanded scope is "all".
            if (explicitPlanScope.isEmpty()) {
                return Optional.of(ImmutableGlobalScope.builder()
                .entityTypes(relatedTypes)
                    .build());
            }
        }

        return Optional.empty();
    }

    /**
     * Gets the expanded entity oids in the scope based on the immediate oids in that scope. The
     * expanded entities are only apply to a number of entities entity types (e.g., accounts) which
     * are replaced by a number of other entities.
     *
     * @param immediateOidsInScope the immediate oids in the scope.
     * @param scope the input scope.
     * @param relatedTypes the related types that we care about.
     * @return the expanded oids.
     */
    @Nonnull
    private Set<Long> findExpandedOids(@Nonnull final Set<Long> immediateOidsInScope,
                                       @Nonnull final ApiId scope,
                                       @Nonnull final Set<ApiEntityType> relatedTypes) {
        if (immediateOidsInScope.size() == 0) {
            return immediateOidsInScope;
        }

        Set<Long> expandedOidsInScope;

        if (!relatedTypes.isEmpty()) {
            // We replace the proxy entities after first finding related type entities, so that the
            // supply chain search for related entities has the correct starting point (the original
            // entities in the request, rather than the replacement entities).
            try {
                // we should use scopeOid(without scope expanding) in case of resource group or
                // group of resource groups otherwise we miss special resource group logic for supplyChain
                final Set<Long> fetchedScope =
                        !scope.isResourceGroupOrGroupOfResourceGroups() ? immediateOidsInScope :
                                Collections.singleton(scope.oid());
                expandedOidsInScope = supplyChainFetcherFactory.expandAggregatedEntities(
                        supplyChainFetcherFactory.expandScope(fetchedScope, relatedTypes.stream()
                                .map(ApiEntityType::apiStr)
                                .collect(Collectors.toList()), scope.getTopologyContextId()));
            } catch (Exception ex) {
                logger.error("The operation to get the expanded entities associated with list of " +
                        "OIDs {}, with types {} failed. Going with unexpanded entities.",
                    immediateOidsInScope.stream().map(String::valueOf).collect(Collectors.joining(", ")),
                    relatedTypes.stream().map(ApiEntityType::apiStr).collect(Collectors.joining(", ")), ex);
                expandedOidsInScope = immediateOidsInScope;
            }
        } else {
            expandedOidsInScope = supplyChainFetcherFactory.expandAggregatedEntities(immediateOidsInScope);
        }

        if (!scope.isPlan()) {
            UserScopeUtils.checkAccess(userSessionContext, expandedOidsInScope);
        }

        if (userSessionContext.isUserScoped() && userSessionContext.isUserObserver()) {
            Set<Long> userScopedOids = StatQueryScopeLazyLoader.fetchExpandedOids(userSessionContext, relatedTypes);
            expandedOidsInScope.retainAll(userScopedOids);
        }

        return expandedOidsInScope;
    }

    /**
     * Expands input scope given the input stats.
     *
     * @param scope the input scope.
     * @param statistics the requested stats.
     * @return expanded scope represented by a {@link StatsQueryScope} object.
     */
    @Nonnull
    public StatsQueryScope expandScope(@Nonnull final ApiId scope,
                                       @Nonnull final List<StatApiInputDTO> statistics) {
        final Set<ApiEntityType> relatedTypes = CollectionUtils.emptyIfNull(statistics).stream()
            .map(StatApiInputDTO::getRelatedEntityType)
            .filter(Objects::nonNull)
            .map(ApiEntityType::fromString)
            .collect(Collectors.toSet());

        final Optional<GlobalScope> globalScope = findGlobalScope(scope, relatedTypes);
        final Set<Long> scopeOids;

        if (globalScope.isPresent() && !supplyChainFetcherFactory.containsApplicationType(relatedTypes)) {
            scopeOids = null;
        } else {
            scopeOids = scope.getScopeOids(userSessionContext, statistics);
        }

        return new StatQueryScopeLazyLoader(
                this, scope, globalScope.orElse(null),
                scopeOids, relatedTypes, userSessionContext);
    }
}
