package com.vmturbo.api.component.external.api.util.stats;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.MarketMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.UserScopeUtils;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.UIEntityType;

/**
 * Responsible for expanding the {@link ApiId} that a stats query is scoped to into the list
 * of entities in the scope.
 */
public class StatsQueryScopeExpander {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Map Entity types to be expanded to the RelatedEntityType to retrieve. For example,
     * replace requests for stats for a DATACENTER entity with the PHYSICAL_MACHINEs
     * in that DATACENTER.
     */
    private static final Map<UIEntityType, UIEntityType> ENTITY_TYPES_TO_EXPAND = ImmutableMap.of(
        UIEntityType.DATACENTER, UIEntityType.PHYSICAL_MACHINE
    );

    private final GroupExpander groupExpander;

    private final RepositoryApi repositoryApi;

    private final SupplyChainFetcherFactory supplyChainFetcherFactory;

    private final UserSessionContext userSessionContext;

    public StatsQueryScopeExpander(@Nonnull final GroupExpander groupExpander,
                                   @Nonnull final RepositoryApi repositoryApi,
                                   @Nonnull final SupplyChainFetcherFactory supplyChainFetcherFactory,
                                   @Nonnull final UserSessionContext userSessionContext) {
        this.groupExpander = groupExpander;
        this.repositoryApi = repositoryApi;
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
        Set<UIEntityType> entityTypes();

        /**
         * If set, restrict the query only to the specified environment type.
         */
        Optional<EnvironmentType> environmentType();
    }

    /**
     * The expanded scope of a stats query.
     */
    public static class StatsQueryScope {
        private final Set<Long> scope;
        private final Optional<GlobalScope> all;

        /**
         * Do not use directly! Use {@link StatsQueryScope#getGlobalScope()} or {@link StatsQueryScope#some(Set)}.
         */
        private StatsQueryScope(final Set<Long> scope, final Optional<GlobalScope> all) {
            this.scope = scope;
            this.all = all;
        }

        /**
         * If the scope is global, returns a {@link GlobalScope} describing the global scope.
         */
        public Optional<GlobalScope> getGlobalScope() {
            return all;
        }

        /**
         * Returns the specific entities in the expanded scope.
         * Note - if {@link StatsQueryScope#getGlobalScope()} is non-empty, this will be empty. But it may
         * also be empty if the expanded scope contains nothing.
         */
        @Nonnull
        public Set<Long> getEntities() {
            return scope;
        }

        @Nonnull
        public static StatsQueryScope all(@Nonnull final GlobalScope globalScope) {
            return new StatsQueryScope(Collections.emptySet(), Optional.of(globalScope));
        }

        @Nonnull
        public static StatsQueryScope some(@Nonnull final Set<Long> scope) {
            return new StatsQueryScope(scope, Optional.empty());
        }
    }

    @Nonnull
    public StatsQueryScope expandScope(@Nonnull final ApiId scope,
                                       @Nonnull final List<StatApiInputDTO> statistics) throws OperationFailedException {
        // expand any ServiceEntities that should be replaced by related ServiceEntities,
        // e.g. DataCenter is replaced by the PhysicalMachines in the DataCenter
        // and perform supply chain traversal to fetch connected entities
        // whose type is included in the "relatedEntityType" fields of the present query
        final List<UIEntityType> relatedTypes = CollectionUtils.emptyIfNull(statistics).stream()
            .map(StatApiInputDTO::getRelatedEntityType)
            .filter(Objects::nonNull)
            .map(UIEntityType::fromString)
            .collect(Collectors.toList());

        // Full market.
        if (scope.isRealtimeMarket()) {
            if (userSessionContext.isUserScoped()) {
                return StatsQueryScope.some(userSessionContext
                    .getUserAccessScope().accessibleOids().toSet());
            } else {
                return StatsQueryScope.all(ImmutableGlobalScope.builder()
                    .entityTypes(relatedTypes)
                    .build());
            }
        }

        final Set<Long> immediateOidsInScope;
        if (scope.isGlobalTempGroup()) {
            // If it's a global temp group group, we don't worry about fully expanding it.
            List<UIEntityType> entityTypes = relatedTypes.isEmpty() ?
                Collections.singletonList(scope.getCachedGroupInfo().get().getEntityType()) :
                relatedTypes;
            return StatsQueryScope.all(ImmutableGlobalScope.builder()
                .entityTypes(entityTypes)
                .environmentType(scope.getCachedGroupInfo().get().getGlobalEnvType())
                .build());
        } if (scope.isGroup()) {
            immediateOidsInScope = groupExpander.expandOids(Collections.singleton(scope.oid()));
        } else if (scope.isTarget()) {
            immediateOidsInScope = repositoryApi.newSearchRequest(
                SearchProtoUtil.makeSearchParameters(
                    SearchProtoUtil.discoveredBy(scope.oid()))
                    .build())
                .getOids();
        } else if (scope.isPlan()) {
            Set<Long> explicitPlanScope = scope.getPlanInstance()
                .map(MarketMapper::getPlanScopeIds)
                .orElse(Collections.emptySet());
            // If the plan is not scoped, it must be defined on the entire market, so
            // the expanded scope is "all".
            if (explicitPlanScope.isEmpty()) {
                return StatsQueryScope.all(ImmutableGlobalScope.builder()
                    .entityTypes(relatedTypes)
                    .build());
            } else {
                immediateOidsInScope = explicitPlanScope;
            }
        } else {
            immediateOidsInScope = Sets.newHashSet(scope.oid());
        }

        final Set<Long> expandedOidsInScope;

        if (!relatedTypes.isEmpty()) {
            // We replace the proxy entities after first finding related type entities, so that the
            // supply chain search for related entities has the correct starting point (the original
            // entities in the request, rather than the replacement entities).
            expandedOidsInScope = expandGroupingServiceEntities(supplyChainFetcherFactory.expandScope(
                immediateOidsInScope, relatedTypes.stream()
                    .map(UIEntityType::apiStr)
                    .collect(Collectors.toList())));
        } else {
            expandedOidsInScope = expandGroupingServiceEntities(immediateOidsInScope);
        }

        // if the user is scoped and this is not a plan, we need to check if the user has
        // access to the resulting entity set.
        if (!scope.isPlan()) {
            UserScopeUtils.checkAccess(userSessionContext, expandedOidsInScope);
        }

        return StatsQueryScope.some(expandedOidsInScope);
    }

    /**
     * Replace specific types of ServiceEntities with "constituents". For example, a DataCenter SE
     * is replaced by the PhysicalMachine SE's related to that DataCenter.
     *<p>
     * ServiceEntities of other types not to be expanded are copied to the output result set.
     *<p>
     * See 6.x method SupplyChainUtils.getUuidsFromScopesByRelatedType() which uses the
     * marker interface EntitiesProvider to determine which Service Entities to expand.
     *<p>
     * Errors fetching the supply chain are logged and ignored - the input OID will be copied
     * to the output in case of an error or missing relatedEntityType info in the supply chain.
     *<p>
     * First, it will fetch entities which need to expand, then check if any input entity oid
     * belongs to those entities. Because if input entity set is large, it will cost a lot time to
     * fetch huge entity from Repository. Instead, if first fetch those entities which need to expand
     * , the amount will be much less than the input entity set size since right now only DataCenter
     * could expand.
     *
     * @param entityOidsToExpand a set of ServiceEntity OIDs to examine
     * @return a set of ServiceEntity OIDs with types that should be expanded replaced by the
     * "constituent" ServiceEntity OIDs as computed by the supply chain.
     */
    private Set<Long> expandGroupingServiceEntities(Collection<Long> entityOidsToExpand) {
        // Early return if the input is empty, to prevent making
        // the initial RPC call.
        if (entityOidsToExpand.isEmpty()) {
            return Collections.emptySet();
        }

        final Set<Long> expandedEntityOids = Sets.newHashSet();
        // get all service entities which need to expand.
        final Map<Long, MinimalEntity> expandServiceEntities = ENTITY_TYPES_TO_EXPAND.keySet().stream()
            .flatMap(entityType -> repositoryApi.newSearchRequest(SearchProtoUtil.makeSearchParameters(
                SearchProtoUtil.entityTypeFilter(entityType)).build())
                .getMinimalEntities())
            .collect(Collectors.toMap(MinimalEntity::getOid, Function.identity()));

        // go through each entity and check if it needs to expand.
        for (Long oidToExpand : entityOidsToExpand) {
            try {
                // if expandServiceEntityMap contains oid, it means current oid entity needs to expand.
                if (expandServiceEntities.containsKey(oidToExpand)) {
                    final MinimalEntity expandEntity = expandServiceEntities.get(oidToExpand);
                    final String relatedEntityType =
                        ENTITY_TYPES_TO_EXPAND.get(UIEntityType.fromType(expandEntity.getEntityType())).apiStr();
                    // fetch the supply chain map:  entity type -> SupplyChainNode
                    Map<String, SupplyChainNode> supplyChainMap = supplyChainFetcherFactory
                        .newNodeFetcher()
                        .entityTypes(Collections.singletonList(relatedEntityType))
                        .addSeedUuid(Long.toString(expandEntity.getOid()))
                        .fetch();
                    SupplyChainNode relatedEntities = supplyChainMap.get(relatedEntityType);
                    if (relatedEntities != null) {
                        expandedEntityOids.addAll(RepositoryDTOUtil.getAllMemberOids(relatedEntities));
                    } else {
                        logger.warn("RelatedEntityType {} not found in supply chain for {}; " +
                            "the entity is discarded", relatedEntityType, expandEntity.getOid());
                    }
                } else {
                    expandedEntityOids.add(oidToExpand);
                }
            } catch (OperationFailedException e) {
                logger.warn("Error fetching supplychain for {}: ", oidToExpand, e.getMessage());
                // include the OID unexpanded
                expandedEntityOids.add(oidToExpand);
            }
        }
        return expandedEntityOids;
    }
}
