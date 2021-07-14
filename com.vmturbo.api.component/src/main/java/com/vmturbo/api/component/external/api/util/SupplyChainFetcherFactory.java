package com.vmturbo.api.component.external.api.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.mapper.aspect.EntityAspectMapper;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainEntryDTO;
import com.vmturbo.api.enums.EntityDetailType;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.api.mappers.EnvironmentTypeMapper;
import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeveritiesResponse;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeveritiesResponse.TypeCase;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.MultiEntityRequest;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.SeverityCountsResponse;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc.EntitySeverityServiceBlockingStub;
import com.vmturbo.common.protobuf.cloud.CloudCommon.EntityFilter;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatsQuery;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudCostStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudCostStatsResponse;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetMultiSupplyChainsRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainResponse;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainStatsRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainGroupBy;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainScope;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainSeed;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainStat;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.EnvironmentTypeUtil;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.UIEntityState;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.commons.Pair;
import com.vmturbo.group.api.GroupAndMembers;
import com.vmturbo.platform.common.dto.CommonDTO;

/**
 * A factory class for various {@link SupplychainFetcher}s.
 */
public class SupplyChainFetcherFactory {

    /**
     * Sometimes we need to expand aggregators to some of their aggregated
     * entities. In the case of cloud, we need to be able to expand aggregators
     * such as region, zone, and business account to aggregated entities whose
     * type belongs in this set.
     */
    private static final Set<ApiEntityType> SCOPE_EXPANSION_TYPES_FOR_CLOUD = ImmutableSet.of(
            ApiEntityType.APPLICATION,
            ApiEntityType.APPLICATION_SERVER,
            ApiEntityType.APPLICATION_COMPONENT,
            ApiEntityType.BUSINESS_APPLICATION,
            ApiEntityType.BUSINESS_TRANSACTION,
            ApiEntityType.CONTAINER,
            ApiEntityType.CONTAINER_POD,
            ApiEntityType.DATABASE,
            ApiEntityType.DATABASE_SERVER,
            ApiEntityType.DATABASE_SERVER_TIER,
            ApiEntityType.DATABASE_TIER,
            ApiEntityType.LOAD_BALANCER,
            ApiEntityType.SERVICE,
            ApiEntityType.STORAGE,
            ApiEntityType.VIRTUAL_MACHINE,
            ApiEntityType.VIRTUAL_VOLUME);

    /**
     * Sometimes we need to expand aggregators to some of their aggregated entities.
     * For example in global views of the application entities.
     */
    private static final Set<ApiEntityType> SCOPE_EXPANSION_TYPES_FOR_APPLICATIONS = ImmutableSet.of(
        ApiEntityType.BUSINESS_APPLICATION,
        ApiEntityType.SERVICE,
        ApiEntityType.APPLICATION_COMPONENT,
        ApiEntityType.BUSINESS_TRANSACTION);

    /**
     * This maps aggregator entity types (such as region or datacenter), to
     * the set of types of the entities that we will get after their expansion.
     * For example, when we expand datacenters, we want to fetch all aggregated
     * PMs. When we expand VDCs, we want to fetch all related VMs. When we
     * expand cloud aggregators, we want to get entities of all the types in
     * {@link #SCOPE_EXPANSION_TYPES_FOR_CLOUD}.
     */
    private static final Map<ApiEntityType, Set<ApiEntityType>> ENTITY_TYPES_TO_EXPAND = ImmutableMap.of(
            ApiEntityType.DATACENTER, Collections.singleton(ApiEntityType.PHYSICAL_MACHINE),
            ApiEntityType.REGION, SCOPE_EXPANSION_TYPES_FOR_CLOUD,
            ApiEntityType.BUSINESS_ACCOUNT, SCOPE_EXPANSION_TYPES_FOR_CLOUD,
            ApiEntityType.AVAILABILITY_ZONE, SCOPE_EXPANSION_TYPES_FOR_CLOUD,
            ApiEntityType.VIRTUAL_DATACENTER, Collections.singleton(ApiEntityType.VIRTUAL_MACHINE));

    private static final Logger logger = LogManager.getLogger();

    private final SupplyChainServiceBlockingStub supplyChainRpcService;

    private final EntitySeverityServiceBlockingStub severityRpcService;

    private final CostServiceBlockingStub costServiceBlockingStub;

    private final RepositoryApi repositoryApi;

    private final GroupExpander groupExpander;

    private UuidMapper uuidMapper;

    private final long realtimeTopologyContextId;

    private static final long PLACEHOLDER_KEY = 1L;

    //Mapper for getting aspects for entity or group
    private final EntityAspectMapper entityAspectMapper;

    public SupplyChainFetcherFactory(@Nonnull final SupplyChainServiceBlockingStub supplyChainService,
            @Nonnull final EntitySeverityServiceBlockingStub entitySeverityServiceBlockingStub,
            @Nonnull final RepositoryApi repositoryApi,
            @Nonnull final GroupExpander groupExpander,
            @Nonnull final EntityAspectMapper entityAspectMapper,
            CostServiceBlockingStub costServiceBlockingStub,
            final long realtimeTopologyContextId) {
        this.supplyChainRpcService = supplyChainService;
        this.severityRpcService = entitySeverityServiceBlockingStub;
        this.repositoryApi = repositoryApi;
        this.entityAspectMapper = entityAspectMapper;
        this.groupExpander = groupExpander;
        this.costServiceBlockingStub = costServiceBlockingStub;
        this.realtimeTopologyContextId = realtimeTopologyContextId;
    }

    public void setUuidMapper(UuidMapper uuidMapper) {
        this.uuidMapper = uuidMapper;
    }

    /**
     * Create a new {@link SupplyChainNodeFetcherBuilder} to retrieve raw {@link SupplyChainNode}s
     * matching various criteria. Users of this method can customize the builder and then call
     * {@link SupplyChainNodeFetcherBuilder#fetch()} to synchronously get the data.
     *
     * @return The {@link SupplyChainNodeFetcherBuilder}.
     */
    @Nonnull
    public SupplyChainNodeFetcherBuilder newNodeFetcher() {
        return new SupplyChainNodeFetcherBuilder();
    }

    /**
     * Create a new {@link SupplychainApiDTOFetcherBuilder} to retrieve a {@link SupplychainApiDTO}
     * containing the supply chain matching various criteria. Users of this method can customize
     * the builder and then call {@link SupplychainApiDTOFetcherBuilder#fetch()} to synchronously
     * get the data.
     *
     * @return The {@link SupplychainApiDTOFetcherBuilder}.
     */
    @Nonnull
    public SupplychainApiDTOFetcherBuilder newApiDtoFetcher() {
        return new SupplychainApiDTOFetcherBuilder();
    }

    /**
     * Utility method to expand a seed into the set of all ids in its supply chain scope for either real-
     * time or plan topologies.
     *
     * @param entityUuids ids of entities in the seed.
     * @param relatedEntityTypes entity types of entities to fetch (if empty, fetch all entities).
     * @param topologyContextId the optional topologyContextId corresponding to a plan topology
     * @return the ids of the supply chain requested.
     * @throws OperationFailedException operation failed.
     */
    public Set<Long> expandScope(
            @Nonnull Set<Long> entityUuids,
            @Nonnull List<String> relatedEntityTypes,
            Long topologyContextId)
            throws OperationFailedException {
        final SupplyChainNodeFetcherBuilder builder =
            newNodeFetcher()
                .addSeedUuids(
                    entityUuids.stream().map(Object::toString).collect(Collectors.toList()))
                .entityTypes(relatedEntityTypes);
        if (Objects.nonNull(topologyContextId) && topologyContextId != realtimeTopologyContextId) {
            builder.topologyContextId(topologyContextId);
        }
        return builder.fetchEntityIds();
    }

    /**
     * Utility method to expand a seed into the set of all ids in its real-time supply chain scope.
     *
     * @param entityUuids ids of entities in the seed.
     * @param relatedEntityTypes entity types of entities to fetch (if empty, fetch all entities).
     * @return the ids of the supply chain requested.
     * @throws OperationFailedException operation failed.
     */
    public Set<Long> expandScope(@Nonnull Set<Long> entityUuids,
            @Nonnull List<String> relatedEntityTypes)
            throws OperationFailedException {
        return expandScope(entityUuids, relatedEntityTypes, null);
    }

    /**
     * Expand multiple aggregated entities with the minimum number of RPC calls.
     *
     * @param entityOidsToExpand Groups of entity OIDs to expand, arranged by an ID. The ID can be
     *        any number; it doesn't have to refer to an object in the system.
     * @return Map from input id to the set of entity OIDs
     */
    public Map<Long, Set<Long>> bulkExpandAggregatedEntities(Map<Long, Set<Long>> entityOidsToExpand) {
        return expandAggregatedEntities(entityOidsToExpand, ApiEntityType.ENTITY_TYPES_TO_EXPAND);
    }

    /**
     * Calls the expand aggregate function with {@link #ENTITY_TYPES_TO_EXPAND}.
     *
     * @param entityOidsToExpand the input set of ServiceEntity oids
     * @return the input set with oids of aggregating entities substituted by their expansions.
     */
    public Set<Long> expandAggregatedEntities(Set<Long> entityOidsToExpand) {
        return expandAggregatedEntities(
                Collections.singletonMap(PLACEHOLDER_KEY, entityOidsToExpand),
                ApiEntityType.ENTITY_TYPES_TO_EXPAND)
            .get(PLACEHOLDER_KEY);
    }

    /**
     * Calls the expand aggregate function with {@link #ENTITY_TYPES_TO_EXPAND}.
     *
     * @param entityOidsToExpand the input set of ServiceEntity oids
     * @return the input set with oids of aggregating entities substituted by their expansions along with the types
     *     that were included in expansion.
     */
    public ScopeExpansionResult expandAggregatedEntitiesWithTypes(Set<Long> entityOidsToExpand) {
        return expandAggregatedEntitiesWithTypes(
                Collections.singletonMap(PLACEHOLDER_KEY, entityOidsToExpand),
                ApiEntityType.ENTITY_TYPES_TO_EXPAND)
                .get(PLACEHOLDER_KEY);
    }

    /**
     * Expand aggregator entities according to the a given entity map.
     *
     * <p>The method takes a set of entity oids. It expands each entity whose type
     * is in the key set of the given map to the aggregated entities
     * of the corresponding type. It will leave all other entities unchanged. For
     * example, if the input set of oids contains the oids of a datacenter and a VM,
     * the result will contain the oids of the VM and all the PMs aggregated by
     * the datacenter.</p>
     *
     * @param entityOidsToExpand the input set of ServiceEntity oids
     * @return the input set with oids of aggregating entities substituted by their
     *         expansions
     */
    private Map<Long, Set<Long>> expandAggregatedEntities(Map<Long, Set<Long>> entityOidsToExpand,
                                              Map<ApiEntityType, Set<ApiEntityType>>  expandingMap) {
        return expandAggregatedEntitiesWithTypes(entityOidsToExpand, expandingMap).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, mapEntry -> mapEntry.getValue().getExpandedScope()));
    }

    /**
     * Expand aggregator entities according to the a given entity map.
     *
     * <p>The method takes a set of entity oids. It expands each entity whose type
     * is in the key set of the given map to the aggregated entities
     * of the corresponding type. It will leave all other entities unchanged. For
     * example, if the input set of oids contains the oids of a datacenter and a VM,
     * the result will contain the oids of the VM and all the PMs aggregated by
     * the datacenter.</p>
     *
     * @param entityOidsToExpand the input set of ServiceEntity oids
     * @return the input set with oids of aggregating entities substituted by their
     *         expansions
     */
    private Map<Long, ScopeExpansionResult> expandAggregatedEntitiesWithTypes(Map<Long, Set<Long>> entityOidsToExpand,
                                                          Map<ApiEntityType, Set<ApiEntityType>>  expandingMap) {
        // Build up a list of ApiIds for all the ids in the input. This is to help do the
        // expansion in bulk, and utilize any client-side cached information about these ids.
        Map<Long, ApiId> allIds = entityOidsToExpand.values().stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toMap(Function.identity(), uuidMapper::fromOid,
                    (o1, o2) -> o1));
        if (allIds.isEmpty()) {
            // If there are no entities we just have a bunch of empty lists.
            return entityOidsToExpand.entrySet().stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            mapEntry -> new ScopeExpansionResult(Collections.emptySet(), mapEntry.getValue())));
        }

        // Ensure that the entity oids are resolved (i.e. we know if its an entity).
        uuidMapper.bulkResolveEntities(allIds.values());

        // For each collection in the input, create a SupplyChainSeed.
        // This seed contains all the entities in that collection which need to be expanded.
        List<SupplyChainSeed> supplyChainSeeds = new ArrayList<>(entityOidsToExpand.size());
        // This will be the response, containing the set of entities each seed should be expanded
        // to.
        final Map<Long, ScopeExpansionResult> expandedResponse = new HashMap<>(entityOidsToExpand.size());
        for (Entry<Long, Set<Long>> inputOidGroup : entityOidsToExpand.entrySet()) {
            Long oidGroupIdx = inputOidGroup.getKey();
            Collection<Long> oidGroup = inputOidGroup.getValue();
            Set<Long> unexpandedOids = new HashSet<>();
            Set<Long> oidsToExpand = new HashSet<>();
            Set<ApiEntityType> apiEntityTypesToExpandTo = new HashSet<>();
            oidGroup.forEach(oid -> {
                ApiId id = allIds.get(oid);
                if (id.isEntity()) {
                    // Some entity types need to be expanded.
                    id.getCachedEntityInfo().ifPresent(entityInfo -> {
                        Set<ApiEntityType> expandTo = expandingMap.get(entityInfo.getEntityType());
                        if (!CollectionUtils.isEmpty(expandTo)) {
                            oidsToExpand.add(oid);
                            apiEntityTypesToExpandTo.addAll(expandTo);
                        } else {
                            unexpandedOids.add(oid);
                        }
                    });
                } else {
                    // Non-entities don't get expanded.
                    unexpandedOids.add(oid);
                }
            });

            // This adds the unexpanded oids to the response at the right index.
            Set<Integer> typesToExpandTo = apiEntityTypesToExpandTo.stream().map(ApiEntityType::typeNumber).collect(Collectors.toSet());
            expandedResponse.put(oidGroupIdx, new ScopeExpansionResult(typesToExpandTo, unexpandedOids));

            // If some of the entities in this group of oids need to be expanded, we add a
            // supply chain seed for this group.
            if (!oidsToExpand.isEmpty()) {
                SupplyChainSeed.Builder seedBldr = SupplyChainSeed.newBuilder()
                        // The seed OID is the index in the response list.
                        .setSeedOid(oidGroupIdx)
                        .setScope(SupplyChainScope.newBuilder()
                            .addAllStartingEntityOid(oidsToExpand));
                // Only want the types we are looking to expand to.
                typesToExpandTo.forEach(t -> seedBldr.getScopeBuilder().addEntityTypesToInclude(t));
                supplyChainSeeds.add(seedBldr.build());
            }
        }

        if (!supplyChainSeeds.isEmpty()) {
            try {
                supplyChainRpcService.getMultiSupplyChains(GetMultiSupplyChainsRequest.newBuilder()
                        .addAllSeeds(supplyChainSeeds)
                        .build()).forEachRemaining(response -> {
                    if (response.hasSeedOid() && response.hasSupplyChain()) {
                        for (SupplyChainNode relatedEntities : response.getSupplyChain().getSupplyChainNodesList()) {
                            // Add the expanded entities into the response at the index specified by
                            // the seed.
                            expandedResponse.computeIfAbsent(
                                    response.getSeedOid(),
                                    k -> new ScopeExpansionResult(Collections.emptySet(), Collections.emptySet()))
                                    .addAllToExpandedScope(RepositoryDTOUtil.getAllMemberOids(relatedEntities));
                        }
                    } else if (response.hasError()) {
                        if (response.hasSeedOid()) {
                            final long seedOid = response.getSeedOid();
                            logger.error("Failed to get supply chain for seed {}. Error: {}",
                                    entityOidsToExpand.get(seedOid), response.getError());
                            expandedResponse.put(seedOid,
                                    new ScopeExpansionResult(Collections.emptySet(),
                                            entityOidsToExpand.get(seedOid)));
                        } else {
                            logger.error("Failed to get supply chain. Error: {}", response.getError());
                        }
                    }
                });
            } catch (StatusRuntimeException e) {
                logger.error("Failed to query supply chain service. Error: {}. Returning unexpanded seeds.", e.toString());
                supplyChainSeeds.forEach(seed -> {
                    // Include unexpanded seed.
                    final long seedOid = seed.getSeedOid();
                    expandedResponse.put(seedOid,
                            new ScopeExpansionResult(Collections.emptySet(),
                                    entityOidsToExpand.get(seedOid)));
                });
            }
        }

        return expandedResponse;
    }

    /**
     * Expand service providers to regions.
     *
     * @param serviceProviderOids the input set of ServiceEntity oids
     * @return the input set with oids of regions connected to the service providers
     */
    public Set<Long> expandServiceProviders(@Nonnull final Set<Long> serviceProviderOids) {
        return this.repositoryApi.expandServiceProvidersToRegions(serviceProviderOids);
    }

    /**
     * Checks if there is an application type entity in the given set.
     * @param relatedTypes set of entities that we want to check
     * @return true if contains, false otherwise.
     */
    public boolean containsApplicationType(Set<ApiEntityType> relatedTypes) {
        return relatedTypes.stream()
            .anyMatch(SCOPE_EXPANSION_TYPES_FOR_APPLICATIONS::contains);
    }

    /**
     * A builder for a {@link SupplychainNodeFetcher} that returns the raw
     * {@link SupplyChainNode}s, arranged by entity type.
     */
    public class SupplyChainNodeFetcherBuilder extends
            SupplyChainFetcherBuilder<SupplyChainNodeFetcherBuilder, Map<String, SupplyChainNode>> {

        @Override
        public Map<String, SupplyChainNode> fetch() throws OperationFailedException {
            try {
                return new SupplychainNodeFetcher(
                        realtimeTopologyContextId,
                        topologyContextId,
                        seedUuids,
                        entityTypes,
                        entityStates,
                        environmentType,
                        supplyChainRpcService,
                        groupExpander,
                        enforceUserScope,
                        repositoryApi).fetch();
            } catch (InterruptedException | ExecutionException | TimeoutException | ConversionException e) {
                throw new OperationFailedException("Failed to fetch supply chain! Error: "
                        + e.getMessage());
            }
        }

        @Override
        public Set<Long> fetchEntityIds() throws OperationFailedException {
            return new SupplychainNodeFetcher(
                        realtimeTopologyContextId, topologyContextId, seedUuids, entityTypes,
                        entityStates, environmentType, supplyChainRpcService, groupExpander,
                        enforceUserScope, repositoryApi)
                    .fetchEntityIds();
        }

        @Override
        @Nonnull
        public List<SupplyChainStat> fetchStats(@Nonnull final List<SupplyChainGroupBy> groupBy)
            throws OperationFailedException {
            try {
                return new SupplychainNodeFetcher(realtimeTopologyContextId, topologyContextId,
                        seedUuids, entityTypes, entityStates, environmentType, supplyChainRpcService,
                        groupExpander, enforceUserScope, repositoryApi)
                    .fetchStats(groupBy);
            } catch (StatusRuntimeException e) {
                throw new OperationFailedException("Failed to fetch supply chain stats! Error: " +
                    e.getMessage());
            }
        }
    }

    /**
     * A builder for a {@link SupplychainApiDTOFetcher} that returns a
     * {@link SupplychainApiDTO} representing the supply chain.
     */
    public class SupplychainApiDTOFetcherBuilder extends SupplyChainFetcherBuilder<SupplychainApiDTOFetcherBuilder, SupplychainApiDTO> {
        protected EntityDetailType entityDetailType;
        protected Collection<String> aspectsToInclude;
        protected Boolean includeHealthSummary = false;
        protected EntityAspectMapper entityAspectMapper = null;

        /**
         * Specify the level of service entity detail to include in the result
         * - default is no detail.
         *
         * NOTE:  this setting is not currently supported in XL.
         *
         * @param entityDetailType what level of detail to include in the supplychain result
         * @return the flow-style OperationBuilder for this SupplyChainFetcher
         */
        @Nonnull
        public SupplychainApiDTOFetcherBuilder entityDetailType(
            @Nullable final EntityDetailType entityDetailType) {
            this.entityDetailType = entityDetailType;
            return this;
        }

        /**
         * Specify a list of aspects to include in the result.
         *
         * <p>Only applies if entityDetailType is set to 'aspects'. Defaults to including all
         * aspects, if null or not set.</p>
         *
         * @param aspectsToInclude the aspects to include, or null to include all aspects
         * @return the flow-style OperationBuilder for this SupplyChainFetcher
         */
        @Nonnull
        public SupplychainApiDTOFetcherBuilder aspectsToInclude(
            @Nullable final Collection<String> aspectsToInclude) {
            this.aspectsToInclude = aspectsToInclude;
            return this;
        }

        /**
         * Assign an {@link EntityAspectMapper} to map aspects to supply chain SEs.
         *
         * @param entityAspectMapper an {@link EntityAspectMapper} to use for assigning aspects to supply chain SEs
         * @return the flow-style OperationBuilder for this SupplyChainFetcher
         */
        @Nonnull
        public SupplychainApiDTOFetcherBuilder entityAspectMapper(
                @Nullable final EntityAspectMapper entityAspectMapper) {
            this.entityAspectMapper = entityAspectMapper;
            return this;
        }

        /**
         * Should the 'health summary' be populated in the result - default is no.
         *
         * If the 'health summary' is not included, then return the full details of
         * all the ServiceEntities in the supplychain.
         *
         * @param includeHealthSummary should the healthSummary be included in the supplychain result
         * @return the flow-style OperationBuilder for this SupplyChainFetcher
         */
        @Nonnull
        public SupplychainApiDTOFetcherBuilder includeHealthSummary(
                final boolean includeHealthSummary) {
            this.includeHealthSummary = includeHealthSummary;
            return this;
        }

        @Override
        @Nonnull
        public SupplychainApiDTO fetch() throws OperationFailedException, InterruptedException {
            try {
                final SupplychainApiDTO dto = new SupplychainApiDTOFetcher(
                        realtimeTopologyContextId, topologyContextId, seedUuids, entityTypes,
                        entityStates, environmentType, entityDetailType, aspectsToInclude,
                        includeHealthSummary, supplyChainRpcService, severityRpcService, repositoryApi,
                        groupExpander, entityAspectMapper, enforceUserScope, costServiceBlockingStub)
                    .fetch();
                return dto;
            } catch (ExecutionException | TimeoutException | ConversionException e) {
                throw new OperationFailedException("Failed to fetch supply chain! Error: "
                        + e.getMessage());
            }
        }

        @Override
        @Nonnull
        public Set<Long> fetchEntityIds() throws OperationFailedException {
            return new SupplychainApiDTOFetcher(
                realtimeTopologyContextId, topologyContextId, seedUuids, entityTypes,
                entityStates, environmentType, entityDetailType, aspectsToInclude,
                includeHealthSummary, supplyChainRpcService, severityRpcService,
                repositoryApi, groupExpander, entityAspectMapper, enforceUserScope,
                costServiceBlockingStub)
                .fetchEntityIds();
        }

        @Override
        @Nonnull
        public List<SupplyChainStat> fetchStats(@Nonnull final List<SupplyChainGroupBy> groupBy)
            throws OperationFailedException {
            try {
                return new SupplychainApiDTOFetcher(
                        realtimeTopologyContextId, topologyContextId, seedUuids, entityTypes,
                        entityStates, environmentType, entityDetailType, aspectsToInclude,
                        includeHealthSummary, supplyChainRpcService, severityRpcService,
                        repositoryApi, groupExpander, entityAspectMapper, enforceUserScope,
                        costServiceBlockingStub)
                        .fetchStats(groupBy);
            } catch (StatusRuntimeException e) {
                throw new OperationFailedException("Failed to fetch supply chain stats! Error: " +
                    e.getMessage());
            }
        }
    }

    /**
     * A builder class to simplify creating a {@link SupplychainFetcher}.
     *
     * None of the parameters are required.
     *
     * @param <B> The builder subtype, used to allow method chaining with common setter methods.
     * @param <T> The return type of the {@link SupplychainFetcher} the builder builds.
     *            For now, the builder doesn't actually return the operation itself, but returns
     *            the result of running the operation. In the future, if we want to allow running
     *            the operation asynchronously while the calling code does other things we should
     *            return the operation itself, and this parameter would be different.
     */
    @VisibleForTesting
    public abstract class SupplyChainFetcherBuilder<B extends SupplyChainFetcherBuilder<B, T>, T> {

        // all fields are optional; see the setter for each field for a description
        protected long topologyContextId = realtimeTopologyContextId;

        protected final Set<String> seedUuids = Sets.newHashSet();

        protected final Set<ApiEntityType> entityTypes = Sets.newHashSet();

        protected final Set<EntityState> entityStates = Sets.newHashSet();

        protected boolean enforceUserScope = true;

        protected Optional<EnvironmentTypeEnum.EnvironmentType> environmentType = Optional.empty();

        /**
         * Synchronously fetch the supply chain with the parameters specified in the builder.
         *
         * @return The return type of the {@link SupplychainFetcher} being built.
         * @throws OperationFailedException If any of the calls/processing required for the fetch
         *                                  operation fail.
         * @throws InterruptedException If the thread is interrupted while waiting for the operation.
         */
        public abstract T fetch() throws OperationFailedException, InterruptedException;

        /**
         * Synchronously fetch the supply chain with the parameters specified in the builder
         * and return the ids of all the contained entities
         *
         * @return The set of ids of all contained entities.
         * @throws OperationFailedException If any of the calls/processing required for the fetch
         *                                  operation fail.
         * @throws InterruptedException If the thread is interrupted while waiting for the operation.
         */
        public abstract Set<Long> fetchEntityIds() throws OperationFailedException, InterruptedException;

        /**
         * The seed UUID to start the supply chain generation from; may be SE, Group, Cluster.
         * The default is the entire topology.
         *
         * @param seedUuid a single UUID to serve as the seed for the supplychain generation
         * @return the flow-style OperationBuilder for this SupplyChainFetcher
         */
        @SuppressWarnings("SameParameterValue")
        public B addSeedUuid(@Nonnull final String seedUuid) {
            seedUuids.add(seedUuid);
            return (B)this;
        }

        /**
         * The seed UUIDs to start the supply chain generation from; may be SE, Group, Cluster.
         * The default is the entire topology.
         *
         * @param uuids a list of uuids, each of which will be the seed of a supplychain; the result
         *              is the union of the supplychains from each seed
         * @return the flow-style OperationBuilder for this SupplyChainFetcher
         */
        public B addSeedUuids(@Nullable Collection<String> uuids) {
            if (uuids != null) {
                this.seedUuids.addAll(uuids);
            }
            return (B)this;
        }

        public B addSeedOids(@Nullable Collection<Long> oids) {
            if (oids != null) {
                oids.forEach(oid -> this.seedUuids.add(Long.toString(oid)));
            }
            return (B)this;
        }

        /**
         * the topologyContext in which to perform the supplychain lookup - default is the Live Topology
         * @param topologyContextId the topologyContextId on which the supplychain operations should
         *                          be performed - default is the Live Topology
         * @return the flow-style OperationBuilder for this SupplyChainFetcher
         */
        public B topologyContextId(long topologyContextId) {
            this.topologyContextId = topologyContextId;
            return (B)this;
        }

        /**
         * A list of service entity types to include in the answer - default is all entity types.
         * 'null' or the empty list indicates no filtering; all entity types will be included.
         *
         * @param entityTypes a list of the entity types to be included in the result
         * @return the flow-style OperationBuilder for this SupplyChainFetcher
         */
        public B entityTypes(@Nullable List<String> entityTypes) {
            if (entityTypes != null) {
                entityTypes.stream()
                    .flatMap(type -> {
                        if (type.equals(StringConstants.WORKLOAD)) {
                            // The "Workload" type is UI-only, and represents a collection of
                            // entity types that count as a workload in our system. Expand the
                            // magic type into the real types it represents.
                            return ApiEntityType.WORKLOAD_ENTITY_TYPES.stream();
                        } else {
                            return Stream.of(ApiEntityType.fromString(type));
                        }
                    })
                    .forEach(this.entityTypes::add);
            }
            return (B)this;
        }

        /**
         * A list of entity states to include in the answer - default is all states.
         * 'null' or the empty list indicates no filtering; all entity states will be included.
         *
         * <p/>Note - entities that don't match the state will not be considered during supply
         * chain traversal. Therefore, any entities connected to them will not be included (if they
         * are not traversed to via some other entity with a matching state).
         *
         * @param entityStates a list of the entity states to be included in the result.
         * @return the flow-style OperationBuilder for this SupplyChainFetcher
         */
        public B entityStates(@Nullable List<com.vmturbo.api.enums.EntityState> entityStates) {
            if (entityStates != null) {
                entityStates.stream()
                    .map(state -> UIEntityState.fromString(state.name()))
                    .map(UIEntityState::toEntityState)
                    .forEach(this.entityStates::add);
            }
            return (B)this;
        }

        /**
         * Limit the response to service entities in this environment e.g. ON_PREM, CLOUD, HYBRID
         * - default is all environments.
         *
         * @param environmentType what environment to limit the responses to
         * @return the flow-style OperationBuilder for this SupplyChainFetcher
         */
        @Nonnull
        public B apiEnvironmentType(@Nullable final com.vmturbo.api.enums.EnvironmentType environmentType) {
            if (environmentType != null) {
                this.environmentType = Optional.of(EnvironmentTypeMapper.fromApiToXL(environmentType));
            }
            return (B)this;
        }

        @Nonnull
        public B environmentType(@Nullable final EnvironmentType environmentType) {
            this.environmentType = Optional.ofNullable(environmentType);
            return (B)this;
        }

        /**
         * Whether the results should be confined to the user access scope or not. If true (default)
         * and the requesting user has an access scope assigned, hen the supply chain will only
         * contain entities that are part of the user's entity access scope. If false, then all
         * entities in the supply chain will be returned.
         *
         * @param enforceUserScope
         * @return
         */
        @Nonnull
        public B enforceUserScope(final boolean enforceUserScope) {
            this.enforceUserScope = enforceUserScope;
            return (B)this;
        }

        @Nonnull
        public abstract List<SupplyChainStat> fetchStats(@Nonnull List<SupplyChainGroupBy> groupBy)
            throws OperationFailedException;
    }

    /**
     * Internal Class to handle a single SupplyChain fetch operation. Processes the {@link SupplyChain}
     * returned from the supply chain RPC service, and hands the nodes off to sub-classes for
     * processing.
     */
    private abstract static class SupplychainFetcher<T> {

        protected final Logger logger = LogManager.getLogger(getClass());

        private final long realtimeTopologyContextId;

        private final long topologyContextId;

        protected final Set<String> seedUuids;

        private final Set<ApiEntityType> entityTypes;

        private final Set<EntityState> entityStates;

        private final Optional<EnvironmentTypeEnum.EnvironmentType> environmentType;

        private final SupplyChainServiceBlockingStub supplyChainRpcService;

        private final GroupExpander groupExpander;

        protected final boolean enforceUserScope;

        private final RepositoryApi repositoryApi;

        private SupplychainFetcher(final long realtimeTopologyContextId,
                                   final long topologyContextId,
                                   @Nullable final Set<String> seedUuids,
                                   @Nullable final Set<ApiEntityType> entityTypes,
                                   @Nullable final Set<EntityState> entityStates,
                                   @Nonnull final Optional<EnvironmentTypeEnum.EnvironmentType> environmentType,
                                   @Nonnull SupplyChainServiceBlockingStub supplyChainRpcService,
                                   @Nonnull GroupExpander groupExpander,
                                   final boolean enforceUserScope,
                                   @Nonnull final RepositoryApi repositoryApi) {
            this.realtimeTopologyContextId = realtimeTopologyContextId;
            this.topologyContextId = topologyContextId;
            this.seedUuids = seedUuids;
            this.entityTypes = entityTypes;
            this.entityStates = entityStates;
            this.environmentType = environmentType;
            this.supplyChainRpcService = supplyChainRpcService;
            this.groupExpander = groupExpander;
            this.enforceUserScope = enforceUserScope;
            this.repositoryApi = repositoryApi;
        }

        public abstract T processSupplyChain(List<SupplyChainNode> supplyChainNodes)
                throws InterruptedException, ConversionException;

        public final T fetch() throws InterruptedException, ExecutionException, TimeoutException,
                ConversionException, OperationFailedException {
            return processSupplyChain(fetchSupplyChainNodes());
        }

        /**
         * Fetch the requested supply chain using {@link #fetch()} and then return the ids
         * of all the entities in the supply chain.
         *
         * @return the set of ids of all the entities in the supply chain.s
         * @throws OperationFailedException If there is an error with scope expansion.
         */
        public final Set<Long> fetchEntityIds() throws  OperationFailedException {
            return fetchSupplyChainNodes().stream()
                    .map(SupplyChainNode::getMembersByStateMap)
                    .map(Map::values)
                    .flatMap(memberList ->
                        memberList.stream().map(MemberList::getMemberOidsList).flatMap(List::stream))
                    .collect(Collectors.toSet());
        }

        private Optional<SupplyChainScope> createSupplyChainScope()
                throws OperationFailedException {
            SupplyChainScope.Builder scopeBuilder = SupplyChainScope.newBuilder();
            // if list of seed uuids has limited scope,then expand it; if global scope, don't expand
            if (UuidMapper.hasLimitedScope(seedUuids)) {

                // expand any groups in the input list of seeds
                Set<Long> expandedUuids = groupExpander.expandUuids(CollectionUtils.emptyIfNull(seedUuids));
                // empty expanded list?  If so, return immediately
                if (expandedUuids.isEmpty()) {
                    return Optional.empty();
                }
                // otherwise add the expanded list of seed uuids to the request
                scopeBuilder.addAllStartingEntityOid(expandedUuids);
            }

            // If entityTypes is specified, include that in the request
            if (CollectionUtils.isNotEmpty(entityTypes)) {
                entityTypes.forEach(type -> {
                    scopeBuilder.addEntityTypesToInclude(type.typeNumber());
                });
            }

            if (CollectionUtils.isNotEmpty(entityStates)) {
                scopeBuilder.addAllEntityStatesToInclude(entityStates);
            }

            environmentType.ifPresent(scopeBuilder::setEnvironmentType);
            return Optional.of(scopeBuilder.build());
        }

        final List<SupplyChainStat> fetchStats(@Nonnull final List<SupplyChainGroupBy> groupBy)
                throws OperationFailedException {
            Optional<SupplyChainScope> scope = createSupplyChainScope();
            if (scope.isPresent()) {
                return supplyChainRpcService.getSupplyChainStats(GetSupplyChainStatsRequest.newBuilder()
                        .setScope(scope.get())
                        .addAllGroupBy(groupBy)
                        .build())
                    .getStatsList();
            } else {
                return Collections.emptyList();
            }
        }

        /**
         * Fetch the requested supply chain in a blocking fashion, waiting at most the duration
         * of the timeout.
         *
         * @return The {@link SupplychainApiDTO} populated with the supply chain search results.
         * @throws OperationFailedException If there is an error expanding scope IDs.
         */
        final List<SupplyChainNode> fetchSupplyChainNodes() throws OperationFailedException {
            if (UuidMapper.hasLimitedScope(seedUuids)
                    && seedUuids.size() == 1) {
                final String groupUuid = seedUuids.iterator().next();
                final Optional<GroupAndMembers> groupWithMembers =
                    groupExpander.getGroupWithMembersAndEntities(groupUuid);

                if (groupWithMembers.isPresent()) {
                    final Grouping group = groupWithMembers.get().group();

                    // If the scope is RG or group of RG build supply chain
                    // such that it only has only RG entities and regions
                    if (isResourceGroupOrGroupOfResourceGroup(group)) {
                        return createSupplyChainForResourceGroup(groupWithMembers.get());
                    }

                    // START Mad(ish) Hax.
                    // Handle a very particular special case where we are asking for the supply chain
                    // of a group, restricted to the entity type of the group (e.g. give me the supply
                    // chain of Group 1 of PhysicalMachines, containing only PhysicalMachine nodes).
                    // The request is, essentially, asking for the members of the group, so we don't need
                    // to do any supply chain queries.
                    //
                    // The reason this even happens is because some information (e.g. grouped severities
                    // for supply chain stats, or aspects for entities) is only available via the
                    // supply chain API. In the long term there should be a better API to retrieve this
                    // (e.g. some sort of "entity counts" API for grouped severities,
                    //       and/or options on the /search API for aspects)
                    if (CollectionUtils.isNotEmpty(entityTypes)) {
                        final Set<ApiEntityType> groupTypes = GroupProtoUtil.getEntityTypes(group);

                        if (groupTypes.containsAll(entityTypes)) {
                            if (groupWithMembers.get().entities().isEmpty()) {
                                return Collections.emptyList();
                            }

                            final Map<ApiEntityType, Set<Long>> typeToMembers =
                                groupExpander.expandUuidToTypeToEntitiesMap(group.getId());

                            return entityTypes.stream()
                                .map(type -> createSupplyChainNode(type,
                                    typeToMembers.get(type),
                                    group, null, null))
                                .filter(Optional::isPresent)
                                .map(Optional::get)
                                .collect(Collectors.toList());
                        }
                        // END Mad(ish) Hax.
                    }
                }
            }

            Optional<SupplyChainScope> scope = createSupplyChainScope();
            if (!scope.isPresent()) {
                return Collections.emptyList();
            }

            final GetSupplyChainRequest.Builder builder = GetSupplyChainRequest.newBuilder()
                .setScope(scope.get());
            final long topologyContextId = getTopologyContextId();
            if (topologyContextId != realtimeTopologyContextId) {
                builder.setContextId(getTopologyContextId());
            }
            final GetSupplyChainRequest request = builder.build();

            final GetSupplyChainResponse response = supplyChainRpcService.getSupplyChain(request);
            if (!response.getSupplyChain().getMissingStartingEntitiesList().isEmpty()) {
                logger.warn("{} of {} seed entities were not found for the supply chain: {}.",
                    response.getSupplyChain().getMissingStartingEntitiesCount(),
                    CollectionUtils.size(seedUuids),
                    response.getSupplyChain().getMissingStartingEntitiesList());
            }
            return response.getSupplyChain().getSupplyChainNodesList();
        }

        /**
         * Gets a group and check if it is a resource group or group of resource group.
         * @param group the input group.
         * @return true if this a resource group or groups of resource groups.
         */
        private boolean isResourceGroupOrGroupOfResourceGroup(Grouping group) {
            final GroupDTO.GroupDefinition definition = group.getDefinition();

            // If group is static
            if (definition.hasStaticGroupMembers()) {
                if (definition.getType() == CommonDTO.GroupDTO.GroupType.RESOURCE) {
                    return true;
                }

                final GroupDTO.StaticMembers statMembers = definition.getStaticGroupMembers();

                return statMembers.getMembersByTypeCount() == 1
                        && statMembers.getMembersByType(0).getType().hasGroup()
                        && statMembers.getMembersByType(0)
                    .getType().getGroup() == CommonDTO.GroupDTO.GroupType.RESOURCE;
            }

            // If the group is dynamic nested group
            if (definition.hasGroupFilters()) {
                return definition.getGroupFilters().getGroupFilterCount() == 1
                    && definition.getGroupFilters().getGroupFilter(0).getGroupType()
                            == CommonDTO.GroupDTO.GroupType.RESOURCE;
            }

            return false;
        }

        /**
         * Gets a resource group or group of resource groups and build
         * a supply chain for it only including entities insides resource groups
         * and underlying regions.
         *
         * @param groupAndMembers the input group.
         * @return the list of supply chain nodes.
         */
        private List<SupplyChainNode> createSupplyChainForResourceGroup(
            GroupAndMembers groupAndMembers) {
            final Set<Long> entities = new HashSet<>(groupAndMembers.entities());
            final Map<ApiEntityType, Set<ApiEntityType>> connectionsProvider = new HashMap<>();
            final Map<ApiEntityType, Set<ApiEntityType>> connectionsConsumer = new HashMap<>();
            final Map<ApiEntityType, Set<Long>> entitiesMap = new HashMap<>();
            final boolean limitedTypes = CollectionUtils.isNotEmpty(entityTypes);

            repositoryApi.entitiesRequest(entities).getFullEntities()
                .forEach(entity -> {
                    final ApiEntityType firstEntityType =
                        ApiEntityType.fromType(entity.getEntityType());
                    final boolean entityInScope =
                        !limitedTypes || entityTypes.contains(firstEntityType);

                    if (entityInScope) {
                        entitiesMap.computeIfAbsent(ApiEntityType.fromType(entity.getEntityType()),
                            t -> new HashSet<>()).add(entity.getOid());
                    }

                    // initialize providers with the set of provider of
                    // entity commodities
                    Set<Pair<Long, ApiEntityType>> providers =
                        entity.getCommoditiesBoughtFromProvidersList().stream()
                        .filter(c -> c.hasProviderEntityType() && c.hasProviderId())
                        .map(c -> new Pair<>(c.getProviderId(),
                            ApiEntityType.fromType(c.getProviderEntityType())))
                        .collect(Collectors.toSet());

                    // add those entities that are entities connected to
                    // to the list of providers.
                    entity.getConnectedEntityListList()
                        .forEach(ce -> providers.add(new Pair<>(ce.getConnectedEntityId(),
                            ApiEntityType.fromType(ce.getConnectedEntityType()))));

                    // create a required relations for each provider
                    providers
                        .forEach(p -> {
                            // If the type is not part of requested entity continue
                            if (limitedTypes && !entityTypes.contains(p.second)) {
                                return;
                            }

                            final boolean isRegion = (ApiEntityType.REGION == p.second);
                            // If it is region add it to supply chain
                            if (isRegion) {
                                entitiesMap.computeIfAbsent(ApiEntityType.REGION,
                                    t -> new HashSet<>()).add(p.first);
                            }

                            // we only care about connection of current entity to those
                            // entities are part that are part of the resource group
                            // or those that are region
                            if ((entities.contains(p.first) || isRegion) && entityInScope) {
                                final ApiEntityType consumerType =
                                    ApiEntityType.fromType(entity.getEntityType());
                                final ApiEntityType providerType = p.second;
                                connectionsProvider
                                    .computeIfAbsent(consumerType, t -> new HashSet<>())
                                    .add(providerType);
                                connectionsConsumer
                                    .computeIfAbsent(providerType, t -> new HashSet<>())
                                    .add(consumerType);
                            }
                        });
                });

            return entitiesMap.entrySet()
                .stream()
                .map(e -> createSupplyChainNode(e.getKey(), e.getValue(),
                    groupAndMembers.group(), connectionsProvider.get(e.getKey()),
                    connectionsConsumer.get(e.getKey())))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
        }

        private Optional<SupplyChainNode> createSupplyChainNode(ApiEntityType type,
                                                                Set<Long> entities,
                                                                final Grouping group,
                                                                final Set<ApiEntityType> providerSet,
                                                                final Set<ApiEntityType> consumerSet
        ) {
            if (CollectionUtils.isEmpty(entities)) {
                return Optional.empty();
            }

            final Set<Long> filteredMembers;
            // if environment type is not specified, no need to filter
            if (environmentType.isPresent()) {
                // if global scope group, then check the environment directly,
                // no need to filter if it matches
                if (group.getDefinition().getOptimizationMetadata().getIsGlobalScope() &&
                    EnvironmentTypeUtil.match(environmentType.get(),
                                              group.getDefinition().getOptimizationMetadata()
                                                      .getEnvironmentType())) {
                    filteredMembers = entities;
                } else {
                    // normal cases, fetch all members and filter by environment type
                    filteredMembers = repositoryApi.entitiesRequest(entities)
                        .getMinimalEntities()
                        .filter(minimalEntity ->
                            EnvironmentTypeUtil.match(environmentType.get(),
                                                      minimalEntity.getEnvironmentType()))
                        .map(MinimalEntity::getOid)
                        .collect(Collectors.toSet());
                }
            } else {
                filteredMembers = entities;
            }
            final SupplyChainNode.Builder nodeBuilder = SupplyChainNode.newBuilder()
                .setEntityType(type.typeNumber())
                .putMembersByState(EntityState.POWERED_ON_VALUE,
                    MemberList.newBuilder()
                        .addAllMemberOids(filteredMembers)
                        .build());

            if (providerSet != null) {
                providerSet.forEach(t -> {
                    nodeBuilder.addConnectedProviderTypes(t.typeNumber());
                });
            }

            if (consumerSet != null) {
                consumerSet.forEach(t -> {
                    nodeBuilder.addConnectedConsumerTypes(t.typeNumber());
                });
            }

            return Optional.of(nodeBuilder.build());
        }

        protected long getTopologyContextId() {
            return topologyContextId;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("topologyContextId", topologyContextId)
                    .add("seedUuids", seedUuids)
                    .add("entityTypes", entityTypes)
                    .add("environmentType", environmentType)
                    .toString();
        }

    }

    /**
     * A {@link SupplychainFetcher} that returns the {@link SupplyChainNode}s retrieved from
     * the repository, arranged by entity type.
     *
     * The main use case for this fetcher is if you just want the members of a particular set
     * of entity types in the supply chain of some scope. If you want any related information
     * it's worth considering {@link SupplychainApiDTOFetcher}.
     */
    private static class SupplychainNodeFetcher extends SupplychainFetcher<Map<String, SupplyChainNode>> {

        private SupplychainNodeFetcher(final long realtimeTopologyContextId,
                                       final long topologyContextId,
                                       @Nullable final Set<String> seedUuids,
                                       @Nullable final Set<ApiEntityType> entityTypes,
                                       @Nullable final Set<EntityState> entityStates,
                                       @Nonnull final Optional<EnvironmentType> environmentType,
                                       @Nonnull final SupplyChainServiceBlockingStub supplyChainRpcService,
                                       @Nonnull final GroupExpander groupExpander,
                                       final boolean enforceUserScope,
                                       @Nonnull final RepositoryApi repositoryApi) {
            super(realtimeTopologyContextId, topologyContextId, seedUuids, entityTypes, entityStates,
                    environmentType, supplyChainRpcService, groupExpander, enforceUserScope, repositoryApi);
        }

        @Override
        @Nonnull
        public Map<String, SupplyChainNode> processSupplyChain(
                @Nonnull final List<SupplyChainNode> supplyChainNodes) {
            return supplyChainNodes.stream()
                .collect(Collectors.toMap(t -> ApiEntityType.fromType(t.getEntityType()).apiStr(), Function.identity()));
        }
    }

    /**
     * A {@link SupplychainFetcher} that returns a {@link SupplychainApiDTO} ready for API/UI
     * consumption.
     *
     * Also handles fetching the health status if required.
     *
     * If there are more than one seed UUID, then the supply chains from each are merged
     * by ServiceEntity. Note that the list of OIDs for each entity type are merged without
     * duplication. In order to avoid duplication, the OIDs from each supplychain are compiled
     * in the 'shadowOidMap' internally. This 'shadowOidMap' is also used to calculate the count
     * of entities of each type when populating the result {@link SupplychainApiDTO}.
     */
    private static class SupplychainApiDTOFetcher extends SupplychainFetcher<SupplychainApiDTO> {

        private final EntityDetailType entityDetailType;

        private final Collection<String> aspectsToInclude;

        private final EntitySeverityServiceBlockingStub severityRpcService;

        private final CostServiceBlockingStub costServiceBlockingStub;

        private final Boolean includeHealthSummary;

        private final RepositoryApi repositoryApi;

        private final EntityAspectMapper entityAspectMapper;

        private boolean actionOrchestratorAvailable;

        private SupplychainApiDTOFetcher(final long realtimeTopologyContextId,
                                        final long topologyContextId,
                                         @Nullable final Set<String> seedUuids,
                                         @Nullable final Set<ApiEntityType> entityTypes,
                                         @Nullable final Set<EntityState> entityStates,
                                         @Nonnull final Optional<EnvironmentType> environmentType,
                                         @Nullable final EntityDetailType entityDetailType,
                                         @Nullable final Collection<String> aspectsToInclude,
                                         final boolean includeHealthSummary,
                                         @Nonnull final SupplyChainServiceBlockingStub supplyChainRpcService,
                                         @Nonnull final EntitySeverityServiceBlockingStub severityRpcService,
                                         @Nonnull final RepositoryApi repositoryApi,
                                         @Nonnull final GroupExpander groupExpander,
                                         @Nullable final EntityAspectMapper entityAspectMapper,
                                         final boolean enforceUserScope,
                                         @Nonnull final CostServiceBlockingStub costServiceBlockingStub) {
            super(realtimeTopologyContextId, topologyContextId, seedUuids, entityTypes, entityStates, environmentType, supplyChainRpcService,
                    groupExpander, enforceUserScope, repositoryApi);

            if (entityDetailType == null) {
                this.entityDetailType = EntityDetailType.compact;
            } else {
                this.entityDetailType = entityDetailType;
            }

            this.aspectsToInclude = aspectsToInclude;
            this.includeHealthSummary = includeHealthSummary;
            this.severityRpcService = Objects.requireNonNull(severityRpcService);
            this.entityAspectMapper = entityAspectMapper;
            this.repositoryApi = Objects.requireNonNull(repositoryApi);
            this.costServiceBlockingStub = Objects.requireNonNull(costServiceBlockingStub);

            actionOrchestratorAvailable = true;
        }

        /**
         * Handle one supplychain response from the SupplyChain service. Tabulate the OIDs;
         * if requested, fetch the entity details from the Repository; also fetch the
         * Severity information.
         *
         * Use the option 'includeHealthSummary' during the result processing
         * to determine what sort of results to return - health information (from the
         * {@link EntitySeverityServiceGrpc}) vs. the individual ServiceEntities.
         *
         * @throws InterruptedException if thread has been interrupted
         * @throws ConversionException if errors faced during converting data to API DTOs
         */
        @Override
        public SupplychainApiDTO processSupplyChain(
                @Nonnull final List<SupplyChainNode> supplyChainNodes)
                throws InterruptedException, ConversionException {
            final SupplychainApiDTO resultApiDTO = new SupplychainApiDTO();
            resultApiDTO.setSeMap(new HashMap<>());

            for (SupplyChainNode supplyChainNode : supplyChainNodes) {
                final Set<Long> memberOidsList = RepositoryDTOUtil.getAllMemberOids(supplyChainNode);

                // fetch service entities, if requested
                final Map<String, ServiceEntityApiDTO> serviceEntityApiDTOS = new HashMap<>();
                // If we only have one member we need its name for the supplychain
                if ((entityDetailType != EntityDetailType.compact) || memberOidsList.size() == 1) {
                    RepositoryApi.MultiEntityRequest request = repositoryApi.entitiesRequest(memberOidsList);
                    if (entityDetailType == EntityDetailType.aspects) {
                        request.useAspectMapper(entityAspectMapper, aspectsToInclude);
                    }
                    request.getSEList().forEach(e -> serviceEntityApiDTOS.put(e.getUuid(), e));
                }

                final Map<Severity, Long> severities = new HashMap<>();
                if (includeHealthSummary || (entityDetailType != EntityDetailType.compact)) {
                    // fetch severities, either to include in a health summary or to decorate SE's
                    try {
                        logger.debug("Collecting severities for {}", supplyChainNode.getEntityType());

                        // If we have already determined the AO is unavailable, avoid lots of other calls to the AO that
                        // will likely almost certainly fail and delay the response to the client.
                        if (actionOrchestratorAvailable) {
                            final MultiEntityRequest severityRequest = MultiEntityRequest.newBuilder()
                                .setTopologyContextId(getTopologyContextId())
                                .addAllEntityIds(memberOidsList)
                                .build();
                            if (entityDetailType != EntityDetailType.compact) {
                                fetchEntitySeverities(severityRequest, serviceEntityApiDTOS, severities);
                            } else {
                                fetchSeverityCounts(severityRequest, severities);
                            }
                        }
                    } catch (RuntimeException e) {
                        if (e instanceof StatusRuntimeException) {
                            // This is a gRPC StatusRuntimeException
                            Status status = ((StatusRuntimeException)e).getStatus();
                            logger.warn("Unable to fetch severities: {} caused by {}.",
                                    status.getDescription(), status.getCause());
                            if (status.getCode() == Code.UNAVAILABLE) {
                                actionOrchestratorAvailable = false;
                            }
                        } else {
                            logger.error("Error when fetching severities: ", e);
                        }
                    }
                }

                // add the results from this {@link SupplyChainNode} to the aggregate
                // {@link SupplychainApiDTO} result
                compileSupplyChainNode(supplyChainNode, severities, serviceEntityApiDTOS, resultApiDTO);
            }

            // set information about cost to supplyChain entities if it is required and possible
            populateCostPrice(resultApiDTO);

            return resultApiDTO;
        }

        /**
         * Inside this method get call to cost component for entities which support following rules:
         * 1. this is entity with Cloud entityType
         * 2. for this entityType should exist data in cost component
         * 3. requested full information(depends on EntityDetailType) about entities
         * @param apiDTO {@link SupplychainApiDTO}
         */
        private void populateCostPrice(SupplychainApiDTO apiDTO) {
            if (!(entityDetailType == EntityDetailType.aspects ||
                    entityDetailType == EntityDetailType.entity)) {
                return;
            }
            for (Entry<String, SupplychainEntryDTO> entry : apiDTO.getSeMap().entrySet()) {
                if (!ApiEntityType.ENTITY_TYPES_WITH_COST.contains(entry.getKey())) {
                    continue;
                }
                final Set<Long> entitiesIds = entry.getValue().getInstances().values().stream()
                        .filter(ent -> ent.getEnvironmentType() ==
                                com.vmturbo.api.enums.EnvironmentType.CLOUD)
                        .map(ent -> Long.valueOf(ent.getUuid()))
                        .collect(Collectors.toSet());
                if (!entitiesIds.isEmpty()) {
                    final Iterator<GetCloudCostStatsResponse> costStatsResponse =
                        costServiceBlockingStub.getCloudCostStats(
                            GetCloudCostStatsRequest.newBuilder()
                                .addCloudCostStatsQuery(CloudCostStatsQuery.newBuilder()
                                    .setEntityFilter(EntityFilter.newBuilder()
                                        .addAllEntityId(entitiesIds)))
                                .build());
                    // collect all records from responses
                    final List<CloudCostStatRecord> allRecords = new ArrayList<>();
                    while (costStatsResponse.hasNext()) {
                        allRecords.addAll(costStatsResponse.next().getCloudStatRecordList());
                    }
                    final HashMap<Long, Float> costToEntity = new HashMap<>();
                    if (!allRecords.isEmpty()) {
                        final long firstQueryId = allRecords.get(0).getQueryId();
                        final long firstDate = allRecords.get(0).getSnapshotDate();
                        final List<CloudCostStatRecord.StatRecord> matchingRecords = new ArrayList<>();

                        // collect only stat records associated with the first query and snapshot
                        // since records are in date order, break after finding a non-matching record
                        for (final CloudCostStatRecord r : allRecords) {
                            if (r.getQueryId() == firstQueryId && r.getSnapshotDate() == firstDate) {
                                matchingRecords.addAll(r.getStatRecordsList());
                            } else {
                                break;
                            }
                        }
                        matchingRecords.forEach(el -> costToEntity.merge(el.getAssociatedEntityId(),
                            el.getValues().getTotal(), Float::sum));
                    }
                    if (!costToEntity.isEmpty()) {
                        entry.getValue().getInstances().forEach((s, serviceEntityApiDTO) -> {
                            final Float costPrice = costToEntity.getOrDefault(Long.valueOf(s), 0F);
                            serviceEntityApiDTO.setCostPrice(costPrice);
                        });
                    }
                }
            }
        }

        private void fetchSeverityCounts(@Nonnull final MultiEntityRequest severityCountRequest,
                                         @Nonnull final Map<Severity, Long> severities) {
            Preconditions.checkArgument(includeHealthSummary);

            final SeverityCountsResponse response =
                severityRpcService.getSeverityCounts(severityCountRequest);
            response.getCountsList().forEach(severityCount -> {
                final Severity severity = severityCount.getSeverity();
                final long currentCount = severities.getOrDefault(severity, 0L);
                severities.put(severity, currentCount + severityCount.getEntityCount());
            });

            final long currentNormalCount = severities.getOrDefault(Severity.NORMAL, 0L);
            severities.put(Severity.NORMAL,
                currentNormalCount + response.getUnknownEntityCount());
        }

        private void fetchEntitySeverities(@Nonnull final MultiEntityRequest entitySeverityRequest,
                                           @Nonnull final Map<String, ServiceEntityApiDTO> serviceEntityApiDTOS,
                                           @Nonnull final Map<Severity, Long> severities) {
            Objects.requireNonNull(entityDetailType);
            Iterable<EntitySeveritiesResponse> response = () ->
                severityRpcService.getEntitySeverities(entitySeverityRequest);
            StreamSupport.stream(response.spliterator(), false)
                .forEach(chunk -> {
                    if (chunk.getTypeCase() == TypeCase.ENTITY_SEVERITY) {
                        chunk.getEntitySeverity().getEntitySeverityList().forEach(entitySeverity -> {
                            // If no severity is provided by the AO, default to normal
                            Severity effectiveSeverity = entitySeverity.hasSeverity()
                                ? entitySeverity.getSeverity()
                                : Severity.NORMAL;
                            // if the SE is being collected, update the severity
                            final String oidString = Long.toString(entitySeverity.getEntityId());
                            if (serviceEntityApiDTOS.containsKey(oidString)) {
                                serviceEntityApiDTOS
                                    // fetch the ServiceEntityApiDTO for this ID
                                    .get(oidString)
                                    // update the severity
                                    .setSeverity(effectiveSeverity.name());
                            }

                            // if healthSummary is being created, increment the count
                            if (includeHealthSummary) {
                                severities.put(entitySeverity.getSeverity(), severities
                                    .getOrDefault(entitySeverity.getSeverity(), 0L) + 1L);
                            }
                        });
                    }
                });
        }

        /**
         * Compile the supply chain node and its associated severities into the {@link SupplychainApiDTO}
         * to be built by the fetcher.
         *
         @param node The {@link SupplyChainNode} to be compiled in.
          * @param severityMap A map showing how many entities in the node map to various kinds of severities.
         *@param serviceEntityApiDTOS if requested (see includeHealthSummary) them map from OID to
         *                             {@link ServiceEntityApiDTO} to return
         */
        private synchronized void compileSupplyChainNode(
                @Nonnull final SupplyChainNode node,
                @Nonnull final Map<Severity, Long> severityMap,
                @Nonnull final Map<String, ServiceEntityApiDTO> serviceEntityApiDTOS,
                @Nonnull final SupplychainApiDTO resultApiDTO) {
            logger.debug("Compiling results for {}", node.getEntityType());

            // This is thread-safe because we're doing it in a synchronized method.
            resultApiDTO.getSeMap().computeIfAbsent(ApiEntityType.fromType(node.getEntityType()).apiStr(), entityType -> {
                // first SupplychainEntryDTO for this entity type; create one and just store the values
                final SupplychainEntryDTO supplyChainEntry = new SupplychainEntryDTO();
                supplyChainEntry.setConnectedConsumerTypes(node.getConnectedConsumerTypesList().stream()
                    .map(ApiEntityType::fromSdkTypeToEntityTypeString)
                    .collect(Collectors.toSet()));
                supplyChainEntry.setConnectedProviderTypes(node.getConnectedProviderTypesList().stream()
                    .map(ApiEntityType::fromSdkTypeToEntityTypeString)
                    .collect(Collectors.toSet()));
                supplyChainEntry.setDepth(node.getSupplyChainDepth());
                supplyChainEntry.setInstances(serviceEntityApiDTOS);

                // Set health summary if we were able to retrieve severities.
                final Map<String, Integer> healthSummary = severityMap.entrySet().stream()
                        .collect(Collectors.toMap(
                                entry -> ActionDTOUtil.getSeverityName(entry.getKey()),
                                entry -> entry.getValue().intValue()));
                supplyChainEntry.setHealthSummary(healthSummary);

                // Compile the entities count from the members-by-state map, since
                // the member OIDs field is deprecated.
                int entitiesCount = 0;
                final Map<String, Integer> stateSummary = new HashMap<>();
                for (final Entry<Integer, MemberList> entry : node.getMembersByStateMap().entrySet()) {
                    entitiesCount += entry.getValue().getMemberOidsCount();
                    final UIEntityState uiState =
                            UIEntityState.fromEntityState(EntityState.forNumber(entry.getKey()));
                    stateSummary.compute(uiState.apiStr(),
                        (k, existingValue) -> {
                            if (existingValue != null) {
                                logger.warn("Multiple states in supply chain node for entity type " +
                                        "{} map to API state {}", node.getEntityType(), k);
                                return existingValue + entry.getValue().getMemberOidsCount();
                            } else {
                                return entry.getValue().getMemberOidsCount();
                            }
                    });
                }
                supplyChainEntry.setStateSummary(stateSummary);
                supplyChainEntry.setEntitiesCount(entitiesCount);
                return supplyChainEntry;
            });
        }

        @Override
        public String toString() {
            return super.toString() + "\n" + MoreObjects.toStringHelper(this)
                    .add("entityDetailType", entityDetailType)
                    .add("includeHealthSummary", includeHealthSummary)
                    .add("actionOrchestratorAvailable", actionOrchestratorAvailable)
                    .toString();
        }
    }

    /**
     * A representation of the results of scope expansion.
     */
    public class ScopeExpansionResult {

        // The set of expanded entity ids
        private Set<Long> expandedScope;

        // The types of entities for which scope expansion was attempted
        private Set<Integer> derivedTypes;

        /**
         * Create a representation of the results of scope expansion.
         *
         * @param derivedTypes the entity types for which scope expansion was attempted (if any)
         * @param expandedScope the resulting scope after scope expansion
         */
        public ScopeExpansionResult(Set<Integer> derivedTypes, Set<Long> expandedScope) {
            this.expandedScope = new HashSet<>(expandedScope);
            this.derivedTypes = Collections.unmodifiableSet(derivedTypes);
        }

        public void addAllToExpandedScope(Set<Long> allMemberOids) {
            expandedScope.addAll(allMemberOids);
        }

        public Set<Long> getExpandedScope() {
            return expandedScope;
        }

        public Set<Integer> getDerivedTypes() {
            return derivedTypes;
        }
    }
}
