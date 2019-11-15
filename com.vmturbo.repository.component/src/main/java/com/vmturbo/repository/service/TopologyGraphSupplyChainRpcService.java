package com.vmturbo.repository.service;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetMultiSupplyChainsRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetMultiSupplyChainsResponse;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainResponse;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainStatsRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainStatsResponse;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainScope;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainSeed;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainStat;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceImplBase;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.common.protobuf.topology.UIEnvironmentType;
import com.vmturbo.components.api.SetOnce;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.repository.listener.realtime.LiveTopologyStore;
import com.vmturbo.repository.listener.realtime.RepoGraphEntity;
import com.vmturbo.repository.listener.realtime.SourceRealtimeTopology;
import com.vmturbo.repository.service.SupplyChainMerger.MergedSupplyChain;
import com.vmturbo.repository.service.SupplyChainMerger.MergedSupplyChainException;
import com.vmturbo.repository.service.SupplyChainMerger.SingleSourceSupplyChain;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.supplychain.SupplyChainResolver;

/**
 * An implementation of {@link SupplyChainServiceImplBase} (see SupplyChain.proto) that uses
 * the in-memory topology graph for resolving supply chain queries.
 *
 * <p>TODO (roman, May 27 2019): OM-46673 - Reduce duplication between this and
 * {@link ArangoSupplyChainRpcService} (e.g. various utility methods).
 */
public class TopologyGraphSupplyChainRpcService extends SupplyChainServiceImplBase {

    private static final Logger logger = LogManager.getLogger();

    private final ArangoSupplyChainRpcService arangoSupplyChainService;

    private final RealtimeSupplyChainResolver realtimeSupplyChainResolver;

    private final LiveTopologyStore liveTopologyStore;

    private final SupplyChainStatistician supplyChainStatistician;

    private final long realtimeTopologyContextId;

    /**
     * The entity types to ignore when traversing the topology to construct global supply chain.
     * Currently these are cloud entity types which we don't want to show in global supply chain.
     */
    public static final Set<Integer> IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN =
        ArangoSupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN;

    /**
     * The entity types to ignore when traversing the topology to construct account supply chain.
     * BUSINESS_ACCOUNT should be included since it is the starting vertex.
     */
    public static final Set<Integer> IGNORED_ENTITY_TYPES_FOR_ACCOUNT_SUPPLY_CHAIN =
        ArangoSupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_ACCOUNT_SUPPLY_CHAIN;

    /**
     * Constructor for {@link TopologyGraphSupplyChainRpcService} instances.
     *
     * @param userSessionContext To limit supply chains to user scopes.
     * @param supplyChainResolver Generic utility to help resolve realtime supply chains.
     * @param liveTopologyStore To get the realtime topology from.
     * @param arangoSupplyChainService For delegation of plan supply chain requests.
     * @param supplyChainStatistician For supply chain stats on the realtime topology.
     * @param realtimeTopologyContextId The context ID that indicates "realtime" (vs. plan).
     */
    public TopologyGraphSupplyChainRpcService(@Nonnull final UserSessionContext userSessionContext,
                                              @Nonnull final SupplyChainResolver<RepoGraphEntity> supplyChainResolver,
                                              @Nonnull final LiveTopologyStore liveTopologyStore,
                                              @Nonnull final ArangoSupplyChainRpcService arangoSupplyChainService,
                                              @Nonnull final SupplyChainStatistician supplyChainStatistician,
                                              final long realtimeTopologyContextId) {
        this(new RealtimeSupplyChainResolver(userSessionContext, supplyChainResolver),
            liveTopologyStore, arangoSupplyChainService, supplyChainStatistician, realtimeTopologyContextId);
    }

    @VisibleForTesting
    TopologyGraphSupplyChainRpcService(@Nonnull final RealtimeSupplyChainResolver realtimeSupplyChainResolver,
                                       @Nonnull final LiveTopologyStore liveTopologyStore,
                                       @Nonnull final ArangoSupplyChainRpcService arangoSupplyChainService,
                                       @Nonnull final SupplyChainStatistician supplyChainStatistician,
                                       final long realtimeTopologyContextId) {
        this.realtimeSupplyChainResolver = realtimeSupplyChainResolver;
        this.liveTopologyStore = liveTopologyStore;
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.supplyChainStatistician = supplyChainStatistician;
        this.arangoSupplyChainService = arangoSupplyChainService;
    }


    /**
     * Fetch supply chain information as determined by the given {@link GetSupplyChainRequest}.
     * The request may be to calculate either the supply chain for an individual ServiceEntity OID,
     * a merged supply chain derived from a starting list of ServiceEntityOIDs, or
     * a request for supply chain information for the entire topology.
     *
     * <p>The supply chain information includes, organized by Entity Type:
     * <ul>
     *     <li>the entity type
     *     <li>the depth in the dependency tree of the Service Entities of this type
     *     <li>the list of entity types that provide resources to ServiceEntities of this type
     *     <li>the list of entity types that consume resources from ServiceEntities of this type
     *     <li>the OIDs of the ServiceEntities of this type in the supplychain
     * </ul>
     *
     * @param request the request indicating the OIDs for the service entities from which
     *                the supply chain should be calculated; and the topology context ID
     *                identifying from which topology, either the Live Topology or a Plan Topology,
     *                the supply chain information should be drawn, and optional entityType filter
     * @param responseObserver the gRPC response stream onto which each resulting SupplyChainNode is
     *                         returned
     */
    @Override
    public void getSupplyChain(GetSupplyChainRequest request,
                               StreamObserver<GetSupplyChainResponse> responseObserver) {
        final Optional<Long> contextId = request.hasContextId() ?
            Optional.of(request.getContextId()) : Optional.empty();

        if (contextId.map(context -> context == realtimeTopologyContextId).orElse(true)) {
            try {
                final SupplyChain supplyChain =
                    realtimeSupplyChainResolver.getRealtimeSupplyChain(request.getScope(),
                        request.getEnforceUserScope(), liveTopologyStore.getSourceTopology());
                responseObserver.onNext(GetSupplyChainResponse.newBuilder()
                    .setSupplyChain(supplyChain)
                    .build());
                responseObserver.onCompleted();
            } catch (MergedSupplyChainException e) {
                responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
            }
        } else {
            arangoSupplyChainService.getSupplyChain(request, responseObserver);
        }
    }


    @Override
    public void getSupplyChainStats(GetSupplyChainStatsRequest request,
                                    StreamObserver<GetSupplyChainStatsResponse> responseObserver) {
        final SupplyChainScope scope = request.getScope();
        final Optional<SourceRealtimeTopology> optRealtimeTopology = liveTopologyStore.getSourceTopology();
        try {
            if (optRealtimeTopology.isPresent()) {
                final TopologyGraph<RepoGraphEntity> topoGraph = optRealtimeTopology.get().entityGraph();
                final SupplyChain supplyChain =
                    realtimeSupplyChainResolver.getRealtimeSupplyChain(scope, true, optRealtimeTopology);
                final List<SupplyChainStat> stats = supplyChainStatistician.calculateStats(supplyChain,
                    request.getGroupByList(),
                    topoGraph::getEntity);
                responseObserver.onNext(GetSupplyChainStatsResponse.newBuilder()
                    .addAllStats(stats)
                    .build());
            } else {
                // If there is no realtime topology available, return an empty stats response.
                logger.info("No realtime topology available for supply chain stat request: {}",
                    request);
                responseObserver.onNext(GetSupplyChainStatsResponse.getDefaultInstance());
            }
            responseObserver.onCompleted();
        } catch (MergedSupplyChainException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
        }
    }

    @Override
    public void getMultiSupplyChains(GetMultiSupplyChainsRequest request,
                                     StreamObserver<GetMultiSupplyChainsResponse> responseObserver) {
        final Optional<Long> contextId = request.hasContextId() ?
                Optional.of(request.getContextId()) : Optional.empty();
        // For now we essentially call the individual supply chain RPC multiple times.
        // In the future we can try to optimize this.
        for (SupplyChainSeed supplyChainSeed : request.getSeedsList()) {
            final GetSupplyChainRequest supplyChainRequest =
                    supplyChainSeedToRequest(contextId, supplyChainSeed);

            getSupplyChain(supplyChainRequest, new StreamObserver<GetSupplyChainResponse>() {
                private final SetOnce<GetSupplyChainResponse> response = new SetOnce<>();

                @Override
                public void onNext(final GetSupplyChainResponse response) {
                    this.response.trySetValue(response);
                }

                @Override
                public void onError(final Throwable throwable) {
                    logger.error("Encountered error for supply chain seed {}. Error: {}",
                        supplyChainSeed, throwable.getMessage());
                    responseObserver.onNext(GetMultiSupplyChainsResponse.newBuilder()
                        .setSeedOid(supplyChainSeed.getSeedOid())
                        .setError(throwable.getMessage())
                        .build());
                }

                @Override
                public void onCompleted() {
                    final GetMultiSupplyChainsResponse.Builder builder = GetMultiSupplyChainsResponse.newBuilder()
                        .setSeedOid(supplyChainSeed.getSeedOid());

                    response.getValue()
                        .map(GetSupplyChainResponse::getSupplyChain)
                        .ifPresent(builder::setSupplyChain);

                    responseObserver.onNext(builder.build());
                }
            });
        }

        responseObserver.onCompleted();
    }

    @Nonnull
    private GetSupplyChainRequest supplyChainSeedToRequest(final Optional<Long> contextId,
                                                        @Nonnull final SupplyChainSeed supplyChainSeed) {
        GetSupplyChainRequest.Builder reqBuilder = GetSupplyChainRequest.newBuilder();
        reqBuilder.setScope(supplyChainSeed.getScope());
        contextId.ifPresent(reqBuilder::setContextId);
        return reqBuilder.build();
    }

    /**
     * Utility to resolve the realtime supply chain given a {@link SupplyChainScope}.
     *
     * <p>Takes care of special cases, multiple starting entity types, and user scope enforcement -
     * all the stuff that the generic {@link SupplyChainResolver} doesn't handle (yet).
     */
    @VisibleForTesting
    static class RealtimeSupplyChainResolver {

        private final UserSessionContext userSessionContext;

        private final SupplyChainResolver<RepoGraphEntity> supplyChainResolver;

        RealtimeSupplyChainResolver(@Nonnull final UserSessionContext userSessionContext,
                                    @Nonnull final SupplyChainResolver<RepoGraphEntity> supplyChainResolver) {
            this.userSessionContext = userSessionContext;
            this.supplyChainResolver = supplyChainResolver;
        }

        @Nonnull
        SupplyChain getRealtimeSupplyChain(final SupplyChainScope scope,
                                           final boolean enforceUserScope,
                                           Optional<SourceRealtimeTopology> realtimeTopologyOpt) throws MergedSupplyChainException {
            final Optional<UIEnvironmentType> envType = scope.hasEnvironmentType() ?
                Optional.of(UIEnvironmentType.fromEnvType(scope.getEnvironmentType())) :
                Optional.empty();
            if (scope.getStartingEntityOidCount() > 0) {
                return getMultiSourceSupplyChain(scope.getStartingEntityOidList(),
                    scope.getEntityTypesToIncludeList(),
                    envType,
                    enforceUserScope,
                    realtimeTopologyOpt);
            } else {
                return getGlobalSupplyChain(scope.getEntityTypesToIncludeList(),
                    envType,
                    enforceUserScope,
                    realtimeTopologyOpt);
            }
        }

        /**
         * Fetch the supply chain for each element in a list of starting ServiceEntity OIDs.
         * The result is a stream of {@link SupplyChainNode} elements, one per entity type
         * in the supply chain. If requested, restrict the supply chain information to entities from
         * a given list of entityTypes.
         *
         * <p>The SupplyChainNodes returned represent the result of merging, without duplication,
         * the supply chains derived from each of the starting Vertex OIDs. The elements merged
         * into each SupplyChainNode are: connected_provider_types, connected_consumer_types,
         * and member_oids.
         *
         * @param startingVertexOids the list of the ServiceEntity OIDs to start with, generating the
         *                           supply
         * @param entityTypesToIncludeList if given and not empty, restrict the supply chain nodes
         *                                 to be returned to entityTypes in this list
         * @param envType If set, the environment type restriction to apply to the supply chain.
         * @param enforceUserScope whether or not user scope rules should be enforced
         * @param realtimeTopologyOpt The topology to use to calculate the supply chain.
         * @return The {@link SupplyChain} protobuf to return to the caller.
         * @throws MergedSupplyChainException If there is an error assembling the supply chain.
         */
        private SupplyChain getMultiSourceSupplyChain(@Nonnull final Collection<Long> startingVertexOids,
                                                      @Nonnull final List<String> entityTypesToIncludeList,
                                                      @Nonnull final Optional<UIEnvironmentType> envType,
                                                      final boolean enforceUserScope,
                                                      @Nonnull final Optional<SourceRealtimeTopology> realtimeTopologyOpt)
                throws MergedSupplyChainException {
            if (!realtimeTopologyOpt.isPresent()) {
                return SupplyChain.getDefaultInstance();
            }

            final SupplyChainMerger supplyChainMerger = new SupplyChainMerger();

            final SourceRealtimeTopology realtimeTopology = realtimeTopologyOpt.get();

            final Map<UIEntityType, Set<Long>> startingOidsByType = realtimeTopology.entityGraph()
                .getEntities(Sets.newHashSet(startingVertexOids))
                .collect(Collectors.groupingBy(entity -> UIEntityType.fromType(entity.getEntityType()),
                    Collectors.mapping(RepoGraphEntity::getOid, Collectors.toSet())));

            startingOidsByType.forEach((type, entitiesOfType) -> {
                final SingleSourceSupplyChain singleSourceSupplyChain =
                    getRealtimeSingleSourceSupplyChain(entitiesOfType, envType,
                        enforceUserScope, Collections.emptySet(), getExclusionEntityTypes(type),
                        realtimeTopology);

                // remove BusinessAccount from supply chain nodes, since we don't want to show it
                if (type == UIEntityType.BUSINESS_ACCOUNT) {
                    singleSourceSupplyChain.removeSupplyChainNodes(Sets.newHashSet(UIEntityType.BUSINESS_ACCOUNT));
                }

                // add supply chain starting from original entity
                supplyChainMerger.addSingleSourceSupplyChain(singleSourceSupplyChain);

                // handle the special case for cloud if zone is returned in supply chain
                getAndAddSupplyChainFromZoneForRealtime(singleSourceSupplyChain, supplyChainMerger,
                    type, envType, enforceUserScope, realtimeTopology);
            });

            final MergedSupplyChain supplyChain = supplyChainMerger.merge();
            return supplyChain.getSupplyChain(entityTypesToIncludeList);
        }

        /**
         * Handles the special case for cloud if zone is returned in supply chain. In current cloud
         * topology, we can not traverse to region if not starting from zone. So if any zone is in the
         * supply chain, we need to get another supply chain starting from zone and then merge onto
         * existing one.
         *
         * @param singleSourceSupplyChain The {@link SingleSourceSupplyChain} to check for the
         *                                special case.
         * @param supplyChainMerger The merger for the "final" supply chain currently being
         *                          constructed. May be modified by this method.
         * @param startingVertexEntityType The entity type of the starting vertex for the
         *                                 {@link SingleSourceSupplyChain}.
         * @param envType If set, the environment type restriction on the supply chain.
         * @param enforceUserScope Whether or not to limit supply chain entities to the user's scope.
         * @param realtimeTopology The topology to use to calculate the supply chain.
         */
        private void getAndAddSupplyChainFromZoneForRealtime(
                @Nonnull SingleSourceSupplyChain singleSourceSupplyChain,
                @Nonnull SupplyChainMerger supplyChainMerger,
                @Nonnull UIEntityType startingVertexEntityType,
                @Nonnull Optional<UIEnvironmentType> envType,
                final boolean enforceUserScope,
                @Nonnull final SourceRealtimeTopology realtimeTopology) {
            // collect all the availability zones' ids returned by the supply chain
            final Set<Long> zoneIds = singleSourceSupplyChain.getSupplyChainNodes().stream()
                .filter(supplyChainNode ->
                    UIEntityType.fromString(supplyChainNode.getEntityType()) == UIEntityType.AVAILABILITY_ZONE)
                .flatMap(supplyChainNode -> RepositoryDTOUtil.getAllMemberOids(supplyChainNode).stream())
                .collect(Collectors.toSet());
            if (zoneIds.isEmpty()) {
                return;
            }

            if (UIEntityType.REGION.equals(startingVertexEntityType)) {
                // if starting from region, we can only get all related zones, we need to
                // traverse all paths starting from zones to find other entities, so we set
                // inclusionEntityTypes to be empty
                final SingleSourceSupplyChain zonesSupplyChain =
                    getRealtimeSingleSourceSupplyChain(zoneIds, envType, enforceUserScope,
                        Collections.emptySet(), getExclusionEntityTypes(UIEntityType.AVAILABILITY_ZONE),
                        realtimeTopology);
                supplyChainMerger.addSingleSourceSupplyChain(zonesSupplyChain);
            } else if (!UIEntityType.AVAILABILITY_ZONE.equals(startingVertexEntityType)) {
                // if starting from other entity types (not zone, since we can get all we need
                // starting from zone), it can not traverse to regions due to current
                // topology relationship, we need to traverse from zone and get the related
                // region, but we don't want to traverse all paths, so we set inclusionEntityTypes
                // to be ["AvailabilityZone", "Region"] to improve performance
                final SingleSourceSupplyChain zonesSupplyChain =
                    getRealtimeSingleSourceSupplyChain(zoneIds, envType, enforceUserScope,
                        Sets.newHashSet(EntityType.AVAILABILITY_ZONE_VALUE, EntityType.REGION_VALUE),
                        getExclusionEntityTypes(UIEntityType.AVAILABILITY_ZONE),
                        realtimeTopology);
                supplyChainMerger.addSingleSourceSupplyChain(zonesSupplyChain);
            }
        }

        /**
         * Get the global supply chain. While technically not a supply chain, return a stream of the
         * same supply chain information ({@link SupplyChainNode} calculated over all the
         * ServiceEntities in the given topology context. If requested, restrict the supply chain
         * information to entities from a given list of entityTypes.
         *
         * @param entityTypesToIncludeList if given and non-empty, then restrict supply chain nodes
         *                                 returned to the entityTypes listed here
         * @param environmentType If set, limit the supply chain to a particular environment.
         * @param enforceUserScope If set, restrict the supply chain to entities in this user's scope.
         * @param realtimeTopologyOpt The realtime topology to use to calculate the supply chain.
         * @return The {@link SupplyChain} protobuf describing the supply chain.
         * @throws MergedSupplyChainException If there is an error assembling the supply chain.
         */
        private SupplyChain getGlobalSupplyChain(@Nullable List<String> entityTypesToIncludeList,
                                                 @Nonnull final Optional<UIEnvironmentType> environmentType,
                                                 final boolean enforceUserScope,
                                                 @Nonnull final Optional<SourceRealtimeTopology> realtimeTopologyOpt)
            throws MergedSupplyChainException {
            // If the user is scoped and we're enforcing scoping rules, then convert the
            // request to a multi-source supply chain where the starting entities are the set of
            // scope group entities.
            if (enforceUserScope && userSessionContext.isUserScoped()) {
                return getMultiSourceSupplyChain(userSessionContext.getUserAccessScope().getScopeGroupMembers().toSet(),
                    entityTypesToIncludeList,
                    environmentType,
                    enforceUserScope,
                    realtimeTopologyOpt);
            }

            try (DataMetricTimer timer = GLOBAL_SUPPLY_CHAIN_DURATION_SUMMARY.startTimer()) {
                final SupplyChain.Builder supplyChainBuilder = SupplyChain.newBuilder();
                realtimeTopologyOpt.ifPresent(realtimeTopology -> {
                    realtimeTopology.globalSupplyChainNodes(environmentType, supplyChainResolver).values().stream()
                        .filter(node -> CollectionUtils.isEmpty(entityTypesToIncludeList) || entityTypesToIncludeList.contains(node.getEntityType()))
                        .forEach(supplyChainBuilder::addSupplyChainNodes);
                });
                return supplyChainBuilder.build();
            }
        }

        /**
         * Get the exclusion entity types for the given starting entity type.
         *
         * @param startingVertexEntityType The starting entity type.
         * @return The set of types to exclude for this starting vertex's supply chain.
         */
        private Set<Integer> getExclusionEntityTypes(@Nonnull UIEntityType startingVertexEntityType) {
            return UIEntityType.BUSINESS_ACCOUNT.equals(startingVertexEntityType)
                ? IGNORED_ENTITY_TYPES_FOR_ACCOUNT_SUPPLY_CHAIN
                : IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN;
        }

        @Nonnull
        private SingleSourceSupplyChain getRealtimeSingleSourceSupplyChain(
                @Nonnull final Set<Long> startingVertexOid,
                @Nonnull final Optional<UIEnvironmentType> envType,
                final boolean enforceUserScope,
                @Nonnull final Set<Integer> inclusionEntityTypes,
                @Nonnull final Set<Integer> exclusionEntityTypes,
                @Nonnull final SourceRealtimeTopology realtimeTopology) {
            try (DataMetricTimer timer = SINGLE_SOURCE_SUPPLY_CHAIN_DURATION_SUMMARY.labels("realtime").startTimer()) {
                logger.debug("Getting a supply chain starting from {} in realtime topology",
                    startingVertexOid);

                final SingleSourceSupplyChain singleSourceSupplyChain =
                    new SingleSourceSupplyChain(startingVertexOid);

                final Optional<EnvironmentType> targetEnvType = envType.flatMap(UIEnvironmentType::toEnvType);
                final Optional<EntityAccessScope> scope;
                if (enforceUserScope) {
                    scope = Optional.of(userSessionContext.getUserAccessScope());
                } else {
                    scope = Optional.empty();
                }

                final Predicate<RepoGraphEntity> entityPredicate = e ->
                    // No target env type, or matching target env type.
                    (!targetEnvType.isPresent() || e.getEnvironmentType() == targetEnvType.get())
                        // No inclusion entity types, or the entity's type is IN the inclusion.
                        && (inclusionEntityTypes.isEmpty() || inclusionEntityTypes.contains(e.getEntityType()))
                        // No exclusion entity types, or the entity's type is NOT in the exclusion.
                        && (exclusionEntityTypes.isEmpty() || !exclusionEntityTypes.contains(e.getEntityType()))
                        // No scope present, or the entity is in the scope.
                        && (!scope.isPresent() || scope.get().contains(e.getOid()));

                supplyChainResolver.getSupplyChainNodes(startingVertexOid,
                    realtimeTopology.entityGraph(), entityPredicate).values()
                        .forEach(singleSourceSupplyChain::addSupplyChainNode);

                return singleSourceSupplyChain;
            }
        }
    }

    private static final DataMetricSummary GLOBAL_SUPPLY_CHAIN_DURATION_SUMMARY = DataMetricSummary
        .builder()
        .withName("repo_mem_global_supply_chain_duration_seconds")
        .withHelp("Duration in seconds it takes repository to retrieve global supply chain from the in-memory graph.")
        .build()
        .register();
    private static final DataMetricSummary SINGLE_SOURCE_SUPPLY_CHAIN_DURATION_SUMMARY = DataMetricSummary
        .builder()
        .withName("repo_mem_single_source_supply_chain_duration_seconds")
        .withLabelNames("type")
        .withHelp("Duration in seconds it takes repository to retrieve single source supply chain from the in-memory graph.")
        .build()
        .register();

}
