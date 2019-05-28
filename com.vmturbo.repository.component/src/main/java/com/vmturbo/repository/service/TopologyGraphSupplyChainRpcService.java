package com.vmturbo.repository.service;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetMultiSupplyChainsRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetMultiSupplyChainsResponse;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainResponse;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainSeed;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceImplBase;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.common.protobuf.topology.UIEnvironmentType;
import com.vmturbo.components.api.SetOnce;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.repository.listener.realtime.RepoGraphEntity;
import com.vmturbo.repository.listener.realtime.LiveTopologyStore;
import com.vmturbo.repository.listener.realtime.SourceRealtimeTopology;
import com.vmturbo.repository.service.SupplyChainMerger.MergedSupplyChain;
import com.vmturbo.repository.service.SupplyChainMerger.SingleSourceSupplyChain;
import com.vmturbo.topology.graph.supplychain.SupplyChainResolver;

/**
 * An implementation of {@link SupplyChainServiceImplBase} (see SupplyChain.proto) that uses
 * the in-memory topology graph for resolving supply chain queries.
 *
 * TODO (roman, May 27 2019): OM-46673 - Reduce duplication between this and
 * {@link ArangoSupplyChainRpcService} (e.g. various utility methods).
 */
public class TopologyGraphSupplyChainRpcService extends SupplyChainServiceImplBase {

    private final Logger logger = LogManager.getLogger();

    private final ArangoSupplyChainRpcService arangoSupplyChainService;

    private final UserSessionContext userSessionContext;

    private final SupplyChainResolver<RepoGraphEntity> supplyChainResolver;

    private final LiveTopologyStore liveTopologyStore;

    private final long realtimeTopologyContextId;

    // the entity types to ignore when traversing the topology to construct global supply chain,
    // currently these are cloud entity types which we don't want to show in global supply chain
    public static final Set<Integer> IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN =
        ArangoSupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN;

    // the entity types to ignore when traversing the topology to construct account supply chain,
    // BUSINESS_ACCOUNT should be inclueded since it is the starting vertex
    public static final Set<Integer> IGNORED_ENTITY_TYPES_FOR_ACCOUNT_SUPPLY_CHAIN =
        ArangoSupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_ACCOUNT_SUPPLY_CHAIN;

    public TopologyGraphSupplyChainRpcService(@Nonnull final UserSessionContext userSessionContext,
                                              @Nonnull final SupplyChainResolver<RepoGraphEntity> supplyChainResolver,
                                              @Nonnull final LiveTopologyStore liveTopologyStore,
                                              @Nonnull final ArangoSupplyChainRpcService arangoSupplyChainService,
                                              final long realtimeTopologyContextId) {
        this.userSessionContext = Objects.requireNonNull(userSessionContext);
        this.supplyChainResolver = supplyChainResolver;
        this.liveTopologyStore = liveTopologyStore;
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.arangoSupplyChainService = arangoSupplyChainService;
    }

    /**
     * Fetch supply chain information as determined by the given {@link GetSupplyChainRequest}.
     * The request may be to calculate either the supply chain for an individual ServiceEntity OID,
     * a merged supply chain derived from a starting list of ServiceEntityOIDs, or
     * a request for supply chain information for the entire topology.
     *
     * The supply chain information includes, organized by Entity Type:
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

        final Optional<UIEnvironmentType> envType = request.hasEnvironmentType() ?
                Optional.of(UIEnvironmentType.fromEnvType(request.getEnvironmentType())) :
                Optional.empty();
        if (contextId.map(context -> context == realtimeTopologyContextId).orElse(true)) {
            if (request.getStartingEntityOidCount() > 0) {
                getMultiSourceSupplyChain(request.getStartingEntityOidList(),
                    request.getEntityTypesToIncludeList(),
                    envType,
                    request.getEnforceUserScope(),
                    liveTopologyStore.getSourceTopology(),
                    responseObserver);
            } else {
                getGlobalSupplyChain(request.getEntityTypesToIncludeList(),
                    envType,
                    liveTopologyStore.getSourceTopology(),
                    responseObserver);
            }
        } else {
            arangoSupplyChainService.getSupplyChain(request, responseObserver);
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
        if (supplyChainSeed.hasEnvironmentType()) {
            reqBuilder.setEnvironmentType(supplyChainSeed.getEnvironmentType());
        }
        reqBuilder.addAllEntityTypesToInclude(supplyChainSeed.getEntityTypesToIncludeList());
        reqBuilder.addAllStartingEntityOid(supplyChainSeed.getStartingEntityOidList());
        contextId.ifPresent(reqBuilder::setContextId);
        return reqBuilder.build();
    }

    /**
     * Get the global supply chain. While technically not a supply chain, return a stream of the
     * same supply chain information ({@link SupplyChainNode} calculated over all the
     * ServiceEntities in the given topology context. If requested, restrict the supply chain
     * information to entities from a given list of entityTypes.
     *
     * @param entityTypesToIncludeList if given and non-empty, then restrict supply chain nodes
     *                                 returned to the entityTypes listed here
     * @param responseObserver the gRPC response stream onto which each resulting SupplyChainNode is
     */
    private void getGlobalSupplyChain(@Nullable List<String> entityTypesToIncludeList,
                                      @Nonnull final Optional<UIEnvironmentType> environmentType,
                                      @Nonnull final Optional<SourceRealtimeTopology> realtimeTopologyOpt,
                                      @Nonnull final StreamObserver<GetSupplyChainResponse> responseObserver) {
        GLOBAL_SUPPLY_CHAIN_DURATION_SUMMARY.startTimer().time(() -> {
            final SupplyChain.Builder supplyChainBuilder = SupplyChain.newBuilder();
            realtimeTopologyOpt.ifPresent(realtimeTopology -> {
                realtimeTopology.globalSupplyChainNodes().values().stream()
                    .filter(node -> CollectionUtils.isEmpty(entityTypesToIncludeList) || entityTypesToIncludeList.contains(node.getEntityType()))
                    .forEach(supplyChainBuilder::addSupplyChainNodes);
            });
            responseObserver.onNext(GetSupplyChainResponse.newBuilder()
                .setSupplyChain(supplyChainBuilder)
                .build());
            responseObserver.onCompleted();
        });
    }

    /**
     * Fetch the supply chain for each element in a list of starting ServiceEntity OIDs.
     * The result is a stream of {@link SupplyChainNode} elements, one per entity type
     * in the supply chain. If requested, restrict the supply chain information to entities from
     * a given list of entityTypes.
     *
     * The SupplyChainNodes returned represent the result of merging, without duplication,
     * the supply chains derived from each of the starting Vertex OIDs. The elements merged
     * into each SupplyChainNode are: connected_provider_types, connected_consumer_types,
     * and member_oids.
     *  @param startingVertexOids the list of the ServiceEntity OIDs to start with, generating the
     *                           supply
     * @param entityTypesToIncludeList if given and not empty, restrict the supply chain nodes
     *                                 to be returned to entityTypes in this list
     * @param envType
     * @param enforceUserScope whether or not user scope rules should be enforced
     * @param responseObserver the gRPC response stream onto which each resulting SupplyChainNode is
     */
    private void getMultiSourceSupplyChain(@Nonnull final List<Long> startingVertexOids,
                                           @Nonnull final List<String> entityTypesToIncludeList,
                                           @Nonnull final Optional<UIEnvironmentType> envType,
                                           final boolean enforceUserScope,
                                           @Nonnull final Optional<SourceRealtimeTopology> realtimeTopologyOpt,
                                           @Nonnull final StreamObserver<GetSupplyChainResponse> responseObserver) {
        if (!realtimeTopologyOpt.isPresent()) {
            responseObserver.onNext(GetSupplyChainResponse.getDefaultInstance());
            responseObserver.onCompleted();
            return;
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
                    enforceUserScope, Collections.emptySet(), getExclusionEntityTypes(type));

            // remove BusinessAccount from supply chain nodes, since we don't want to show it
            if (type == UIEntityType.BUSINESS_ACCOUNT) {
                singleSourceSupplyChain.removeSupplyChainNodes(Sets.newHashSet(UIEntityType.BUSINESS_ACCOUNT));
            }

            // add supply chain starting from original entity
            supplyChainMerger.addSingleSourceSupplyChain(singleSourceSupplyChain);

            // handle the special case for cloud if zone is returned in supply chain
            getAndAddSupplyChainFromZoneForRealtime(singleSourceSupplyChain, supplyChainMerger,
                type, envType, enforceUserScope);
        });

        final MergedSupplyChain supplyChain = supplyChainMerger.merge();
        if (supplyChain.getErrors().isEmpty()) {
            responseObserver.onNext(GetSupplyChainResponse.newBuilder()
                .setSupplyChain(supplyChain.getSupplyChain(entityTypesToIncludeList))
                .build());
            responseObserver.onCompleted();
        } else {
            responseObserver.onError(Status.INTERNAL.withDescription(
                supplyChain.getErrors().stream()
                    .collect(Collectors.joining(", "))).asException());
        }
    }

    /**
     * Handles the special case for cloud if zone is returned in supply chain. In current cloud
     * topology, we can not traverse to region if not starting from zone. So if any zone is in the
     * supply chain, we need to get another supply chain starting from zone and then merge onto
     * existing one.
     */
    private void getAndAddSupplyChainFromZoneForRealtime(
            @Nonnull SingleSourceSupplyChain singleSourceSupplyChain,
            @Nonnull SupplyChainMerger supplyChainMerger,
            @Nonnull UIEntityType startingVertexEntityType,
            @Nonnull Optional<UIEnvironmentType> envType,
            final boolean enforceUserScope) {
        // collect all the availability zones' ids returned by the supply chain
        final Set<Long> zoneIds = singleSourceSupplyChain.getSupplyChainNodes().stream()
            .filter(supplyChainNode ->
                UIEntityType.fromString(supplyChainNode.getEntityType()) == UIEntityType.AVAILABILITY_ZONE)
            .flatMap(supplyChainNode -> RepositoryDTOUtil.getAllMemberOids(supplyChainNode).stream())
            .collect(Collectors.toSet());

        if (UIEntityType.REGION.equals(startingVertexEntityType)) {
            // if starting from region, we can only get all related zones, we need to
            // traverse all paths starting from zones to find other entities, so we set
            // inclusionEntityTypes to be empty
            final SingleSourceSupplyChain zonesSupplyChain =
                getRealtimeSingleSourceSupplyChain(zoneIds, envType, enforceUserScope,
                    Collections.emptySet(), getExclusionEntityTypes(UIEntityType.AVAILABILITY_ZONE));
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
                    getExclusionEntityTypes(UIEntityType.AVAILABILITY_ZONE));
            supplyChainMerger.addSingleSourceSupplyChain(zonesSupplyChain);
        }
    }

    /**
     * Query ArangoDB and get the entity type for the given entity oid.
     *
     * @param oid oid of the entity to get entity type for
     * @return entity type in the string value of {@link UIEntityType}
     */
    public Optional<UIEntityType> getUIEntityType(@Nonnull Long oid) {
        return liveTopologyStore.getSourceTopology()
            .flatMap(graph -> graph.entityGraph().getEntity(oid))
            .map(entity -> UIEntityType.fromType(entity.getEntityType()));
    }

    /**
     * Get the exclusion entity types for the given starting entity type.
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
                    @Nonnull final Set<Integer> exclusionEntityTypes) {
        try (DataMetricTimer timer = SINGLE_SOURCE_SUPPLY_CHAIN_DURATION_SUMMARY.labels("realtime").startTimer()) {
            logger.debug("Getting a supply chain starting from {} in realtime topology",
                startingVertexOid);

            final SingleSourceSupplyChain singleSourceSupplyChain =
                new SingleSourceSupplyChain(startingVertexOid);

            logger.info("Getting live supply chain the fast way.");

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

            liveTopologyStore.getSourceTopology().ifPresent(topologyGraph -> {
                supplyChainResolver.getSupplyChainNodes(startingVertexOid,
                    topologyGraph.entityGraph(), entityPredicate).values()
                        .forEach(singleSourceSupplyChain::addSupplyChainNode);
            });
            return singleSourceSupplyChain;
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
