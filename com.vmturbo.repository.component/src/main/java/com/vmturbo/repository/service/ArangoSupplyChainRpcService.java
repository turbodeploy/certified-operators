package com.vmturbo.repository.service;

import static javaslang.API.$;
import static javaslang.API.Case;
import static javaslang.API.Match;
import static javaslang.Patterns.Left;
import static javaslang.Patterns.Right;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javaslang.control.Either;

import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetMultiSupplyChainsRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetMultiSupplyChainsResponse;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainResponse;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainScope;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainSeed;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceImplBase;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.common.protobuf.topology.UIEnvironmentType;
import com.vmturbo.components.api.SetOnce;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.repository.service.SupplyChainMerger.MergedSupplyChain;
import com.vmturbo.repository.service.SupplyChainMerger.MergedSupplyChainException;
import com.vmturbo.repository.service.SupplyChainMerger.SingleSourceSupplyChain;
import com.vmturbo.topology.graph.supplychain.GlobalSupplyChainCalculator;

/**
 * An implementation of SupplyChainService that uses arango to retrieve the supply chain.
 */
public class ArangoSupplyChainRpcService extends SupplyChainServiceImplBase {

    private final Logger logger = LogManager.getLogger();

    private final SupplyChainService supplyChainService;

    private final GraphDBService graphDBService;

    private final UserSessionContext userSessionContext;

    private final long realtimeTopologyContextId;

    private static final DataMetricSummary GLOBAL_SUPPLY_CHAIN_DURATION_SUMMARY = DataMetricSummary
        .builder()
        .withName("repo_global_supply_chain_duration_seconds")
        .withHelp("Duration in seconds it takes repository to retrieve global supply chain.")
        .build()
        .register();
    private static final DataMetricSummary SINGLE_SOURCE_SUPPLY_CHAIN_DURATION_SUMMARY = DataMetricSummary
        .builder()
        .withName("repo_single_source_supply_chain_duration_seconds")
        .withHelp("Duration in seconds it takes repository to retrieve single source supply chain.")
        .build()
        .register();

    // the entity types to ignore when traversing the topology to construct global supply chain,
    // currently these are cloud entity types which we don't want to show in global supply chain
    /**
     * @deprecated Use {@link GlobalSupplyChainCalculator#IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN}
     * instead.
     */
    @Deprecated
    public static final Set<Integer> IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN = ImmutableSet.of(
            EntityType.COMPUTE_TIER_VALUE,
            EntityType.STORAGE_TIER_VALUE,
            EntityType.DATABASE_TIER_VALUE,
            EntityType.DATABASE_SERVER_TIER_VALUE,
            EntityType.BUSINESS_ACCOUNT_VALUE,
            EntityType.CLOUD_SERVICE_VALUE,
            EntityType.HYPERVISOR_SERVER_VALUE,
            EntityType.PROCESSOR_POOL_VALUE,
            EntityType.SERVICE_PROVIDER_VALUE
    );

    // the entity types to ignore when traversing the topology to construct account supply chain,
    // BUSINESS_ACCOUNT should be included since it is the starting vertex
    public static final Set<Integer> IGNORED_ENTITY_TYPES_FOR_ACCOUNT_SUPPLY_CHAIN =
            IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN.stream()
                    .filter(entityType -> entityType != EntityType.BUSINESS_ACCOUNT_VALUE)
                    .collect(Collectors.collectingAndThen(Collectors.toSet(), Collections::unmodifiableSet));

    public ArangoSupplyChainRpcService(@Nonnull final GraphDBService graphDBService,
                                       @Nonnull final SupplyChainService supplyChainService,
                                       @Nonnull final UserSessionContext userSessionContext,
                                       final long realtimeTopologyContextId) {
        this.graphDBService = Objects.requireNonNull(graphDBService);
        this.supplyChainService = Objects.requireNonNull(supplyChainService);
        this.userSessionContext = Objects.requireNonNull(userSessionContext);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
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
        final SupplyChainScope scope = request.getScope();

        final Optional<UIEnvironmentType> envType = scope.hasEnvironmentType() ?
                Optional.of(UIEnvironmentType.fromEnvType(scope.getEnvironmentType())) :
                Optional.empty();
        if (scope.getStartingEntityOidCount() > 0) {
            getMultiSourceSupplyChain(scope.getStartingEntityOidList(),
                    scope.getEntityTypesToIncludeList(), contextId, envType,
                    request.getFilterForDisplay(), responseObserver);
        } else {
            getGlobalSupplyChain(scope.getEntityTypesToIncludeList(), envType,
                    contextId, request.getFilterForDisplay(), responseObserver);
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
     * Get the global supply chain. While technically not a supply chain, return a stream of the
     * same supply chain information ({@link SupplyChainNode} calculated over all the
     * ServiceEntities in the given topology context. If requested, restrict the supply chain
     * information to entities from a given list of entityTypes.
     *
     * @param entityTypesToIncludeList if given and non-empty, then restrict supply chain nodes
     *                                 returned to the entityTypes listed here
     * @param contextId the unique identifier for the topology context from which the supply chain
     *                  information should be derived
     * @param filterForDisplay whether or not to filter out non-display nodes (such as business accounts)
     *                         from the supply chain view.
     * @param responseObserver the gRPC response stream onto which each resulting SupplyChainNode is
     */
    private void getGlobalSupplyChain(@Nullable List<String> entityTypesToIncludeList,
                                                        @Nonnull final Optional<UIEnvironmentType> environmentType,
                                                        @Nonnull final Optional<Long> contextId,
                                                        final boolean filterForDisplay,
                                                        @Nonnull final StreamObserver<GetSupplyChainResponse> responseObserver) {
        GLOBAL_SUPPLY_CHAIN_DURATION_SUMMARY.startTimer().time(() -> {
            supplyChainService.getGlobalSupplyChain(contextId, environmentType,
                    filterForDisplay ? IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN : Collections.emptySet())
                .subscribe(supplyChainNodes -> {
                    final GetSupplyChainResponse.Builder respBuilder =
                        GetSupplyChainResponse.newBuilder();
                    final SupplyChain.Builder supplyChainBuilder = SupplyChain.newBuilder();
                    supplyChainNodes.values().stream()
                        // if entityTypes are to be limited, restrict to SupplyChainNode types in the list
                        .filter(supplyChainNode -> CollectionUtils.isEmpty(entityTypesToIncludeList)
                                || entityTypesToIncludeList.contains(supplyChainNode.getEntityType()))
                        .forEach(supplyChainBuilder::addSupplyChainNodes);
                    respBuilder.setSupplyChain(supplyChainBuilder);

                    responseObserver.onNext(respBuilder.build());
                    responseObserver.onCompleted();
                }, error -> responseObserver.onError(Status.INTERNAL.withDescription(
                    error.getMessage()).asException()));
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
     * @param contextId the unique identifier for the topology context from which the supply chain
     *                  information should be derived
     * @param envType
     * @param filterForDisplay if true, then entity types not intended for display in the UI supply
     *                         chain will be filtered out.
     * @param responseObserver the gRPC response stream onto which each resulting SupplyChainNode is
     */
    private void getMultiSourceSupplyChain(@Nonnull final List<Long> startingVertexOids,
                                           @Nonnull final List<String> entityTypesToIncludeList,
                                           @Nonnull final Optional<Long> contextId,
                                           @Nonnull final Optional<UIEnvironmentType> envType,
                                           final boolean filterForDisplay,
                                           @Nonnull final StreamObserver<GetSupplyChainResponse> responseObserver) {
        final SupplyChainMerger supplyChainMerger = new SupplyChainMerger();

        // multiple starting entities may traverse to same zones in supply chain, we don't want
        // to query supply chain for same zone multiple times, use a cache to improve performance
        final Map<Long, SingleSourceSupplyChain> zoneSupplyChainOnlyRegion = Maps.newHashMap();
        final Map<Long, SingleSourceSupplyChain> zoneSupplyChainComplete = Maps.newHashMap();

        for (Long oid : startingVertexOids) {
            Optional<UIEntityType> startingVertexEntityTypeOpt = getUIEntityType(oid);
            if (!startingVertexEntityTypeOpt.isPresent()) {
                continue;
            }
            final UIEntityType startingVertexEntityType = startingVertexEntityTypeOpt.get();
            final SingleSourceSupplyChain singleSourceSupplyChain = getSingleSourceSupplyChain(oid,
                contextId, envType, Collections.emptySet(),
                filterForDisplay ? getExclusionEntityTypes(startingVertexEntityType) : Collections.emptySet());

            // remove BusinessAccount from supply chain nodes, since we don't want to show it
            if (startingVertexEntityType == UIEntityType.BUSINESS_ACCOUNT && filterForDisplay) {
                singleSourceSupplyChain.removeSupplyChainNodes(Sets.newHashSet(UIEntityType.BUSINESS_ACCOUNT));
            }

            // add supply chain starting from original entity
            supplyChainMerger.addSingleSourceSupplyChain(singleSourceSupplyChain);

            // handle the special case for cloud if zone is returned in supply chain
            getAndAddSupplyChainFromZone(singleSourceSupplyChain, supplyChainMerger,
                startingVertexEntityType, contextId, envType, filterForDisplay,
                    zoneSupplyChainComplete, zoneSupplyChainOnlyRegion);
        }

        final MergedSupplyChain supplyChain = supplyChainMerger.merge();
        try {
            responseObserver.onNext(GetSupplyChainResponse.newBuilder()
                .setSupplyChain(supplyChain.getSupplyChain(entityTypesToIncludeList))
                .build());
            responseObserver.onCompleted();
        } catch (MergedSupplyChainException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
        }
    }

    /**
     * Handles the special case for cloud if zone is returned in supply chain. In current cloud
     * topology, we can not traverse to region if not starting from zone. So if any zone is in the
     * supply chain, we need to get another supply chain starting from zone and then merge onto
     * existing one.
     */
    private void getAndAddSupplyChainFromZone(
                    @Nonnull SingleSourceSupplyChain singleSourceSupplyChain,
                    @Nonnull SupplyChainMerger supplyChainMerger,
                    @Nonnull UIEntityType startingVertexEntityType,
                    @Nonnull Optional<Long> contextId,
                    @Nonnull Optional<UIEnvironmentType> envType,
                    final boolean filterForDisplay,
                    @Nonnull Map<Long, SingleSourceSupplyChain> zoneSupplyChainComplete,
                    @Nonnull Map<Long, SingleSourceSupplyChain> zoneSupplyChainOnlyRegion) {
        // collect all the availability zones' ids returned by the supply chain
        final Set<Long> zoneIds = singleSourceSupplyChain.getSupplyChainNodes().stream()
            .filter(supplyChainNode -> UIEntityType.fromString(supplyChainNode.getEntityType()) == UIEntityType.AVAILABILITY_ZONE)
            .flatMap(supplyChainNode -> RepositoryDTOUtil.getAllMemberOids(supplyChainNode).stream())
            .collect(Collectors.toSet());

        if (startingVertexEntityType == UIEntityType.REGION) {
            // if starting from region, we can only get all related zones, we need to
            // traverse all paths starting from zones to find other entities, so we set
            // inclusionEntityTypes to be empty
            zoneIds.stream()
                .map(zoneId -> zoneSupplyChainComplete.computeIfAbsent(zoneId,
                    k -> getSingleSourceSupplyChain(zoneId, contextId, envType, Collections.emptySet(),
                        filterForDisplay
                                ? getExclusionEntityTypes(UIEntityType.AVAILABILITY_ZONE)
                                : Collections.emptySet())))
                .forEach(supplyChainMerger::addSingleSourceSupplyChain);
        } else if (startingVertexEntityType != UIEntityType.AVAILABILITY_ZONE) {
            // if starting from other entity types (not zone, since we can get all we need
            // starting from zone), it can not traverse to regions due to current
            // topology relationship, we need to traverse from zone and get the related
            // region, but we don't want to traverse all paths, so we set inclusionEntityTypes
            // to be ["AvailabilityZone", "Region"] to improve performance
            zoneIds.stream()
                .map(zoneId -> zoneSupplyChainOnlyRegion.computeIfAbsent(zoneId,
                    k -> getSingleSourceSupplyChain(zoneId, contextId, envType,
                        Sets.newHashSet(EntityType.AVAILABILITY_ZONE_VALUE, EntityType.REGION_VALUE),
                        filterForDisplay
                            ? getExclusionEntityTypes(UIEntityType.AVAILABILITY_ZONE)
                            : Collections.emptySet())))
                .forEach(supplyChainMerger::addSingleSourceSupplyChain);
        }
    }

    /**
     * Query ArangoDB and get the entity type for the given entity oid.
     *
     * @param oid oid of the entity to get entity type for
     * @return entity type in the string value of {@link UIEntityType}
     */
    public Optional<UIEntityType> getUIEntityType(@Nonnull Long oid) {
        Either<String, Collection<ServiceEntityApiDTO>> result =
            graphDBService.searchServiceEntityById(Long.toString(oid));
        return Match(result).of(
            Case(Right($()), entity -> entity.size() == 1
                ? Optional.of(UIEntityType.fromString(entity.iterator().next().getClassName()))
                : Optional.empty()),
            Case(Left($()), err -> Optional.empty())
        );
    }

    /**
     * Get the exclusion entity types for the given starting entity type.
     */
    private Set<Integer> getExclusionEntityTypes(@Nonnull UIEntityType startingVertexEntityType) {
        return startingVertexEntityType == UIEntityType.BUSINESS_ACCOUNT
            ? IGNORED_ENTITY_TYPES_FOR_ACCOUNT_SUPPLY_CHAIN
            : IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN;
    }

    /**
     * Get the supply chain local to a specific starting node by walking the graph topology beginning
     * with the starting node. The search is done within the topology corresponding to the given
     * topology context ID, which might be the Live Topology or a Plan Topology.
     */
    private SingleSourceSupplyChain getSingleSourceSupplyChain(@Nonnull final Long startingVertexOid,
                                                               @Nonnull final Optional<Long> contextId,
                                                               @Nonnull final Optional<UIEnvironmentType> envType,
                                                               @Nonnull final Set<Integer> inclusionEntityTypes,
                                                               @Nonnull final Set<Integer> exclusionEntityTypes) {
        logger.debug("Getting a supply chain starting from {} in topology {}",
            startingVertexOid, contextId.map(Object::toString).orElse("DEFAULT"));
        final SingleSourceSupplyChain singleSourceSupplyChain =
            new SingleSourceSupplyChain(Collections.singleton(startingVertexOid));

        // if we are requesting a plan supply chain, we will not enforce use scoping restrictions.
        Optional<EntityAccessScope> accessScope = (contextId.isPresent() && contextId.get() != realtimeTopologyContextId)
                ? Optional.empty()
                : Optional.of(userSessionContext.getUserAccessScope());

        SINGLE_SOURCE_SUPPLY_CHAIN_DURATION_SUMMARY.startTimer().time(() -> {
            Either<Throwable, Stream<SupplyChainNode>> supplyChain = graphDBService.getSupplyChain(
                contextId, envType, startingVertexOid.toString(), accessScope,
                    inclusionEntityTypes, exclusionEntityTypes);

            Match(supplyChain).of(
                Case(Right($()), v -> {
                    v.forEach(singleSourceSupplyChain::addSupplyChainNode);
                    return null;
                }),
                Case(Left($()), err -> {
                    singleSourceSupplyChain.setError(err);
                    return null;
                }));
        });
        return singleSourceSupplyChain;
    }
}
