package com.vmturbo.repository.service;

import static javaslang.API.$;
import static javaslang.API.Case;
import static javaslang.API.Match;
import static javaslang.Patterns.Left;
import static javaslang.Patterns.Right;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import javaslang.control.Either;

import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.repository.SupplyChain.MultiSupplyChainsRequest;
import com.vmturbo.common.protobuf.repository.SupplyChain.MultiSupplyChainsResponse;
import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainRequest;
import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainSeed;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceImplBase;
import com.vmturbo.components.api.SetOnce;
import com.vmturbo.components.common.mapping.UIEnvironmentType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.repository.constant.RepoObjectType.RepoEntityType;

/**
 * A gRPC service for retrieving supply chain information.
 */
public class SupplyChainRpcService extends SupplyChainServiceImplBase {
    private static final Logger logger = LoggerFactory.getLogger(RepositoryRpcService.class);

    private final SupplyChainService supplyChainService;

    private final GraphDBService graphDBService;

    private final UserSessionContext userSessionContext;

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
    public static final Set<Integer> IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN = ImmutableSet.of(
            EntityType.COMPUTE_TIER_VALUE,
            EntityType.STORAGE_TIER_VALUE,
            EntityType.DATABASE_TIER_VALUE,
            EntityType.DATABASE_SERVER_TIER_VALUE,
            EntityType.BUSINESS_ACCOUNT_VALUE,
            EntityType.CLOUD_SERVICE_VALUE
    );

    // the entity types to ignore when traversing the topology to construct account supply chain,
    // BUSINESS_ACCOUNT should be inclueded since it is the starting vertex
    public static final Set<Integer> IGNORED_ENTITY_TYPES_FOR_ACCOUNT_SUPPLY_CHAIN =
            IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN.stream()
                    .filter(entityType -> entityType != EntityType.BUSINESS_ACCOUNT_VALUE)
                    .collect(Collectors.collectingAndThen(Collectors.toSet(), Collections::unmodifiableSet));

    public SupplyChainRpcService(@Nonnull final GraphDBService graphDBService,
                                 @Nonnull final SupplyChainService supplyChainService,
                                 @Nonnull final UserSessionContext userSessionContext) {
        this.graphDBService = Objects.requireNonNull(graphDBService);
        this.supplyChainService = Objects.requireNonNull(supplyChainService);
        this.userSessionContext = Objects.requireNonNull(userSessionContext);
    }

    /**
     * Fetch supply chain information as determined by the given {@link SupplyChainRequest}.
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
    public void getSupplyChain(SupplyChainRequest request,
                               StreamObserver<SupplyChainNode> responseObserver) {
        final Optional<Long> contextId = request.hasContextId() ?
            Optional.of(request.getContextId()) : Optional.empty();

        final Optional<UIEnvironmentType> envType = request.hasEnvironmentType() ?
                Optional.of(UIEnvironmentType.fromEnvType(request.getEnvironmentType())) :
                Optional.empty();
        if (request.getStartingEntityOidCount() > 0) {
            getMultiSourceSupplyChain(request.getStartingEntityOidList(),
                    request.getEntityTypesToIncludeList(), contextId, envType,
                    responseObserver);
        } else {
            getGlobalSupplyChain(request.getEntityTypesToIncludeList(), envType,
                    contextId, responseObserver);
        }
    }

    @Override
    public void getMultiSupplyChains(MultiSupplyChainsRequest request,
                                     StreamObserver<MultiSupplyChainsResponse> responseObserver) {
        final Optional<Long> contextId = request.hasContextId() ?
                Optional.of(request.getContextId()) : Optional.empty();
        final SetOnce<Throwable> error = new SetOnce<>();
        // For now we essentially call the individual supply chain RPC multiple times.
        // In the future we can try to optimize this.
        for (SupplyChainSeed supplyChainSeed : request.getSeedsList()) {
            final SupplyChainRequest supplyChainRequest =
                    supplyChainSeedToRequest(contextId, supplyChainSeed);

            getSupplyChain(supplyChainRequest, new StreamObserver<SupplyChainNode>() {
                private final List<SupplyChainNode> nodes = new ArrayList<>();

                @Override
                public void onNext(final SupplyChainNode supplyChainNode) {
                    nodes.add(supplyChainNode);
                }

                @Override
                public void onError(final Throwable throwable) {
                    error.trySetValue(throwable);
                }

                @Override
                public void onCompleted() {
                    responseObserver.onNext(MultiSupplyChainsResponse.newBuilder()
                            .setSeedOid(supplyChainSeed.getSeedOid())
                            .addAllSupplyChainNodes(nodes)
                            .build());
                }
            });

            if (error.getValue().isPresent()) {
                break;
            }
        }

        if (error.getValue().isPresent()) {
            responseObserver.onError(error.getValue().get());
        } else {
            responseObserver.onCompleted();
        }
    }

    @Nonnull
    private SupplyChainRequest supplyChainSeedToRequest(final Optional<Long> contextId,
                                                        @Nonnull final SupplyChainSeed supplyChainSeed) {
        SupplyChainRequest.Builder reqBuilder = SupplyChainRequest.newBuilder();
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
     * @param contextId the unique identifier for the topology context from which the supply chain
     *                  information should be derived
     * @param responseObserver the gRPC response stream onto which each resulting SupplyChainNode is
     */
    private void getGlobalSupplyChain(@Nullable List<String> entityTypesToIncludeList,
                                                        @Nonnull final Optional<UIEnvironmentType> environmentType,
                                                        @Nonnull final Optional<Long> contextId,
                                                        @Nonnull final StreamObserver<SupplyChainNode> responseObserver) {
        GLOBAL_SUPPLY_CHAIN_DURATION_SUMMARY.startTimer().time(() -> {
            supplyChainService.getGlobalSupplyChain(contextId, environmentType,
                    IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN)
                .subscribe(supplyChainNodes -> {
                    supplyChainNodes.values().stream()
                            // if entityTypes are to be limited, restrict to SupplyChainNode types in the list
                            .filter(supplyChainNode -> CollectionUtils.isEmpty(entityTypesToIncludeList)
                                    || entityTypesToIncludeList.contains(supplyChainNode.getEntityType()))
                            .forEach(responseObserver::onNext);
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
     * @param responseObserver the gRPC response stream onto which each resulting SupplyChainNode is
     */
    private void getMultiSourceSupplyChain(@Nonnull final List<Long> startingVertexOids,
                                           @Nullable final List<String> entityTypesToIncludeList,
                                           @Nonnull final Optional<Long> contextId,
                                           @Nonnull final Optional<UIEnvironmentType> envType,
                                           @Nonnull final StreamObserver<SupplyChainNode> responseObserver) {
        final SupplyChainMerger supplyChainMerger = new SupplyChainMerger();

        // multiple starting entities may traverse to same zones in supply chain, we don't want
        // to query supply chain for same zone multiple times, use a cache to improve performance
        final Map<Long, SingleSourceSupplyChain> zoneSupplyChainOnlyRegion = Maps.newHashMap();
        final Map<Long, SingleSourceSupplyChain> zoneSupplyChainComplete = Maps.newHashMap();

        for (Long oid : startingVertexOids) {
            Optional<String> startingVertexEntityTypeOpt = getRepoEntityType(oid);
            if (!startingVertexEntityTypeOpt.isPresent()) {
                continue;
            }
            final String startingVertexEntityType = startingVertexEntityTypeOpt.get();
            final SingleSourceSupplyChain singleSourceSupplyChain = getSingleSourceSupplyChain(oid,
                contextId, envType, Collections.emptySet(), getExclusionEntityTypes(startingVertexEntityType));

            // remove BusinessAccount from supply chain nodes, since we don't want to show it
            if (RepoEntityType.BUSINESS_ACCOUNT.getValue().equals(startingVertexEntityType)) {
                singleSourceSupplyChain.removeSupplyChainNodes(Sets.newHashSet(RepoEntityType.BUSINESS_ACCOUNT));
            }

            // add supply chain starting from original entity
            supplyChainMerger.addSingleSourceSupplyChain(singleSourceSupplyChain);

            // handle the special case for cloud if zone is returned in supply chain
            getAndAddSupplyChainFromZone(singleSourceSupplyChain, supplyChainMerger,
                startingVertexEntityType, contextId, envType, zoneSupplyChainComplete, zoneSupplyChainOnlyRegion);
        }

        final MergedSupplyChain supplyChain = supplyChainMerger.merge();
        if (supplyChain.errors.isEmpty()) {
            supplyChain.getSupplyChainNodes().stream()
                    // if entityTypes are to be limited, restrict to SupplyChainNode types in the list
                    .filter(supplyChainNode -> CollectionUtils.isEmpty(entityTypesToIncludeList)
                            || entityTypesToIncludeList.contains(supplyChainNode.getEntityType()))
                    .forEach(responseObserver::onNext);
            responseObserver.onCompleted();
        } else {
            responseObserver.onError(Status.INTERNAL.withDescription(
                supplyChain.errors.stream()
                    .collect(Collectors.joining(", "))).asException());
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
                    @Nonnull String startingVertexEntityType,
                    @Nonnull Optional<Long> contextId,
                    @Nonnull Optional<UIEnvironmentType> envType,
                    @Nonnull Map<Long, SingleSourceSupplyChain> zoneSupplyChainComplete,
                    @Nonnull Map<Long, SingleSourceSupplyChain> zoneSupplyChainOnlyRegion) {
        // collect all the availability zones' ids returned by the supply chain
        final Set<Long> zoneIds = singleSourceSupplyChain.getSupplyChainNodes().stream()
            .filter(supplyChainNode -> RepoEntityType.AVAILABILITY_ZONE.getValue().equals(
                supplyChainNode.getEntityType()))
            .flatMap(supplyChainNode -> RepositoryDTOUtil.getAllMemberOids(supplyChainNode).stream())
            .collect(Collectors.toSet());

        if (RepoEntityType.REGION.getValue().equals(startingVertexEntityType)) {
            // if starting from region, we can only get all related zones, we need to
            // traverse all paths starting from zones to find other entities, so we set
            // inclusionEntityTypes to be empty
            zoneIds.stream()
                .map(zoneId -> zoneSupplyChainComplete.computeIfAbsent(zoneId,
                    k -> getSingleSourceSupplyChain(zoneId, contextId, envType,
                        Collections.emptySet(),
                        getExclusionEntityTypes(RepoEntityType.AVAILABILITY_ZONE.getValue()))))
                .forEach(supplyChainMerger::addSingleSourceSupplyChain);
        } else if (!RepoEntityType.AVAILABILITY_ZONE.getValue().equals(startingVertexEntityType)) {
            // if starting from other entity types (not zone, since we can get all we need
            // starting from zone), it can not traverse to regions due to current
            // topology relationship, we need to traverse from zone and get the related
            // region, but we don't want to traverse all paths, so we set inclusionEntityTypes
            // to be ["AvailabilityZone", "Region"] to improve performance
            zoneIds.stream()
                .map(zoneId -> zoneSupplyChainOnlyRegion.computeIfAbsent(zoneId,
                    k -> getSingleSourceSupplyChain(zoneId, contextId, envType,
                        Sets.newHashSet(EntityType.AVAILABILITY_ZONE_VALUE, EntityType.REGION_VALUE),
                        getExclusionEntityTypes(RepoEntityType.AVAILABILITY_ZONE.getValue()))))
                .forEach(supplyChainMerger::addSingleSourceSupplyChain);
        }
    }

    /**
     * Query ArangoDB and get the entity type for the given entity oid.
     *
     * @param oid oid of the entity to get entity type for
     * @return entity type in the string value of {@link RepoEntityType}
     */
    public Optional<String> getRepoEntityType(@Nonnull Long oid) {
        Either<String, Collection<ServiceEntityApiDTO>> result =
            graphDBService.searchServiceEntityById(Long.toString(oid));
        return Match(result).of(
            Case(Right($()), entity -> entity.size() == 1
                ? Optional.of(entity.iterator().next().getClassName())
                : Optional.empty()),
            Case(Left($()), err -> Optional.empty())
        );
    }

    /**
     * Get the exclusion entity types for the given starting entity type.
     */
    private Set<Integer> getExclusionEntityTypes(@Nonnull String startingVertexEntityType) {
        return RepoEntityType.BUSINESS_ACCOUNT.getValue().equals(startingVertexEntityType)
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
        final SingleSourceSupplyChain singleSourceSupplyChain = new SingleSourceSupplyChain();

        SINGLE_SOURCE_SUPPLY_CHAIN_DURATION_SUMMARY.startTimer().time(() -> {
            Either<String, Stream<SupplyChainNode>> supplyChain = graphDBService.getSupplyChain(
                contextId, envType, startingVertexOid.toString(),
                    Optional.of(userSessionContext.getUserAccessScope()),
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

    /**
     * This class is used to accumulate supply chain information related to a single
     * starting vertex OID. It maintains the starting vertex OID, a list of
     * {@link SupplyChainNode}s generated during the graph walk, and (optionally) an
     * error message if an error is found during the walking process.
     */
    private static class SingleSourceSupplyChain {

        private final List<SupplyChainNode> supplyChainNodes = new ArrayList<>();
        private Optional<String> error = Optional.empty();

        /**
         * Remember a {@link SupplyChainNode} for later.
         *
         * @param node the {@link SupplyChainNode} to be remembered
         */
        void addSupplyChainNode(@Nonnull final SupplyChainNode node) {
            supplyChainNodes.add(Objects.requireNonNull(node));
        }

        /**
         * Return the list of {@link SupplyChainNode} elements that have been remembered
         *
         * @return a list of all of the {@link SupplyChainNode}s that have been stored
         */
        List<SupplyChainNode> getSupplyChainNodes() {
            return supplyChainNodes;
        }

        /**
         * Remove SupplyChainNodes for the given set of entity types. For example: we get supply
         * chain starting from BusinessAccount, but we don't want to show BusinessAccount node in
         * the supply chain, then we need to remove the BusinessAccount node and modify related nodes.
         *
         * @param repoEntityTypes the set of entity types {@link RepoEntityType} to remove SupplyChainNode for
         */
        void removeSupplyChainNodes(@Nonnull Set<RepoEntityType> repoEntityTypes) {
            final Set<String> entityTypes = repoEntityTypes.stream()
                .map(RepoEntityType::getValue)
                .collect(Collectors.toSet());

            final List<SupplyChainNode> newSupplyChainNodes = supplyChainNodes.stream()
                    // e.g. remove BusinessAccount node
                    .filter(supplyChainNode -> !entityTypes.contains(supplyChainNode.getEntityType()))
                    .map(supplyChainNode -> {
                        final SupplyChainNode.Builder nodeBuilder = supplyChainNode.toBuilder();
                        // e.g. remove BusinessAccount from nodes providers list
                        nodeBuilder.clearConnectedProviderTypes();
                        nodeBuilder.addAllConnectedProviderTypes(
                            supplyChainNode.getConnectedProviderTypesList().stream()
                                .filter(type -> !entityTypes.contains(type))
                                .collect(Collectors.toList()));
                        // e.g. remove BusinessAccount from nodes consumers list
                        nodeBuilder.clearConnectedConsumerTypes();
                        nodeBuilder.addAllConnectedConsumerTypes(
                            supplyChainNode.getConnectedConsumerTypesList().stream()
                                .filter(type -> !entityTypes.contains(type))
                                .collect(Collectors.toList()));
                        return nodeBuilder.build();
                    }).collect(Collectors.toList());
            supplyChainNodes.clear();
            supplyChainNodes.addAll(newSupplyChainNodes);
        }

        /**
         * Remember an error message related to this Supply Chain generation process.
         *
         * @param error a String describing the error that has occurred
         */
        void setError(@Nonnull final String error) {
            this.error = Optional.of(error);
        }

        /**
         * Fetch an error message that has been saved, or Optional.empty() if none
         *
         * @return the error message that has been saved, or Optional.empty() if none
         */
        public Optional<String> getError() {
            return error;
        }
    }

    /**
     * A class to used to hold the individual {@link SupplyChainNode}s created by merging
     * the supply chain information for each individual supply chain. It also
     */
    private static class MergedSupplyChain {
        private final Collection<SupplyChainNode> supplyChainNodes;
        private final List<String> errors;

        MergedSupplyChain(@Nonnull final Collection<SupplyChainNode> supplyChainNodes,
                                 @Nonnull final List<String> errors) {
            this.supplyChainNodes = supplyChainNodes;
            this.errors = errors;
        }

        Collection<SupplyChainNode> getSupplyChainNodes() {
            return supplyChainNodes;
        }

        public List<String> getErrors() {
            return errors;
        }
    }

    /**
     * A class to handle a single merged {@link SupplyChainNode}. This class handles the merging
     * task, where a new individual SupplyChainNode is merged into the aggregate here. Note that,
     * as for the {@link SupplyChainNode} which will eventually be returned to the caller,
     * this MergedSupplyChainNode holds the information pertaining to a single EntityType.
     */
    private static class MergedSupplyChainNode {
        private final Map<Integer, Set<Long>> membersByState;
        private final Set<String> connectedConsumerTypes;
        private final Set<String> connectedProviderTypes;
        private Optional<Integer> supplyChainDepth;
        private final String entityType;

        /**
         * Create an instance of the class to aggregate multiple individual {@link SupplyChainNode}s
         * for the same Entity Type. Initialize this instance based on an initial individual
         * SupplyChainNode.
         *
         * @param supplyChainNode first the initial {@link SupplyChainNode} to aggregate; initialize
         *                        the internal variables for capturing the merged information later
         */
        MergedSupplyChainNode(@Nonnull final SupplyChainNode supplyChainNode) {
            this.connectedConsumerTypes = new HashSet<>(supplyChainNode.getConnectedConsumerTypesCount());
            this.connectedProviderTypes = new HashSet<>(supplyChainNode.getConnectedProviderTypesCount());
            this.supplyChainDepth = supplyChainNode.hasSupplyChainDepth() ?
                Optional.of(supplyChainNode.getSupplyChainDepth()) :
                Optional.empty();
            this.entityType = supplyChainNode.getEntityType();
            this.membersByState = new HashMap<>(supplyChainNode.getMembersByStateCount());
            supplyChainNode.getMembersByStateMap().forEach((state, membersForState) ->
                    membersByState.put(state, new HashSet<>(membersForState.getMemberOidsList())));
        }

        /**
         * Aggregate information from an input {@link SupplyChainNode} into the supply chain
         * information already seen. We add to the memberOids, connectedConsumerTypes,
         * connectedProviderTypes, all without replacement. We track the supplyChainDepth,
         * and log a warning if the new supplyChainDepth does not match the original.
         *
         * @param supplyChainNode the new individual {@link SupplyChainNode} to aggregate into the
         *                        information saved from the previous SupplyChainNodes
         */
        void mergeWith(@Nonnull final SupplyChainNode supplyChainNode) {
            // Handle node depth
            if (supplyChainNode.hasSupplyChainDepth()) {
                if (supplyChainDepth.isPresent() &&
                        supplyChainDepth.get() != supplyChainNode.getSupplyChainDepth()) {
                    // if there's a mismatch, log the occurance and use the original
                    logger.warn("Mismatched supply chain depth on entity type {} when merging supply chains. " +
                            "Old depth {}, new depth {}",
                        supplyChainNode.getEntityType(),
                        supplyChainDepth.get(),
                        supplyChainNode.getSupplyChainDepth());
                } else {
                    this.supplyChainDepth = Optional.of(supplyChainNode.getSupplyChainDepth());
                }
            }

            // Merge by taking the union of known members and connected types with the new ones.
            supplyChainNode.getMembersByStateMap().forEach((state, membersForState) -> {
                final Set<Long> allMembersForState = membersByState.computeIfAbsent(state,
                        k -> new HashSet<>(membersForState.getMemberOidsCount()));
                allMembersForState.addAll(membersForState.getMemberOidsList());
            });
            connectedConsumerTypes.addAll(supplyChainNode.getConnectedConsumerTypesList());
            connectedProviderTypes.addAll(supplyChainNode.getConnectedProviderTypesList());
        }

        /**
         * Create an instance of {@link SupplyChainNode} to return to the caller based on the
         * values accumulated over all the individual instances of SupplyChainNode.
         *
         * @return a new {@link SupplyChainNode} initialized from the information accumulated from
         * all the individual SupplyChainNodes
         */
        SupplyChainNode toSupplyChainNode() {
            final SupplyChainNode.Builder builder = SupplyChainNode.newBuilder()
                .setEntityType(entityType)
                .addAllConnectedConsumerTypes(connectedConsumerTypes)
                .addAllConnectedProviderTypes(connectedProviderTypes);
            membersByState.forEach((state, membersForState) -> {
                builder.putMembersByState(state, MemberList.newBuilder()
                    .addAllMemberOids(membersForState)
                    .build());
            });
            supplyChainDepth.ifPresent(builder::setSupplyChainDepth);

            return builder.build();
        }
    }

    /**
     * A class to merge all individual instances of {@link MergedSupplyChainNode} we've derived
     * from each starting Entity OID listed in the request. This class accepts and accumulates
     * individual {@link SingleSourceSupplyChain} instances, and then calculates the merged
     * supply chains when requested.
     */
    private static class SupplyChainMerger {
        private final List<SingleSourceSupplyChain> supplyChains = new ArrayList<>();

        /**
         * Accumulate a {@link SingleSourceSupplyChain} for later merging.
         *
         * @param supplyChain an {@link SingleSourceSupplyChain} to record for later merging
         */
        void addSingleSourceSupplyChain(@Nonnull final SingleSourceSupplyChain supplyChain) {
            supplyChains.add(Objects.requireNonNull(supplyChain));
        }

        /**
         * Calculate the merged {@link SupplyChainNode}s, by Entity Type, for all the
         * {@link SingleSourceSupplyChain} instances we've accumulated.
         *
         * @return a single {@link MergedSupplyChain} containing all the individual
         * {@link MergedSupplyChainNode}s derived here
         */
        MergedSupplyChain merge() {
            final Map<String, MergedSupplyChainNode> nodesByType = new HashMap<>();
            final List<String> errors = new ArrayList<>();

            supplyChains.forEach(supplyChain -> {
                if (supplyChain.getError().isPresent()) {
                    errors.add(supplyChain.getError().get());
                } else {
                    supplyChain.getSupplyChainNodes().forEach(node -> mergeNode(nodesByType, node));
                }
            });

            return new MergedSupplyChain(nodesByType.values().stream()
                    .map(MergedSupplyChainNode::toSupplyChainNode)
                    .collect(Collectors.toList()), errors);
        }

        /**
         * Merge a new SupplyChainNode into the curent set of {@link MergedSupplyChainNode}s.
         * Look up the previous MergedSupplyChainNode for the entity type of this one, create
         * a new MergedSupplyChainNode if necessary, and then merge the given {@link SupplyChainNode}
         * into it.
         *
         * @param nodesByType a map from Entity Type to the {@link MergedSupplyChainNode}
         *                    for that Entity Type.
         * @param supplyChainNode a new {@link SupplyChainNode} to be merged into the existing data
         */
        private void mergeNode(@Nonnull final Map<String, MergedSupplyChainNode> nodesByType,
                               @Nonnull final SupplyChainNode supplyChainNode) {
            final MergedSupplyChainNode mergedNode =
                nodesByType.computeIfAbsent(supplyChainNode.getEntityType(),
                    type -> new MergedSupplyChainNode(supplyChainNode));

            mergedNode.mergeWith(supplyChainNode);
        }
    }
}
