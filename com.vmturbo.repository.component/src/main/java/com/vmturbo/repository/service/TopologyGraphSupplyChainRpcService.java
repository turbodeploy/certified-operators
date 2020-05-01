package com.vmturbo.repository.service;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;

import io.grpc.stub.StreamObserver;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
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
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.EnvironmentTypeUtil;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.components.api.SetOnce;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.repository.listener.realtime.LiveTopologyStore;
import com.vmturbo.repository.listener.realtime.RepoGraphEntity;
import com.vmturbo.repository.listener.realtime.SourceRealtimeTopology;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.supplychain.GlobalSupplyChainCalculator;
import com.vmturbo.topology.graph.supplychain.SupplyChainCalculator;
import com.vmturbo.topology.graph.supplychain.TraversalRulesLibrary;

/**
 * An implementation of {@link SupplyChainServiceImplBase} (see SupplyChain.proto) that uses
 * the in-memory topology graph for resolving supply chain queries.
 */
public class TopologyGraphSupplyChainRpcService extends SupplyChainServiceImplBase {

    private static final Logger logger = LogManager.getLogger();

    private final ArangoSupplyChainRpcService arangoSupplyChainService;

    private final LiveTopologyStore liveTopologyStore;

    private final SupplyChainStatistician supplyChainStatistician;

    private final long realtimeTopologyContextId;

    private final InMemorySupplyChainResolver inMemorySupplyChainResolver;

    public TopologyGraphSupplyChainRpcService(@Nonnull final UserSessionContext userSessionContext,
                                              @Nonnull final LiveTopologyStore liveTopologyStore,
                                              @Nonnull final ArangoSupplyChainRpcService arangoSupplyChainService,
                                              @Nonnull final SupplyChainStatistician supplyChainStatistician,
                                              @Nonnull final SupplyChainCalculator supplyChainCalculator,
                                              final long realtimeTopologyContextId) {
        this.liveTopologyStore = liveTopologyStore;
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.supplyChainStatistician = supplyChainStatistician;
        this.arangoSupplyChainService = arangoSupplyChainService;
        inMemorySupplyChainResolver = new InMemorySupplyChainResolver(userSessionContext,
                                                                      liveTopologyStore,
                                                                      supplyChainCalculator);
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
            // this is a real time topology request
                // to be serviced by in-memory topology
            inMemorySupplyChainResolver.serveInMemoryRequest(request, responseObserver);
        } else {
            // this is not a real time topology request
                // To be serviced by a topology stored in ArangoDB.
                // This service is not currently used.
                // We should consider either improving it
                // to use the same algorithm as the real-time
                // topology, or to remove it altogether.
            arangoSupplyChainService.getSupplyChain(request, responseObserver);
        }
    }

    @Override
    public void getSupplyChainStats(GetSupplyChainStatsRequest request,
                                    StreamObserver<GetSupplyChainStatsResponse> responseObserver) {
        // the request will be answered by mimicking a regular GetSupplyChainRequest,
        // calling getSupplyChain on it, and then retrieving statistics from the response.

        // get live topology or return if it doesn't exist
        final TopologyGraph<RepoGraphEntity> topologyGraph =
                liveTopologyStore.getSourceTopology().map(SourceRealtimeTopology::entityGraph).orElse(null);
        if (topologyGraph == null) {
            responseObserver.onNext(GetSupplyChainStatsResponse.getDefaultInstance());
            responseObserver.onCompleted();
            return;
        }

        // prepare the GetSupplyChainRequest object
        final GetSupplyChainRequest supplyChainRequest = GetSupplyChainRequest.newBuilder()
                                                                .setScope(request.getScope())
                                                                .build();

        // this is where the result will be stored
        final AtomicReference<SupplyChain> supplyChain = new AtomicReference<>();

        // latch to be used as a signal when getSupplyChain is finished
        final CountDownLatch latch = new CountDownLatch(1);

        // prepare the stream observer
        final StreamObserver<GetSupplyChainResponse> supplyChainResponseStreamObserver =
                new StreamObserver<GetSupplyChainResponse>() {
                    @Override
                    public void onNext(final GetSupplyChainResponse getSupplyChainResponse) {
                        supplyChain.set(getSupplyChainResponse.getSupplyChain());
                    }

                    @Override
                    public void onError(final Throwable throwable) {
                        latch.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        latch.countDown();
                    }
                };

        // call and get the result
        getSupplyChain(supplyChainRequest, supplyChainResponseStreamObserver);
        try {
            latch.await();
        } catch (InterruptedException e) {
            responseObserver.onError(e);
            return;
        }

        // get the statistics and return them
        final List<SupplyChainStat> stats =
            supplyChainStatistician.calculateStats(supplyChain.get(),
                                                   request.getGroupByList(),
                                                   topologyGraph::getEntity,
                    realtimeTopologyContextId);
        responseObserver.onNext(GetSupplyChainStatsResponse.newBuilder()
                                    .addAllStats(stats)
                                    .build());
        responseObserver.onCompleted();
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
     * Helper class that deals with the generation of supply chains
     * from the in-memory topologies.
     */
    private static class InMemorySupplyChainResolver {
        /**
         * Traversal rules for the production of a scoped supply chain.
         */
        private static final TraversalRulesLibrary<RepoGraphEntity> TRAVERSAL_RULES_LIBRARY =
                new TraversalRulesLibrary<>();

        private final UserSessionContext userSessionContext;
        private final LiveTopologyStore liveTopologyStore;
        private final SupplyChainCalculator supplyChainCalculator;

        InMemorySupplyChainResolver(@Nonnull UserSessionContext userSessionContext,
                                    @Nonnull LiveTopologyStore liveTopologyStore,
                                    @Nonnull SupplyChainCalculator supplyChainCalculator) {
            this.userSessionContext = Objects.requireNonNull(userSessionContext);
            this.liveTopologyStore = Objects.requireNonNull(liveTopologyStore);
            this.supplyChainCalculator = Objects.requireNonNull(supplyChainCalculator);
        }

        /**
         * Serve a supply chain request from the in-memory topology.
         *
         * @param request the request
         * @param responseObserver the response observer
         */
        private void serveInMemoryRequest(
                @Nonnull GetSupplyChainRequest request,
                @Nonnull StreamObserver<GetSupplyChainResponse> responseObserver) {
            // extract scope and desired environment type (if any),
            // and desired included entity types (if any)
            final SupplyChainScope scope = request.hasScope() ? request.getScope()
                    : SupplyChainScope.getDefaultInstance();
            final Optional<EnvironmentType> environmentType =
                    scope.hasEnvironmentType() ? Optional.of(scope.getEnvironmentType()) : Optional.empty();
            final Collection<String> includedEntityTypes = scope.getEntityTypesToIncludeList();

            // apply user scope as necessary.
            final EntityAccessScope userScope = userSessionContext.isUserScoped()
                    ? userSessionContext.getUserAccessScope()
                    : EntityAccessScope.DEFAULT_ENTITY_ACCESS_SCOPE;

            // calculate the entity filter of the traversal
            // taking into account required environment, user scope
            // and excluded types
            final Predicate<RepoGraphEntity> entityPredicate =
                    getEntityFilter(scope, getExcludedTypes(request), userScope);

            if (scope.getStartingEntityOidCount() > 0) {
                // this is a scoped supply chain request
                getScopedSupplyChain(scope.getStartingEntityOidList(), entityPredicate,
                        translateTypeNamesToTypeIds(includedEntityTypes), request.getFilterForDisplay(),
                        responseObserver);
            } else if (!userScope.containsAll()) {
                // this is a user-scoped global supply chain request
                getScopedSupplyChain(userScope.getScopeGroupMembers().toSet(), entityPredicate,
                        translateTypeNamesToTypeIds(includedEntityTypes), request.getFilterForDisplay(),
                        responseObserver);
            } else {
                // We DON'T use the "entityPredicate" directly, because it captures some of the
                // criteria we handle in optimized ways in the global supply chain request -
                // most notably the environment type.

                // this is a global supply chain request
                getGlobalSupplyChain(scope, request.getFilterForDisplay(), responseObserver);
            }
        }

        /**
         * Compute a scoped supply chain.
         *
         * @param scope the initial seed
         * @param entityPredicate predicate to be used to filter entities
         *                        during the traversal
         * @param includedTypes filter final result to only include entities
         *                      of these types
         * @param filterForDisplay if true, exclude entity types not intended for display in the UI
         *                         supply chain
         * @param responseObserver use this observer to return the response
         */
        private void getScopedSupplyChain(@Nonnull Collection<Long> scope,
                                          @Nonnull Predicate<RepoGraphEntity> entityPredicate,
                                          @Nonnull Collection<Integer> includedTypes,
                                          final boolean filterForDisplay,
                                          @Nonnull StreamObserver<GetSupplyChainResponse> responseObserver) {
            try (DataMetricTimer timer =
                     SINGLE_SOURCE_SUPPLY_CHAIN_DURATION_SUMMARY.labels("realtime").startTimer()) {
                logger.debug("Getting a supply chain starting from {} in realtime topology", scope);

                // calculate supply chain
                final Map<Integer, SupplyChainNode> supplyChainNodesMap =
                    liveTopologyStore.getSourceTopology()
                        .map(topologyGraph ->
                                supplyChainCalculator.getSupplyChainNodes(topologyGraph.entityGraph(), scope,
                                        entityPredicate, TRAVERSAL_RULES_LIBRARY))
                        .orElse(Collections.emptyMap());

                // remove unwanted entity types from the result
                if (!includedTypes.isEmpty()) {
                    supplyChainNodesMap.keySet().removeIf(type -> !includedTypes.contains(type));
                }

                if (filterForDisplay) {
                    // remove business accounts from the result
                    supplyChainNodesMap.remove(EntityType.BUSINESS_ACCOUNT_VALUE);
                }

                // return result
                final Collection<SupplyChainNode> supplyChainNodes = supplyChainNodesMap.values();
                responseObserver.onNext(GetSupplyChainResponse.newBuilder()
                        .setSupplyChain(SupplyChain.newBuilder()
                                .addAllSupplyChainNodes(supplyChainNodes))
                        .build());
                responseObserver.onCompleted();
            }
        }

        /**
         * Get the global supply chain. While technically not a supply chain, return a stream of the
         * same supply chain information ({@link SupplyChainNode} calculated over all the
         * ServiceEntities in the given topology context. If requested, restrict the supply chain
         * information to entities from a given list of entityTypes.
         * @param supplyChainScope The scope for the supply chain call (note - we expect that the
         *                        seed OIDs in the scope are empty).
         * @param filterForDisplay If set to true, then nodes for entity types that are in the supply
        *                         chain but not normally displayed in the supply chain UI will be
        *                         filtered out from the results.
         * @param responseObserver the gRPC response stream onto which each resulting SupplyChainNode is
         */
        private void getGlobalSupplyChain(
                @Nonnull final SupplyChainScope supplyChainScope,
                final boolean filterForDisplay,
                @Nonnull final StreamObserver<GetSupplyChainResponse> responseObserver) {
            final Collection<String> entityTypesToIncludeList = supplyChainScope.getEntityTypesToIncludeList();
            final Optional<EnvironmentType> environmentType = supplyChainScope.hasEnvironmentType()
                    ? Optional.of(supplyChainScope.getEnvironmentType()) : Optional.empty();

            // The entity type and environment type filters are handled separately for the global
            // supply chain for optimization purposes. All OTHER restrictions on the nodes to
            // include fall under "additionalFilters". It is set only if other restrictions exist.
            // That allows us to use the cached global supply chains for most requests.
            final Optional<Predicate<RepoGraphEntity>> additionalFilters;
            if (!supplyChainScope.getEntityStatesToIncludeList().isEmpty()) {
                List<EntityState> states = supplyChainScope.getEntityStatesToIncludeList();
                additionalFilters = Optional.of(e -> states.contains(e.getEntityState()));
            } else {
                additionalFilters = Optional.empty();
            }

            // compute a predicate that filters out supply chain nodes from the final result of the
            // traversal, according to desired entity types. If no "desired" types are specified AND
            // filterForDisplay is true (which is the default case), then the default behavior will
            // be to exclude the entity types defined in
            // GlobalSupplyChainCalculator.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN as well as
            // Business Accounts.
            // If "filterForDisplay" is false, then no entity types will be excluded by default.
            final Predicate<SupplyChainNode> filteringPredicate =
                    CollectionUtils.isNotEmpty(entityTypesToIncludeList)
                            // use "include" list.
                        ? node -> entityTypesToIncludeList.contains(node.getEntityType())
                        : filterForDisplay
                            // default behavior: exclude business accounts. Note that the "ignored
                            // "entity types for global supply chain" are also being excluded, inside
                            // the GlobalSupplyChainCalculator.getSupplyChainNodes(...) method.
                            ? node -> !ApiEntityType.fromString(node.getEntityType()).equals(ApiEntityType.BUSINESS_ACCOUNT)
                            // filterForDisplay is disabled -- don't filter anything
                            : node -> true;

            // create an entity-type-based predicate that is applied DURING supply chain traversal, and
            // will be used to skip entities matching the predicate during processing. Entities of
            // a type not matching the predicate will not be traversed. This can be used to speed up
            // traversal and prune the results before the filteringPredicate is applied. If filterForDisplay
            // is disabled, then no entity types will be skipped.
            final Predicate<Integer> entityTypesToSkip = filterForDisplay
                    ? GlobalSupplyChainCalculator.DEFAULT_ENTITY_TYPE_FILTER
                    : entityType -> false;

            // compute and return the global supply chain
            GLOBAL_SUPPLY_CHAIN_DURATION_SUMMARY.startTimer().time(() -> {
                final SupplyChain.Builder supplyChainBuilder = SupplyChain.newBuilder();
                liveTopologyStore.getSourceTopology().ifPresent(realtimeTopology ->
                        realtimeTopology.globalSupplyChainNodes(environmentType, additionalFilters, entityTypesToSkip).values().stream()
                                .filter(filteringPredicate)
                                .forEach(supplyChainBuilder::addSupplyChainNodes));
                responseObserver.onNext(GetSupplyChainResponse.newBuilder()
                        .setSupplyChain(supplyChainBuilder)
                        .build());
                responseObserver.onCompleted();
            });
        }

        /**
         * Calculate which entity types should be not be considered
         * in a supply chain traversal.
         *
         * <p>If request.getFilterForDisplay() is set to "False", then NO types
         * will be excluded.
         *
         * @param request the initial request
         * @return the set of entity type ids to be excluded from
         *         the traversal
         */
        private Collection<Integer> getExcludedTypes(@Nonnull GetSupplyChainRequest request) {
            // if we are not filtering anything for display, don't do any filtering.
            if (request.hasFilterForDisplay() && !request.getFilterForDisplay()) {
                return Collections.emptySet();
            }
            // the default filter set excludes entity types not intended for display in the UI
            // supply chain.
            return GlobalSupplyChainCalculator.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN;
        }

        @Nonnull
        private static Collection<Integer> translateTypeNamesToTypeIds(
                @Nonnull Collection<String> includedTypesStrings) {
            return includedTypesStrings.stream()
                    .map(entityType -> ApiEntityType.fromString(entityType).typeNumber())
                    .collect(Collectors.toList());
        }

        /**
         * Calculate the entity predicate that should filter out entities
         * during a scoped supply chain traversal, given various parameters.
         *
         * @param scope The scope for the supply chain query. Some parameters in the scope affect
         *              resolution.
         * @param exclusionEntityTypes entity types to exclude
         * @param userScope returned entities should necessarily belong in this
         *              set, if this set is specified
         * @return the predicate that returns true iff an entity satisfies
         *         all the above requirements
         */
        private Predicate<RepoGraphEntity> getEntityFilter(
                @Nonnull SupplyChainScope scope,
                @Nonnull Collection<Integer> exclusionEntityTypes,
                @Nonnull EntityAccessScope userScope) {
            Predicate<RepoGraphEntity> predicateBuilder = e -> true;
            if (scope.hasEnvironmentType()) {
                final EnvironmentType environmentType = scope.getEnvironmentType();
                predicateBuilder = predicateBuilder.and(e ->
                                        EnvironmentTypeUtil.match(environmentType, e.getEnvironmentType()));
            }
            if (!scope.getEntityStatesToIncludeList().isEmpty()) {
                final Set<EntityState> stateList = Sets.newHashSet(scope.getEntityStatesToIncludeList());
                predicateBuilder = predicateBuilder.and(e -> stateList.contains(e.getEntityState()));
            }
            if (!exclusionEntityTypes.isEmpty()) {
                predicateBuilder = predicateBuilder.and(
                        e -> !exclusionEntityTypes.contains(e.getEntityType()));
            }
            if (!userScope.containsAll()) {
                predicateBuilder = predicateBuilder.and(e -> userScope.contains(e.getOid()));
            }
            return predicateBuilder;
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
