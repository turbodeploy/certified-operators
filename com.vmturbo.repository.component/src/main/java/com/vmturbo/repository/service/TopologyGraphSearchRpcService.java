package com.vmturbo.repository.service;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.common.protobuf.PaginationProtoUtil;
import com.vmturbo.common.protobuf.PaginationProtoUtil.PaginatedResults;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.CountEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.EntityCountResponse;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesResponse;
import com.vmturbo.common.protobuf.search.Search.SearchEntityOidsRequest;
import com.vmturbo.common.protobuf.search.Search.SearchEntityOidsResponse;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.SearchQuery;
import com.vmturbo.common.protobuf.search.Search.SearchTagValuesRequest;
import com.vmturbo.common.protobuf.search.Search.SearchTagValuesResponse;
import com.vmturbo.common.protobuf.search.Search.SearchTagsRequest;
import com.vmturbo.common.protobuf.search.Search.SearchTagsResponse;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceImplBase;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.common.protobuf.topology.EnvironmentTypeUtil;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntityBatch;
import com.vmturbo.components.api.tracing.Tracing;
import com.vmturbo.repository.listener.realtime.LiveTopologyStore;
import com.vmturbo.repository.listener.realtime.RepoGraphEntity;
import com.vmturbo.repository.listener.realtime.SourceRealtimeTopology;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.util.BaseGraphEntity;

/**
 * An implementation of {@link SearchServiceImplBase} (see Search.proto) that uses
 * the in-memory topology graph for resolving search queries.
 */
public class TopologyGraphSearchRpcService extends SearchServiceImplBase {

    private final Logger logger = LogManager.getLogger();

    private final LiveTopologyStore liveTopologyStore;

    private final LiveTopologyPaginator liveTopologyPaginator;

    private final PartialEntityConverter partialEntityConverter;

    private final int maxEntitiesPerChunk;

    private final UserSessionContext userSessionContext;

    private final GatedSearchResolver gatedSearchResolver;

    /**
     * Constructor.
     *
     * @param liveTopologyStore Provides access to live topologies.
     * @param liveTopologyPaginator Helper class to paginate results.
     * @param partialEntityConverter Converts entities to {@link PartialEntity} objects.
     * @param userSessionContext To enforce user scope.
     * @param maxEntitiesPerChunk Maximum entities in a single response message in a stream.
     * @param maxConcurrentSearches Maximum concurrent searches, to throttle requests.
     * @param concurrentSearchTimeout Timeout when waiting for access to search.
     * @param concurrentSearchTimeUnit Time unit for the timeout.
     */
    public TopologyGraphSearchRpcService(@Nonnull final LiveTopologyStore liveTopologyStore,
                         @Nonnull final LiveTopologyPaginator liveTopologyPaginator,
                         @Nonnull final PartialEntityConverter partialEntityConverter,
                         @Nonnull final UserSessionContext userSessionContext,
                         final int maxEntitiesPerChunk,
                         final int maxConcurrentSearches,
                         final long concurrentSearchTimeout,
                         @Nonnull final TimeUnit concurrentSearchTimeUnit) {
        this.liveTopologyStore = Objects.requireNonNull(liveTopologyStore);
        this.liveTopologyPaginator = Objects.requireNonNull(liveTopologyPaginator);
        this.partialEntityConverter = Objects.requireNonNull(partialEntityConverter);
        this.maxEntitiesPerChunk = maxEntitiesPerChunk;
        this.gatedSearchResolver = new GatedSearchResolver(liveTopologyStore,
                userSessionContext, maxConcurrentSearches,
                concurrentSearchTimeUnit.toMillis(concurrentSearchTimeout));
        this.userSessionContext = userSessionContext;
    }

    @Nonnull
    private String logParams(@Nonnull final String prefix, List<SearchParameters> params) {
        return prefix + params.stream()
            .map(param -> {
                try {
                    return JsonFormat.printer().print(param);
                } catch (InvalidProtocolBufferException e) {
                    return e.getMessage();
                }
            })
            .collect(Collectors.joining("\n"));
    }

    @Override
    public void countEntities(final CountEntitiesRequest request, final StreamObserver<EntityCountResponse> responseObserver) {
        logger.trace("Counting entity OIDs with search request: {}", request);

        // Return empty result if current topology doesn't exist.
        Optional<SourceRealtimeTopology> topologyGraphOpt = liveTopologyStore.getSourceTopology();
        if (!topologyGraphOpt.isPresent()) {
            logger.warn("No real-time topology exists for searching request");
            responseObserver.onNext(EntityCountResponse.getDefaultInstance());
            responseObserver.onCompleted();
            return;
        }
        SearchQuery searchQuery = request.getSearch();
        try {
            Tracing.log(() -> logParams("Starting entity count with params - ", searchQuery.getSearchParametersList()));

            final long entityCount = gatedSearchResolver.search(Collections.emptyList(), searchQuery).count();

            Tracing.log(() -> "Counted " + entityCount + " entities.");

            logger.trace("countEntities for request {} found {} entities.", request, entityCount);
            responseObserver.onNext(EntityCountResponse.newBuilder()
                .setEntityCount((int)entityCount)
                .build());
            responseObserver.onCompleted();
        } catch (Throwable e) {
            logger.error("countEntities failed for request {} with exception", request, e);
            responseObserver.onError(e);
        }
    }

    /**
     * Get a full list of entity oids based on search parameters, this rpc call will not perform
     * pagination.
     *
     * @param request {@link SearchEntitiesRequest}.
     * @param responseObserver response observer of {@link SearchEntityOidsResponse}.
     */
    @Override
    public void searchEntityOids(SearchEntityOidsRequest request,
                                 StreamObserver<SearchEntityOidsResponse> responseObserver) {
        logger.debug("Searching for entity OIDs with request: {}", request);

        // Return empty result if current topology doesn't exist.
        Optional<SourceRealtimeTopology> topologyGraphOpt = liveTopologyStore.getSourceTopology();
        if (!topologyGraphOpt.isPresent()) {
            logger.warn("No real-time topology exists for searching request");
            responseObserver.onNext(
                SearchEntityOidsResponse.newBuilder().addAllEntities(Collections.emptyList()).build());
            responseObserver.onCompleted();
            return;
        }

        final SearchQuery searchQuery = request.getSearch();

        try {
            Tracing.log(() -> logParams("Starting entity oid search with params - ", searchQuery.getSearchParametersList()));

            final Stream<RepoGraphEntity> entities =
                gatedSearchResolver.search(request.getEntityOidList(), searchQuery);
            final SearchEntityOidsResponse.Builder responseBuilder = SearchEntityOidsResponse.newBuilder();
            entities.map(RepoGraphEntity::getOid).forEach(responseBuilder::addEntities);

            Tracing.log(() -> "Got " + responseBuilder.getEntitiesCount() + " results.");

            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (Throwable e) {
            logger.error("Search entity OIDs failed for request {} with exception", request, e);
            final Status status = Status.INVALID_ARGUMENT.withCause(e).withDescription(e.getMessage());
            responseObserver.onError(status.asRuntimeException());
        }
    }

    @Override
    public void searchEntitiesStream(SearchEntitiesRequest request,
                                     StreamObserver<PartialEntityBatch> responseObserver) {
        // Return empty result if current topology doesn't exist.
        if (!liveTopologyStore.getSourceTopology().isPresent()) {
            logger.warn("No real-time topology exists for searching request");
            responseObserver.onError(Status.UNAVAILABLE
                .withDescription("No real-time topology exists.").asException());
            return;
        }

        final SearchQuery searchQuery = request.getSearch();

        Tracing.log(() -> logParams("Starting entity stream search with params - ", searchQuery.getSearchParametersList()));

        try {
            final Stream<RepoGraphEntity> searchResults = gatedSearchResolver.search(request.getEntityOidList(), searchQuery);
            final Stream<PartialEntity> entities = partialEntityConverter.createPartialEntities(searchResults, request.getReturnType());

            // send the results in batches, if needed
            Iterators.partition(entities.iterator(), maxEntitiesPerChunk)
                .forEachRemaining(chunk -> {
                    PartialEntityBatch batch = PartialEntityBatch.newBuilder()
                        .addAllEntities(chunk)
                        .build();
                    Tracing.log(() -> "Sending chunk of " + batch.getEntitiesCount() + " entities.");
                    logger.debug("Sejding entity batch of {} items ({} bytes)", batch.getEntitiesCount(), batch.getSerializedSize());
                    responseObserver.onNext(batch);
                });
            responseObserver.onCompleted();
        } catch (InterruptedException e) {
            logger.error("Interrupted while waiting to execute search for request {}", request, e);
            final Status status = Status.INTERNAL.withCause(e).withDescription(e.getMessage());
            responseObserver.onError(status.asRuntimeException());
        } catch (TimeoutException e) {
            logger.error("Search entity failed for request {}. Timed out waiting for query permit: {}",
                request, e.getMessage());
            final Status status = Status.UNAVAILABLE.withCause(e).withDescription(e.getMessage());
            responseObserver.onError(status.asRuntimeException());
        } catch (RuntimeException e) {
            logger.error("Search entity failed for request {} with exception", request, e);
            final Status status = Status.INTERNAL.withCause(e).withDescription(e.getMessage());
            responseObserver.onError(status.asRuntimeException());
        }
    }

    @Override
    public void searchEntities(SearchEntitiesRequest request,
                               StreamObserver<SearchEntitiesResponse> responseObserver) {
        logger.debug("Searching for entities with request: {}", request);

        // Return empty result if current topology doesn't exist.
        if (!liveTopologyStore.getSourceTopology().isPresent()) {
            logger.warn("No real-time topology exists for searching request");
            responseObserver.onNext(SearchEntitiesResponse.getDefaultInstance());
            responseObserver.onCompleted();
            return;
        }

        try {
            PaginationProtoUtil.validatePaginationParams(request.getPaginationParams());
        } catch (IllegalArgumentException e) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription(e.getMessage()).asException());
            responseObserver.onCompleted();
        }

        final SearchQuery searchQuery = request.getSearch();
        try {
            Tracing.log(() -> logParams("Starting entity search with params - ", searchQuery.getSearchParametersList()));

            final Stream<RepoGraphEntity> entities = gatedSearchResolver.search(request.getEntityOidList(), searchQuery);
            final PaginatedResults<RepoGraphEntity> paginatedResults =
                liveTopologyPaginator.paginate(entities, request.getPaginationParams());

            Tracing.log(() -> "Completed search and pagination. Got page with "
                    + paginatedResults.nextPageEntities().size() + "entities.");

            final SearchEntitiesResponse.Builder respBuilder = SearchEntitiesResponse.newBuilder()
                .setPaginationResponse(paginatedResults.paginationResponse());
            List<RepoGraphEntity> nextPageEntities = paginatedResults.nextPageEntities();
            partialEntityConverter.createPartialEntities(nextPageEntities.stream(), request.getReturnType())
                    .forEach(respBuilder::addEntities);
            responseObserver.onNext(respBuilder.build());
            responseObserver.onCompleted();
        } catch (Throwable e) {
            logger.error("Search entity failed for request {} with exception", request, e);
            final Status status = Status.ABORTED.withCause(e).withDescription(e.getMessage());
            responseObserver.onError(status.asRuntimeException());
        }
    }

    @Override
    public void searchTagValues(SearchTagValuesRequest request, StreamObserver<SearchTagValuesResponse> responseObserver) {

        // Return empty result if current topology doesn't exist.
        if (!liveTopologyStore.getSourceTopology().isPresent()) {
            logger.warn("No real-time topology exists for searching request");
            responseObserver.onNext(SearchTagValuesResponse.getDefaultInstance());
            responseObserver.onCompleted();
            return;
        }

        final int entityType = request.getEntityType();
        final String tagKey = request.getTagKey();
        final SourceRealtimeTopology topology = liveTopologyStore.getSourceTopology().get();
        final TopologyGraph<RepoGraphEntity> graph = topology.entityGraph();
        final Map<String, LongSet> entitiesByValuesMap = topology.globalTags().getEntitiesByValueMap(tagKey);
        final Predicate<RepoGraphEntity> entityTypeFilter = entity -> entityType == entity.getEntityType();
        final SearchTagValuesResponse.Builder response = SearchTagValuesResponse.newBuilder();
        final Set<Long> oids = graph.entities().filter(entityTypeFilter).map(BaseGraphEntity::getOid).collect(Collectors.toSet());

        for (Map.Entry<String, LongSet> entry : entitiesByValuesMap.entrySet()) {
            final String tagValue = entry.getKey();
            final Set<Long> oidsFiltered = entry.getValue().stream().filter(oids::contains).collect(Collectors.toSet());
            if (!oidsFiltered.isEmpty()) {
                response.putEntitiesByTagValue(tagValue, Search.TaggedEntities.newBuilder().addAllOid(oidsFiltered).build());
            }
        }

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    /**
     * Request tags from the repository.  Currently, no pagination is supported, for simplicity.
     *
     * @param request the request.
     * @param responseObserver a stream observer that contains the result.
     */
    @Override
    public void searchTags(SearchTagsRequest request,
            StreamObserver<SearchTagsResponse> responseObserver) {

        // Return empty result if current topology doesn't exist.
        if (!liveTopologyStore.getSourceTopology().isPresent()) {
            logger.warn("No real-time topology exists for searching request");
            responseObserver.onNext(SearchTagsResponse.getDefaultInstance());
            responseObserver.onCompleted();
            return;
        }

        final SourceRealtimeTopology topology = liveTopologyStore.getSourceTopology().get();
        final TopologyGraph<RepoGraphEntity> graph = topology.entityGraph();

        Tracing.log(() -> {
            try {
                return "Searching tags. Request: " + JsonFormat.printer().print(request);
            } catch (InvalidProtocolBufferException e) {
                // This shouldn't happen because gRPC shouldn't give us an invalid protobuf!
                return "Searching tags. Error: " + e.getMessage();
            }
        });

        final Stream<RepoGraphEntity> matchingEntities;
        if (request.getEntitiesCount() > 0) {
            // Look for specific entities.
            matchingEntities = request.getEntitiesList().stream()
                .map(graph::getEntity)
                .filter(Optional::isPresent)
                .map(Optional::get);
        } else {
            matchingEntities = graph.entities();
        }

        // filter entities according to the requested environment type
        // and entity type
        final Predicate<EnvironmentType> environmentTypePredicate =
                request.hasEnvironmentType()
                        ? EnvironmentTypeUtil.matchingPredicate(request.getEnvironmentType())
                        : e -> true;
        final Predicate<RepoGraphEntity> environmentTypeFilter = entity ->
                environmentTypePredicate.test(entity.getEnvironmentType());
        final Predicate<RepoGraphEntity> entityTypeFilter =
                request.hasEntityType()
                        ? entity -> request.getEntityType() == entity.getEntityType()
                        : entity -> true;

        // if the user is scoped, attach a filter to the matching entities.
        Predicate<RepoGraphEntity> accessFilter = userSessionContext.isUserScoped()
                ? e -> userSessionContext.getUserAccessScope().contains(e.getOid())
                : e -> true;

        LongSet targetEntities = new LongOpenHashSet();
        matchingEntities
            .filter(entityTypeFilter)
            .filter(environmentTypeFilter)
            .filter(accessFilter)
            .forEach(e -> targetEntities.add(e.getOid()));
        final Map<String, Set<String>> resultWithSetsOfValues =
            topology.globalTags().getTagsForEntities(targetEntities);


        Tracing.log(() -> "Got " + resultWithSetsOfValues.size() + " tag results");

        final Tags.Builder tagsBuilder = Tags.newBuilder();
        resultWithSetsOfValues.forEach((key, values) -> {
            TagValuesDTO tagVals = TagValuesDTO.newBuilder()
                .addAllValues(values)
                .build();
            tagsBuilder.putTags(key, tagVals);
        });

        responseObserver.onNext(SearchTagsResponse.newBuilder()
            .setTags(tagsBuilder)
            .build());
        responseObserver.onCompleted();
    }

    /**
     * Throttles search requests to avoid excessive concurrent searches, which can cause
     * CPU exhaustion and prevent any of the searches from completing in good time.
     */
    static class GatedSearchResolver {
        private final LiveTopologyStore liveTopologyStore;
        private final UserSessionContext userSessionContext;

        private final Semaphore concurrentSearchSemaphore;

        private final long concurrentSearchWaitLimitMs;

        GatedSearchResolver(@Nonnull final LiveTopologyStore liveTopologyStore,
                            @Nonnull final UserSessionContext userSessionContext,
                            final int concurrentSearchCount,
                            final long concurrentSearchWaitLimitMs) {
            Preconditions.checkArgument(concurrentSearchCount > 0);
            this.liveTopologyStore = liveTopologyStore;
            this.userSessionContext = userSessionContext;
            this.concurrentSearchSemaphore = new Semaphore(concurrentSearchCount);
            this.concurrentSearchWaitLimitMs = concurrentSearchWaitLimitMs;
        }

        @Nonnull
        Stream<RepoGraphEntity> search(@Nonnull final List<Long> entityOidList,
                @Nonnull final SearchQuery search) throws InterruptedException, TimeoutException {
            final boolean success =
                    concurrentSearchSemaphore.tryAcquire(concurrentSearchWaitLimitMs, TimeUnit.MILLISECONDS);
            if (!success) {
                throw new TimeoutException("Timed out after " + concurrentSearchWaitLimitMs + "ms");
            }
            try {
                return internalSearch(entityOidList, search);
            } finally {
                concurrentSearchSemaphore.release();
            }
        }

        @Nonnull
        Stream<RepoGraphEntity> internalSearch(@Nonnull final List<Long> entityOidList,
                @Nonnull final SearchQuery search) {
            final List<SearchParameters> finalParams;
            // If we got an explicit entity OID list, treat it as a new "starting" filter.
            // If there were no existing search params, we can just create a new parameter.
            // If there WERE existing search params, we need to merge this new filter into each
            // of the input params.
            //
            // TODO (roman, July 3 2019): Remove the separate list of OID inputs. Convert all callers
            // to provide a list of IDs as the starting filter.
            if (!entityOidList.isEmpty()) {
                final PropertyFilter idFilter = SearchProtoUtil.idFilter(entityOidList);
                if (search.getSearchParametersList().isEmpty()) {
                    finalParams = Collections.singletonList(SearchProtoUtil.makeSearchParameters(idFilter).build());
                } else {
                    finalParams = search.getSearchParametersList().stream()
                            .map(oldParam -> {
                                final SearchParameters.Builder bldr = oldParam.toBuilder();
                                // Add the previous starting filter as the first search filter. This preserves
                                // the order of filters relative to each other.
                                bldr.addSearchFilter(SearchProtoUtil.searchFilterProperty(idFilter));
                                return bldr.build();
                            })
                            .collect(Collectors.toList());
                }
            } else {
                // If there are no explicitly provided starting OIDs, use the provided search params
                // normally. This is the main execution path.
                finalParams = search.getSearchParametersList();
            }

            Stream<RepoGraphEntity> results = liveTopologyStore.queryRealtimeTopology(SearchQuery.newBuilder(search)
                    .clearSearchParameters()
                    .addAllSearchParameters(finalParams)
                    .build());
            // if the user is scoped, add a filter to the results.
            if (userSessionContext.isUserScoped()) {
                EntityAccessScope entityAccessScope = userSessionContext.getUserAccessScope();
                return results.filter(e -> entityAccessScope.contains(e.getOid()));
            } else {
                return results;
            }
        }

    }
}
