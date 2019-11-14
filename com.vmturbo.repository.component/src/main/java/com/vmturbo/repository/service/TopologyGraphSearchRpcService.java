package com.vmturbo.repository.service;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.common.protobuf.search.Search.CountEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.EntityCountResponse;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesResponse;
import com.vmturbo.common.protobuf.search.Search.SearchEntityOidsRequest;
import com.vmturbo.common.protobuf.search.Search.SearchEntityOidsResponse;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.SearchTagsRequest;
import com.vmturbo.common.protobuf.search.Search.SearchTagsResponse;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceImplBase;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntityBatch;
import com.vmturbo.components.api.tracing.Tracing;
import com.vmturbo.repository.listener.realtime.LiveTopologyStore;
import com.vmturbo.repository.listener.realtime.RepoGraphEntity;
import com.vmturbo.repository.listener.realtime.SourceRealtimeTopology;
import com.vmturbo.repository.service.LiveTopologyPaginator.PaginatedResults;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.search.SearchResolver;


/**
 * An implementation of {@link SearchServiceImplBase} (see Search.proto) that uses
 * the in-memory topology graph for resolving search queries.
 */
public class TopologyGraphSearchRpcService extends SearchServiceImplBase {

    private final Logger logger = LogManager.getLogger();

    private final LiveTopologyStore liveTopologyStore;

    private final SearchResolver<RepoGraphEntity> searchResolver;

    private final LiveTopologyPaginator liveTopologyPaginator;

    private final PartialEntityConverter partialEntityConverter;

    private final int maxEntitiesPerChunk;

    private final UserSessionContext userSessionContext;

    public TopologyGraphSearchRpcService(@Nonnull final LiveTopologyStore liveTopologyStore,
                         @Nonnull final SearchResolver<RepoGraphEntity> searchResolver,
                         @Nonnull final LiveTopologyPaginator liveTopologyPaginator,
                         @Nonnull final PartialEntityConverter partialEntityConverter,
                         @Nonnull final UserSessionContext userSessionContext,
                         final int maxEntitiesPerChunk) {
        this.liveTopologyStore = Objects.requireNonNull(liveTopologyStore);
        this.searchResolver = Objects.requireNonNull(searchResolver);
        this.liveTopologyPaginator = Objects.requireNonNull(liveTopologyPaginator);
        this.partialEntityConverter = Objects.requireNonNull(partialEntityConverter);
        this.maxEntitiesPerChunk = maxEntitiesPerChunk;
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
        final TopologyGraph<RepoGraphEntity> topologyGraph = topologyGraphOpt.get().entityGraph();
        List<SearchParameters> searchParametersList = request.getSearchParametersList();
        try {
            Tracing.log(() -> logParams("Starting entity count with params - ", searchParametersList));

            final long entityCount =
                searchResolver.search(searchParametersList, topologyGraph).count();

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

        final List<SearchParameters> searchParameters = request.getSearchParametersList();

        try {
            Tracing.log(() -> logParams("Starting entity oid search with params - ", searchParameters));

            final Stream<RepoGraphEntity> entities =
                internalSearch(request.getEntityOidList(), searchParameters);
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
        final List<SearchParameters> searchParameters = request.getSearchParametersList();

        Tracing.log(() -> logParams("Starting entity stream search with params - ", searchParameters));

        final Stream<PartialEntity> entities = internalSearch(request.getEntityOidList(), searchParameters)
            .map(entity -> partialEntityConverter.createPartialEntity(entity, request.getReturnType()));

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
            liveTopologyPaginator.validatePaginationParams(request.getPaginationParams());
        } catch (IllegalArgumentException e) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription(e.getMessage()).asException());
            responseObserver.onCompleted();
        }

        final List<SearchParameters> searchParameters = request.getSearchParametersList();
        try {
            Tracing.log(() -> logParams("Starting entity search with params - ", searchParameters));

            final Stream<RepoGraphEntity> entities = internalSearch(request.getEntityOidList(), searchParameters);
            final PaginatedResults paginatedResults =
                liveTopologyPaginator.paginate(entities, request.getPaginationParams());

            Tracing.log(() -> "Completed search and pagination. Got page with " +
                paginatedResults.nextPageEntities().size() + "entities.");

            final SearchEntitiesResponse.Builder respBuilder = SearchEntitiesResponse.newBuilder()
                .setPaginationResponse(paginatedResults.paginationResponse());
            paginatedResults.nextPageEntities()
                .forEach(entity -> respBuilder.addEntities(
                    partialEntityConverter.createPartialEntity(entity, request.getReturnType())));
            responseObserver.onNext(respBuilder.build());
            responseObserver.onCompleted();
        } catch (Throwable e) {
            logger.error("Search entity failed for request {} with exception", request, e);
            final Status status = Status.ABORTED.withCause(e).withDescription(e.getMessage());
            responseObserver.onError(status.asRuntimeException());
        }
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

        final TopologyGraph<RepoGraphEntity> graph =
            liveTopologyStore.getSourceTopology().get().entityGraph();
        Stream<RepoGraphEntity> matchingEntities;

        Tracing.log(() -> {
            try {
                return "Searching tags. Request: " + JsonFormat.printer().print(request);
            } catch (InvalidProtocolBufferException e) {
                // This shouldn't happen because gRPC shouldn't give us an invalid protobuf!
                return "Searching tags. Error: " + e.getMessage();
            }
        });

        if (request.getEntitiesCount() > 0) {
            // Look for specific entities.
            matchingEntities = request.getEntitiesList().stream()
                .map(graph::getEntity)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .filter(entity -> {
                    if (request.hasEnvironmentType() && request.getEnvironmentType() != entity.getEnvironmentType()) {
                        return false;
                    }
                    if (request.hasEntityType() && request.getEntityType() != entity.getEntityType()) {
                        return false;
                    }
                    return true;
                });
        } else if (request.hasEntityType()) {
            // Look for entities by type
            // This kind of sucks, but we don't expect this tags query to happen often.
            matchingEntities = graph.entitiesOfType(request.getEntityType())
                .filter(entity -> !request.hasEnvironmentType() || request.getEnvironmentType() == entity.getEnvironmentType());
        } else {
            // This REALLY sucks, but we don't expect this global tags query to happen often.
            matchingEntities = graph.entities();
        }

        // if the user is scoped, attach a filter to the matching entities.
        Predicate<RepoGraphEntity> accessFilter = userSessionContext.isUserScoped()
                ? e -> userSessionContext.getUserAccessScope().contains(e.getOid())
                : e -> true;

        final Map<String, Set<String>> resultWithSetsOfValues = new HashMap<>();
        matchingEntities
            .filter(accessFilter)
            .map(RepoGraphEntity::getTags)
            .forEach(tagsMap -> tagsMap.forEach((key, tagValues) -> {
                final Set<String> vals = resultWithSetsOfValues.computeIfAbsent(key, k -> new HashSet<>());
                vals.addAll(tagValues);
            }));

        Tracing.log(() -> "Got " + resultWithSetsOfValues.size() + " tag results");

        final Tags.Builder tagsBuilder = Tags.newBuilder();
        resultWithSetsOfValues.forEach((key, values) -> {
            TagValuesDTO tagVals = TagValuesDTO.newBuilder()
                .addAllValues(values)
                .build();
            tagsBuilder.putTags(key, tagVals);
        });

        try {
            responseObserver.onNext(SearchTagsResponse.newBuilder()
                .setTags(tagsBuilder)
                .build());
            responseObserver.onCompleted();
        } catch (Throwable e) {
            responseObserver.onError(
                Status.ABORTED.withCause(e).withDescription(e.getMessage()).asRuntimeException());
        }
    }

    protected Stream<RepoGraphEntity> internalSearch(@Nonnull final List<Long> entityOidList,
                                                   @Nonnull final List<SearchParameters> params) {
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
            if (params.isEmpty()) {
                finalParams = Collections.singletonList(SearchProtoUtil.makeSearchParameters(idFilter).build());
            } else {
                finalParams = params.stream()
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
            finalParams = params;
        }

        return liveTopologyStore.getSourceTopology()
            .map(realtimeTopology -> {
                final Stream<RepoGraphEntity> results;
                if (finalParams.isEmpty()) {
                    results = realtimeTopology.entityGraph().entities();
                } else {
                    results = searchResolver.search(finalParams, realtimeTopology.entityGraph());
                }

                // if the user is scoped, add a filter to the results.
                if (userSessionContext.isUserScoped()) {
                    EntityAccessScope entityAccessScope = userSessionContext.getUserAccessScope();
                    return results.filter(e -> entityAccessScope.contains(e.getOid()));
                }
                return results;
            })
            .orElse(Stream.empty());
    }
}
