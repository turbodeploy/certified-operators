package com.vmturbo.repository.service;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Sets;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.search.Search.CountEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.EntityCountResponse;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesResponse;
import com.vmturbo.common.protobuf.search.Search.SearchEntityOidsRequest;
import com.vmturbo.common.protobuf.search.Search.SearchEntityOidsResponse;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.SearchTagsRequest;
import com.vmturbo.common.protobuf.search.Search.SearchTagsResponse;
import com.vmturbo.common.protobuf.search.Search.SearchTopologyEntityDTOsRequest;
import com.vmturbo.common.protobuf.search.Search.SearchTopologyEntityDTOsResponse;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceImplBase;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.repository.listener.realtime.RepoGraphEntity;
import com.vmturbo.repository.listener.realtime.LiveTopologyStore;
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

    public TopologyGraphSearchRpcService(@Nonnull final LiveTopologyStore liveTopologyStore,
                                         @Nonnull final SearchResolver<RepoGraphEntity> searchResolver,
                                         @Nonnull final LiveTopologyPaginator liveTopologyPaginator) {
        this.liveTopologyStore = Objects.requireNonNull(liveTopologyStore);
        this.searchResolver = Objects.requireNonNull(searchResolver);
        this.liveTopologyPaginator = Objects.requireNonNull(liveTopologyPaginator);
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
            final long entityCount =
                searchResolver.search(searchParametersList, topologyGraph).count();
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
            final Stream<RepoGraphEntity> entities =
                internalSearch(request.getEntityOidList(), searchParameters);
            final SearchEntityOidsResponse.Builder responseBuilder = SearchEntityOidsResponse.newBuilder();
            entities.map(RepoGraphEntity::getOid).forEach(responseBuilder::addEntities);

            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (Throwable e) {
            logger.error("Search entity OIDs failed for request {} with exception", request, e);
            final Status status = Status.INVALID_ARGUMENT.withCause(e).withDescription(e.getMessage());
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
            liveTopologyPaginator.validatePaginationParams(request.getPaginationParams());
        } catch (IllegalArgumentException e) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription(e.getMessage()).asException());
            responseObserver.onCompleted();
        }

        final List<SearchParameters> searchParameters = request.getSearchParametersList();
        try {
            final Stream<RepoGraphEntity> entities = internalSearch(request.getEntityOidList(), searchParameters);
            final PaginatedResults paginatedResults =
                liveTopologyPaginator.paginate(entities, request.getPaginationParams());

            final SearchEntitiesResponse.Builder respBuilder = SearchEntitiesResponse.newBuilder()
                .setPaginationResponse(paginatedResults.paginationResponse());
            paginatedResults.nextPageEntities()
                .forEach(entity -> respBuilder.addEntities(entity.getThinEntity()));
            responseObserver.onNext(respBuilder.build());
            responseObserver.onCompleted();
        } catch (Throwable e) {
            logger.error("Search entity failed for request {} with exception", request, e);
            final Status status = Status.ABORTED.withCause(e).withDescription(e.getMessage());
            responseObserver.onError(status.asRuntimeException());
        }
    }

    @Override
    public void searchTopologyEntityDTOs(SearchTopologyEntityDTOsRequest request,
                                         StreamObserver<SearchTopologyEntityDTOsResponse> responseObserver) {
        logger.debug("Searching for TopologyEntityDTOs with request: {}", request);

        // Return empty result if current topology doesn't exist.
        if (!liveTopologyStore.getSourceTopology().isPresent()) {
            logger.warn("No real-time topology exists for searching request");
            responseObserver.onNext(SearchTopologyEntityDTOsResponse.getDefaultInstance());
            responseObserver.onCompleted();
            return;
        }

        final List<SearchParameters> searchParameters = request.getSearchParametersList();
        try {
            final SearchTopologyEntityDTOsResponse.Builder responseBuilder =
                SearchTopologyEntityDTOsResponse.newBuilder();

            internalSearch(request.getEntityOidList(), searchParameters)
                .forEach(entity -> responseBuilder.addTopologyEntityDtos(entity.getTopologyEntity()));

            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (Throwable e) {
            logger.error("Search TopologyEntityDTOs failed for request {} with exception", request, e);
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
        if (request.getEntitiesCount() > 0) {
            // Look for specific entities.
            matchingEntities = request.getEntitiesList().stream()
                .map(oid -> graph.getEntity(oid))
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


        final Map<String, Set<String>> resultWithSetsOfValues = new HashMap<>();
        matchingEntities
            .map(entity -> entity.getTags())
            .forEach(tagsMap -> tagsMap.forEach((key, tagValues) -> {
                final Set<String> vals = resultWithSetsOfValues.computeIfAbsent(key, k -> new HashSet<>());
                vals.addAll(tagValues);
            }));

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

    private Stream<RepoGraphEntity> internalSearch(@Nonnull final List<Long> entityOidList,
                                                   @Nonnull final List<SearchParameters> params) {
        return liveTopologyStore.getSourceTopology()
            .map(realtimeTopology -> {
                final Set<Long> entityOidSet = Sets.newHashSet(entityOidList);
                final Stream<RepoGraphEntity> results;
                if (params.isEmpty()) {
                    if (entityOidList.isEmpty()) {
                        results = realtimeTopology.entityGraph().entities();
                    } else {
                        results = realtimeTopology.entityGraph()
                            .getEntities(entityOidSet);
                    }
                } else {
                    results = searchResolver.search(params, realtimeTopology.entityGraph())
                        .filter(entity -> entityOidSet.isEmpty() || entityOidSet.contains(entity.getOid()));
                }

                return results;
            })
            .orElse(Stream.empty());
    }
}
