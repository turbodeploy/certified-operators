package com.vmturbo.repository.service;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import javaslang.control.Either;
import reactor.core.publisher.Flux;

import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.Entity;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.SearchRequest;
import com.vmturbo.common.protobuf.search.Search.SearchResponse;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceImplBase;
import com.vmturbo.repository.dto.ServiceEntityRepoDTO;
import com.vmturbo.repository.graph.result.ScopedEntity;
import com.vmturbo.repository.search.AQLRepr;
import com.vmturbo.repository.search.SearchDTOConverter;
import com.vmturbo.repository.search.SearchHandler;
import com.vmturbo.repository.topology.TopologyDatabase;
import com.vmturbo.repository.topology.TopologyDatabases;
import com.vmturbo.repository.topology.TopologyLifecycleManager;

public class SearchService extends SearchServiceImplBase {

    private final Logger logger = LoggerFactory.getLogger(SearchService.class);
    private final SupplyChainService supplyChainService;
    private final TopologyLifecycleManager lifecycleManager;
    private final SearchHandler searchHandler;


    public SearchService(final SupplyChainService supplyChainService,
                         final TopologyLifecycleManager lifecycleManager,
                         final SearchHandler searchHandler
                         ) {
        this.supplyChainService = checkNotNull(supplyChainService);
        this.lifecycleManager = checkNotNull(lifecycleManager);
        this.searchHandler = checkNotNull(searchHandler);
    }

    @Override
    public void searchEntityOids(SearchRequest request,
                                 StreamObserver<SearchResponse> responseObserver) {
        logger.info("Searching for entity OIDs with request: {}", request);

        // Return empty result if current topology doesn't exist.
        Optional<TopologyDatabase> realtimeDb = lifecycleManager.getRealtimeDatabase();
        if (!realtimeDb.isPresent()) {
            logger.warn("No real-time topology exists for searching request");
            responseObserver.onNext(
                    SearchResponse.newBuilder().addAllEntities(Collections.emptyList()).build());
            responseObserver.onCompleted();
            return;
        }

        final String db = TopologyDatabases.getDbName(realtimeDb.get());
        try {
            Optional<Set<Long>> entitiesSet = Optional.empty();
            List<SearchParameters> searchParametersList = request.getSearchParametersList();
            for (SearchParameters searchParameters : searchParametersList) {
                final List<AQLRepr> aqlReprs = SearchDTOConverter.toAqlRepr(searchParameters);
                final Either<Throwable, Collection<String>> result =
                        searchHandler.searchEntityOids(aqlReprs, db);

                if (result.isLeft()) {
                    searchFailResponse(result.getLeft(), responseObserver);
                    return;
                }
                final Set<Long> searchResult = result.get().stream().map(Long::parseLong)
                        .collect(Collectors.toSet());
                // when optional is empty then we save the first search results to entitiesSet,
                // because we need save first search results in order to perform retainAll,
                // otherwise intersection result will be empty.
                entitiesSet = entitiesSet.isPresent() ? entitiesSet.map(entities -> {
                    entities.retainAll(searchResult);
                    return entities;
                }) : Optional.of(searchResult);
            }
            final Set<Long> entities =  entitiesSet.orElse(Collections.emptySet());
            responseObserver.onNext(SearchResponse.newBuilder().addAllEntities(entities).build());
            responseObserver.onCompleted();
        } catch (Throwable e) {
            logger.error("Search entity OIDs failed with an exception", e);
            final Status status = Status.INVALID_ARGUMENT.withCause(e).withDescription(e.getMessage());
            responseObserver.onError(status.asRuntimeException());
        }
    }

    @Override
    public void searchEntities(SearchRequest request,
                               StreamObserver<Search.Entity> responseObserver) {
        logger.debug("Searching for entities with request: {}", request);

        // Return empty result if current topology doesn't exist.
        if (!lifecycleManager.getRealtimeDatabase().isPresent()) {
            logger.warn("No real-time topology exists for searching request");
            responseObserver.onCompleted();
            return;
        }

        final String db = TopologyDatabases.getDbName(
                lifecycleManager.getRealtimeDatabase().get());
        try {
            Optional<Set<Entity>> entitiesSet = Optional.empty();
            List<SearchParameters> searchParametersList = request.getSearchParametersList();
            for (SearchParameters searchParameters : searchParametersList) {
                final List<AQLRepr> aqlReprs = SearchDTOConverter.toAqlRepr(searchParameters);
                final Either<Throwable, Collection<ServiceEntityRepoDTO>> result =
                        searchHandler.searchEntities(aqlReprs, db);

                if (result.isLeft()) {
                    searchFailResponse(result.getLeft(), responseObserver);
                    return;
                }
                final Set<Entity> searchResult = result.get().stream()
                        .map(SearchDTOConverter::toSearchEntity).collect(Collectors.toSet());
                // when optional is empty then we save the first search results to entitiesSet,
                // because we need save first search results in order to perform retainAll,
                // otherwise intersection result will be empty.
                entitiesSet = entitiesSet.isPresent() ? entitiesSet.map(entities -> {
                    entities.retainAll(searchResult);
                    return entities;
                }) : Optional.of(searchResult);
            }
            final Set<Entity> entities = entitiesSet.orElse(Collections.emptySet());
            entities.forEach(responseObserver::onNext);
            responseObserver.onCompleted();
        } catch (Throwable e) {
            logger.error("Search entity failed with an exception", e);
            final Status status = Status.INVALID_ARGUMENT.withCause(e).withDescription(e.getMessage());
            responseObserver.onError(status.asRuntimeException());
        }
    }

    /**
     * Request a collection of {@link BaseApiDTO}s from the repository. Currently, only
     * support searching with entity types and scope.
     *
     * @param query Not yet used
     * @param types The types of entities, e.g., VirtualMachine, PhysicalMachine, ...
     * @param scope The scope used for searching, e.g., a single entity or the global environment
     * @param state Not yet used
     * @param groupType Not yet used
     * @param related Not yet used
     * @return
     * @throws Exception
     */
    public Collection<BaseApiDTO> getSearchResults(String query,
                                                   List<String> types,
                                                   String scope,
                                                   String state,
                                                   Boolean related,
                                                   String groupType) throws Exception {
        return getSearchResults(types, scope);
    }

    private void searchFailResponse(Throwable error, StreamObserver<?> responseObserver) {
        logger.error("The search failed", error);
        final Status aborted = Status.ABORTED.withCause(error)
                .withDescription(error.getMessage());
        responseObserver.onError(aborted.asRuntimeException());
    }

    private Collection<BaseApiDTO> getSearchResults(List<String> types,
                                                    String scope) throws Exception {
        if (!lifecycleManager.getRealtimeTopologyId().isPresent()) {
            return new HashSet<BaseApiDTO>();
        }

        final long contextId = lifecycleManager.getRealtimeTopologyId().get().getContextId();
        Flux<ScopedEntity> scopedEntities = supplyChainService.scopedEntities(contextId, scope, types);
        return scopedEntities.toStream().map(SearchService::convert).collect(Collectors.toList());
    }

    private static ServiceEntityApiDTO convert(ScopedEntity se) {
        ServiceEntityApiDTO dto = new ServiceEntityApiDTO();

        dto.setUuid(Long.toString(se.getOid()));
        dto.setDisplayName(se.getDisplayName());
        dto.setClassName(se.getEntityType());
        dto.setState(se.getState());
        // XL-only - assume ON_PREM for now
        dto.setEnvironmentType(EnvironmentType.ONPREM);

        return dto;
    }
}
