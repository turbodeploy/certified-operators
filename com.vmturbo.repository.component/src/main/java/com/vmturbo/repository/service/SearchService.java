package com.vmturbo.repository.service;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import javaslang.control.Either;
import reactor.core.publisher.Flux;

import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;
import com.vmturbo.common.protobuf.search.Search.Entity;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesResponse;
import com.vmturbo.common.protobuf.search.Search.EntityCountResponse;
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
    public void countEntities(final SearchRequest request, final StreamObserver<EntityCountResponse> responseObserver) {
        logger.debug("Counting entity OIDs with search request: {}", request);

        // Return empty result if current topology doesn't exist.
        Optional<TopologyDatabase> realtimeDb = lifecycleManager.getRealtimeDatabase();
        if (!realtimeDb.isPresent()) {
            logger.warn("No real-time topology exists for searching request");
            responseObserver.onNext(
                    EntityCountResponse.getDefaultInstance().newBuilder()
                            .setEntityCount(0)
                            .build());
            responseObserver.onCompleted();
            return;
        }

        final String db = TopologyDatabases.getDbName(realtimeDb.get());
        try {
            int entityCount = 0;
            List<SearchParameters> searchParametersList = request.getSearchParametersList();
            for (SearchParameters searchParameters : searchParametersList) {
                final List<AQLRepr> aqlReprs = SearchDTOConverter.toAqlRepr(searchParameters);
                final Either<Throwable, Collection<String>> result =
                        searchHandler.searchEntityOids(aqlReprs, db, Optional.empty(), Collections.emptyList());

                if (result.isLeft()) {
                    Throwable error = result.getLeft();
                    logger.error("The search failed", error);
                    final Status aborted = Status.ABORTED.withCause(error)
                            .withDescription(error.getMessage());
                    responseObserver.onError(aborted.asRuntimeException());
                    return;
                }

                // count up the # of entities in the response.
                entityCount += result.get().size();
            }
            logger.info("countEntities found {} entities.", entityCount);
            responseObserver.onNext(EntityCountResponse.newBuilder()
                    .setEntityCount(entityCount)
                    .build());
            responseObserver.onCompleted();
        } catch (Throwable e) {
            logger.error("countEntities failed for request {} with exception", request, e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void searchEntityOids(SearchRequest request,
                                 StreamObserver<SearchResponse> responseObserver) {
        logger.debug("Searching for entity OIDs with request: {}", request);

        // Return empty result if current topology doesn't exist.
        Optional<TopologyDatabase> realtimeDb = lifecycleManager.getRealtimeDatabase();
        if (!realtimeDb.isPresent()) {
            logger.warn("No real-time topology exists for searching request");
            responseObserver.onNext(
                    SearchResponse.newBuilder().addAllEntities(Collections.emptyList()).build());
            responseObserver.onCompleted();
            return;
        }
        final Optional<StatusException> statusExceptionOptional =
                isValidPaginationParameter(request);
        if (statusExceptionOptional.isPresent()) {
            responseObserver.onError(statusExceptionOptional.get());
            return;
        }

        final Optional<PaginationParameters> paginationParams =
                request.hasPaginationParams()
                        ? Optional.of(request.getPaginationParams())
                        : Optional.empty();
        final Function<Collection<String>, List<Long>> convertToLong =
                entityOid -> entityOid.stream()
                        .map(Long::parseLong)
                        .collect(Collectors.toList());
        final SearchEntityPagination<String> searchFunction = searchHandler::searchEntityOids;
        final List<SearchParameters> searchParameters = request.getSearchParametersList();
        try {
            // if there is only one search parameter, it can apply pagination directly.
            final List<Long> entities = searchParameters.size() == 1
                    ? searchWithOnlyOneParameter(request, searchParameters.get(0), paginationParams,
                        searchFunction, convertToLong)
                    : searchEntityOidMultiParameters(request, searchParameters, paginationParams);
            final SearchResponse.Builder responseBuilder = SearchResponse.newBuilder()
                    .addAllEntities(entities);
            if (paginationParams.isPresent()) {
                responseBuilder.setPaginationResponse(PaginationResponse.newBuilder());
                final long skipCount = paginationParams.get().hasCursor()
                        ? Long.parseLong(paginationParams.get().getCursor())
                        : 0;
                if (entities.size() == paginationParams.get().getLimit()) {
                    final long nextCursor = skipCount + paginationParams.get().getLimit();
                    responseBuilder.getPaginationResponseBuilder()
                            .setNextCursor(String.valueOf(nextCursor));
                }
            }
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (Throwable e) {
            logger.error("Search entity OIDs failed for request {} with exception", request, e);
            final Status status = Status.INVALID_ARGUMENT.withCause(e).withDescription(e.getMessage());
            responseObserver.onError(status.asRuntimeException());
        }
    }

    @Override
    public void searchEntities(SearchRequest request,
                               StreamObserver<SearchEntitiesResponse> responseObserver) {
        logger.debug("Searching for entities with request: {}", request);

        // Return empty result if current topology doesn't exist.
        if (!lifecycleManager.getRealtimeDatabase().isPresent()) {
            logger.warn("No real-time topology exists for searching request");
            responseObserver.onCompleted();
            return;
        }
        final Optional<StatusException> statusExceptionOptional =
                isValidPaginationParameter(request);
        if (statusExceptionOptional.isPresent()) {
            responseObserver.onError(statusExceptionOptional.get());
            return;
        }

        final Optional<PaginationParameters> paginationParams =
                    request.hasPaginationParams()
                            ? Optional.of(request.getPaginationParams())
                            : Optional.empty();
        final Function<Collection<ServiceEntityRepoDTO>, List<Entity>> convertToEntity =
                serviceEntityRepoDTOS -> serviceEntityRepoDTOS.stream()
                        .map(SearchDTOConverter::toSearchEntity)
                        .collect(Collectors.toList());
        final SearchEntityPagination<ServiceEntityRepoDTO> searchFunction = searchHandler::searchEntities;
        final List<SearchParameters> searchParameters = request.getSearchParametersList();
        try {
            // if there is only one search parameter, it can apply pagination directly.
            final List<Entity> entities = (searchParameters.size() == 1)
                    ? searchWithOnlyOneParameter(request, searchParameters.get(0), paginationParams,
                        searchFunction, convertToEntity)
                    : searchEntitiesMultiParameters(request, searchParameters, paginationParams);
            final Comparator<Entity> comparator = getComparator(paginationParams);
            final List<Entity> sortedEntities = entities.stream()
                    .sorted(paginationParams.isPresent() && paginationParams.get().getAscending()
                            ? comparator
                            : comparator.reversed())
                    .collect(Collectors.toList());
            final SearchEntitiesResponse.Builder responseBuilder = SearchEntitiesResponse.newBuilder()
                    .addAllEntities(sortedEntities);
            if (paginationParams.isPresent()) {
                responseBuilder.setPaginationResponse(PaginationResponse.newBuilder());
                final long skipCount = paginationParams.get().hasCursor()
                        ? Long.parseLong(paginationParams.get().getCursor())
                        : 0;
                if (entities.size() == paginationParams.get().getLimit()) {
                    final long nextCursor = skipCount + paginationParams.get().getLimit();
                    responseBuilder.getPaginationResponseBuilder()
                            .setNextCursor(String.valueOf(nextCursor));
                }
            }
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (Throwable e) {
            logger.error("Search entity failed for request {} with exception", request, e);
            final Status status = Status.ABORTED.withCause(e).withDescription(e.getMessage());
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

    /**
     * Check if request pagination parameters is valid or not.
     *
     * @param request a {@link SearchRequest}.
     * @return if request pagination parameter is invalid, return a {@link StatusException}, otherwise
     *          return empty optional.
     */
    private Optional<StatusException> isValidPaginationParameter(SearchRequest request) {
        if (request.hasPaginationParams() && request.getPaginationParams().getLimit() <= 0) {
            return Optional.of(Status.INVALID_ARGUMENT
                    .withDescription("Pagination limit must be a positive integer.")
                    .asException());

        }
        if (request.hasPaginationParams() && request.getPaginationParams().hasCursor()) {
            try {
                final long cursor = Long.parseLong(request.getPaginationParams().getCursor());
                if (cursor < 0) {
                    return Optional.of(Status.INVALID_ARGUMENT
                            .withDescription("Pagination cursor must be a non-negative integer.")
                            .asException());
                }
            } catch (NumberFormatException e) {
                return Optional.of(Status.INVALID_ARGUMENT
                        .withDescription("Pagination cursor must be a number: " +
                                request.getPaginationParams().getCursor())
                        .asException());
            }
        }
        return Optional.empty();
    }

    /**
     * Define a functional interface which wraps up the search method which take same parameters and
     * return different type.
     */
    @FunctionalInterface
    private interface SearchEntityPagination<RET> {
        Either<Throwable, Collection<RET>> apply(
                @Nonnull final List<AQLRepr> aqlReprs,
                @Nonnull final String database,
                @Nonnull final Optional<PaginationParameters> paginationParams,
                @Nonnull final List<String> oids);
    }

    /**
     * For search query with only one {@link SearchParameters}, it can apply the pagination directly
     * into converted AQLs. This helper function combine the common parts of search entities and
     * search entity oid method together.
     *
     * @param request a {@link SearchRequest}.
     * @param searchParameter a {@link SearchParameters}.
     * @param paginationParams a {@link PaginationParameters}.
     * @param searchEntityPagination a function interface which include search entity with search oids
     *                               methods.
     * @param convert a convert function which convert collection of {@link TYPE} to a list of {@link RET}.
     * @param <RET> the final result type.
     * @param <TYPE> the intermediate result type.
     * @return a list of {@link RET}.
     * @throws Throwable if query failed.
     */
    private <RET, TYPE> List<RET> searchWithOnlyOneParameter(
            @Nonnull final SearchRequest request,
            @Nonnull final SearchParameters searchParameter,
            @Nonnull final Optional<PaginationParameters> paginationParams,
            @Nonnull final SearchEntityPagination searchEntityPagination,
            @Nonnull final Function<Collection<TYPE>, List<RET>> convert) throws Throwable {
        // if search query only has one search parameter, then it can apply pagination directly.
        final List<String> candidateEntityOids = request.getEntityOidList().stream()
                .map(String::valueOf)
                .collect(Collectors.toList());
        final String db = TopologyDatabases.getDbName(
                lifecycleManager.getRealtimeDatabase().get());
        final List<AQLRepr> aqlReprs = SearchDTOConverter.toAqlRepr(searchParameter);
        final Either<Throwable, Collection<TYPE>> result =
                searchEntityPagination.apply(aqlReprs, db, paginationParams, candidateEntityOids);
        if (result.isLeft()) {
            throw result.getLeft();
        }
        final List<RET> searchResult = convert.apply(result.get());
        return searchResult;
    }

    /**
     * For a search query with multiple {@link SearchParameters}, it needs to get all candidate
     * entity oids first, then perform pagination based on those entity oids. Finally, it will query
     * database to only get paginated X entity full information. Because, for each {@link SearchParameters}
     * it will been converted to AQLs and query database, but pagination parameters can not be applied
     * to each {@link SearchParameters}.
     * <p>
     * For example: there is a query with two {@link SearchParameters}: 1: get all VMs which name
     * is "foo", 2: get all VMs which host name is "bar". If we apply pagination (top 5, sorted by entity
     * name) into both search parameters, its results are not always correct.
     *
     * @param request a {@link SearchRequest}.
     * @param searchParametersList a list of {@link SearchParameters}.
     * @param paginationParams a {@link PaginationParameters}.
     * @return a list of {@link Entity}.
     * @throws Throwable if query failed.
     */
    private List<Entity> searchEntitiesMultiParameters(
            @Nonnull final SearchRequest request,
            @Nonnull final List<SearchParameters> searchParametersList,
            @Nonnull final Optional<PaginationParameters> paginationParams) throws Throwable {
        final List<Long> entityCandidateOids =
                searchEntityOidMultiParametersWithoutPagination(request, searchParametersList, paginationParams);
        final long skipCount = paginationParams.isPresent() && paginationParams.get().hasCursor()
                ? Long.parseLong(paginationParams.get().getCursor())
                : 0;
        final List<Long> entityOids = paginationParams.isPresent()
                ? entityCandidateOids.stream()
                    .skip(skipCount)
                    .limit(paginationParams.get().getLimit())
                    .collect(Collectors.toList())
                : entityCandidateOids;
        final Either<Throwable, Collection<ServiceEntityRepoDTO>> entities =
                searchHandler.getEntitiesByOids(Sets.newHashSet(entityOids), lifecycleManager.getRealtimeTopologyId());
        if (entities.isLeft()) {
            throw entities.getLeft();
        }
        return entities.get().stream()
                .map(SearchDTOConverter::toSearchEntity)
                .collect(Collectors.toList());
    }

    /**
     * Get comparator for {@link Entity}.
     *
     * @param paginationParameters {@link PaginationParameters}.
     * @return a {@link Comparator}.
     */
    private Comparator<Entity> getComparator(
            @Nullable final Optional<PaginationParameters> paginationParameters) {
        if (!paginationParameters.isPresent()) {
            // if it is not pagination call, use the oid order as default comparator.
            return Comparator.comparing(entity -> entity.getOid());
        }
        // Because Arangodb pagination ignore upper/lower case, in order to be consistent,
        // it also need to compare display name with ignore upper/lower case.
        return Comparator.comparing(entity -> entity.getDisplayName().toLowerCase());
    }

    /**
     * For search entity oids with multiple search parameters. It needs get all candidate
     * entity oids first, then perform pagination based on those entity oids.
     * <p>
     * For example: there is a query with two {@link SearchParameters}: 1: get all VMs which name
     * is "foo", 2: get all VMs which host name is "bar". If we apply pagination (top 5, sorted by entity
     * name) into both search parameters, its results are not always correct.
     * @param request a {@link SearchRequest}.
     * @param searchParametersList a list of {@link SearchParameters}.
     * @param paginationParams a {@link PaginationParameters}.
     * @return a list of entity oids.
     * @throws Throwable if query failed.
     */
    private List<Long> searchEntityOidMultiParameters(
            @Nonnull final SearchRequest request,
            @Nonnull final List<SearchParameters> searchParametersList,
            @Nonnull final Optional<PaginationParameters> paginationParams) throws Throwable {
        final List<Long> entityCandidateOids =
                searchEntityOidMultiParametersWithoutPagination(request, searchParametersList, paginationParams);
        final long skipCount = paginationParams.isPresent() && paginationParams.get().hasCursor()
                ? Long.parseLong(paginationParams.get().getCursor())
                : 0;
        final List<Long> entityOids = paginationParams.isPresent()
                ? entityCandidateOids.stream()
                .skip(skipCount)
                .limit(paginationParams.get().getLimit())
                .collect(Collectors.toList())
                : entityCandidateOids;
        return entityOids;
    }

    /**
     * Search entity oids without pagination. It will return all entity oids which matched with
     * search criteria.
     *
     * @param request a {@link SearchRequest}.
     * @param searchParametersList a list of {@link SearchParameters}.
     * @param paginationParameters {@link PaginationParameters} contains parameters for pagination.
     * @return a list of entity oids.
     * @throws Throwable if query failed.
     */
    private List<Long> searchEntityOidMultiParametersWithoutPagination(
            @Nonnull final SearchRequest request,
            @Nonnull final List<SearchParameters> searchParametersList,
            @Nonnull final Optional<PaginationParameters> paginationParameters) throws Throwable {
        final List<String> candidateEntityOids = request.getEntityOidList().stream()
                .map(String::valueOf)
                .collect(Collectors.toList());
        final String db = TopologyDatabases.getDbName(
                lifecycleManager.getRealtimeDatabase().get());
        Optional<List<Long>> entitiesList = Optional.empty();
        // only remove limit from pagination parameter in order to get all matched entity oids.
        Optional<PaginationParameters> paginationParamOnlySort = paginationParameters.isPresent()
                ? Optional.of(PaginationParameters.newBuilder(paginationParameters.get())
                    .clearLimit()
                    .build())
                : Optional.empty();
        for (SearchParameters searchParameters : searchParametersList) {
            final List<AQLRepr> aqlReprs = SearchDTOConverter.toAqlRepr(searchParameters);
            final Either<Throwable, Collection<String>> result =
                    searchHandler.searchEntityOids(aqlReprs, db, paginationParamOnlySort, candidateEntityOids);

            if (result.isLeft()) {
                throw result.getLeft();
            }
            final List<Long> searchResult = result.get().stream()
                    .map(Long::parseLong)
                    .collect(Collectors.toList());
            // when optional is empty then we save the first search results to entitiesSet,
            // because we need save first search results in order to perform retainAll,
            // otherwise intersection result will be empty.
            entitiesList = entitiesList.isPresent() ? entitiesList.map(entities -> {
                entities.retainAll(searchResult);
                return entities;
            }) : Optional.of(searchResult);
        }
        return entitiesList.orElse(Collections.emptyList());
    }
}
