package com.vmturbo.api.component.communication;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.grpc.stub.StreamObserver;

import org.springframework.http.ResponseEntity;
import org.springframework.util.CollectionUtils;

import com.vmturbo.api.component.external.api.mapper.EntityDetailsMapper;
import com.vmturbo.api.component.external.api.mapper.PaginationMapper;
import com.vmturbo.api.component.external.api.mapper.PriceIndexPopulator;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.component.external.api.mapper.SeverityPopulator;
import com.vmturbo.api.component.external.api.mapper.aspect.EntityAspectMapper;
import com.vmturbo.api.component.external.api.util.businessaccount.BusinessAccountMapper;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.businessunit.BusinessUnitApiDTO;
import com.vmturbo.api.dto.entity.EntityDetailsApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.api.pagination.EntityOrderBy;
import com.vmturbo.api.pagination.EntityPaginationRequest;
import com.vmturbo.api.pagination.EntityPaginationRequest.EntityPaginationResponse;
import com.vmturbo.api.pagination.PaginationRequest;
import com.vmturbo.api.pagination.PaginationResponse;
import com.vmturbo.api.pagination.PaginationUtil;
import com.vmturbo.api.pagination.SearchOrderBy;
import com.vmturbo.api.pagination.SearchPaginationRequest;
import com.vmturbo.api.pagination.SearchPaginationRequest.SearchPaginationResponse;
import com.vmturbo.common.protobuf.PaginationProtoUtil;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.common.Pagination;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest.Builder;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceStub;
import com.vmturbo.common.protobuf.search.Search.CountEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.GraphRequest;
import com.vmturbo.common.protobuf.search.Search.GraphResponse;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesResponse;
import com.vmturbo.common.protobuf.search.Search.SearchEntityOidsRequest;
import com.vmturbo.common.protobuf.search.Search.SearchEntityOidsResponse;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.SearchQuery;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceStub;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.EntityWithConnections;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntityBatch;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * This is the preferred way to access the repository from the API.
 * Using the {@link RepositoryApi} involves three steps:
 *    1) Get a "request" object using one of the {@link RepositoryApi} methods.
 *    2) Customize the request object (if necessary).
 *    3) Retrieve the results at the appropriate detail level from the request object.
 *
 * If you know the IDs of the entities you're looking for, use:
 *    {@link RepositoryApi#entitiesRequest(Set)} or {@link RepositoryApi#entityRequest(long)}
 *
 * If not, use {@link RepositoryApi#newSearchRequest(SearchParameters)}.
 */
public class RepositoryApi {

    private final SeverityPopulator severityPopulator;

    private final long realtimeTopologyContextId;

    private final RepositoryServiceBlockingStub repositoryService;
    private final RepositoryServiceStub repositoryServiceAsyncStub;

    private final SearchServiceBlockingStub searchServiceBlockingStub;
    private final SearchServiceStub searchServiceAsyncStub;

    private final ServiceEntityMapper serviceEntityMapper;
    private final BusinessAccountMapper businessAccountMapper;
    private final PaginationMapper paginationMapper;
    private final EntityDetailsMapper entityDetailsMapper;
    private final PriceIndexPopulator priceIndexPopulator;

    public RepositoryApi(@Nonnull final SeverityPopulator severityPopulator,
                         @Nonnull final RepositoryServiceBlockingStub repositoryService,
                         @Nonnull final RepositoryServiceStub repositoryServiceAsyncStub,
                         @Nonnull final SearchServiceBlockingStub searchServiceBlockingStub,
                         @Nonnull final SearchServiceStub searchServiceAsyncStub,
                         @Nonnull final ServiceEntityMapper serviceEntityMapper,
                         @Nonnull final BusinessAccountMapper businessAccountMapper,
                         @Nonnull final PaginationMapper paginationMapper,
                         @Nonnull final EntityDetailsMapper entityDetailsMapper,
                         @Nonnull final PriceIndexPopulator priceIndexPopulator,
                         final long realtimeTopologyContextId) {
        this.severityPopulator = Objects.requireNonNull(severityPopulator);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.searchServiceBlockingStub = Objects.requireNonNull(searchServiceBlockingStub);
        this.searchServiceAsyncStub = Objects.requireNonNull(searchServiceAsyncStub);
        this.serviceEntityMapper = Objects.requireNonNull(serviceEntityMapper);
        this.repositoryService = Objects.requireNonNull(repositoryService);
        this.businessAccountMapper = Objects.requireNonNull(businessAccountMapper);
        this.repositoryServiceAsyncStub = Objects.requireNonNull(repositoryServiceAsyncStub);
        this.entityDetailsMapper = Objects.requireNonNull(entityDetailsMapper);
        this.priceIndexPopulator = Objects.requireNonNull(priceIndexPopulator);
        this.paginationMapper = paginationMapper;
    }

    /**
     * Create a new search request.
     *
     * @param params The {@link SearchParameters} to use for the request. Use the utility methods
     *               in {@link com.vmturbo.common.protobuf.search.SearchProtoUtil} where possible.
     * @return The {@link SearchRequest}, which can be further customized.
     */
    @Nonnull
    public SearchRequest newSearchRequest(@Nonnull final SearchParameters params) {
        return newSearchRequestMulti(Collections.singleton(params));
    }

    /**
     * Do a search of the topology graph.
     * @param lookupRequest request
     * @return response object
     */
    @Nonnull
    public GraphResponse graphSearch(GraphRequest lookupRequest) {
        return this.searchServiceBlockingStub.graphSearch(lookupRequest);
    }
    /**
     * Create a new search request.
     *
     * @param params The {@link SearchParameters} to use for the request. Use the utility methods
     *               in {@link com.vmturbo.common.protobuf.search.SearchProtoUtil} where possible.
     * @return The {@link SearchRequest}, which can be further customized.
     */
    @Nonnull
    public SearchRequest newSearchRequestMulti(@Nonnull Collection<SearchParameters> params) {
        return new SearchRequest(realtimeTopologyContextId, searchServiceBlockingStub,
                searchServiceAsyncStub, severityPopulator, serviceEntityMapper,
                entityDetailsMapper, priceIndexPopulator, params);
    }

    /**
     * Create a new paginated search request.
     *
     * @param searchQuery The search query.
     * @param scopeOids An extra OID filter to use as a starting filter - used when the query
     *                  applies within a particular scope.
     *                  TODO (roman, Sept 28 2020): Factor this into query.
     * @param paginationRequest The {@link SearchPaginationRequest}.
     * @return The {@link PaginatedSearchRequest}.
     */
    @Nonnull
    public PaginatedSearchRequest newPaginatedSearch(@Nonnull final SearchQuery searchQuery,
            @Nonnull final Set<Long> scopeOids,
            @Nonnull final SearchPaginationRequest paginationRequest) {
        return new PaginatedSearchRequest(realtimeTopologyContextId, searchServiceBlockingStub, searchQuery, scopeOids,
                serviceEntityMapper, paginationRequest, severityPopulator, paginationMapper, businessAccountMapper);
    }

    /**
     * Create a new paginated entities request.
     *
     * @param searchQuery The search query.
     * @param scopeOids An extra OID filter to use as a starting filter - used when the query
     *                  applies within a particular scope. If the set is empty, all entities matching
     *                  the search are returned.
     * @param paginationRequest The {@link SearchPaginationRequest}.
     * @return The {@link PaginatedSearchRequest}.
     */
    @Nonnull
    public PaginatedEntitiesRequest newPaginatedEntities(@Nonnull final SearchQuery searchQuery,
            @Nonnull final Set<Long> scopeOids,
            @Nonnull final EntityPaginationRequest paginationRequest) {
        return new PaginatedEntitiesRequest(realtimeTopologyContextId, searchServiceBlockingStub,
                searchQuery, scopeOids, serviceEntityMapper, paginationRequest, severityPopulator, paginationMapper,
                businessAccountMapper);
    }

    /**
     * Create a new entity request for a single entity.
     *
     * @param oid The OID of the target entity.
     * @return The {@link SingleEntityRequest}, which can be further customized.
     */
    @Nonnull
    public SingleEntityRequest entityRequest(final long oid) {
        return new SingleEntityRequest(realtimeTopologyContextId, repositoryService,
                repositoryServiceAsyncStub, severityPopulator, serviceEntityMapper,
                entityDetailsMapper, oid);
    }

    /**
     * Create a new entity request for a collection of entities.
     *
     * @param oids The OIDs of the target entities. Note - if the set is empty, there will be no
     *             results unless you also call {@link MultiEntityRequest#allowGetAll()}.
     * @return The {@link MultiEntityRequest}, which can be further customized.
     */
    @Nonnull
    public MultiEntityRequest entitiesRequest(@Nonnull final Set<Long> oids) {
        return new MultiEntityRequest(realtimeTopologyContextId, repositoryService,
                repositoryServiceAsyncStub, severityPopulator, serviceEntityMapper,
                entityDetailsMapper, oids);
    }

    /**
     * Create a new request to fetch the region in which some entities reside.
     *
     * @param oids the collection of entities
     * @return a search request for the region
     */
    public SearchRequest getRegion(@Nonnull Collection<Long> oids) {
        return requestForTypeBasedTraversal(oids,
                                            TraversalDirection.AGGREGATED_BY,
                                            EntityType.REGION);
    }

    /**
     * Create a new request to fetch the business account which owns some of the entities (such as a VM).
     *
     * @param oids Collection of oids which are owned by a Business Account.
     * @return Search Request for the Business Account.
     */
    public SearchRequest getOwningBusinessAccount(@Nonnull Collection<Long> oids) {
        return requestForTypeBasedTraversal(oids, TraversalDirection.OWNED_BY,
                        EntityType.BUSINESS_ACCOUNT);
    }

    /**
     * Create a new request to traverse a specific direction and search
     * for entities of a specific type.
     *
     * @param oids the collection of entities to start from
     * @param traversalDirection the direction to travel
     * @param entityType the entity type to look for
     * @return the search request
     */
    public SearchRequest requestForTypeBasedTraversal(@Nonnull Collection<Long> oids,
                                                      @Nonnull TraversalDirection traversalDirection,
                                                      @Nonnull EntityType entityType) {
        final SearchParameters searchParameters =
                SearchParameters.newBuilder()
                        .setStartingFilter(SearchProtoUtil.idFilter(oids))
                        .addSearchFilter(SearchProtoUtil.searchFilterTraversal(
                                SearchProtoUtil.traverseToType(traversalDirection, entityType)))
                        .build();
        return new SearchRequest(realtimeTopologyContextId, searchServiceBlockingStub,
                                 searchServiceAsyncStub,
                                 severityPopulator, serviceEntityMapper, entityDetailsMapper,
                                 priceIndexPopulator, Collections.singleton(searchParameters));
    }

    /**
     * Method returns repository contents by the specified OIDs. All the contents are mapped to the
     * required types.
     *
     * @param oids oids of entities to retrieve
     * @param entityTypes entity types to retrieve
     * @param allAccounts whether all accounts are queried (true) of only some of them
     *         (false). This value is used for queries optimization purpose. {@code false} value
     *         does not affect results, it is just executed a little bit longer.
     * @return query result, mapped to REST classes
     * @throws ConversionException if error faced converting objects to API DTOs
     * @throws InterruptedException if current thread has been interrupted
     */
    @Nonnull
    public RepositoryRequestResult getByIds(@Nonnull Collection<Long> oids,
            @Nonnull Set<EntityType> entityTypes, boolean allAccounts)
            throws ConversionException, InterruptedException {
        if (oids.isEmpty()) {
            return new RepositoryRequestResult(Collections.emptySet(), Collections.emptySet());
        }
        final Collection<ServiceEntityApiDTO> seList = getServiceEntities(oids, entityTypes);
        final Collection<BusinessUnitApiDTO> buList;
        if (entityTypes.isEmpty() || entityTypes.contains(EntityType.BUSINESS_ACCOUNT)) {
            final Collection<Long> buOids = new LinkedList<>(oids);
            // Load all the rest OIDs. We assume that only BusinessAccounts should be left here
            buOids.removeAll(Collections2.transform(seList, se -> Long.parseLong(se.getUuid())));
            buList = getBusinessUnits(buOids, allAccounts);
        } else {
            buList = Collections.emptyList();
        }
        severityPopulator.populate(realtimeTopologyContextId, seList);
        return new RepositoryRequestResult(buList, seList);
    }

    /**
     * Expand the list of oids of service providers to the list of oids of the connected regions.
     *
     * @param serviceProviders set of service provider oids
     * @return set of region oids
     */
    public Set<Long> expandServiceProvidersToRegions(@Nonnull final Set<Long> serviceProviders) {
        return this.entitiesRequest(serviceProviders)
                .getEntitiesWithConnections()
                .flatMap(e -> e.getConnectedEntitiesList().stream())
                .filter(ConnectedEntity::hasConnectedEntityId)
                .filter(ConnectedEntity::hasConnectedEntityType)
                .filter(c -> c.getConnectedEntityType() == EntityType.REGION_VALUE
                        && c.getConnectionType() == ConnectionType.OWNS_CONNECTION)
                .map(ConnectedEntity::getConnectedEntityId)
                .collect(Collectors.toSet());
    }

    /**
     * Returns converted service entity DTOs.
     * Business users require another request to Repository in order to get all the required
     * data that's why business accounts in the repository response are just omited in this method.
     *
     * @param oids OIDs to retrieve
     * @param entityTypes entity types to retrieve
     * @return collection of converted service entities WITHOUT any business accounts
     */
    @Nonnull
    private Collection<ServiceEntityApiDTO> getServiceEntities(@Nonnull Collection<Long> oids,
            @Nonnull Set<EntityType> entityTypes) {
        if (entityTypes.equals(Collections.singleton(EntityType.BUSINESS_ACCOUNT)) || oids.isEmpty()) {
            return Collections.emptyList();
        }
        final RetrieveTopologyEntitiesRequest request = RetrieveTopologyEntitiesRequest.newBuilder()
                .addAllEntityOids(oids)
                .setTopologyContextId(realtimeTopologyContextId)
                .setTopologyType(TopologyType.SOURCE)
                .addAllEntityType(entityTypes.stream()
                        .filter(entityType -> entityType != EntityType.BUSINESS_ACCOUNT)
                        .map(EntityType::getNumber)
                        .collect(Collectors.toSet()))
                .setReturnType(Type.API)
                .build();
        final Iterator<PartialEntityBatch> iterator =
                repositoryService.retrieveTopologyEntities(request);
        final Collection<ApiPartialEntity> serviceEntitiesTE = new ArrayList<>();
        while (iterator.hasNext()) {
            final PartialEntityBatch batch = iterator.next();
            for (PartialEntity partialEntity : batch.getEntitiesList()) {
                final ApiPartialEntity entity = partialEntity.getApi();
                if (entity.getEntityType() != EntityType.BUSINESS_ACCOUNT_VALUE) {
                    // Business accounts will be loaded separately. Just ignore them here.
                    serviceEntitiesTE.add(entity);
                }
            }
        }
        return serviceEntityMapper.toServiceEntityApiDTO(serviceEntitiesTE);
    }

    @Nonnull
    private Collection<BusinessUnitApiDTO> getBusinessUnits(@Nonnull Collection<Long> oids,
            boolean allAccounts) {
        if (oids.isEmpty()) {
            return Collections.emptyList();
        }
        final RetrieveTopologyEntitiesRequest request = RetrieveTopologyEntitiesRequest.newBuilder()
                .addAllEntityOids(oids)
                .setTopologyContextId(realtimeTopologyContextId)
                .setTopologyType(TopologyType.SOURCE)
                .addEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .setReturnType(Type.FULL)
                .build();
        final Iterator<PartialEntityBatch> iterator =
                repositoryService.retrieveTopologyEntities(request);
        final List<TopologyEntityDTO> businessAccountsTE = new ArrayList<>();
        while (iterator.hasNext()) {
            final PartialEntityBatch batch = iterator.next();
            for (PartialEntity partialEntity : batch.getEntitiesList()) {
                final TopologyEntityDTO entity = partialEntity.getFullEntity();
                    businessAccountsTE.add(entity);
            }
        }
        return businessAccountMapper.convert(businessAccountsTE, allAccounts);
    }

    /**
     * Utility interface to abstract away the details of the RPC call that returns a
     * {@link PartialEntityBatch}.
     */
    @FunctionalInterface
    private interface BatchRPC {

        /**
         * Perform the RPC call, given a particular {@link PartialEntity.Type}.
         */
        Iterator<PartialEntityBatch> doRpc(@Nonnull PartialEntity.Type type);
    }

    /**
     * Utility class to retrieve {@link PartialEntity}s at various detail levels.
     * Used internally by the various request classes.
     */
    static class PartialEntityRetriever {
        private final BatchRPC retriever;
        private final ServiceEntityMapper serviceEntityMapper;
        private final SeverityPopulator severityPopulator;
        private final EntityDetailsMapper entityDetailsMapper;

        private PartialEntityRetriever(@Nonnull final BatchRPC retriever,
                                       @Nonnull final ServiceEntityMapper serviceEntityMapper,
                                       @Nonnull final SeverityPopulator severityPopulator,
                                       @Nonnull final EntityDetailsMapper entityDetailsMapper) {
            this.retriever = retriever;
            this.serviceEntityMapper = serviceEntityMapper;
            this.severityPopulator = severityPopulator;
            this.entityDetailsMapper = entityDetailsMapper;
        }

        private Stream<PartialEntity> entityStream(@Nonnull final Type type) {
            return RepositoryDTOUtil.topologyEntityStream(retriever.doRpc(type));
        }

        @Nonnull
        Stream<TopologyEntityDTO> getFullEntities() {
            return entityStream(Type.FULL)
                .map(PartialEntity::getFullEntity);
        }

        @Nonnull
        Stream<MinimalEntity> getMinimalEntities() {
            return entityStream(Type.MINIMAL)
                .map(PartialEntity::getMinimal);
        }

        @Nonnull
        Stream<EntityWithConnections> getEntitiesWithConnections() {
            return entityStream(Type.WITH_CONNECTIONS)
                .map(PartialEntity::getWithConnections);
        }

        @Nonnull
        Stream<ApiPartialEntity> getEntities() {
            return entityStream(Type.API)
                .map(PartialEntity::getApi);
        }

        @Nonnull
        Map<Long, ServiceEntityApiDTO> getSEMap(final long contextId,
                @Nullable final EntityAspectMapper aspectMapper,
                @Nullable final Collection<String> requestedAspects)
                throws InterruptedException, ConversionException {
            final Map<Long, ServiceEntityApiDTO> entities;
            if (aspectMapper != null) {
                entities = serviceEntityMapper.entitiesWithAspects(
                        getFullEntities().collect(Collectors.toList()), aspectMapper,
                        requestedAspects);
            } else {
                entities = serviceEntityMapper.toServiceEntityApiDTOMap(
                        getEntities().collect(Collectors.toList()));
            }
            severityPopulator.populate(contextId, entities.values());
            return entities;
        }

        @Nonnull
        List<ServiceEntityApiDTO> getSEList(final long contextId,
                @Nullable final EntityAspectMapper aspectMapper,
                @Nullable final Collection<String> requestedAspects)
                throws ConversionException, InterruptedException {
            return new ArrayList<>(getSEMap(contextId, aspectMapper, requestedAspects).values());
        }

        @Nonnull
        List<EntityDetailsApiDTO> getEntitiesDetails() {
            return entityDetailsMapper.toEntitiesDetails(
                    getFullEntities().collect(Collectors.toList()));
        }
    }

    /**
     * Encapsulates a "paginated" search request - i.e. where the client sends pagination-related
     * parameters, and is able to return a number of pagination response type
     * (either {@link SearchPaginationResponse} or {@link EntityPaginationResponse} are supported
     * via subclasses).
     *
     * @param <E> The DTO EntityType of the request/response.
     * @param <O> The OrderBy type (either search or entities)
     * @param <R> The ResponseType (either SearchPaginationResponse or EntityPaginationResponse).
     * @param <T> The Request type (either SearchPaginationRequest or EntityPaginationRequest).
     * @param <S> The supertype of the base type. Needed so that methods can return a self-reference.
     */
    private static abstract class BasePaginatedSearchRequest<E extends BaseApiDTO, O extends Enum<O>,
        R extends PaginationResponse<E, O>,
        T extends PaginationRequest<E, O, R>,
        S extends BasePaginatedSearchRequest<E, O, R, T, S>> {
        private final long realtimeContextId;
        private final SearchServiceBlockingStub searchServiceBlockingStub;
        private final SearchQuery searchQuery;
        private final Set<Long> startingOids;
        private final SeverityPopulator severityPopulator;
        private final ServiceEntityMapper serviceEntityMapper;
        private EntityAspectMapper aspectMapper = null;
        private Collection<String> requestedAspects = null;
        private final T paginationRequest;
        private final PaginationMapper paginationMapper;
        private final BusinessAccountMapper businessAccountMapper;
        private final Function<ServiceEntityApiDTO, E> entityMapper;

        private BasePaginatedSearchRequest(long realtimeContextId, SearchServiceBlockingStub searchServiceBlockingStub,
                                           SearchQuery searchQuery, Set<Long> startingOids,
                                           ServiceEntityMapper serviceEntityMapper, T paginationRequest,
                                           SeverityPopulator severityPopulator, PaginationMapper paginationMapper,
                                           BusinessAccountMapper businessAccountMapper,
                                           @Nonnull final Function<ServiceEntityApiDTO, E> entityMapper) {
            this.realtimeContextId = realtimeContextId;
            this.searchServiceBlockingStub = searchServiceBlockingStub;
            this.searchQuery = searchQuery;
            this.startingOids = startingOids;
            this.serviceEntityMapper = serviceEntityMapper;
            this.severityPopulator = severityPopulator;
            this.paginationRequest = paginationRequest;
            this.paginationMapper = paginationMapper;
            this.businessAccountMapper = businessAccountMapper;
            this.entityMapper = Objects.requireNonNull(entityMapper);
        }

        /**
         * Request specific aspects in the response.
         *
         * <p/>Note: Aspect mapping can be quite expensive. Avoid if the API doesn't require it!
         *
         * @param aspectMapper The aspect mapper to use.
         * @param aspectNames The aspects to request.
         * @return The request, for chaining.
         */
        @Nonnull
        public S requestAspects(@Nonnull final EntityAspectMapper aspectMapper,
                                                     @Nullable final Collection<String> aspectNames) {
            this.aspectMapper = aspectMapper;
            if (!CollectionUtils.isEmpty(aspectNames)) {
                requestedAspects = aspectNames;
            }
            return getSelf();
        }

        /**
         * Get the {@link SearchPaginationResponse}. This makes the call to the search service,
         * and converts the result.
         *
         * @return The {@link SearchPaginationResponse}.
         * @throws InterruptedException If there is an issue waiting for a dependent operation.
         * @throws ConversionException If there is an issue with entity conversion.
         */
        @Nonnull
        public R getResponse() throws InterruptedException, ConversionException {
            final boolean mapAspects = aspectMapper != null;
            SearchEntitiesResponse response = searchServiceBlockingStub.searchEntities(SearchEntitiesRequest.newBuilder()
                .setSearch(searchQuery)
                .addAllEntityOid(startingOids)
                .setReturnType(mapAspects ? Type.FULL : Type.API)
                .setPaginationParams(paginationMapper.toProtoParams(paginationRequest))
                .build());
            final Map<Long, ServiceEntityApiDTO> entities;
            if (mapAspects) {
                entities = serviceEntityMapper.entitiesWithAspects(
                    Collections2.transform(response.getEntitiesList(), PartialEntity::getFullEntity),
                    aspectMapper, requestedAspects);
            } else {
                entities = serviceEntityMapper.toServiceEntityApiDTOMap(
                    Collections2.transform(response.getEntitiesList(), PartialEntity::getApi));
            }
            severityPopulator.populate(realtimeContextId, entities.values());
            final List<E> orderedEntities = response.getEntitiesList().stream()
                .map(e -> mapAspects ? e.getFullEntity().getOid() : e.getApi().getOid())
                .map(entities::get)
                .filter(Objects::nonNull)
                .map(this.entityMapper)
                .collect(Collectors.toList());

            return PaginationProtoUtil.getNextCursor(response.getPaginationResponse())
                .map(nextCursor -> paginationRequest.nextPageResponse(
                    orderedEntities, nextCursor,
                    response.getPaginationResponse().getTotalRecordCount()))
                .orElseGet(() -> paginationRequest.finalPageResponse(
                    orderedEntities,
                    response.getPaginationResponse().getTotalRecordCount()));
        }

        /**
         * Get the {@link SearchPaginationResponse}. This makes the call to the search service,
         * and converts the result to {@link BusinessUnitApiDTO).
         *
         * @param allAccounts
         * @return The {@link SearchPaginationResponse}.
         */
        @Nonnull
        public R getBusinessUnitsResponse(boolean allAccounts) {
            SearchEntitiesResponse response = searchServiceBlockingStub.searchEntities(SearchEntitiesRequest.newBuilder()
                    .setSearch(searchQuery)
                    .addAllEntityOid(startingOids)
                    .setReturnType(Type.FULL)
                    .setPaginationParams(paginationMapper.toProtoParams(paginationRequest))
                    .build());
            final Map<Long, E> buDTOMap = businessAccountMapper.convert(Lists.transform(response.getEntitiesList(),
                    PartialEntity::getFullEntity), allAccounts).stream()
                    .collect(Collectors.toMap(dto -> Long.valueOf(dto.getUuid()),
                    dto -> (E)dto));
            final List<E> orderedBuDTOs = response.getEntitiesList().stream()
                    .map(e -> e.getFullEntity().getOid())
                    .map(buDTOMap::get)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
            return PaginationProtoUtil.getNextCursor(response.getPaginationResponse())
                    .map(nextCursor -> paginationRequest.nextPageResponse(
                            orderedBuDTOs, nextCursor,
                            response.getPaginationResponse().getTotalRecordCount()))
                    .orElseGet(() -> paginationRequest.finalPageResponse(
                            orderedBuDTOs,
                            response.getPaginationResponse().getTotalRecordCount()));
        }

        /**
         * Get a reference to {@link this} object.
         *
         * @return a reference to {@link this} object.
         */
        protected abstract S getSelf();
    }

    /**
     * Encapsulates a "paginated" search request - i.e. where the client sends pagination-related
     * parameters, and expects a {@link SearchPaginationResponse}.
     */
    public static class PaginatedSearchRequest
        extends BasePaginatedSearchRequest<BaseApiDTO,
                                           SearchOrderBy,
                                           SearchPaginationResponse,
                                           SearchPaginationRequest,
                                           PaginatedSearchRequest> {

        public PaginatedSearchRequest(long realtimeContextId,
                                      SearchServiceBlockingStub searchServiceBlockingStub,
                                      SearchQuery searchQuery,
                                      Set<Long> startingOids,
                                      ServiceEntityMapper serviceEntityMapper,
                                      SearchPaginationRequest paginationRequest,
                                      SeverityPopulator severityPopulator,
                                      PaginationMapper paginationMapper,
                                      BusinessAccountMapper businessAccountMapper) {
            super(realtimeContextId, searchServiceBlockingStub, searchQuery, startingOids,
                    serviceEntityMapper, paginationRequest, severityPopulator, paginationMapper,
                    businessAccountMapper, e -> e);
        }

        @Override
        protected PaginatedSearchRequest getSelf() {
            return this;
        }
    }

    public static class PaginatedEntitiesRequest
        extends BasePaginatedSearchRequest<ServiceEntityApiDTO,
                                            EntityOrderBy,
                                            EntityPaginationResponse,
                                            EntityPaginationRequest,
        PaginatedEntitiesRequest> {

        public PaginatedEntitiesRequest(long realtimeContextId,
                                        SearchServiceBlockingStub searchServiceBlockingStub,
                                        SearchQuery searchQuery,
                                        Set<Long> startingOids,
                                        ServiceEntityMapper serviceEntityMapper,
                                        EntityPaginationRequest paginationRequest,
                                        SeverityPopulator severityPopulator,
                                        PaginationMapper paginationMapper,
                                        BusinessAccountMapper businessAccountMapper) {
            super(realtimeContextId, searchServiceBlockingStub, searchQuery, startingOids,
                    serviceEntityMapper, paginationRequest, severityPopulator, paginationMapper,
                    businessAccountMapper, e -> e);
        }

        @Override
        protected PaginatedEntitiesRequest getSelf() {
            return this;
        }
    }

    /**
     * A request for a dynamic search over the entities in the realtime topology.
     */
    public static class SearchRequest {
        private final long realtimeContextId;

        private final SearchServiceBlockingStub searchServiceBlockingStub;

        private final SearchServiceStub searchServiceStub;

        private final ServiceEntityMapper serviceEntityMapper;

        private final SeverityPopulator severityPopulator;

        private final Collection<SearchParameters> params;

        private EntityAspectMapper aspectMapper = null;

        private final PriceIndexPopulator priceIndexPopulator;

        private PartialEntityRetriever retriever;

        private boolean populatePriceIndex = false;

        private SearchRequest(final long realtimeContextId,
                             @Nonnull final SearchServiceBlockingStub searchServiceBlockingStub,
                             @Nonnull final SearchServiceStub searchServiceStub,
                             @Nonnull final SeverityPopulator severityPopulator,
                             @Nonnull final ServiceEntityMapper serviceEntityMapper,
                             @Nonnull final EntityDetailsMapper entityDetailsMapper,
                             @Nonnull final PriceIndexPopulator priceIndexPopulator,
                             @Nonnull final Collection<SearchParameters> params) {
            this.realtimeContextId = realtimeContextId;
            this.searchServiceBlockingStub = searchServiceBlockingStub;
            this.searchServiceStub = Objects.requireNonNull(searchServiceStub);
            this.serviceEntityMapper = serviceEntityMapper;
            this.severityPopulator = severityPopulator;
            this.priceIndexPopulator = priceIndexPopulator;
            this.params = params;

            this.retriever = new PartialEntityRetriever(type ->
                    searchServiceBlockingStub.searchEntitiesStream(SearchEntitiesRequest.newBuilder()
                            .setSearch(SearchQuery.newBuilder()
                                    .addAllSearchParameters(params))
                            .setReturnType(type)
                            .build()),
                    serviceEntityMapper,
                    severityPopulator,
                    entityDetailsMapper);
        }

        /**
         * Use a particular {@link EntityAspectMapper} when converting the entities returned by
         * this request to {@link ServiceEntityApiDTO}s.
         *
         * Note: Aspect mapping can be quite expensive. Avoid if the API doesn't require it!
         *
         * @param aspectMapper The mapper.
         * @return The request, for chaining.
         */
        @Nonnull
        public SearchRequest useAspectMapper(@Nonnull final EntityAspectMapper aspectMapper) {
            this.aspectMapper = aspectMapper;
            return this;
        }

        /**
         * Use {@link PriceIndexPopulator} to populate priceIndex info in response to
         * this request's list of {@link ServiceEntityApiDTO}s.
         * @param populatePriceIndex - flag to determine if priceIndex should be included in response
         * @return The request, for chaining.
         */
        @Nonnull
        public SearchRequest usePriceIndexPopulator(boolean populatePriceIndex) {
            this.populatePriceIndex = populatePriceIndex;
            return this;
        }

        /**
         * Handles pagination request on searchParameters configured on object.
         * @param paginationParams pagination parameters
         * @return response with serviceEntityDtos
         * @throws InterruptedException if current thread has been interrupted while populating
         * severity data
         * @throws ConversionException if error faced during severity population
         */
        @Nonnull
        public ResponseEntity<List<ServiceEntityApiDTO>> getPaginatedSEList(
                @Nonnull final Pagination.PaginationParameters paginationParams)
                throws InterruptedException, ConversionException {
            SearchEntitiesRequest searchEntitiesRequest = SearchEntitiesRequest.newBuilder()
                    .setPaginationParams(paginationParams)
                    .setSearch(SearchQuery.newBuilder()
                            .addAllSearchParameters(params))
                    .setReturnType(Type.API)
                    .build();

            SearchEntitiesResponse response = searchServiceBlockingStub.searchEntities(searchEntitiesRequest);

            final List<ApiPartialEntity> apiPartialEntities = response.getEntitiesList().stream()
                    .map(PartialEntity::getApi)
                    .collect(Collectors.toList());
            List<ServiceEntityApiDTO> serviceEntityApiDTOList = this.serviceEntityMapper.toServiceEntityApiDTO(apiPartialEntities);

            Pagination.PaginationResponse paginationResponse = response.getPaginationResponse();

            //Add severityInfo
            severityPopulator.populate(realtimeContextId, serviceEntityApiDTOList);

            if (this.populatePriceIndex) {
                this.priceIndexPopulator.populateRealTimeEntities(serviceEntityApiDTOList);
            }

            return PaginationUtil.buildResponseEntity(serviceEntityApiDTOList, null, paginationResponse.getNextCursor(), paginationResponse.getTotalRecordCount());
        }

        /**
         * Execute the request and return the set of matching OIDs. Note: this involves making
         * a remote call.
         */
        @Nonnull
        public Set<Long> getOids() {
            return Sets.newHashSet(searchServiceBlockingStub.searchEntityOids(
                SearchEntityOidsRequest.newBuilder()
                    .setSearch(SearchQuery.newBuilder()
                        .addAllSearchParameters(params))
                    .build()).getEntitiesList());
        }

        /**
         * Returns a future to retrieve OIDs from the request.
         *
         * @return a future for OIDs retrieved from the request
         */
        @Nonnull
        public Future<Set<Long>> getOidsFuture() {
            final OidSetObserver observer = new OidSetObserver();
            searchServiceStub.searchEntityOids(
                    SearchEntityOidsRequest.newBuilder()
                        .setSearch(SearchQuery.newBuilder()
                            .addAllSearchParameters(params))
                        .build(),
                    observer);
            return observer.getFuture();
        }

        /**
         * Execute the request and return the number of matches. Note: this involves making
         * a remote call.
         */
        public long count() {
            return searchServiceBlockingStub.countEntities(CountEntitiesRequest.newBuilder()
                .setSearch(SearchQuery.newBuilder()
                    .addAllSearchParameters(params))
                .build()).getEntityCount();
        }

        /**
         * Get the full {@link TopologyEntityDTO}s. Note - this is expensive. Only do this
         * if absolutely necessary. Try {@link SearchRequest#getEntities()} instead!
         */
        @Nonnull
        public Stream<TopologyEntityDTO> getFullEntities() {
            return retriever.getFullEntities();
        }

        @Nonnull
        public Stream<MinimalEntity> getMinimalEntities() {
            return retriever.getMinimalEntities();
        }

        /**
         * Get the {@link EntityWithConnections} representation of entities that match this
         * search.
         *
         * @return The {@link EntityWithConnections} stream.
         */
        @Nonnull
        public Stream<EntityWithConnections> getEntitiesWithConnections() {
            return retriever.getEntitiesWithConnections();
        }

        @Nonnull
        public Stream<ApiPartialEntity> getEntities() {
            return retriever.getEntities();
        }

        /**
         * Get the {@link ServiceEntityApiDTO}s that match the search, arranged by OID.
         *
         * The {@link ServiceEntityApiDTO}s will be fully populated with target information,
         * and aspects if an aspect mapper was added via {@link SearchRequest#useAspectMapper(EntityAspectMapper)}.
         *
         * @throws InterruptedException if thread has been interrupted
         * @throws ConversionException if errors faced during converting data to API DTOs
         */
        @Nonnull
        public Map<Long, ServiceEntityApiDTO> getSEMap()
                throws InterruptedException, ConversionException {
            return retriever.getSEMap(realtimeContextId, aspectMapper, null);
        }

        /**
         * Get the {@link ServiceEntityApiDTO}s that match the search.
         *
         * The {@link ServiceEntityApiDTO}s will be fully populated with target information,
         * and aspects if an aspect mapper was added via {@link SearchRequest#useAspectMapper(EntityAspectMapper)}.
         *
         * @throws InterruptedException if thread has been interrupted
         * @throws ConversionException on error converting to API DTOs
         */
        @Nonnull
        public List<ServiceEntityApiDTO> getSEList()
                throws InterruptedException, ConversionException {
            final List<ServiceEntityApiDTO> serviceEntityApiDTOList = retriever.getSEList(realtimeContextId, aspectMapper, null);
            if (this.populatePriceIndex) {
                this.priceIndexPopulator.populateRealTimeEntities(serviceEntityApiDTOList);
            }
            return serviceEntityApiDTOList;
        }

    }

    /**
     * A request for a single entity.
     * This is a convenient alternative to {@link MultiEntityRequest} so that callers can operate
     * on {@link Optional}s instead of entities.
     */
    public static class SingleEntityRequest extends EntitiesRequest<SingleEntityRequest> {

        private SingleEntityRequest(final long realtimeContextId,
                @Nonnull final RepositoryServiceBlockingStub repositoryServiceBlockingStub,
                @Nonnull final RepositoryServiceStub repositoryServiceStub,
                @Nonnull final SeverityPopulator severityPopulator,
                @Nonnull final ServiceEntityMapper serviceEntityMapper,
                @Nonnull final EntityDetailsMapper entityDetailsMapper,
                final long oid) {
            super(SingleEntityRequest.class, realtimeContextId, repositoryServiceBlockingStub,
                    repositoryServiceStub, severityPopulator, serviceEntityMapper, entityDetailsMapper,
                    Collections.singleton(oid));
        }

        /**
         * Get the full {@link TopologyEntityDTO}. Note - this is expensive. Only do this
         * if absolutely necessary. Try {@link SearchRequest#getEntities()} instead!
         */
        @Nonnull
        public Optional<TopologyEntityDTO> getFullEntity() {
            return retriever.getFullEntities().findFirst();
        }

        @Nonnull
        public Optional<MinimalEntity> getMinimalEntity() {
            return retriever.getMinimalEntities().findFirst();
        }

        /**
         * Get the {@link EntityWithConnections} representation of this entity.
         *
         * @return  The {@link EntityWithConnections}.
         */
        @Nonnull
        public Optional<EntityWithConnections> getEntityWithConnections() {
            return retriever.getEntitiesWithConnections().findFirst();
        }

        @Nonnull
        public Optional<ApiPartialEntity> getEntity() {
            return retriever.getEntities().findFirst();
        }

        /**
         * Get the {@link ServiceEntityApiDTO}s that matches the OID.
         *
         * The {@link ServiceEntityApiDTO} will be fully populated with target information,
         * and aspects if an aspect mapper was added via {@link SingleEntityRequest#useAspectMapper(EntityAspectMapper)}.
         *
         * @throws InterruptedException if thread has been interrupted
         * @throws ConversionException if errors faced during converting data to API DTOs
         */
        @Nonnull
        public Optional<ServiceEntityApiDTO> getSE()
                throws ConversionException, InterruptedException {
            return retriever.getSEList(getContextId(), aspectMapper, aspects).stream().findFirst();
        }

        /**
         * Get the {@link EntityDetailsApiDTO}s that matches the OID.
         * The {@link EntityDetailsApiDTO} will fully populated with the entity OID and a list
         * of properties.
         *
         * @return an optional entity with details.
         */
        @Nonnull
        public Optional<EntityDetailsApiDTO> getEntityDetails() {
            return retriever.getEntitiesDetails().stream().findFirst();
        }
    }

    /**
     * A multi-get for entities in the topology, by ID.
     */
    public static class MultiEntityRequest extends EntitiesRequest<MultiEntityRequest> {
        public MultiEntityRequest(final long topologyContextId,
                  @Nonnull final RepositoryServiceBlockingStub repositoryServiceBlockingStub,
                  @Nonnull final RepositoryServiceStub repositoryServiceStub,
                  @Nonnull final SeverityPopulator severityPopulator,
                  @Nonnull final ServiceEntityMapper serviceEntityMapper,
                  @Nonnull final EntityDetailsMapper entityDetailsMapper,
                  @Nonnull final Set<Long> oids) {
            super(MultiEntityRequest.class, topologyContextId, repositoryServiceBlockingStub,
                    repositoryServiceStub, severityPopulator, serviceEntityMapper,
                    entityDetailsMapper, oids);
        }

        /**
         * Get the full {@link TopologyEntityDTO}s. Note - this is expensive. Only do this
         * if absolutely necessary. Try {@link SearchRequest#getEntities()} instead!
         */
        @Nonnull
        public Stream<TopologyEntityDTO> getFullEntities() {
            return retriever.getFullEntities();
        }

        @Nonnull
        public Stream<MinimalEntity> getMinimalEntities() {
            return retriever.getMinimalEntities();
        }

        /**
         * Returns a collection of a minimal entities in a async fashion - using {@link
         * StreamObserver}.
         *
         * @param observer observer to append minimal entities to
         */
        @Nonnull
        public void getMinimalEntities(StreamObserver<MinimalEntity> observer) {
            final StreamObserver<PartialEntityBatch> effectiveObserver =
                    new MappingObserver<>(observer, batch -> batch.getEntitiesList()
                            .stream()
                            .map(PartialEntity::getMinimal)
                            .collect(Collectors.toList()));
            getRepositoryServiceStub().retrieveTopologyEntities(
                    getRequestBuilder().setReturnType(Type.MINIMAL).build(), effectiveObserver);
        }

        /**
         * Get the entities targetted by this request as {@link EntityWithConnections}.
         *
         * @return {@link EntityWithConnections} stream.
         */
        @Nonnull
        public Stream<EntityWithConnections> getEntitiesWithConnections() {
            return retriever.getEntitiesWithConnections();
        }

        @Nonnull
        public Stream<ApiPartialEntity> getEntities() {
            return retriever.getEntities();
        }

        /**
         * Get the {@link ServiceEntityApiDTO}s that match the search, arranged by OID.
         *
         * The {@link ServiceEntityApiDTO}s will be fully populated with target information,
         * and aspects if an aspect mapper was added via {@link SearchRequest#useAspectMapper(EntityAspectMapper)}.
         *
         * @throws InterruptedException if thread has been interrupted
         * @throws ConversionException if errors faced during converting data to API DTOs
         */
        @Nonnull
        public Map<Long, ServiceEntityApiDTO> getSEMap()
                throws InterruptedException, ConversionException {
            return retriever.getSEMap(getContextId(), aspectMapper, aspects);
        }

        /**
         * Get the {@link ServiceEntityApiDTO}s that match the search.
         *
         * The {@link ServiceEntityApiDTO}s will be fully populated with target information,
         * and aspects if an aspect mapper was added via {@link MultiEntityRequest#useAspectMapper(EntityAspectMapper)}.
         *
         * @throws InterruptedException if thread has been interrupted
         * @throws ConversionException if errors faced during converting data to API DTOs
         */
        @Nonnull
        public List<ServiceEntityApiDTO> getSEList()
                throws InterruptedException, ConversionException {
            return retriever.getSEList(getContextId(), aspectMapper, aspects);
        }
    }

    public static class EntitiesRequest<REQ extends EntitiesRequest> {
        private final long topologyContextId;

        private final Class<REQ> clazz;

        private boolean allowGetAll = false;

        protected EntityAspectMapper aspectMapper = null;

        protected Collection<String> aspects = null;

        protected final PartialEntityRetriever retriever;
        private final RetrieveTopologyEntitiesRequest.Builder requestBuilder;
        private final RepositoryServiceStub repositoryServiceStub;

        private EntitiesRequest(@Nonnull final Class<REQ> clazz,
                                final long topologyContextId,
                                @Nonnull final RepositoryServiceBlockingStub repositoryServiceBlockingStub,
                                @Nonnull final RepositoryServiceStub repositoryServiceStub,
                                @Nonnull final SeverityPopulator severityPopulator,
                                @Nonnull final ServiceEntityMapper serviceEntityMapper,
                                @Nonnull final EntityDetailsMapper entityDetailsMapper,
                                @Nonnull final Set<Long> targetId) {
            this.clazz = clazz;
            this.topologyContextId = topologyContextId;
            this.repositoryServiceStub = Objects.requireNonNull(repositoryServiceStub);

            requestBuilder = RetrieveTopologyEntitiesRequest.newBuilder()
                    .addAllEntityOids(targetId)
                    .setTopologyContextId(topologyContextId)
                    .setTopologyType(TopologyType.SOURCE);
            final BatchRPC batchRetriever = type -> {
                if (targetId.isEmpty() && !allowGetAll) {
                    return Collections.emptyIterator();
                } else {
                    return repositoryServiceBlockingStub.retrieveTopologyEntities(
                            requestBuilder.setReturnType(type).build());
                }
            };
            this.retriever = new PartialEntityRetriever(batchRetriever, serviceEntityMapper,
                    severityPopulator, entityDetailsMapper);
        }

        @Nonnull
        public REQ contextId(@Nullable final Long contextId) {
            this.requestBuilder.setTopologyContextId(
                    contextId != null ? contextId : topologyContextId);
            return clazz.cast(this);
        }

        /**
         * By default, if the input list of OIDs is empty we will return nothing.
         * If you REALLY REALLY need it, you can use this to return all.
         */
        @Nonnull
        public REQ allowGetAll() {
            allowGetAll = true;
            return clazz.cast(this);
        }

        /**
         * Get the entities from the projected topology.
         *
         * Note: retrieving from the projected topology may be more expensive than retrieving from
         * the source topology. Therefore, callers should only retrieve from the projected topology
         * if truly necessary (e.g. entity is not in original topology).
         */
        @Nonnull
        public REQ projectedTopology() {
            requestBuilder.setTopologyType(TopologyType.PROJECTED);
            return clazz.cast(this);
        }

        /**
         * Use a particular {@link EntityAspectMapper} when converting the entities returned by
         * this request to {@link ServiceEntityApiDTO}s.
         *
         * Note: Aspect mapping can be quite expensive. Avoid if the API doesn't require it!
         *
         * @param aspectMapper The mapper.
         * @return The request, for chaining.
         */
        @Nonnull
        public REQ useAspectMapper(EntityAspectMapper aspectMapper) {
            this.aspectMapper = aspectMapper;
            return clazz.cast(this);
        }

        /**
         * Use a particular {@link EntityAspectMapper} when converting the entities returned by
         * this request to {@link ServiceEntityApiDTO}s.
         *
         * @param aspectMapper The input aspect mapper.
         * @param aspectNames The input list of aspect names.
         *
         * @return The request, for chaining.
         */
        @Nonnull
        public REQ useAspectMapper(EntityAspectMapper aspectMapper,
                @Nonnull Collection<String> aspectNames) {
            this.aspectMapper = aspectMapper;
            this.aspects = aspectNames;
            return clazz.cast(this);
        }

        /**
         * Restrict the request to a set of entity types.
         *
         * @param types The entity types.
         * @return The request, for chaining.
         */
        @Nonnull
        public REQ restrictTypes(@Nonnull final Collection<ApiEntityType> types) {
            types.forEach(type -> requestBuilder.addEntityType(type.typeNumber()));
            return clazz.cast(this);
        }

        protected long getContextId() {
            return requestBuilder.getTopologyContextId();
        }

        @Nonnull
        protected RepositoryServiceStub getRepositoryServiceStub() {
            return repositoryServiceStub;
        }

        @Nonnull
        protected Builder getRequestBuilder() {
            return requestBuilder;
        }
    }

    /**
     * Response from a repository holding all the available data: entities, business accounts.
     */
    public static class RepositoryRequestResult {
        private final Collection<BusinessUnitApiDTO> businessAccounts;
        private final Collection<ServiceEntityApiDTO> serviceEntities;

        /**
         * Constructs repository response.
         *
         * @param businessAccounts collection of business accounts retrieved
         * @param serviceEntities collection of service entities retrieved.
         */
        public RepositoryRequestResult(@Nonnull Collection<BusinessUnitApiDTO> businessAccounts,
                @Nonnull Collection<ServiceEntityApiDTO> serviceEntities) {
            this.businessAccounts = Objects.requireNonNull(businessAccounts);
            this.serviceEntities = Objects.requireNonNull(serviceEntities);
        }

        @Nonnull
        public Collection<BusinessUnitApiDTO> getBusinessAccounts() {
            return businessAccounts;
        }

        @Nonnull
        public Collection<ServiceEntityApiDTO> getServiceEntities() {
            return serviceEntities;
        }

        @Nonnull
        public Iterable<BaseApiDTO> getAllResults() {
            return Iterables.concat(businessAccounts, serviceEntities);
        }
    }

    /**
     * Observer for entity oids response.
     */
    private static class OidSetObserver extends
            FutureObserver<SearchEntityOidsResponse, Set<Long>> {
        private Set<Long> result;

        @Override
        public void onNext(SearchEntityOidsResponse value) {
            this.result = new HashSet<>(value.getEntitiesList());
        }

        @Nonnull
        @Override
        protected Set<Long> createResult() {
            return result;
        }
    }

    /**
     * Protobuf observer for retrieving a map from a streaming response in batched mode and pass it
     * to non-batched observer.
     *
     * @param <S> source message (received by gRPC)
     * @param <R> result message (sent to a non-batching observer)
     */
    private static class MappingObserver<S, R> implements StreamObserver<S> {
        private final StreamObserver<R> receiver;
        private final Function<S, Collection<R>> mapping;

        MappingObserver(@Nonnull StreamObserver<R> receiver,
                @Nonnull Function<S, Collection<R>> mapping) {
            this.receiver = Objects.requireNonNull(receiver);
            this.mapping = Objects.requireNonNull(mapping);
        }

        @Override
        public void onNext(S value) {
            for (R converted : mapping.apply(value)) {
                receiver.onNext(converted);
            }
        }

        @Override
        public void onError(Throwable t) {
            receiver.onError(t);
        }

        @Override
        public void onCompleted() {
            receiver.onCompleted();
        }
    }
}
