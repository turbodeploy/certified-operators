package com.vmturbo.api.component.communication;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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
import com.google.common.collect.Sets;

import io.grpc.stub.StreamObserver;

import org.apache.commons.collections.CollectionUtils;

import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.component.external.api.mapper.SeverityPopulator;
import com.vmturbo.api.component.external.api.mapper.aspect.EntityAspectMapper;
import com.vmturbo.api.component.external.api.util.businessaccount.BusinessAccountMapper;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.businessunit.BusinessUnitApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.entityaspect.CloudAspectApiDTO;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.template.TemplateApiDTO;
import com.vmturbo.api.enums.AspectName;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest.Builder;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceStub;
import com.vmturbo.common.protobuf.search.Search.CountEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.SearchEntityOidsRequest;
import com.vmturbo.common.protobuf.search.Search.SearchEntityOidsResponse;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.EntityWithConnections;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntityBatch;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.UIEntityType;
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

    public RepositoryApi(@Nonnull final SeverityPopulator severityPopulator,
                         @Nonnull final RepositoryServiceBlockingStub repositoryService,
                         @Nonnull final RepositoryServiceStub repositoryServiceAsyncStub,
                         @Nonnull final SearchServiceBlockingStub searchServiceBlockingStub,
                         @Nonnull final SearchServiceStub searchServiceAsyncStub,
                         @Nonnull final ServiceEntityMapper serviceEntityMapper,
                         @Nonnull BusinessAccountMapper businessAccountMapper,
                         final long realtimeTopologyContextId) {
        this.severityPopulator = Objects.requireNonNull(severityPopulator);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.searchServiceBlockingStub = Objects.requireNonNull(searchServiceBlockingStub);
        this.searchServiceAsyncStub = Objects.requireNonNull(searchServiceAsyncStub);
        this.serviceEntityMapper = Objects.requireNonNull(serviceEntityMapper);
        this.repositoryService = Objects.requireNonNull(repositoryService);
        this.businessAccountMapper = Objects.requireNonNull(businessAccountMapper);
        this.repositoryServiceAsyncStub = Objects.requireNonNull(repositoryServiceAsyncStub);
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
     * Create a new search request.
     *
     * @param params The {@link SearchParameters} to use for the request. Use the utility methods
     *               in {@link com.vmturbo.common.protobuf.search.SearchProtoUtil} where possible.
     * @return The {@link SearchRequest}, which can be further customized.
     */
    @Nonnull
    public SearchRequest newSearchRequestMulti(@Nonnull Collection<SearchParameters> params) {
        return new SearchRequest(realtimeTopologyContextId, searchServiceBlockingStub,
                searchServiceAsyncStub, severityPopulator, serviceEntityMapper, params);
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
                repositoryServiceAsyncStub, severityPopulator, serviceEntityMapper, oid);
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
                repositoryServiceAsyncStub, severityPopulator, serviceEntityMapper, oids);
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
                                 severityPopulator, serviceEntityMapper,
                                 Collections.singleton(searchParameters));
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
            buOids.removeAll(Collections2.transform(seList, se -> Long.parseLong(se.getUuid())));
            buList = getBusinessUnits(buOids, allAccounts);
        } else {
            buList = Collections.emptyList();
        }
        severityPopulator.populate(realtimeTopologyContextId, seList);
        return new RepositoryRequestResult(buList, seList);
    }

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
                serviceEntitiesTE.add(entity);
            }
        }
        return serviceEntitiesTE.stream()
                .map(serviceEntityMapper::toServiceEntityApiDTO)
                .collect(Collectors.toList());
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

        private PartialEntityRetriever(@Nonnull final BatchRPC retriever,
                                       @Nonnull final ServiceEntityMapper serviceEntityMapper,
                                       @Nonnull final SeverityPopulator severityPopulator) {
            this.retriever = retriever;
            this.serviceEntityMapper = serviceEntityMapper;
            this.severityPopulator = severityPopulator;
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
                                                @Nullable EntityAspectMapper aspectMapper,
                                                @Nullable Collection<String> requestedAspects)
                throws InterruptedException, ConversionException {
            final Map<Long, ServiceEntityApiDTO> entities;
            if (aspectMapper == null) {
                // Do the conversion in bulk.
                entities = serviceEntityMapper.toServiceEntityApiDTOMap(getEntities()
                        .collect(Collectors.toList()));
            } else {
                entities = new HashMap<>();
                final Iterator<TopologyEntityDTO> iterator = getFullEntities().iterator();
                while (iterator.hasNext()) {
                    final TopologyEntityDTO entity = iterator.next();
                    entities.put(entity.getOid(), entityWithAspects(entity, aspectMapper, requestedAspects));
                }
                setEntitiesCost(entities.values());
            }
            severityPopulator.populate(contextId, entities.values());
            return entities;
        }

        @Nonnull
        List<ServiceEntityApiDTO> getSEList(final long contextId,
                @Nullable EntityAspectMapper aspectMapper,
                @Nullable Collection<String> requestedAspects)
                throws ConversionException, InterruptedException {
            final List<ServiceEntityApiDTO> entities;

            if (aspectMapper == null) {
                entities = getEntities()
                    .map(serviceEntityMapper::toServiceEntityApiDTO)
                    .collect(Collectors.toList());
            } else {
                entities = new ArrayList<>();
                final Iterator<TopologyEntityDTO> iterator = getFullEntities().iterator();
                while (iterator.hasNext()) {
                    final TopologyEntityDTO entity = iterator.next();
                    entities.add(entityWithAspects(entity, aspectMapper, requestedAspects));
                }
            }
            severityPopulator.populate(contextId, entities);
            return entities;
        }

        @Nonnull
        private ServiceEntityApiDTO entityWithAspects(@Nonnull final TopologyEntityDTO entity,
                @Nonnull EntityAspectMapper aspectMapper,
                @Nullable Collection<String> requestedAspects)
                throws ConversionException, InterruptedException {
            final ServiceEntityApiDTO se = serviceEntityMapper.toServiceEntityApiDTO(entity);
            if (CollectionUtils.isNotEmpty(requestedAspects)) {
                final Map<AspectName, EntityAspect> aspectsByName = new HashMap<>();
                final TemplateApiDTO template = new TemplateApiDTO();
                BaseApiDTO cloudTemplate = null;

                for (String requestedAspect: requestedAspects) {
                    final AspectName aspectName = AspectName.fromString(requestedAspect);
                    final EntityAspect entityAspect = aspectMapper.getAspectByEntity(entity, aspectName);
                    // aspect may be null, for example: cloud aspect for on_prem vm
                    if (entityAspect != null) {
                        aspectsByName.put(aspectName, entityAspect);
                        if (entityAspect instanceof CloudAspectApiDTO) {
                            CloudAspectApiDTO cloudAspect = (CloudAspectApiDTO)entityAspect;
                            if (cloudTemplate == null) {
                                cloudTemplate = cloudAspect.getTemplate();
                            }
                        }
                    }
                }

                if (cloudTemplate != null) {
                    template.setUuid(cloudTemplate.getUuid());
                    template.setDisplayName(cloudTemplate.getDisplayName());
                    se.setTemplate(template);
                }

                se.setAspectsByName(aspectsByName);
            } else {
                se.setAspectsByName(aspectMapper.getAspectsByEntity(entity));
            }

            return se;
        }

        /**
         * Set the cost price for entity from the input list, if available.
         *
         * @param entities The input list of service entities.
         */
        private void setEntitiesCost(Collection<ServiceEntityApiDTO> entities) {
            Map<Long, Double>  entityCostMap = serviceEntityMapper.computeCostPriceByEntity(
                    entities.stream()
                        .map(entity -> Long.valueOf(entity.getUuid()))
                        .collect(Collectors.toSet()));

                for (ServiceEntityApiDTO entity : entities) {
                    Double costPrice = entityCostMap.get(Long.valueOf(entity.getUuid()));
                    if (costPrice == null) {
                        entity.setCostPrice(0.0f);
                    } else {
                        entity.setCostPrice(costPrice.floatValue());
                    }
                }
        }
    }

    /**
     * A request for a dynamic search over the entities in the realtime topology.
     */
    public static class SearchRequest {
        private final long realtimeContextId;

        private final SearchServiceBlockingStub searchServiceBlockingStub;
        private final SearchServiceStub searchServiceStub;

        private final Collection<SearchParameters> params;

        private EntityAspectMapper aspectMapper = null;

        private PartialEntityRetriever retriever;

        private SearchRequest(final long realtimeContextId,
                             @Nonnull final SearchServiceBlockingStub searchServiceBlockingStub,
                             @Nonnull final SearchServiceStub searchServiceStub,
                             @Nonnull final SeverityPopulator severityPopulator,
                             @Nonnull final ServiceEntityMapper serviceEntityMapper,
                             @Nonnull final Collection<SearchParameters> params) {
            this.realtimeContextId = realtimeContextId;
            this.searchServiceBlockingStub = searchServiceBlockingStub;
            this.searchServiceStub = Objects.requireNonNull(searchServiceStub);
            this.params = params;

            this.retriever = new PartialEntityRetriever(type ->
                searchServiceBlockingStub.searchEntitiesStream(SearchEntitiesRequest.newBuilder()
                    .addAllSearchParameters(params)
                    .setReturnType(type)
                    .build()),
                serviceEntityMapper,
                severityPopulator);
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
         * Execute the request and return the set of matching OIDs. Note: this involves making
         * a remote call.
         */
        @Nonnull
        public Set<Long> getOids() {
            return Sets.newHashSet(searchServiceBlockingStub.searchEntityOids(
                SearchEntityOidsRequest.newBuilder()
                    .addAllSearchParameters(params)
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
                    SearchEntityOidsRequest.newBuilder().addAllSearchParameters(params).build(),
                    observer);
            return observer.getFuture();
        }

        /**
         * Execute the request and return the number of matches. Note: this involves making
         * a remote call.
         */
        public long count() {
            return searchServiceBlockingStub.countEntities(CountEntitiesRequest.newBuilder()
                .addAllSearchParameters(params)
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
            return retriever.getSEList(realtimeContextId, aspectMapper, null);
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
                final long oid) {
            super(SingleEntityRequest.class, realtimeContextId, repositoryServiceBlockingStub,
                    repositoryServiceStub, severityPopulator, serviceEntityMapper,
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
    }

    /**
     * A multi-get for entities in the topology, by ID.
     */
    public static class MultiEntityRequest extends EntitiesRequest<MultiEntityRequest> {
        public MultiEntityRequest(final long realtimeContextId,
                  @Nonnull final RepositoryServiceBlockingStub repositoryServiceBlockingStub,
                  @Nonnull final RepositoryServiceStub repositoryServiceStub,
                  @Nonnull final SeverityPopulator severityPopulator,
                  @Nonnull final ServiceEntityMapper serviceEntityMapper,
                  @Nonnull final Set<Long> oids) {
            super(MultiEntityRequest.class, realtimeContextId, repositoryServiceBlockingStub,
                    repositoryServiceStub, severityPopulator, serviceEntityMapper, oids);
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
        private final long realtimeContextId;

        private final Class<REQ> clazz;

        private boolean allowGetAll = false;

        protected EntityAspectMapper aspectMapper = null;

        protected Collection<String> aspects = null;

        protected final PartialEntityRetriever retriever;
        private final RetrieveTopologyEntitiesRequest.Builder requestBuilder;
        private final RepositoryServiceStub repositoryServiceStub;

        private EntitiesRequest(@Nonnull final Class<REQ> clazz,
                                final long realtimeContextId,
                                @Nonnull final RepositoryServiceBlockingStub repositoryServiceBlockingStub,
                                @Nonnull final RepositoryServiceStub repositoryServiceStub,
                                @Nonnull final SeverityPopulator severityPopulator,
                                @Nonnull final ServiceEntityMapper serviceEntityMapper,
                                @Nonnull final Set<Long> targetId) {
            this.clazz = clazz;
            this.realtimeContextId = realtimeContextId;
            this.repositoryServiceStub = Objects.requireNonNull(repositoryServiceStub);

            requestBuilder = RetrieveTopologyEntitiesRequest.newBuilder()
                    .addAllEntityOids(targetId)
                    .setTopologyContextId(realtimeContextId)
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
                    severityPopulator);
        }

        @Nonnull
        public REQ contextId(@Nullable final Long contextId) {
            this.requestBuilder.setTopologyContextId(
                    contextId != null ? contextId : realtimeContextId);
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
        public REQ restrictTypes(@Nonnull final Collection<UIEntityType> types) {
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
