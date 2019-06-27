package com.vmturbo.api.component.communication;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Sets;

import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.component.external.api.mapper.SeverityPopulator;
import com.vmturbo.api.component.external.api.mapper.aspect.EntityAspectMapper;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest.TopologyType;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.search.Search.CountEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.SearchEntityOidsRequest;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntityBatch;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;

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

    private static final Logger logger = LogManager.getLogger();

    private final SeverityPopulator severityPopulator;

    private final long realtimeTopologyContextId;

    private final RepositoryServiceBlockingStub repositoryService;

    private final SearchServiceBlockingStub searchServiceBlockingStub;

    private final ServiceEntityMapper serviceEntityMapper;

    public RepositoryApi(@Nonnull final SeverityPopulator severityPopulator,
                         @Nonnull final RepositoryServiceBlockingStub repositoryService,
                         @Nonnull final SearchServiceBlockingStub searchServiceBlockingStub,
                         @Nonnull final ServiceEntityMapper serviceEntityMapper,
                         final long realtimeTopologyContextId) {
        this.severityPopulator = Objects.requireNonNull(severityPopulator);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.searchServiceBlockingStub = Objects.requireNonNull(searchServiceBlockingStub);
        this.serviceEntityMapper = Objects.requireNonNull(serviceEntityMapper);
        this.repositoryService = Objects.requireNonNull(repositoryService);
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
        return newSearchRequest(Collections.singleton(params));
    }

    @Nonnull
    private SearchRequest newSearchRequest(@Nonnull Collection<SearchParameters> params) {
        return new SearchRequest(realtimeTopologyContextId, searchServiceBlockingStub,
            severityPopulator, serviceEntityMapper, params);
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
            severityPopulator, serviceEntityMapper, oid);
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
            severityPopulator, serviceEntityMapper, oids);
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
        Stream<ApiPartialEntity> getEntities() {
            return entityStream(Type.API)
                .map(PartialEntity::getApi);
        }

        @Nonnull
        Map<Long, ServiceEntityApiDTO> getSEMap(final long contextId,
                                                @Nullable EntityAspectMapper aspectMapper) {
            final Map<Long, ServiceEntityApiDTO> entities;
            if (aspectMapper == null) {
                entities = getEntities()
                    .collect(Collectors.toMap(
                        ApiPartialEntity::getOid,
                        serviceEntityMapper::toServiceEntityApiDTO));
            } else {
                entities = getFullEntities()
                    .collect(Collectors.toMap(
                        TopologyEntityDTO::getOid,
                        entity -> entityWithAspects(entity, aspectMapper)));
            }
            severityPopulator.populate(contextId, entities);
            return entities;
        }

        @Nonnull
        List<ServiceEntityApiDTO> getSEList(final long contextId,
                                            @Nullable EntityAspectMapper aspectMapper) {
            final List<ServiceEntityApiDTO> entities;
            if (aspectMapper == null) {
                entities = getEntities()
                    .map(serviceEntityMapper::toServiceEntityApiDTO)
                    .collect(Collectors.toList());
            } else {
                entities = getFullEntities()
                    .map(entity -> entityWithAspects(entity, aspectMapper))
                    .collect(Collectors.toList());
            }
            severityPopulator.populate(contextId, entities);
            return entities;
        }

        @Nonnull
        private ServiceEntityApiDTO entityWithAspects(@Nonnull final TopologyEntityDTO entity,
                                                      @Nonnull EntityAspectMapper aspectMapper) {
            final ServiceEntityApiDTO se = serviceEntityMapper.toServiceEntityApiDTO(entity);
            se.setAspects(aspectMapper.getAspectsByEntity(entity));
            return se;
        }

    }

    /**
     * A request for a dynamic search over the entities in the realtime topology.
     */
    public static class SearchRequest {
        private final long realtimeContextId;

        private final SearchServiceBlockingStub searchServiceBlockingStub;

        private final Collection<SearchParameters> params;

        private EntityAspectMapper aspectMapper = null;

        private PartialEntityRetriever retriever;

        private SearchRequest(final long realtimeContextId,
                             @Nonnull final SearchServiceBlockingStub searchServiceBlockingStub,
                             @Nonnull final SeverityPopulator severityPopulator,
                             @Nonnull final ServiceEntityMapper serviceEntityMapper,
                             @Nonnull final Collection<SearchParameters> params) {
            this.realtimeContextId = realtimeContextId;
            this.searchServiceBlockingStub = searchServiceBlockingStub;
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

        @Nonnull
        public Stream<ApiPartialEntity> getEntities() {
            return retriever.getEntities();
        }

        /**
         * Get the {@link ServiceEntityApiDTO}s that match the search, arranged by OID.
         *
         * The {@link ServiceEntityApiDTO}s will be fully populated with target information,
         * and aspects if an aspect mapper was added via {@link SearchRequest#useAspectMapper(EntityAspectMapper)}.
         */
        @Nonnull
        public Map<Long, ServiceEntityApiDTO> getSEMap() {
            return retriever.getSEMap(realtimeContextId, aspectMapper);
        }

        /**
         * Get the {@link ServiceEntityApiDTO}s that match the search.
         *
         * The {@link ServiceEntityApiDTO}s will be fully populated with target information,
         * and aspects if an aspect mapper was added via {@link SearchRequest#useAspectMapper(EntityAspectMapper)}.
         */
        @Nonnull
        public List<ServiceEntityApiDTO> getSEList() {
            return retriever.getSEList(realtimeContextId, aspectMapper);
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
                @Nonnull final SeverityPopulator severityPopulator,
                @Nonnull final ServiceEntityMapper serviceEntityMapper,
                final long oid) {
            super(SingleEntityRequest.class, realtimeContextId,
                repositoryServiceBlockingStub, severityPopulator, serviceEntityMapper,
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

        @Nonnull
        public Optional<ApiPartialEntity> getEntity() {
            return retriever.getEntities().findFirst();
        }

        /**
         * Get the {@link ServiceEntityApiDTO}s that matches the OID.
         *
         * The {@link ServiceEntityApiDTO} will be fully populated with target information,
         * and aspects if an aspect mapper was added via {@link SingleEntityRequest#useAspectMapper(EntityAspectMapper)}.
         */
        @Nonnull
        public Optional<ServiceEntityApiDTO> getSE() {
            return retriever.getSEList(getContextId(), aspectMapper).stream().findFirst();
        }
    }

    /**
     * A multi-get for entities in the topology, by ID.
     */
    public static class MultiEntityRequest extends EntitiesRequest<MultiEntityRequest> {
        public MultiEntityRequest(final long realtimeContextId,
                  @Nonnull final RepositoryServiceBlockingStub repositoryServiceBlockingStub,
                  @Nonnull final SeverityPopulator severityPopulator,
                  @Nonnull final ServiceEntityMapper serviceEntityMapper,
                  @Nonnull final Set<Long> oids) {
            super(MultiEntityRequest.class, realtimeContextId,
                repositoryServiceBlockingStub, severityPopulator, serviceEntityMapper, oids);
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

        @Nonnull
        public Stream<ApiPartialEntity> getEntities() {
            return retriever.getEntities();
        }

        /**
         * Get the {@link ServiceEntityApiDTO}s that match the search, arranged by OID.
         *
         * The {@link ServiceEntityApiDTO}s will be fully populated with target information,
         * and aspects if an aspect mapper was added via {@link SearchRequest#useAspectMapper(EntityAspectMapper)}.
         */
        @Nonnull
        public Map<Long, ServiceEntityApiDTO> getSEMap() {
            return retriever.getSEMap(getContextId(), aspectMapper);
        }

        /**
         * Get the {@link ServiceEntityApiDTO}s that match the search.
         *
         * The {@link ServiceEntityApiDTO}s will be fully populated with target information,
         * and aspects if an aspect mapper was added via {@link MultiEntityRequest#useAspectMapper(EntityAspectMapper)}.
         */
        @Nonnull
        public List<ServiceEntityApiDTO> getSEList() {
            return retriever.getSEList(getContextId(), aspectMapper);
        }
    }

    public static class EntitiesRequest<REQ extends EntitiesRequest> {
        private final long realtimeContextId;

        private final Class<REQ> clazz;

        private boolean allowGetAll = false;

        private boolean projectedTopology = false;

        private Long contextId = null;

        protected EntityAspectMapper aspectMapper = null;

        private Set<Integer> restrictedTypes = new HashSet<>();

        protected final PartialEntityRetriever retriever;

        private EntitiesRequest(@Nonnull final Class<REQ> clazz,
                                final long realtimeContextId,
                                @Nonnull final RepositoryServiceBlockingStub repositoryServiceBlockingStub,
                                @Nonnull final SeverityPopulator severityPopulator,
                                @Nonnull final ServiceEntityMapper serviceEntityMapper,
                                @Nonnull final Set<Long> targetId) {
            this.clazz = clazz;
            this.realtimeContextId = realtimeContextId;
            this.retriever = new PartialEntityRetriever(
                type -> {
                    if (targetId.isEmpty() && !allowGetAll) {
                        return Collections.emptyIterator();
                    }

                    final long topoContextId = contextId == null ? realtimeContextId : contextId;

                    final TopologyType topologyType;
                    if (projectedTopology) {
                        topologyType = TopologyType.PROJECTED;
                    } else if (topoContextId != realtimeContextId) {
                        // In plans we don't store source entities, so requesting them will return
                        // nothing. Look for everything in the projected topology.
                        topologyType = TopologyType.PROJECTED;
                    } else {
                        topologyType = TopologyType.SOURCE;
                    }

                    final RetrieveTopologyEntitiesRequest request = RetrieveTopologyEntitiesRequest.newBuilder()
                        .addAllEntityOids(targetId)
                        .setTopologyContextId(topoContextId)
                        .setTopologyType(topologyType)
                        .addAllEntityType(restrictedTypes)
                        .setReturnType(type)
                        .build();
                    return repositoryServiceBlockingStub.retrieveTopologyEntities(request);
                },
                serviceEntityMapper,
                severityPopulator);
        }

        @Nonnull
        public REQ contextId(@Nullable final Long contextId) {
            this.contextId = contextId;
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
            projectedTopology = true;
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

        protected long getContextId() {
            return contextId == null ? realtimeContextId : contextId;
        }
    }

}
