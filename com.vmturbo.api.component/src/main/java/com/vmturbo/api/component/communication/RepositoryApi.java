package com.vmturbo.api.component.communication;

import java.nio.file.AccessDeniedException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.StatusRuntimeException;

import com.vmturbo.api.component.external.api.mapper.ExceptionMapper;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.component.external.api.mapper.SeverityPopulator;
import com.vmturbo.api.component.external.api.mapper.aspect.EntityAspectMapper;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.UnauthorizedObjectException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest.TopologyType;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesResponse;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.SearchTopologyEntityDTOsRequest;
import com.vmturbo.common.protobuf.search.Search.SearchTopologyEntityDTOsResponse;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.topology.processor.api.TargetData;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.TopologyProcessorException;


/**
 * This class is an API wrapper for the Repository Component.
 */
public class RepositoryApi {

    private final Logger logger = LogManager.getLogger();

    private final SeverityPopulator severityPopulator;

    private final long realtimeTopologyContextId;

    private final RepositoryServiceBlockingStub repositoryService;

    private final SearchServiceBlockingStub searchServiceBlockingStub;

    private final TopologyProcessor topologyProcessor;

    public RepositoryApi(@Nonnull final SeverityPopulator severityPopulator,
                         @Nonnull final RepositoryServiceBlockingStub repositoryService,
                         @Nonnull final SearchServiceBlockingStub searchServiceBlockingStub,
                         @Nonnull final TopologyProcessor topologyProcessor,
                         final long realtimeTopologyContextId) {
        this.severityPopulator = Objects.requireNonNull(severityPopulator);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.searchServiceBlockingStub = Objects.requireNonNull(searchServiceBlockingStub);
        this.repositoryService = Objects.requireNonNull(repositoryService);
        this.topologyProcessor = Objects.requireNonNull(topologyProcessor);
    }

    @Nonnull
    public Collection<ServiceEntityApiDTO> getSearchResults(@Nullable String query,
                                                            @Nullable List<String> types,
                                                            @Nullable String state) {
        // TODO Now, we only support one type of entities in the search
        if (types == null || types.isEmpty()) {
            IllegalArgumentException e = new IllegalArgumentException("Type must be set for search result.");
            logger.error(e);
            throw e;
        }

        final SearchParameters.Builder searchParamsBuilder =
            SearchProtoUtil.makeSearchParameters(SearchProtoUtil.entityTypeFilter(types));

        if (!StringUtils.isEmpty(query)) {
            searchParamsBuilder.addSearchFilter(SearchProtoUtil.searchFilterProperty(
                SearchProtoUtil.nameFilterRegex(query)));
        }

        if (!StringUtils.isEmpty(state)) {
            searchParamsBuilder.addSearchFilter(SearchProtoUtil.searchFilterProperty(
                SearchProtoUtil.stateFilter(state)));
        }

        final List<ServiceEntityApiDTO> entities = new ArrayList<>();
        String nextCursor = "";
        do {
            try {
                final SearchEntitiesResponse response =
                    searchServiceBlockingStub.searchEntities(SearchEntitiesRequest.newBuilder()
                        .addSearchParameters(searchParamsBuilder)
                        .setPaginationParams(PaginationParameters.newBuilder()
                            .setCursor(nextCursor))
                        .build());
                response.getEntitiesList().stream()
                    .map(entity -> ServiceEntityMapper.toServiceEntityApiDTO(entity, Collections.emptyMap()))
                    .forEach(entity -> {
                        entities.add(entity);
                        if (entity != null && entity.getDiscoveredBy() != null) {
                            populateTargetApiDTO(entity.getDiscoveredBy());
                        }
                    });
                nextCursor = response.getPaginationResponse().getNextCursor();
            } catch (StatusRuntimeException e) {
                logger.error("Error retrieving data: {}", e);
                break;
            }
        } while (!nextCursor.isEmpty());

        return severityPopulator.populate(realtimeTopologyContextId, entities);
    }

    /**
     * Requests the full Service Entity description, {@link ServiceEntityApiDTO}, from the
     * Repository Component.
     *
     * The functionality of this method is subsumed by
     * {@link RepositoryApi#getServiceEntityById(long, EntityAspectMapper)}.
     * No new code should use this method.
     * TODO: A future refactoring will remove this method ensuring that no existing caller breaks. (OM-47354)
     *
     * @param serviceEntityId the unique id (uuid/oid) for the service entity to be retrieved.
     * @return the {@link ServiceEntityApiDTO} describing the Service Entity with the requested ID.
     * @throws UnknownObjectException if there is no service entity with the given UUID.
     */
    @Deprecated
    public ServiceEntityApiDTO getServiceEntityForUuid(long serviceEntityId)
            throws UnknownObjectException {
        final SearchParameters params =
                SearchProtoUtil.makeSearchParameters(SearchProtoUtil.idFilter(serviceEntityId))
                        .build();
        final SearchEntitiesResponse response =
                searchServiceBlockingStub.searchEntities(SearchEntitiesRequest.newBuilder()
                        .addSearchParameters(params)
                        .setPaginationParams(PaginationParameters.getDefaultInstance())
                        .build());
        if (response.getEntitiesList().isEmpty()) {
            throw new UnknownObjectException(Long.toString(serviceEntityId));
        } else {
            // We should only get one entity for a particular ID. If we get more than one
            // that means something's wrong in our system, but we'll try to keep
            // going :)
            if (response.getEntitiesCount() > 1) {
                logger.error("Got multiple entities for ID {}! This shouldn't happen.", serviceEntityId);
            }
            final ServiceEntityApiDTO entity =
                ServiceEntityMapper.toServiceEntityApiDTO(response.getEntities(0), Collections.emptyMap());
            if (entity != null && entity.getDiscoveredBy() != null) {
                populateTargetApiDTO(entity.getDiscoveredBy());
            }
            return severityPopulator.populate(realtimeTopologyContextId, entity);
        }
    }

    /**
     * Requests the full Service Entity description, {@link ServiceEntityApiDTO}, from the
     * Repository Component.
     *
     * @param serviceEntityId the unique id (uuid/oid) for the service entity to be retrieved.
     * @param entityAspectMapper An optional {@link EntityAspectMapper}.
     *                           If non-null, we use it to populate the result with entity aspects.
     * @return the {@link ServiceEntityApiDTO} describing the Service Entity with the requested ID.
     * @throws UnknownObjectException if there is no service entity with the given UUID.
     */
    public ServiceEntityApiDTO getServiceEntityById(
            long serviceEntityId, @Nullable final EntityAspectMapper entityAspectMapper)
            throws UnknownObjectException {
        final ServiceEntitiesRequest serviceEntitiesRequest =
                ServiceEntitiesRequest.newBuilder(Collections.singleton(serviceEntityId)).build();
        final Collection<Optional<ServiceEntityApiDTO>> resultSet =
                getServiceEntitiesById(serviceEntitiesRequest, entityAspectMapper).values();
        if (resultSet.isEmpty()) {
            throw new UnknownObjectException("Entity with id " + serviceEntityId + " was not found");
        }
        if (resultSet.size() > 1) {
            final String message = "Multiple entities with id " + serviceEntityId + " were found";
            logger.error(message);
            throw new UnknownObjectException(message);
        }
        return resultSet.iterator().next().orElseThrow(() ->
                  new UnknownObjectException("Entity with id " + serviceEntityId + " was not found"));
    }

    /**
     * Requests several service entity descriptions from the Repository.
     *
     * @param request The {@link ServiceEntitiesRequest} describing the search.
     * @param entityAspectMapper An optional {@link EntityAspectMapper}.
     *                           If non-null, we use it to populate the result with entity aspects.
     * @return A map of OID -> an optional containing the entity, or an empty optional if the entity was not found.
     *         Each OID in entityIds will have a matching entry in the returned map.
     */
    @Nonnull
    public Map<Long, Optional<ServiceEntityApiDTO>> getServiceEntitiesById(
            @Nonnull final ServiceEntitiesRequest request,
            @Nullable final EntityAspectMapper entityAspectMapper) {
        if (request.getEntityIds().isEmpty()) {
            return Collections.emptyMap();
        }
        final long contextId = request.getTopologyContextId().orElse(realtimeTopologyContextId);
        final RetrieveTopologyEntitiesRequest.Builder reqBuilder =
            RetrieveTopologyEntitiesRequest.newBuilder();
        request.getTopologyContextId().ifPresent(reqBuilder::setTopologyContextId);
        reqBuilder.addAllEntityOids(request.getEntityIds());
        if (request.searchProjectedTopology()) {
            reqBuilder.setTopologyType(TopologyType.PROJECTED);
        }

        final Map<Long, Optional<ServiceEntityApiDTO>> results = request.getEntityIds().stream()
            .collect(Collectors.toMap(Function.identity(), id -> Optional.empty()));
        try {
            RepositoryDTOUtil.topologyEntityStream(repositoryService.retrieveTopologyEntities(reqBuilder.build()))
                .forEach(entity -> {
                    final ServiceEntityApiDTO serviceEntityApiDTO =
                            ServiceEntityMapper.toServiceEntityApiDTO(entity, entityAspectMapper);
                    if (serviceEntityApiDTO != null && serviceEntityApiDTO.getDiscoveredBy() != null) {
                        populateTargetApiDTO(serviceEntityApiDTO.getDiscoveredBy());
                    }
                    results.put(entity.getOid(), Optional.of(serviceEntityApiDTO));
                });
            severityPopulator.populate(contextId, results);
            return results;
        } catch (StatusRuntimeException e) {
            logger.error("Failed to retrieve entities due to error: {}", e.getMessage());
            return Collections.emptyMap();
        }
    }

    /**
     * Request several service entity descriptions from the Repository.
     *
     * @param request The {@link ServiceEntitiesRequest} describing the search.
     * @return A map of OID -> an optional containing the entity, or an empty optional if the entity was not found.
     *         Each OID in entityIds will have a matching entry in the returned map.
     */
    @Nonnull
    public Map<Long, Optional<ServiceEntityApiDTO>> getServiceEntitiesById(
            @Nonnull final ServiceEntitiesRequest request) {
        return getServiceEntitiesById(request, null);
    }

    /**
     * Fetches an entity in {@link TopologyEntityDTO} form, given its oid.
     *
     * @param oid the oid of the entity to fetch.
     * @return the entity in a {@link TopologyEntityDTO} format.
     *
     * @throws UnknownObjectException if the entity was not found.
     * @throws OperationFailedException if the operation failed.
     * @throws UnauthorizedObjectException if user does not have proper access privileges.
     * @throws InterruptedException if thread is interrupted during processing.
     * @throws AccessDeniedException if user is properly authenticated.
     */
    @Nonnull
    public TopologyEntityDTO getTopologyEntityDTO(long oid) throws Exception {
        // get information about this entity from the repository
        final SearchTopologyEntityDTOsResponse searchTopologyEntityDTOsResponse;
        try {
            searchTopologyEntityDTOsResponse =
                    searchServiceBlockingStub.searchTopologyEntityDTOs(
                            SearchTopologyEntityDTOsRequest.newBuilder()
                                    .addEntityOid(oid)
                                    .build());
        } catch (StatusRuntimeException e) {
            throw ExceptionMapper.translateStatusException(e);
        }
        if (searchTopologyEntityDTOsResponse.getTopologyEntityDtosCount() == 0) {
            final String message = "Error fetching entity with uuid: " + oid;
            logger.error(message);
            throw new UnknownObjectException(message);
        }
        return searchTopologyEntityDTOsResponse.getTopologyEntityDtos(0);
    }

    /**
     * Populates a {@link TargetApiDTO} structure which has already a target id,
     * with the display name, the probe id and the probe type.
     * If communication with the topology processor fails or if the topology processor
     * does not fetch the requested information, the structure will not be populated.
     *
     * @param targetApiDTO the structure to populate.
     */
    public void populateTargetApiDTO(@Nonnull TargetApiDTO targetApiDTO) {
        if (targetApiDTO.getUuid() == null) {
            return;
        }
        long targetId = Long.valueOf(targetApiDTO.getUuid());

        // get target info from the topology processor
        final TargetInfo targetInfo;
        try {
            targetInfo = topologyProcessor.getTarget(targetId);
        } catch (TopologyProcessorException | CommunicationException e) {
            logger.warn("Error communicating with the topology processor", e);
            return;
        }
        targetApiDTO.setDisplayName(
                TargetData.getDisplayName(targetInfo).orElseGet(() -> {
                    logger.warn("Cannot find the display name of target with id {}", targetId);
                    return "";
                }));

        // fetch information about the probe, and store the probe type in the result
        try {
            targetApiDTO.setType(topologyProcessor.getProbe(targetInfo.getProbeId()).getType());
        } catch (TopologyProcessorException | CommunicationException e) {
            logger.warn("Error communicating with the topology processor", e);
        }
    }

    /**
     * Fetches the entity for the given string uuid from repository.
     *
     * @param uuid string id
     * @return entity or empty if not existing
     */
    public Optional<Search.Entity> fetchEntity(@Nonnull String uuid) {
        try {
            long entityOid = Long.valueOf(uuid);
            List<Search.Entity> entities = searchServiceBlockingStub.searchEntities(
                    SearchEntitiesRequest.newBuilder()
                            .addEntityOid(entityOid)
                            .setPaginationParams(PaginationParameters.newBuilder().setLimit(1).build())
                            .build()).getEntitiesList();
            return entities.isEmpty() ? Optional.empty() : Optional.of(entities.get(0));
        } catch (StatusRuntimeException | NumberFormatException e) {
            return Optional.empty();
        }
    }

    /**
     * A request for a multi-get for information about a set of service entities.
     */
    public static class ServiceEntitiesRequest {

        private final Set<Long> entityIds;

        /**
         * See: {@link Builder#setTopologyContextId(long)}.
         */
        private final Optional<Long> topologyContextId;

        /**
         * See: {@link Builder#searchProjectedTopology()}.
         */
        private final boolean searchProjectedTopology;

        private ServiceEntitiesRequest(final Optional<Long> topologyContextId,
                                       @Nonnull final Set<Long> entityIds,
                                       final boolean searchProjectedTopology) {
            this.topologyContextId = topologyContextId;
            this.entityIds = Objects.requireNonNull(entityIds);
            this.searchProjectedTopology = searchProjectedTopology;
        }

        public Optional<Long> getTopologyContextId() {
            return topologyContextId;
        }

        public Set<Long> getEntityIds() {
            return entityIds;
        }

        public boolean searchProjectedTopology() {
            return searchProjectedTopology;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            } else if (!(other instanceof ServiceEntitiesRequest)) {
                return false;
            } else {
                final ServiceEntitiesRequest otherReq = (ServiceEntitiesRequest)other;
                return Objects.equals(entityIds, otherReq.entityIds) &&
                    Objects.equals(topologyContextId, otherReq.topologyContextId) &&
                    searchProjectedTopology == otherReq.searchProjectedTopology;
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(entityIds, topologyContextId, searchProjectedTopology);
        }

        public static Builder newBuilder(@Nonnull final Set<Long> entityIds) {
            return new Builder(entityIds);
        }

        public static class Builder {
            private Optional<Long> topologyContextId = Optional.empty();
            private boolean searchProjectedTopology = false;
            private final Set<Long> entityIds;

            public Builder(@Nonnull final Set<Long> entityIds) {
                this.entityIds = entityIds;
            }

            /**
             * Overrides the topology context ID to use for the request. The default is to
             * search in the realtime topology context.
             *
             * @param contextId The desired topology context ID.
             * @return The builder, for method chaining.
             */
            public Builder setTopologyContextId(final long contextId) {
                this.topologyContextId = Optional.of(contextId);
                return this;
            }

            /**
             * Requests the search of entities in the projected topology instead of the source
             * topology. The projected topology is the simulated result of applying all actions
             * recommended by the market to the source topology in the same topology context.
             * <p>
             * Search in the projected topology if the entities you're looking for may include
             * entities created (in the simulation) by the market - e.g. if the market recommends
             * provisioning a host, and you're looking for that host.
             *
             * @return The builder, for method chaining.
             */
            public Builder searchProjectedTopology() {
                this.searchProjectedTopology = true;
                return this;
            }

            public ServiceEntitiesRequest build() {
                return new ServiceEntitiesRequest(topologyContextId, entityIds,
                        searchProjectedTopology);
            }
        }
    }
}
