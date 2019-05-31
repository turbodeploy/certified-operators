package com.vmturbo.api.component.communication;

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
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import io.grpc.StatusRuntimeException;

import com.vmturbo.api.component.external.api.mapper.SearchMapper;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.component.external.api.mapper.SeverityPopulator;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest.TopologyType;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesResponse;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;


/**
 * This class is an API wrapper for the Repository Component.
 *
 * TODO (roman, Dec 19 2016): This stuff shouldn't live here. We should migrate the API's to
 * gRPC instead :)
 */
public class RepositoryApi {

    private final Logger logger = LogManager.getLogger();

    private final SeverityPopulator severityPopulator;

    private final long realtimeTopologyContextId;

    private final RepositoryServiceBlockingStub repositoryService;

    private final SearchServiceBlockingStub searchServiceBlockingStub;

    public RepositoryApi(@Nonnull final SeverityPopulator severityPopulator,
                         @Nonnull final RepositoryServiceBlockingStub repositoryService,
                         @Nonnull final SearchServiceBlockingStub searchServiceBlockingStub,
                         final long realtimeTopologyContextId) {
        this.severityPopulator = Objects.requireNonNull(severityPopulator);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.searchServiceBlockingStub = searchServiceBlockingStub;
        this.repositoryService = repositoryService;
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
                // TODO (roman, May 23 2019) OM-44276: We should get the probe type map and pass
                // it to the mapper, in order to populate the type in the returned discoveredBy
                // field.
                response.getEntitiesList().stream()
                    .map(entity -> SearchMapper.seDTO(entity, Collections.emptyMap()))
                    .forEach(entities::add);
                nextCursor = response.getPaginationResponse().getNextCursor();
            } catch (StatusRuntimeException e) {
                logger.error("Error retrieving data: {}", e);
                break;
            }
        } while (!nextCursor.isEmpty());

        return severityPopulator.populate(realtimeTopologyContextId, entities);
    }

    /**
     * Request the full Service Entity description, {@link ServiceEntityApiDTO}, from the
     * Repository Component.
     *
     * @param serviceEntityId the unique id (uuid/oid) for the service entity to be retrieved.
     * @return the {@link ServiceEntityApiDTO} describing the Service Entity with the requested ID.
     * @throws UnknownObjectException if there is no service entity with the given UUID.
     */
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
            // TODO (roman, May 23 2019) OM-44276: We should get the probe type map and pass
            // it to the mapper, in order to populate the type in the returned discoveredBy
            // field.
            final ServiceEntityApiDTO entity =
                SearchMapper.seDTO(response.getEntities(0), Collections.emptyMap());
            return severityPopulator.populate(realtimeTopologyContextId, entity);
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
    public Map<Long, Optional<ServiceEntityApiDTO>> getServiceEntitiesById(@Nonnull final ServiceEntitiesRequest request) {
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
            // TODO (roman, May 23 2019) OM-44276: We should get the probe type map and pass
            // use it to populate the discoveredBy field in the returned entities.
            RepositoryDTOUtil.topologyEntityStream(repositoryService.retrieveTopologyEntities(reqBuilder.build()))
                .forEach(entity -> {
                    results.put(entity.getOid(),
                        Optional.of(ServiceEntityMapper.toServiceEntityApiDTO(entity, null)));
                });
            severityPopulator.populate(contextId, results);
            return results;
        } catch (StatusRuntimeException e) {
            logger.error("Failed to retrieve entities due to error: {}", e.getMessage());
            return Collections.emptyMap();
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
             * Override the topology context ID to use for the request. The default is to
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
             * Request the search of entities in the projected topology instead of the source
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
