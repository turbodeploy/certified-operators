package com.vmturbo.api.component.communication;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import com.vmturbo.api.component.external.api.mapper.SeverityPopulator;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.utils.ParamStrings;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc.EntitySeverityServiceBlockingStub;


/**
 * This class is an API wrapper for the Repository Component.
 *
 * TODO (roman, Dec 19 2016): This stuff shouldn't live here. We should migrate the API's to
 * gRPC instead :)
 */
public class RepositoryApi {

    /**
     * The prefix to paths in the repository.
     */
    private static final String REPOSITORY_PATH_PREFIX = "/repository/";

    /**
     * The supply chain URI component.
     */
    private static final String SUPPLYCHAIN_URI_COMPONENT = "/supplychain/";

    /**
     * The search URI component.
     */
    private static final String SEARCH_URI_COMPONENT = "/search/";

    /**
     * The SE URI component.
     */
    private static final String SERVICE_ENTITY_URI_COMPONENT = "/serviceentity/";

    /**
     * The URI path to search for service entities by OID.
     */
    private static final String SERVICE_ENTITY_MULTIGET_URI = SERVICE_ENTITY_URI_COMPONENT + "query/id";

    /**
     * The URI path to search for service entities by display name.
     */
    private static final String SERVICE_ENTITY_DISPLAY_NAME_URI = SERVICE_ENTITY_URI_COMPONENT + "query/displayname";

    /**
     * The query parameter used to specify the desired display name.
     */
    private static final String DISPLAY_NAME_QUERY_PARAM = "q";

    /**
     * The query parameter to specify whether to search in the projected topology.
     */
    private static final String PROJECTED_TOPOLOGY_QUERY_PARAM = "projected";

    /**
     * The query parameter to specify the topology context to search in.
     */
    private static final String CONTEXT_ID_QUERY_PARAM = "contextId";

    private final Logger logger = LogManager.getLogger();

    private final String repositoryHost;

    private final int repositoryPort;

    private final RestTemplate restTemplate;

    private final EntitySeverityServiceBlockingStub entitySeverityRpc;

    private final long realtimeTopologyContextId;

    public RepositoryApi(@Nonnull final String repositoryHost,
                         final int repositoryPort,
                         @Nonnull final RestTemplate restTemplate,
                         @Nonnull final EntitySeverityServiceBlockingStub entitySeverityRpcService,
                         final long realtimeTopologyContextId) {
        this.restTemplate = Objects.requireNonNull(restTemplate);
        this.repositoryHost = repositoryHost;
        this.repositoryPort = repositoryPort;
        this.entitySeverityRpc = Objects.requireNonNull(entitySeverityRpcService);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
    }

    /**
     * Request a collection of {@link ServiceEntityApiDTO}s from the repository, where each
     * DTO's display name contains a word starting with the provided string.
     *
     * <p>Note: At the time of this writing (Sep 26, 2016), the details of legal query strings
     * depend on ArangoDB's implementation. For example, ArangoDB doesn't handle things like
     * ":" in the string. Also, it will return entities where the displayNamePrefix is the prefix
     * of any word in the display name.
     *
     * @param displayNamePrefix The prefix of the display name to search for.
     * @return The {@link ServiceEntityApiDTO}s matching the query.
     *         The severity is not present for the entities.
     */
    @Nonnull
    public Collection<ServiceEntityApiDTO> getServiceEntityByDisplayName(
            @Nonnull final String displayNamePrefix) {
        final String getEntitiesRequest = newUriBuilder()
            .path(SERVICE_ENTITY_DISPLAY_NAME_URI)
            .queryParam(DISPLAY_NAME_QUERY_PARAM, displayNamePrefix)
            .build()
            .toUriString();

        try {
            final ResponseEntity<List<ServiceEntityApiDTO>> response =
                    restTemplate.exchange(getEntitiesRequest, HttpMethod.GET, null,
                        new ParameterizedTypeReference<List<ServiceEntityApiDTO>>() {});
            return response.getBody();
        } catch (RestClientException e) {
            logger.error("Error retrieving data through REST call {}: {}", getEntitiesRequest, e);
            throw new RuntimeException(
                    "Error retrieving data through REST call " + getEntitiesRequest, e);
        }
    }

    /**
     * Request a collection of {@link ServiceEntityApiDTO}s from the repository. Currently, only
     * support searching with entity types and scope.
     *
     * @param query Not yet used
     * @param types The types of entities, e.g., VirtualMachine, PhysicalMachine, ...
     * @param scope The scope used for searching, e.g., a single entity or the global environment
     * @param state Not yet used
     * @param groupType Not yet used
     * @return collection of service entity DTOs that match the search
     * @throws Exception if the search fails
     */
    @Nonnull
    public Collection<ServiceEntityApiDTO> getSearchResults(String query,
                                                            @Nullable List<String> types,
                                                            @Nonnull String scope,
                                                            @Nullable String state,
                                                            @Nullable String groupType) throws Exception {
        // TODO Now, we only support one type of entities in the search
        if (types == null || types.isEmpty()) {
            IllegalArgumentException e = new IllegalArgumentException(
                      "Invalid types argument for searching results of scope " + scope);
            logger.error(e);
            throw e;
        }
        final UriComponentsBuilder uriBuilder = newUriBuilder().path(SEARCH_URI_COMPONENT);
        // Allow multi-type search, e.g.
        // http://localhost:9000/repository/search/?types=Application&types=VirtualMachine&scope=Market
        types.stream().forEach(type -> uriBuilder.queryParam(ParamStrings.TYPES, type));
        final String getEntitiesRequest = uriBuilder
            .queryParam(ParamStrings.SCOPE, scope)
            .build()
            .toUriString();

        try {
            final List<ServiceEntityApiDTO> entityDtos =
                        restTemplate.exchange(getEntitiesRequest, HttpMethod.GET, null,
                        new ParameterizedTypeReference<List<ServiceEntityApiDTO>>() {})
                        .getBody();

            return SeverityPopulator.populate(entitySeverityRpc, realtimeTopologyContextId, entityDtos);
        } catch (RestClientException e) {
            logger.error("Error retrieving data through REST call {}: {}", getEntitiesRequest, e);
            throw new RuntimeException(
                    "Error retrieving data through REST call " + getEntitiesRequest, e);
        }
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

        final String getServiceEntityRequest = newUriBuilder()
                .pathSegment(SERVICE_ENTITY_URI_COMPONENT, Long.toString(serviceEntityId))
                .build()
                .toUriString();

        try {
            // this call may return a list of matches. The size is expected to be 1, or 0 for not
            // found. More than one is a serious error; The sort-of-baroque usage of
            // restTemplate.exchange() is required to specify the return type from the REST request
            // as a typed List.
            ResponseEntity<List<ServiceEntityApiDTO>> response =
                    restTemplate.exchange(getServiceEntityRequest, HttpMethod.GET, null,
                                          new ParameterizedTypeReference<List<ServiceEntityApiDTO>>() {
                                          });
            List<ServiceEntityApiDTO> results = response.getBody();
            if (results.size() == 0) {
                logger.error("Service entity not found for id: {}", serviceEntityId);
                throw new UnknownObjectException(
                        "service entity not found for id: " + serviceEntityId);
            }
            if (results.size() > 1) {
                logger.error("More than one entity found for id: {}", serviceEntityId);
                throw new RuntimeException("more than one entity found for id: " + serviceEntityId);
            }
            return SeverityPopulator.populate(entitySeverityRpc, realtimeTopologyContextId, results)
                .iterator()
                .next();
        } catch (RestClientException e) {
            logger.error("Error retrieving data through REST call {}: {}", getServiceEntityRequest, e);
            throw new RuntimeException(
                    "Error retrieving data through REST call " + getServiceEntityRequest, e);
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
        final long contextId = request.getTopologyContextId().orElse(realtimeTopologyContextId);
        final String getEntitiesByIdSetRequest = newUriBuilder()
                .path(SERVICE_ENTITY_MULTIGET_URI)
                .queryParam(PROJECTED_TOPOLOGY_QUERY_PARAM, request.searchProjectedTopology())
                .queryParam(CONTEXT_ID_QUERY_PARAM, contextId)
                .build()
                .toUriString();
        final Set<Long> requestedIds = request.getEntityIds();
        final Map<Long, Optional<ServiceEntityApiDTO>> retMap =
                new HashMap<>(requestedIds.size());
        requestedIds.forEach(id -> retMap.put(id, Optional.empty()));
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Set> idList = new HttpEntity<>(requestedIds, headers);
        try {
            final ResponseEntity<List<ServiceEntityApiDTO>> response =
                    restTemplate.exchange(getEntitiesByIdSetRequest, HttpMethod.POST, idList,
                            new ParameterizedTypeReference<List<ServiceEntityApiDTO>>() {});
            final List<ServiceEntityApiDTO> results = response.getBody();

            results.forEach(seDTO -> retMap.put(Long.parseLong(seDTO.getUuid()), Optional.of(seDTO)));

            return retMap;
        } catch (RestClientException e) {
            logger.error("Error retrieving service entities by ID during {}: {}",
                    getEntitiesByIdSetRequest, e);
            throw new RuntimeException("Error retrieving service entities by ID during: " +
                    getEntitiesByIdSetRequest, e);
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

    @Nonnull
    private UriComponentsBuilder newUriBuilder() {
        return UriComponentsBuilder.newInstance()
                .scheme("http")
                .host(repositoryHost)
                .port(repositoryPort)
                .path(REPOSITORY_PATH_PREFIX);
    }
}
