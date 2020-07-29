package com.vmturbo.mediation.udt.explore;

import static com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.GetTopologyDataDefinitionResponse;
import static com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinitionEntry;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

import com.google.common.collect.Sets;

import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesResponse;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.SearchFilterResolver;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntityBatch;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * The class responsible for providing data entities to UDT collectors.
 */
public class DataProvider {

    private static final Logger LOGGER = LogManager.getLogger();

    private final DataRequests requests;
    private final RequestExecutor requestExecutor;
    private final SearchFilterResolver searchFilterResolver;

    /**
     * Constructor.
     *
     * @param requestExecutor - an instance of {@link RequestExecutor}.
     * @param requests        - an instance of {@link DataRequests}.
     * @param filterResolver  - an instance of {@link SearchFilterResolver}.
     */
    @ParametersAreNonnullByDefault
    public DataProvider(RequestExecutor requestExecutor, DataRequests requests,
                        SearchFilterResolver filterResolver) {
        this.requestExecutor = requestExecutor;
        this.requests = requests;
        this.searchFilterResolver = filterResolver;
    }

    /**
     * Makes a request to the Repository Component to get all entities with specified entity type
     * and with defined tag.
     *
     * @param tag        - tag key. Requested entities should have this tag.
     * @param entityType - type of requested entities.
     * @return a set of DTOs.
     */
    @Nonnull
    @ParametersAreNonnullByDefault
    public Set<TopologyEntityDTO> getEntitiesByTag(String tag, EntityType entityType) {
        SearchEntitiesRequest request = requests.entitiesByTagRequest(tag, entityType);
        SearchEntitiesResponse resp = requestExecutor.searchEntities(request);
        return resp.getEntitiesList().stream()
                .map(PartialEntity::getFullEntity).collect(Collectors.toSet());
    }

    /**
     * Retrieves all TDD models.
     *
     * @return a map with Key:TDD-ID, Value:TDD.
     */
    @Nonnull
    public Map<Long, TopologyDataDefinition> getTopologyDataDefinitions() {
        Iterator<GetTopologyDataDefinitionResponse> response = requestExecutor
                .getAllTopologyDataDefinitions(requests.tddRequest());
        LOGGER.trace("Topology data definition response:", response);
        Spliterator<GetTopologyDataDefinitionResponse> spliterator = Spliterators
                .spliteratorUnknownSize(response, 0);
        return StreamSupport.stream(spliterator, false)
                .filter(GetTopologyDataDefinitionResponse::hasTopologyDataDefinition)
                .map(GetTopologyDataDefinitionResponse::getTopologyDataDefinition)
                .collect(Collectors.toMap(TopologyDataDefinitionEntry::getId,
                        TopologyDataDefinitionEntry::getDefinition));
    }

    /**
     * Make a request to the Repository Component looking for topology entities by
     * defined filters.
     *
     * @param searchParameters - parameters for the query.
     * @return a set of DTOs.
     */
    @Nonnull
    @ParametersAreNonnullByDefault
    public Set<TopologyEntityDTO> searchEntities(List<SearchParameters> searchParameters) {
        searchParameters = searchParameters.stream()
                .map(searchFilterResolver::resolveExternalFilters)
                .collect(Collectors.toList());
        SearchEntitiesRequest request = requests.createFilterEntityRequest(searchParameters);
        SearchEntitiesResponse response = requestExecutor.searchEntities(request);
        return response.getEntitiesList().stream()
                .map(PartialEntity::getFullEntity).collect(Collectors.toSet());
    }

    /**
     * Retrieves topology entities by their OIDs.
     *
     * @param oids - OIDs to find.
     * @return a set of DTOs.
     */
    @Nonnull
    public Set<TopologyEntityDTO> getEntitiesByOids(@Nonnull Set<Long> oids) {
        RetrieveTopologyEntitiesRequest request = requests.getEntitiesByOidsRequest(oids);
        Spliterator<PartialEntityBatch> spliterator = Spliterators
                .spliteratorUnknownSize(requestExecutor.retrieveTopologyEntities(request), 0);
        return StreamSupport.stream(spliterator, false)
                .flatMap(batch -> batch.getEntitiesList().stream())
                .map(PartialEntity::getFullEntity)
                .collect(Collectors.toSet());
    }

    /**
     * Retrieves entities OIDs in specified group.
     *
     * @param id - group identification.
     * @return a set of OIDs.
     */
    @Nonnull
    public Set<Long> getGroupMembersIds(@Nonnull GroupID id) {
        final GetMembersRequest request = requests.getGroupMembersRequest(id.getId());
        try {
            final Iterator<GetMembersResponse> membersResponseIterator =
                    requestExecutor.getGroupMembers(request);
            return Sets.newHashSet(membersResponseIterator).stream()
                .flatMap(response -> response.getMemberIdList().stream())
                .collect(Collectors.toSet());
        } catch (StatusRuntimeException e) {
            LOGGER.error("Error getting members of group {} : {}", id, e.getMessage());
            return Sets.newHashSet();
        }
    }
}
