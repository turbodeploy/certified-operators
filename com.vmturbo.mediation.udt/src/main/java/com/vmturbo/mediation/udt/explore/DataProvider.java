package com.vmturbo.mediation.udt.explore;

import static com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.GetTopologyDataDefinitionResponse;
import static com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinitionEntry;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import com.google.common.collect.Sets;

import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesResponse;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.SearchTagValuesRequest;
import com.vmturbo.common.protobuf.search.Search.TaggedEntities;
import com.vmturbo.common.protobuf.search.SearchFilterResolver;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchableProperties;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.ProbeInfo;
import com.vmturbo.topology.processor.api.TargetInfo;

/**
 * The class responsible for providing data entities to UDT collectors.
 */
public class DataProvider {

    private static final Logger LOGGER = LogManager.getLogger();

    private static final List<String> supportedEntityTypes = Arrays.asList(
            ApiEntityType.BUSINESS_APPLICATION.apiStr(),
            ApiEntityType.BUSINESS_TRANSACTION.apiStr(),
            ApiEntityType.SERVICE.apiStr()
    );

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
     * Retrieve a map of tag values to a list of entities OIDs.
     *
     * @param tagKey     - name of the tag.
     * @param entityType - type of entities.
     * @return a map with Key:Tag-value, Value:List if OIDs.
     */
    @Nonnull
    @ParametersAreNonnullByDefault
    public Map<String, TaggedEntities> retrieveTagValues(String tagKey, EntityType entityType) {
        final SearchTagValuesRequest request = requests.getSearchTagValuesRequest(tagKey, entityType);
        return requestExecutor.getTagValues(request);
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

    /**
     * Retrieves ID of the UDT target.
     *
     * @return ID.
     */
    @Nullable
    public Long getUdtTargetId() {
        try {
            final ProbeInfo probe = requestExecutor.findProbe(SDKProbeType.UDT);
            if (Objects.nonNull(probe)) {
                List<TargetInfo> udtTargets = requestExecutor.findTarget(probe.getId());
                // It`s expected only one UDT target registered.
                if (udtTargets.size() == 1) {
                    return udtTargets.get(0).getId();
                }
            }
        } catch (CommunicationException e) {
            LOGGER.error("Error while getting UDT target ID.", e);
        }
        return null;
    }

    /**
     * Retrieves topology entities discovered by specific target.
     *
     * @param targetId - ID of the target.
     * @return set of entities.
     */
    @Nonnull
    public Set<TopologyEntityDTO> searchEntitiesByTargetId(long targetId) {
        final SearchParameters searchParameters = getSearchByTargetIdRequest(targetId);
        return searchEntities(Collections.singletonList(searchParameters));
    }

    @Nonnull
    private SearchParameters getSearchByTargetIdRequest(long targetId) {
        final Search.SearchFilter filter = SearchProtoUtil.searchFilterProperty(
                Search.PropertyFilter.newBuilder()
                        .setPropertyName(SearchableProperties.EXCLUSIVE_DISCOVERING_TARGET)
                        .setStringFilter(SearchProtoUtil.stringFilterExact(
                                Collections.singleton(String.valueOf(targetId)),
                                true, false)).build());
        return SearchParameters.newBuilder().addSearchFilter(filter)
                .setStartingFilter(SearchProtoUtil.entityTypeFilter(supportedEntityTypes))
                .build();
    }
}
