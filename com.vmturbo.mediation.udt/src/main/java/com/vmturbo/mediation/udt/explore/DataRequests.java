package com.vmturbo.mediation.udt.explore;

import static com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import static com.vmturbo.common.protobuf.search.Search.SearchEntitiesRequest;
import static com.vmturbo.common.protobuf.search.Search.SearchFilter;
import static com.vmturbo.common.protobuf.search.Search.SearchParameters;
import static com.vmturbo.common.protobuf.search.Search.SearchParameters.FilterSpecs;
import static com.vmturbo.common.protobuf.search.Search.SearchParameters.newBuilder;
import static com.vmturbo.common.protobuf.search.Search.SearchQuery;
import static com.vmturbo.common.protobuf.search.SearchProtoUtil.entityTypeFilter;
import static com.vmturbo.common.protobuf.search.SearchableProperties.DISPLAY_NAME;
import static com.vmturbo.common.protobuf.search.SearchableProperties.TAGS_TYPE_PROPERTY_NAME;

import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.GetTopologyDataDefinitionsRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.MapFilter;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Class responsible for create gRpc requests used by {@link DataProvider}.
 */
public class DataRequests {

    /**
     * Request: get entities with specified tag and entity type.
     *
     * @param tag        - entity tag key.
     * @param entityType - type of requested entities.
     * @return an instance of {@link SearchEntitiesRequest}.
     */
    @Nonnull
    @ParametersAreNonnullByDefault
    SearchEntitiesRequest entitiesByTagRequest(String tag, EntityType entityType) {
        SearchFilter searchFilter = SearchFilter.newBuilder()
                .setPropertyFilter(PropertyFilter.newBuilder()
                        .setPropertyName(TAGS_TYPE_PROPERTY_NAME)
                        .setMapFilter(MapFilter
                                .newBuilder()
                                .setKey(tag)
                                .setRegex(".+")
                                .build())
                        .build())
                .build();
        return SearchEntitiesRequest.newBuilder()
                .setSearch(SearchQuery.newBuilder()
                        .addSearchParameters(newBuilder()
                                .setStartingFilter(entityTypeFilter(entityType.getNumber()))
                                .addSearchFilter(searchFilter)
                                .build())
                        .build())
                .build();
    }

    /**
     * Request: get all topology data definitions.
     *
     * @return an instance of {@link GetTopologyDataDefinitionsRequest}.
     */
    @Nonnull
    GetTopologyDataDefinitionsRequest tddRequest() {
        return GetTopologyDataDefinitionsRequest.newBuilder().build();
    }

    /**
     * Request: get topology entities by defined filters.
     *
     * @param filters - filters for search request.
     * @param entityType - type of entity.
     * @return an instance of {@link SearchEntitiesRequest}.
     */
    @Nonnull
    @ParametersAreNonnullByDefault
    SearchEntitiesRequest createFilterEntityRequest(List<FilterSpecs> filters, EntityType entityType) {
        SearchQuery.Builder searchQuery = Search.SearchQuery.newBuilder();
        filters.forEach(filter -> searchQuery.addSearchParameters(SearchParameters.newBuilder()
                .setStartingFilter(entityTypeFilter(entityType.getNumber()))
                .setSourceFilterSpecs(filter)
                // In the first implementation we support the filter by name.
                // A search filter should be part of a definition data model and will be added
                // in the further versions.
                .addSearchFilter(SearchProtoUtil.searchFilterProperty(PropertyFilter.newBuilder()
                        .setPropertyName(DISPLAY_NAME)
                        .setStringFilter(PropertyFilter.StringFilter.newBuilder()
                                .setStringPropertyRegex(filter.getExpressionValue())
                                .setCaseSensitive(false)
                                .build())
                        .build()))
                .build()));
        return SearchEntitiesRequest.newBuilder()
                .setSearch(searchQuery)
                .build();
    }

    /**
     * Request: get entities by specified OIDs.
     *
     * @param oids - set of OIDs.
     * @return an instance of {@link RetrieveTopologyEntitiesRequest}.
     */
    @Nonnull
    RetrieveTopologyEntitiesRequest getEntitiesByOidsRequest(@Nonnull Set<Long> oids) {
        return RetrieveTopologyEntitiesRequest
                .newBuilder()
                .addAllEntityOids(oids)
                .build();
    }

    /**
     * Request: get members by group ID.
     *
     * @param groupId - group identification.
     * @return an instance of {@link GetMembersRequest}.
     */
    @Nonnull
    GetMembersRequest getGroupMembersRequest(long groupId) {
        return GetMembersRequest.newBuilder().addId(groupId).build();
    }
}
