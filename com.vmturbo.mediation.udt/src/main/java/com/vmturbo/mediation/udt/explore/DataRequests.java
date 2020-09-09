package com.vmturbo.mediation.udt.explore;

import static com.vmturbo.common.protobuf.search.Search.SearchEntitiesRequest;
import static com.vmturbo.common.protobuf.search.Search.SearchParameters;

import java.util.Collection;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetOwnersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.GetTopologyDataDefinitionsRequest;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.SearchTagValuesRequest;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * Class responsible for create gRpc requests used by {@link DataProvider}.
 */
public class DataRequests {



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
     * @param searchParameters - parameters for search request.
     * @return an instance of {@link SearchEntitiesRequest}.
     */
    @Nonnull
    @ParametersAreNonnullByDefault
    SearchEntitiesRequest createFilterEntityRequest(List<SearchParameters> searchParameters) {
        return SearchEntitiesRequest.newBuilder()
                .setSearch(Search.SearchQuery
                        .newBuilder()
                        .addAllSearchParameters(searchParameters)
                        .build())
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

    /**
     * Request: get groups by filters.
     *
     * @param groupFilter - filters for the request.
     * @return an instance of {@link GetGroupsRequest}.
     */
    @Nonnull
    GetGroupsRequest getGroupRequest(@Nonnull GroupFilter groupFilter) {
        return GetGroupsRequest.newBuilder().setGroupFilter(groupFilter).build();
    }

    /**
     * Request: get groups owners.
     *
     * @param groupIds - IDs of groups.
     * @param type     - type of groups.
     * @return an instance of {@link GetOwnersRequest}.
     */
    @Nonnull
    GetOwnersRequest getGroupOwnerRequest(@Nonnull Collection<Long> groupIds, @Nullable GroupType type) {
        final GetOwnersRequest.Builder builder = GetOwnersRequest.newBuilder().addAllGroupId(groupIds);
        if (type != null) {
            builder.setGroupType(type);
        }
        return builder.build();
    }

    @Nonnull
    SearchTagValuesRequest getSearchTagValuesRequest(@Nonnull String tag, @Nonnull EntityType entityType) {
        return SearchTagValuesRequest.newBuilder()
                .setEntityType(entityType.getNumber())
                .setTagKey(tag)
                .build();
    }

}
