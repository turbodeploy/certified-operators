package com.vmturbo.api.component.external.api.service;

import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.search.SearchFilterResolver;
import com.vmturbo.common.protobuf.search.TargetSearchServiceGrpc.TargetSearchServiceBlockingStub;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * Search filter resolver for search service.
 */
public class SearchServiceFilterResolver extends SearchFilterResolver {
    private final GroupExpander groupExpander;

    /**
     * Consctructs filter resolver.
     *
     * @param targetSearchService gRPC to resolve target searches
     * @param groupExpander group expander to use
     */
    public SearchServiceFilterResolver(@Nonnull TargetSearchServiceBlockingStub targetSearchService,
            @Nonnull GroupExpander groupExpander) {
        super(targetSearchService);
        this.groupExpander = Objects.requireNonNull(groupExpander);
    }

    @Nonnull
    @Override
    protected Set<Long> getGroupMembers(@Nonnull GroupFilter groupFilter) {
        return groupExpander.getGroupsWithMembers(
                GetGroupsRequest.newBuilder().setGroupFilter(groupFilter).build())
                .flatMap(groupAndMembers -> groupAndMembers.members().stream())
                .collect(Collectors.toSet());
    }

    @Nonnull
    @Override
    protected Set<Long> getGroupOwners(@Nonnull Collection<Long> groupIds,
            @Nullable GroupType groupType) {
        return groupExpander.getGroupOwners(groupIds, groupType);
    }
}
