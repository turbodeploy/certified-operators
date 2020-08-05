package com.vmturbo.group.service;

import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.SearchFilterResolver;
import com.vmturbo.common.protobuf.search.TargetSearchServiceGrpc.TargetSearchServiceBlockingStub;
import com.vmturbo.group.group.IGroupStore;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * Search filter resolver for group component.
 */
public class GroupComponentSearchFilterResolver extends SearchFilterResolver {

    private final IGroupStore groupStore;
    private final TargetSearchServiceBlockingStub targetSearchService;

    /**
     * Constructs the resolver using the specified group store.
     *
     * @param groupStore group store to use
     * @param targetSearchService gRPC to resolve target searches
     */
    public GroupComponentSearchFilterResolver(
            @Nonnull TargetSearchServiceBlockingStub targetSearchService,
            @Nonnull IGroupStore groupStore) {
        this.targetSearchService = Objects.requireNonNull(targetSearchService);
        this.groupStore = Objects.requireNonNull(groupStore);
    }

    @Nonnull
    @Override
    protected Set<Long> getGroupMembers(@Nonnull GroupFilter groupFilter) {
        return groupStore.getGroups(groupFilter)
                .stream()
                .map(Grouping::getDefinition)
                .flatMap(clusterInfo -> GroupProtoUtil.getAllStaticMembers(clusterInfo).stream())
                .collect(Collectors.toSet());
    }

    @Nonnull
    @Override
    protected Set<Long> getGroupOwners(@Nonnull Collection<Long> groupIds,
            @Nullable GroupType groupType) {
        return groupStore.getOwnersOfGroups(groupIds, groupType);
    }

    @Nonnull
    @Override
    protected Collection<Long> getTargetIdsFromFilter(@Nonnull PropertyFilter filter) {
        return targetSearchService.searchTargets(filter).getTargetsList();
    }
}
