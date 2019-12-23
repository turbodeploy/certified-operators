package com.vmturbo.group.service;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.search.SearchFilterResolver;
import com.vmturbo.common.protobuf.search.TargetSearchServiceGrpc.TargetSearchServiceBlockingStub;
import com.vmturbo.group.group.IGroupStore;

/**
 * Search filter resolver for group component.
 */
public class GroupComponentSearchFilterResolver extends SearchFilterResolver {

    private final IGroupStore groupStore;

    /**
     * Constructs the resolver using the specified group store.
     *
     * @param groupStore group store to use
     * @param targetSearchService gRPC to resolve target searches
     */
    public GroupComponentSearchFilterResolver(
            @Nonnull TargetSearchServiceBlockingStub targetSearchService,
            @Nonnull IGroupStore groupStore) {
        super(targetSearchService);
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
}
