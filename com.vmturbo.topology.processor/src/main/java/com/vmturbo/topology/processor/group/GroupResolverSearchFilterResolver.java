package com.vmturbo.topology.processor.group;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetOwnersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetOwnersRequest.Builder;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.SearchFilterResolver;
import com.vmturbo.group.api.GroupAndMembers;
import com.vmturbo.group.api.GroupMemberRetriever;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.topology.processor.rpc.SearchTargetsStreamObserver;
import com.vmturbo.topology.processor.rpc.TargetSearchRpcService;

/**
 * This class is used to convert the search filters before using them to search entities in the
 * repository. Some filters contain group / target information that the repository can't search by
 * (for example, "Cloud Provider" and "Kubernetes Cluster" are target names, and the repository can
 * only filter by target IDs).
 *
 * <p></p>This implementation uses TargetSearchRpcService without a stub since it is already in
 * the TP. Therefore we don't need to make an RPC call to use it.
 */
public class GroupResolverSearchFilterResolver extends SearchFilterResolver {

    private final GroupServiceBlockingStub groupServiceClient;
    private final GroupMemberRetriever groupMemberRetriever;
    private final TargetSearchRpcService targetSearchService;

    /**
     * Constructs the resolver using the specified group store.
     *
     * @param groupServiceClient group service
     * @param targetSearchService gRPC to resolve target searches
     */
    public GroupResolverSearchFilterResolver(
            @Nonnull GroupServiceBlockingStub groupServiceClient,
            @Nonnull TargetSearchRpcService targetSearchService) {
        super();
        this.groupServiceClient = groupServiceClient;
        this.groupMemberRetriever = new GroupMemberRetriever(groupServiceClient);
        this.targetSearchService = targetSearchService;
    }

    @Nonnull
    @Override
    protected Set<Long> getGroupMembers(@Nonnull GroupFilter groupFilter) {
        final List<GroupAndMembers> groups = groupMemberRetriever.getGroupsWithMembers(
                GetGroupsRequest.newBuilder().setGroupFilter(groupFilter).build());
        return groups.stream()
                .map(GroupAndMembers::members)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
    }

    @Nonnull
    @Override
    protected Set<Long> getGroupOwners(@Nonnull Collection<Long> groupIds, @Nullable GroupType groupType) {
        final Builder ownersRequest = GetOwnersRequest.newBuilder().addAllGroupId(groupIds);
        if (groupType != null) {
            ownersRequest.setGroupType(groupType);
        }
        return new HashSet<>(groupServiceClient.getOwnersOfGroups(ownersRequest.build())
                .getOwnerIdList());
    }

    @Nonnull
    @Override
    protected Collection<Long> getTargetIdsFromFilter(@Nonnull PropertyFilter filter) {
        SearchTargetsStreamObserver streamObserver = new SearchTargetsStreamObserver();
        targetSearchService.searchTargets(filter, streamObserver);
        return streamObserver.getTargetIds();
    }
}