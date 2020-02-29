package com.vmturbo.group.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;

/**
 * Type to retrieve group and member information from the Group RPC service.
 */
public class GroupMemberRetriever {

    private final GroupServiceBlockingStub groupServiceGrpc;

    /**
     * Creates an instance of GroupMemberRetriever.
     *
     * @param groupServiceGrpc end point to retrieve Grouping information.
     */
    public GroupMemberRetriever(@Nonnull final GroupServiceBlockingStub groupServiceGrpc) {
        this.groupServiceGrpc = Objects.requireNonNull(groupServiceGrpc);
    }

    /**
     * Get multiple groups with their associated members.
     *
     * @param getGroupsRequest A request object outlining the criteria to use to get the groups.
     * @return A stream of {@link GroupAndMembers} describing the groups that matched the request
     *         and the members of those groups.
     */
    public List<GroupAndMembers> getGroupsWithMembers(
            @Nonnull final GetGroupsRequest getGroupsRequest) {
        final Iterator<Grouping> retIt = groupServiceGrpc.getGroups(getGroupsRequest);
        final List<Grouping> groups = new ArrayList<>();
        while (retIt.hasNext()) {
            groups.add(retIt.next());
        }
        return getMembersForGroup(groups);
    }

    /**
     * Given a {@link Grouping}, get its members. If the {@link Grouping} is a dynamic group, this
     * may make a call to the groups component.
     * Note - it's preferable to use this method for convenience, and to allow for (potential)
     * caching in the future.
     *
     * @param groups The {@link Grouping}
     * @return  The {@link GroupAndMembers} describing the groups and its members.
     */
    @Nonnull
    public List<GroupAndMembers> getMembersForGroup(@Nonnull final List<Grouping> groups) {
        final Map<Long, List<Long>> members = new HashMap<>();
        final GetMembersRequest.Builder membersBuilder =
                GetMembersRequest.newBuilder().setExpectPresent(true);
        for (Grouping group: groups) {
            if (group.getDefinition().hasStaticGroupMembers()) {
                members.put(group.getId(), GroupProtoUtil.getStaticMembers(group));
            } else {
                membersBuilder.addId(group.getId());
            }
        }
        populateMembers(members, membersBuilder.build());

        // now get the entities in the groups. If this is a groups-of-groups, the "members" in the groups
        // will be the groups id's, while the "entities" in the groups will be all the service entities
        // contained within those nested groups. If this is NOT a groups-of-groups, the "members" and
        // "entities" will be the same -- the set of service entities contained in the groups.
        final Map<Long, List<Long>> entities = new HashMap<>();
        final GetMembersRequest.Builder entitiesBuilder =
                GetMembersRequest.newBuilder().setExpectPresent(true).setExpandNestedGroups(true);
        for (Grouping group: groups) {
            // If the groups is nested, make a 2nd call with the "expand nested groups" flag to fetch
            // the leaf entities in the nested groups.
            if (GroupProtoUtil.isNestedGroup(group)) {
                entitiesBuilder.addId(group.getId());
            } else {
                entities.put(group.getId(), members.get(group.getId()));
            }
        }
        populateMembers(entities, entitiesBuilder.build());
        final List<GroupAndMembers> result = new ArrayList<>(groups.size());
        for (Grouping group : groups) {
            final long groupId = group.getId();
            final GroupAndMembers groupAndMembers = ImmutableGroupAndMembers.builder()
                    .group(group)
                    .members(members.get(groupId))
                    .entities(entities.get(groupId))
                    .build();
            result.add(groupAndMembers);
        }
        return result;
    }

    private void populateMembers(@Nonnull Map<Long, List<Long>> map,
            @Nonnull GetMembersRequest request) {
        if (request.getIdCount() > 0) {
            final Iterator<GetMembersResponse> groupMembersResp =
                    groupServiceGrpc.getMembers(request);
            while (groupMembersResp.hasNext()) {
                final GetMembersResponse response = groupMembersResp.next();
                map.put(response.getGroupId(), response.getMemberIdList());
            }
        }
    }
}
