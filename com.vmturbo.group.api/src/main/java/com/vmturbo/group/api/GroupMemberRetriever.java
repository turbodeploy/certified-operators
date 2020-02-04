package com.vmturbo.group.api;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
    public Stream<GroupAndMembers> getGroupsWithMembers(
            @Nonnull final GetGroupsRequest getGroupsRequest) {
        final Iterable<Grouping> retIt = () -> groupServiceGrpc.getGroups(getGroupsRequest);
        return StreamSupport.stream(retIt.spliterator(), false)
                // In the future we could support a group API call here.
                .map(this::getMembersForGroup);
    }

    /**
     * Given a {@link Grouping}, get its members. If the {@link Grouping} is a dynamic group, this
     * may make a call to the group component.
     * Note - it's preferable to use this method for convenience, and to allow for (potential)
     * caching in the future.
     *
     * @param group The {@link Grouping}
     * @return  The {@link GroupAndMembers} describing the group and its members.
     */
    @Nonnull
    public GroupAndMembers getMembersForGroup(@Nonnull final Grouping group) {
        ImmutableGroupAndMembers.Builder retBuilder = ImmutableGroupAndMembers.builder()
                .group(group);
        final List<Long> members;
        if (group.getDefinition().hasStaticGroupMembers()) {
            members = GroupProtoUtil.getStaticMembers(group);
        } else {
            final GetMembersResponse groupMembersResp =
                    groupServiceGrpc.getMembers(GetMembersRequest.newBuilder()
                            .setId(group.getId())
                            .setExpectPresent(true)
                            .build());
            members = groupMembersResp.getMembers().getIdsList();
        }

        // now get the entities in the group. If this is a group-of-groups, the "members" in the group
        // will be the group id's, while the "entities" in the group will be all the service entities
        // contained within those nested groups. If this is NOT a group-of-groups, the "members" and
        // "entities" will be the same -- the set of service entities contained in the group.
        final Collection<Long> entities;
        // If the group is nested, make a 2nd call with the "expand nested groups" flag to fetch
        // the leaf entities in the nested groups.
        if (GroupProtoUtil.isNestedGroup(group)) {
            entities = groupServiceGrpc.getMembers(GetMembersRequest.newBuilder()
                    .setId(group.getId())
                    .setExpectPresent(true)
                    .setExpandNestedGroups(true)
                    .build())
                    .getMembers().getIdsList();
        } else {
            entities = members;
        }

        retBuilder.members(members);
        retBuilder.entities(entities);
        return retBuilder.build();
    }
}
