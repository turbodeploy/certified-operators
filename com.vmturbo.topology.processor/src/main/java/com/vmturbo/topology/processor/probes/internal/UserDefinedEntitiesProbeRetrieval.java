package com.vmturbo.topology.processor.probes.internal;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.topology.processor.actions.data.EntityRetriever;

/**
 * A class responsible for collecting groups and group members.
 */
public class UserDefinedEntitiesProbeRetrieval {

    private final GroupServiceBlockingStub groupService;
    private final EntityRetriever entityRetriever;

    /**
     * Constructor.
     *
     * @param groupService    - API interface for requesting the Group service,
     * @param entityRetriever - retriever for collecting entities from tge Repository component.
     */
    @ParametersAreNonnullByDefault
    public UserDefinedEntitiesProbeRetrieval(GroupServiceBlockingStub groupService,
                                             EntityRetriever entityRetriever) {
        this.groupService = groupService;
        this.entityRetriever = entityRetriever;
    }

    /**
     * Collects groups of one group type.
     *
     * @param type - type of requested groups.
     * @return a collection of {@link Grouping} instances.
     */
    @Nonnull
    public Collection<Grouping> getGroups(@Nonnull GroupType type) {
        final GroupFilter filter = GroupFilter.newBuilder().setGroupType(type).build();
        final GetGroupsRequest request = GetGroupsRequest.newBuilder().setGroupFilter(filter).build();
        final Iterator<Grouping> iterator = groupService.getGroups(request);
        return Sets.newHashSet(iterator);
    }

    /**
     * Collects group members be group ID.
     *
     * @param groupId - ID of requested members.
     * @return a collection of {@link TopologyEntityDTO} instances.
     */
    @Nonnull
    public Collection<TopologyEntityDTO> getMembers(long groupId) {
        final GetMembersRequest request = GetMembersRequest.newBuilder().setId(groupId).build();
        final GetMembersResponse membersResponse = groupService.getMembers(request);
        final List<Long> memberIds = membersResponse.getMembers().getIdsList();
        final Collection<TopologyEntityDTO> members = entityRetriever
                .retrieveTopologyEntities(memberIds).collect(Collectors.toSet());
        return members;
    }

}
