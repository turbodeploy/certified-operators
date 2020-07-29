package com.vmturbo.extractor.topology.fetcher;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import io.grpc.StatusRuntimeException;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.topology.fetcher.GroupFetcher.GroupData;

/**
 * Fetch all necessary groups and memberships from group component.
 */
public class GroupFetcher extends DataFetcher<GroupData> {

    private static final Logger logger = LogManager.getLogger();

    private final GroupServiceBlockingStub groupService;

    public GroupFetcher(@Nonnull GroupServiceBlockingStub groupService,
            @Nonnull MultiStageTimer timer,
            @Nonnull Consumer<GroupData> consumer) {
        super(timer, consumer);
        this.groupService = groupService;
    }

    @Override
    protected String getName() {
        return "Load group memberships";
    }

    /**
     * Load group information from group component.
     *
     * @return map linking each entity id to the groups to which it belongs
     */
    @Override
    protected GroupData fetch() {
        // TODO Maybe compute groups from our topology rather than asking group component to
        // do it. That way, groups will be in sync with topology, which is otherwise not
        // guaranteed.
        final Long2ObjectMap<Grouping> groupsById = new Long2ObjectOpenHashMap<>();
        // build map of all groups
        try {
            // request all non-temporary, visible group definitions
            final Iterator<Grouping> groups = groupService.getGroups(GetGroupsRequest.newBuilder()
                    .setGroupFilter(GroupFilter.newBuilder()
                            .setIncludeTemporary(false)
                            .setIncludeHidden(false)
                            .build())
                    .build());
            groups.forEachRemaining(g -> groupsById.put(g.getId(), g));
        } catch (StatusRuntimeException e) {
            logger.error("Error retrieving groups from the group component."
                    + " No group memberships for this round of extraction. Error: {}", e.getMessage());
        }


        final Long2ObjectMap<List<Long>> groupToLeafEntityIds = new Long2ObjectOpenHashMap<>();
        final Long2ObjectMap<List<Long>> groupToDirectMemberIds = new Long2ObjectOpenHashMap<>();
        final Long2ObjectMap<List<Grouping>> leafEntityToGroups = new Long2ObjectOpenHashMap<>();
        if (!groupsById.isEmpty()) {
            populateDirectMembers(groupsById, groupToDirectMemberIds);
            populateLeafEntitiesAndReverseMap(groupsById, groupToDirectMemberIds,
                    groupToLeafEntityIds, leafEntityToGroups);
        }
        logger.info("Fetched {} groups totaling {} members from group service",
                groupsById.size(), leafEntityToGroups.size());
        return new GroupData(leafEntityToGroups, groupToLeafEntityIds, groupToDirectMemberIds);
    }

    private void populateLeafEntitiesAndReverseMap(Long2ObjectMap<Grouping> groupsById,
            Long2ObjectMap<List<Long>> groupToDirectMemberIds,
            Long2ObjectMap<List<Long>> groupToLeafEntityIds,
            Long2ObjectMap<List<Grouping>> leafEntityToGroups) {
        final LongList nestedGroups = new LongArrayList();
        for (Grouping group : groupsById.values()) {
            // keep groups which contain nested groups, to be expanded later
            if (GroupProtoUtil.isNestedGroup(group)) {
                nestedGroups.add(group.getId());
            } else {
                final List<Long> members = groupToDirectMemberIds.getOrDefault(group.getId(),
                        Collections.emptyList());
                groupToLeafEntityIds.put(group.getId(), members);
                members.forEach(member -> leafEntityToGroups.computeIfAbsent(
                        (long)member, m -> new ArrayList<>()).add(group));
            }
        }
        // retrieve (flattened) group memberships for all the nested groups we care about
        if (!nestedGroups.isEmpty()) {
            try {
                final GetMembersRequest getLeafEntitiesRequest = GetMembersRequest.newBuilder()
                        .addAllId(nestedGroups)
                        .setExpectPresent(true)
                        .setEnforceUserScope(false)
                        .setExpandNestedGroups(true)
                        .build();
                groupService.getMembers(getLeafEntitiesRequest).forEachRemaining(ms -> {
                    List<Long> memberIdList = ms.getMemberIdList();
                    groupToLeafEntityIds.put(ms.getGroupId(), memberIdList);
                    memberIdList.forEach(member ->
                            leafEntityToGroups.computeIfAbsent((long)member, m -> new ArrayList<>())
                                    .add(groupsById.get(ms.getGroupId())));
                });
            } catch (StatusRuntimeException e) {
                logger.error("Error retrieving leaf entities for nested groups", e);
            }
        }
    }

    private void populateDirectMembers(Long2ObjectMap<Grouping> groupsById,
            Long2ObjectMap<List<Long>> groupToDirectMemberIds) {
        final LongList dynamicGroupIds = new LongArrayList();
        for (Grouping group : groupsById.values()) {
            if (group.getDefinition().hasStaticGroupMembers()) {
                groupToDirectMemberIds.put(group.getId(), GroupProtoUtil.getStaticMembers(group));
            } else {
                dynamicGroupIds.add(group.getId());
            }
        }
        // get members for dynamic groups
        if (!dynamicGroupIds.isEmpty()) {
            try {
                GetMembersRequest getDynamicGroupMembersRequest = GetMembersRequest.newBuilder()
                        .setExpectPresent(true)
                        .addAllId(dynamicGroupIds)
                        .build();
                groupService.getMembers(getDynamicGroupMembersRequest).forEachRemaining(response ->
                        groupToDirectMemberIds.put(response.getGroupId(), response.getMemberIdList()));
            } catch (StatusRuntimeException e) {
                logger.error("Error getting members for dynamic groups", e);
            }
        }
    }

    public static class GroupData {
        Long2ObjectMap<List<Grouping>> leafEntityToGroups;
        Long2ObjectMap<List<Long>> groupToLeafEntityIds;
        Long2ObjectMap<List<Long>> groupToDirectMemberIds;

        GroupData(Long2ObjectMap<List<Grouping>> leafEntityToGroups,
                Long2ObjectMap<List<Long>> groupToLeafEntityIds,
                Long2ObjectMap<List<Long>> groupToDirectMemberIds) {
            this.leafEntityToGroups = leafEntityToGroups;
            this.groupToLeafEntityIds = groupToLeafEntityIds;
            this.groupToDirectMemberIds = groupToDirectMemberIds;
        }

        public Long2ObjectMap<List<Grouping>> getLeafEntityToGroups() {
            return leafEntityToGroups;
        }

        public Long2ObjectMap<List<Long>> getGroupToLeafEntityIds() {
            return groupToLeafEntityIds;
        }

        public Long2ObjectMap<List<Long>> getGroupToDirectMemberIds() {
            return groupToDirectMemberIds;
        }
    }
}
