package com.vmturbo.extractor.topology.fetcher;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import com.google.common.collect.Streams;

import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;

import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
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
        Long2ObjectMap<List<Grouping>> entityToGroups = new Long2ObjectOpenHashMap<>();
        Long2ObjectMap<Grouping> groupsById = new Long2ObjectOpenHashMap<>();
        // request all non-temporary, visible group definitions
        final Iterator<Grouping> groups = groupService.getGroups(GetGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.newBuilder()
                        .setIncludeTemporary(false)
                        .setIncludeHidden(false)
                        .build())
                .build());
        // filter out the group types we don't care about, build map of all groups, as well
        // as a list of group ids.
        final LongList groupIds = new LongArrayList();
        try {
            Streams.stream(groups)
                    .peek(g -> groupsById.put(g.getId(), g))
                    .mapToLong(Grouping::getId)
                    .forEach(groupIds::add);
        } catch (StatusRuntimeException e) {
            logger.error("Error retrieving groups from the group component."
                    + " No group memberships for this round of extraction. Error: {}", e.getMessage());
        }

        Long2ObjectMap<List<Long>> groupToEntityIds = new Long2ObjectOpenHashMap<>();
        // retrieve (flattened) group memberships for all the groups we care about
        if (!groupIds.isEmpty()) {
            final Iterator<GetMembersResponse> memberships = groupService.getMembers(
                    GetMembersRequest.newBuilder()
                            .addAllId(groupIds)
                            .setExpectPresent(true)
                            .setEnforceUserScope(false)
                            .setExpandNestedGroups(true)
                            .build());
            // and finally add each group's GroupInfo to each of its members
            memberships.forEachRemaining(ms -> {
                List<Long> memberIdList = ms.getMemberIdList();
                groupToEntityIds.put(ms.getGroupId(), memberIdList);
                memberIdList.forEach(member ->
                        entityToGroups.computeIfAbsent((long)member, m -> new ArrayList<>())
                                .add(groupsById.get(ms.getGroupId())));
            });
        }
        logger.info("Fetched {} groups totaling {} members from group service",
                groupIds.size(), entityToGroups.size());
        return new GroupData(entityToGroups, groupToEntityIds);
    }

    public static class GroupData {
        Long2ObjectMap<List<Grouping>> entityToGroups;
        Long2ObjectMap<List<Long>> groupToEntityIds;

        GroupData(Long2ObjectMap<List<Grouping>> entityToGroups,
                  Long2ObjectMap<List<Long>> groupToEntityIds) {
            this.entityToGroups = entityToGroups;
            this.groupToEntityIds = groupToEntityIds;
        }

        public Long2ObjectMap<List<Grouping>> getEntityToGroups() {
            return entityToGroups;
        }

        public Long2ObjectMap<List<Long>> getGroupToEntityIds() {
            return groupToEntityIds;
        }
    }
}
