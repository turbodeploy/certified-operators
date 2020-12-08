package com.vmturbo.group.service;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.collections4.CollectionUtils;
import org.junit.Assert;

import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GetPaginatedGroupsResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.GroupFilters;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.Origin;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.group.group.GroupMembersPlain;
import com.vmturbo.group.group.IGroupStore;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * Mock implementation of group store, suitable for testing.
 */
public class MockGroupStore implements IGroupStore {

    private final Map<Long, Grouping> groups = new HashMap<>();

    /**
     * Registers a new groups into store to be included in lookups.
     *
     * @param group group to add to the store.
     */
    public void addGroup(@Nonnull Grouping group) {
        Assert.assertNull(groups.put(group.getId(), group));
    }

    @Override
    public void createGroup(long oid, @Nonnull Origin origin,
            @Nonnull GroupDefinition groupDefinition, @Nonnull Set<MemberType> expecMemberTypes,
            boolean supportReverseLookup) throws StoreOperationException {
        final Grouping createdGroup = Grouping.newBuilder()
                .setId(oid)
                .setDefinition(groupDefinition)
                .addAllExpectedTypes(expecMemberTypes)
                .setSupportsMemberReverseLookup(supportReverseLookup)
                .build();
        groups.put(oid, createdGroup);
    }

    @Nonnull
    @Override
    public Collection<Grouping> getGroupsById(@Nonnull Collection<Long> groupId) {
        return groupId.stream().map(groups::get).collect(Collectors.toSet());
    }

    @Nonnull
    @Override
    public Grouping updateGroup(long groupId, @Nonnull GroupDefinition groupDefinition,
            @Nonnull Set<MemberType> expectedMemberTypes, boolean supportReverseLookups)
            throws StoreOperationException {
        return null;
    }

    /**
     * Mock for {@link IGroupStore#getPaginatedGroups}.
     * Dummy pagination implementation for testing purposes: sorting ONLY by uuid, NO filtering.
     *
     * @param paginatedGroupsRequest request for groups with necessary filters & pagination
     *                               parameters.
     * @return the paginated response.
     */
    @Nonnull
    @Override
    public GroupDTO.GetPaginatedGroupsResponse getPaginatedGroups(
            @Nonnull GroupDTO.GetPaginatedGroupsRequest paginatedGroupsRequest) {
        long cursor = paginatedGroupsRequest.hasPaginationParameters()
                && paginatedGroupsRequest.getPaginationParameters().hasCursor()
                ? Long.parseLong(paginatedGroupsRequest.getPaginationParameters().getCursor())
                : 0;
        long limit = paginatedGroupsRequest.hasPaginationParameters()
                && paginatedGroupsRequest.getPaginationParameters().hasLimit()
                ? paginatedGroupsRequest.getPaginationParameters().getLimit()
                : 10;
        return GetPaginatedGroupsResponse.newBuilder()
                .addAllGroups(groups.values().stream()
                        .sorted((g1, g2) -> Long.compare(g1.getId(), g2.getId()))
                        .skip(cursor)
                        .limit(limit)
                        .collect(Collectors.toList()))
                .setPaginationResponse(PaginationResponse.newBuilder()
                        .setTotalRecordCount(groups.size())
                        .setNextCursor(String.valueOf(cursor + limit < groups.size()
                                ? cursor + limit
                                : ""))
                        .build())
                .build();
    }

    @Nonnull
    @Override
    public Collection<Grouping> getGroups(@Nonnull GroupFilter groupFilter) {
        if (groupFilter != null) {
            return getGroupIds(
                    GroupFilters.newBuilder().addGroupFilter(groupFilter).build()).stream()
                    .map(groups::get)
                    .collect(Collectors.toSet());
        } else {
            return Collections.emptySet();
        }
    }

    @Nonnull
    @Override
    public Collection<Long> getGroupIds(@Nonnull GroupFilters groupFilter) {
        if (groupFilter != null && groupFilter.getGroupFilterCount() == 0) {
            return groups.keySet();
        }
        return Collections.emptySet();
    }

    @Override
    public void deleteGroup(long groupId) throws StoreOperationException {
        groups.remove(groupId);
    }

    @Override
    public void updateDiscoveredGroups(@Nonnull Collection<DiscoveredGroup> groupsToAdd,
            @Nonnull Collection<DiscoveredGroup> groupsToUpdate, @Nonnull Set<Long> groupsToDelete)
            throws StoreOperationException {

    }

    @Nonnull
    @Override
    public Collection<DiscoveredGroupId> getDiscoveredGroupsIds() {
        return Collections.emptySet();
    }

    @Nonnull
    @Override
    public Set<Long> getGroupsByTargets(@Nonnull Collection<Long> targets) {
        return Collections.emptySet();
    }

    @Nonnull
    @Override
    public Map<Long, Map<String, Set<String>>> getTags(@Nonnull Collection<Long> groupIds) {
        return Collections.emptyMap();
    }

    @Nonnull
    @Override
    public GroupMembersPlain getMembers(@Nonnull Collection<Long> groupId,
            boolean expandNestedGroups) throws StoreOperationException {
        final GroupMembersPlain members = getDirectMembers(groupId);
        if (expandNestedGroups) {
            Set<Long> newGroups = members.getGroupIds();
            while (!newGroups.isEmpty()) {
                final GroupMembersPlain subMembers = getDirectMembers(newGroups);
                newGroups = members.mergeMembers(subMembers);
            }
        }
        return members.unmodifiable();
    }

    @Nonnull
    private GroupMembersPlain getDirectMembers(@Nonnull Collection<Long> groupIds) {
        if (CollectionUtils.isEmpty(groupIds)) {
            return new GroupMembersPlain(Collections.emptySet(), Collections.emptySet(),
                    Collections.emptySet());
        }
        final Collection<StaticMembersByType> members = groupIds.stream()
                .map(groups::get)
                .filter(Objects::nonNull)
                .map(Grouping::getDefinition)
                .map(GroupDefinition::getStaticGroupMembers)
                .map(StaticMembers::getMembersByTypeList)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
        final Set<Long> entities = members.stream()
                .filter(member -> member.getType().hasEntity())
                .map(StaticMembersByType::getMembersList)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
        final Set<Long> groups = members.stream()
                .filter(member -> member.getType().hasGroup())
                .map(StaticMembersByType::getMembersList)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
        final Set<EntityFilters> entitiesFilter = groupIds.stream()
                .map(this.groups::get)
                .filter(Objects::nonNull)
                .map(Grouping::getDefinition)
                .filter(GroupDefinition::hasEntityFilters)
                .map(GroupDefinition::getEntityFilters)
                .collect(Collectors.toSet());
        return new GroupMembersPlain(entities, groups, entitiesFilter);
    }

    @Override
    @Nonnull
    public Map<Long, Set<Long>> getStaticGroupsForEntities(@Nonnull Collection<Long> entityIds,
            @Nonnull Collection<GroupType> groupTypes) {
        if (CollectionUtils.isEmpty(entityIds)) {
            return Collections.emptyMap();
        }
        final Map<Long, Set<Long>> resultMap = new HashMap<>();
        for (Grouping group : groups.values()) {
            if (groupTypes.isEmpty() || groupTypes.contains(group.getDefinition().getType())) {
                final Collection<Long> groupMembers = getGroupStaticEntityMembers(group);
                for (Long entityId : entityIds) {
                    if (groupMembers.contains(entityId)) {
                        resultMap.computeIfAbsent(entityId, key -> new HashSet<>())
                                .add(group.getId());
                    }
                }
            }
        }
        return Collections.unmodifiableMap(resultMap);
    }

    @Nonnull
    private static Collection<Long> getGroupStaticEntityMembers(@Nonnull Grouping group) {
        final StaticMembers staticMembers = group.getDefinition().getStaticGroupMembers();
        if (staticMembers == null) {
            return Collections.emptySet();
        }
        return staticMembers.getMembersByTypeList()
                .stream()
                .filter(member -> member.getType().hasEntity())
                .map(StaticMembersByType::getMembersList)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
    }

    @Override
    public void deleteAllGroups() {
        groups.clear();
    }

    @Nonnull
    @Override
    public Set<Long> getOwnersOfGroups(@Nonnull Collection<Long> groupIds,
            @Nullable GroupType groupType) {
        Stream<Grouping> groupingStream = groupIds.stream().map(groups::get);
        if (groupType != null) {
            groupingStream.filter(gr -> gr.getDefinition().getType() == groupType);
        }
        return groupingStream.map(Grouping::getId).collect(Collectors.toSet());
    }

    @Nonnull
    @Override
    public Set<Long> getExistingGroupIds(@Nonnull Collection<Long> groupIds) {
        final Set<Long> requested = new HashSet<>(groupIds);
        requested.retainAll(groups.keySet());
        return requested;
    }
}
