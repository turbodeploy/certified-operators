package com.vmturbo.api.component.external.api.util;


import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;
import org.immutables.value.Value;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import io.grpc.StatusRuntimeException;

import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.UIEntityType;

/**
 * A utility object to:
 * 1) Process UUIDs and replace ones that refer to groups with the members of the groups.
 * 2) Resolve group membership for UUIDs and OIDs.
 **/
public class GroupExpander {

    private static final Set<String> GLOBAL_SCOPE_SUPPLY_CHAIN = ImmutableSet.of(
        "GROUP-VirtualMachine", "GROUP-PhysicalMachineByCluster", "Market");

    private final GroupServiceBlockingStub groupServiceGrpc;

    public GroupExpander(@Nonnull GroupServiceBlockingStub groupServiceGrpc) {
        this.groupServiceGrpc = groupServiceGrpc;
    }

    /**
     * A utility object to represent a group and its members.
     *
     * TODO (roman, Mar 29 2019): Move this functionality to the group components.
     * Edit the GetMembers call to return the leaf entity IDs, as well as the Group definition.
     */
    @Value.Immutable
    public interface GroupAndMembers {
        /**
         * The {@link Grouping} definition retrieved from the group component.
         *
         * @return the group.
         */
        Grouping group();

        /**
         * The members of the group.
         */
        Collection<Long> members();

        /**
         * The entities in the group. In a non-nested group, this will be the same collection
         * as the {@link GroupAndMembers#members()}. In a nested group, the members will be
         * the immediate groups inside {@link GroupAndMembers#group()}, and the entities will be the
         * leaf entities.
         */
        Collection<Long> entities();
    }

    /**
     * Get the group associated with a particular UUID, if any.
     * @param uuid The string UUID. This may be the OID of a group, an entity, or a magic string
     *             (e.g. Market).
     * @return The {@link Grouping} associated with the UUID, if any.
     */
    public Optional<Grouping> getGroup(@Nonnull final String uuid) {
        if (StringUtils.isNumeric(uuid)) {
            final GetGroupResponse response = groupServiceGrpc.getGroup(GroupID.newBuilder()
                    .setId(Long.parseLong(uuid))
                    .build());
            return response.hasGroup() ? Optional.of(response.getGroup()) : Optional.empty();
        } else {
            return Optional.empty();
        }
    }

    /**
     * Get the entities in this group categorized by their type.
     *
     * @param groupUuid The string UUID. This should be an oid of a group otherwise it will return
     *                  empty.
     * @return The map from the type of the entites to entities of that type in that group.
     */
    public Map<UIEntityType, Set<Long>> expandUuidToTypeToEntitiesMap(Long groupUuid) {

        Optional<GroupAndMembers> groupAndMembers = getGroupWithMembers(String.valueOf(groupUuid));
        if (groupAndMembers.isPresent()) {
            if (groupAndMembers.get().entities().size() == 0) {
                return  Collections.emptyMap();
            }

            final GroupDTO.GroupDefinition group = groupAndMembers.get().group().getDefinition();
            if (GroupProtoUtil.isNestedGroup(groupAndMembers.get().group())) {
                return groupAndMembers.get()
                    .members()
                    .stream()
                    .map(this::expandUuidToTypeToEntitiesMap)
                    .map(Map::entrySet)
                    .flatMap(Set::stream)
                    .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue(),
                        (e1, e2) -> Sets.union(e1, e2)));

            } else if (group.hasStaticGroupMembers()) {
                return group
                    .getStaticGroupMembers()
                    .getMembersByTypeList()
                    .stream()
                    .collect(Collectors.toMap(x -> UIEntityType.fromType(x.getType().getEntity()),
                         x -> new HashSet<>(x.getMembersList())));
            } else if (group.hasEntityFilters()) {
                if (group.getEntityFilters().getEntityFilterCount() == 1) {
                    Map<UIEntityType, Set<Long>> result = new HashMap<>();
                    result.put(UIEntityType.fromType(group.getEntityFilters()
                            .getEntityFilter(0).getEntityType()),
                        new HashSet<>(groupAndMembers.get().members()));
                    return result;

                } else {
                    // This is when this a heterogeneous dynamic group. We don't have that use case
                    // right now (Nov 2019) and no plan to support it therefore return empty
                    return Collections.emptyMap();
                }
            }
        }

        return Collections.emptyMap();
    }

    /**
     * Get the group associated with a particular UUID, as well as its members.
     *
     * @param uuid The string UUID. This may be the OID of a group, an entity, or a magic string.
     * @return If the UUID refers to a group, an {@link Optional} containing the
     *         {@link GroupAndMembers} describing the group and its members. An empty
     *         {@link Optional} otherwise.
     */
    @Nonnull
    public Optional<GroupAndMembers> getGroupWithMembers(@Nonnull final String uuid) {
        // These magic UI strings currently have no associated group in XL, so they are not valid.
        if (uuid.equals(DefaultCloudGroupProducer.ALL_CLOULD_WORKLOAD_AWS_AND_AZURE_UUID) ||
            uuid.equals(DefaultCloudGroupProducer.ALL_CLOUD_VM_UUID)) {
            return Optional.empty();
        }

        if (GLOBAL_SCOPE_SUPPLY_CHAIN.contains(uuid)) {
            return Optional.empty();
        }

        return getGroup(uuid)
            .map(this::getMembersForGroup);
    }

    /**
     * Given a {@link Grouping}, get its members. If the {@link Grouping} is a dynamic group, this
     * may make a call to the group component.
     *
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
     * Process a UUID, expanding if a Group or Cluster uuid into the list of
     * elements. If the UUID is neither, it is simply included in the output list.
     *
     * If uuid is the special UUID "Market", then return an empty list.
     *
     * @param uuid a UUID for which Cluster or Group UUIDs will be expanded.
     * @return UUIDs if the given uuid is for a Group or Cluster; otherwise just the given uuid
     */
    public @Nonnull Set<Long> expandUuid(@Nonnull String uuid) {
        return expandUuids(Collections.singleton(uuid));
    }

    /**
     * Process a list of UUIDs, expanding each Group or Cluster uuid into the list of
     * elements. If a UUID in the input is neither, it is simply included in the output list.
     *
     * If the list contains the special UUID "Market", then return an empty list.
     *
     * Note: Performance optimization
     * Currently, we just checked that if they are the same type (entity, market, group).
     * Although there is a use case where we migrate some onprem VMs into a Cloud Group,
     * so the scope of the plan is both VMs and Group, and the widget use that list.
     * According to Gabriele, UI team can change the widget to do separate calls (OM-32442)
     *
     * @param uuidSet a list of UUIDs for which Cluster or Group UUIDs will be expanded.
     * @return UUIDs from each Group or Cluster in the input list; other UUIDs are passed through
     * @throws StatusRuntimeException if there is an error (other than NOT_FOUND) from the
     * groupServiceGrpc call tp getMembers().
     */
    public @Nonnull Set<Long> expandUuids(@Nonnull Set<String> uuidSet) {
        final Set<Long> oids = new HashSet<>();
        for (String uuidString : uuidSet) {
            // sanity-check the uuidString
            if (StringUtils.isEmpty(uuidString)) {
                throw new IllegalArgumentException("Empty uuid string given: " + uuidSet);
            }
            // is this the special "Market" uuid string? if yes, we just return.
            if (uuidString.equals(UuidMapper.UI_REAL_TIME_MARKET_STR)) {
                return Collections.emptySet();
            }

            oids.add(Long.valueOf(uuidString));
        }
        return expandOids(oids);
    }

    @Nonnull
    public Set<Long> expandOids(@Nonnull final Set<Long> oidSet) {
        Set<Long> answer = Sets.newHashSet();

        boolean isEntity = false;
        for (final Long oid : oidSet) {
            // Assume it's group type at the beginning
            // For subsequent items, if it's group type, we continue the RPC call to Group component.
            // If not, we know it's entity type, and will just add oids to return set.
            if (!isEntity) {
                Optional<GroupAndMembers> groupAndMembers = getGroupWithMembers(Long.toString(oid));
                if (groupAndMembers.isPresent()) {
                    // When expanding, we take the entities (expand all the way).
                    answer.addAll(groupAndMembers.get().entities());
                } else {
                    isEntity = true;
                    answer.add(oid);
                }
            } else {
                answer.add(oid);
            }
        }
        return answer;
    }
}
