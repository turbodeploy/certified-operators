package com.vmturbo.topology.processor.group.policy;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters.EntityFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.SearchParametersCollection;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.topology.processor.group.ResolvedGroup;

public class PolicyGroupingHelper {

    private PolicyGroupingHelper() {}


    /**
     * Create a {@link ResolvedGroup} from a {@link Grouping}, containing provided members.
     *
     * @param group The {@link Grouping}.
     * @param memberIds The members to put in the group.
     * @return A {@link ResolvedGroup}.
     */
    public static ResolvedGroup resolvedGroup(Grouping group, Long... memberIds) {
        return new ResolvedGroup(group,
            Collections.singletonMap(
                GroupProtoUtil.getEntityTypes(group).iterator().next(),
                Sets.newHashSet(memberIds)));
    }

    /**
     * Creates a dynamic group based on the input.
     * @param searchParameters input search param for dynamic group.
     * @param entityType the type of entities in the group.
     * @param oid the oid for the group.
     * @return the created group.
     */
    public static Grouping policyGrouping(SearchParametersCollection searchParameters,
                                                int entityType, long oid) {
        return Grouping.newBuilder()
            .addExpectedTypes(MemberType.newBuilder().setEntity(entityType))
            .setDefinition(GroupDefinition.newBuilder()
                .setEntityFilters(EntityFilters.newBuilder()
                                .addEntityFilter(EntityFilter.newBuilder()
                                        .setEntityType(entityType)
                                        .setSearchParametersCollection(searchParameters)
                                                ))
             )
            .setId(oid)
            .build();
    }

    /**
     * Creates a static group based on the input.
     * @param members the list of member oids.
     * @param entityType the type of entities in the group.
     * @param oid the oid for the group.
     * @return the created group.
     */
    public static Grouping policyGrouping(List<Long> members,
                                                int entityType, long oid) {
        return Grouping.newBuilder()
                        .addExpectedTypes(MemberType.newBuilder().setEntity(entityType))
                        .setDefinition(GroupDefinition.newBuilder()
                        .setStaticGroupMembers(StaticMembers.newBuilder()
                                        .addMembersByType(StaticMembersByType.newBuilder()
                                                        .setType(MemberType.newBuilder()
                                                                        .setEntity(entityType))
                                                        .addAllMembers(members))))
                        .setId(oid).build();
    }

    /**
     * Creates a static group based on the input.
     * @param name the display name of group.
     * @param entityType the type of entities in the group.
     * @param oid the oid for the group.
     * @return the created group.
     */
    public static Grouping policyGrouping(String name, int entityType, long oid) {
        return Grouping.newBuilder()
            .addExpectedTypes(MemberType.newBuilder().setEntity(entityType))
            .setDefinition(GroupDefinition.newBuilder()
            .setStaticGroupMembers(StaticMembers.newBuilder()
                            .addMembersByType(StaticMembersByType.newBuilder()
                                            .setType(MemberType.newBuilder()
                                                            .setEntity(entityType))
                                            )))
            .setId(oid)
            .build();
    }
}
