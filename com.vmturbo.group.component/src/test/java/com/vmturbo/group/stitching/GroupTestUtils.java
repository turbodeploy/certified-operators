package com.vmturbo.group.stitching;

import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredGroupsPoliciesSettings.UploadedGroup;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * Utils for testing group stitching and rpc calls.
 */
public class GroupTestUtils {

    /**
     * private default constructor to prevent this util class from being instantiated or extended.
     */
    private GroupTestUtils() {
    }

    /**
     * Create an {@link UploadedGroup}.
     *
     * @param groupType type of the group
     * @param groupSourceId source id of the group
     * @param membersByType members of the group
     * @return {@link UploadedGroup}
     */
    public static UploadedGroup createUploadedGroup(@Nonnull GroupType groupType,
            @Nonnull String groupSourceId, @Nonnull Map<Integer, Set<Long>> membersByType) {
        final StaticMembers.Builder builder = StaticMembers.newBuilder();
        membersByType.forEach((entityType, members) ->
                builder.addMembersByType(StaticMembersByType.newBuilder()
                        .setType(MemberType.newBuilder()
                                .setEntity(entityType))
                        .addAllMembers(members)));

        return UploadedGroup.newBuilder()
                .setSourceIdentifier(groupSourceId)
                .setDefinition(GroupDefinition.newBuilder()
                        .setType(groupType)
                        .setDisplayName(groupSourceId)
                        .setStaticGroupMembers(builder))
                .build();
    }

    /**
     * Create an {@link UploadedGroup}.
     *
     * @param groupType type of the group
     * @param groupSourceId source id of the group
     * @param membersByType members of the group
     * @param displayName displayName of the group
     * @return {@link UploadedGroup}
     */
    public static UploadedGroup createUploadedGroupWithDisplayName(@Nonnull GroupType groupType,
            @Nonnull String groupSourceId, @Nonnull Map<Integer, Set<Long>> membersByType,
            @Nonnull String displayName) {
        final UploadedGroup initialGroup = createUploadedGroup(groupType, groupSourceId, membersByType);
        final GroupDefinition updatedGroupDefinition = initialGroup.getDefinition().toBuilder().setDisplayName(displayName).build();
        return initialGroup.toBuilder().setDefinition(updatedGroupDefinition).build();
    }
}
