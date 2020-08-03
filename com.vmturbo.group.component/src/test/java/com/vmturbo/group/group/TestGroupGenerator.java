package com.vmturbo.group.group;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.OptimizationMetadata;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.Origin;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.group.group.IGroupStore.DiscoveredGroup;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * Utility class to create various groups suitable for testing purposes.
 */
public class TestGroupGenerator {
    private final AtomicLong counter = new AtomicLong(0);

    /**
     * Creates a discovered group.
     *
     * @param srcId discovered source id
     * @param targetIds related targets
     * @return discovered group
     */
    @Nonnull
    public DiscoveredGroup createUploadedGroup(@Nonnull String srcId,
            @Nonnull Collection<Long> targetIds) {
        final GroupDefinition groupDefinition = createGroupDefinition();
        final List<MemberType> types = Arrays.asList(MemberType.newBuilder().setEntity(1).build(),
                MemberType.newBuilder().setGroup(GroupType.REGULAR).build());
        return new DiscoveredGroup(counter.getAndIncrement(), groupDefinition, srcId,
                new HashSet<>(targetIds), types, false);
    }

    /**
     * Creates a group definition with one static entity member and a tag.
     *
     * @return group definition
     */
    @Nonnull
    public GroupDefinition createGroupDefinition() {
        return GroupDefinition.newBuilder()
                .setType(GroupType.REGULAR)
                .setDisplayName("Group-" + counter.getAndIncrement())
                .setStaticGroupMembers(StaticMembers.newBuilder()
                        .addMembersByType(StaticMembersByType.newBuilder()
                                .setType(MemberType.newBuilder().setEntity(1))
                                .addMembers(counter.getAndIncrement())))
                .setTags(Tags.newBuilder()
                        .putTags("tag", TagValuesDTO.newBuilder()
                                .addValues("tag1")
                                .addValues("tag" + counter.getAndIncrement())
                                .build()))
                .setIsHidden(false)
                .setOptimizationMetadata(OptimizationMetadata.newBuilder()
                        .setIsGlobalScope(false)
                        .setEnvironmentType(EnvironmentType.ON_PREM)
                        .build())
                .build();
    }

    /**
     * Creates user origin.
     *
     * @return user origin
     */
    @Nonnull
    public Origin createUserOrigin() {
        return Origin.newBuilder()
                .setUser(Origin.User.newBuilder().setUsername("user-" + counter.getAndIncrement()))
                .build();
    }

    /**
     * Creates system origin.
     *
     * @return system origin
     */
    @Nonnull
    public Origin createSystemOrigin() {
        return Origin.newBuilder()
                .setSystem(Origin.System.newBuilder()
                        .setDescription("system-group-" + counter.getAndIncrement()))
                .build();
    }

    /**
     * Returns the next OID to use.
     *
     * @return next OID
     */
    public long nextOid() {
        return counter.getAndIncrement();
    }
}
