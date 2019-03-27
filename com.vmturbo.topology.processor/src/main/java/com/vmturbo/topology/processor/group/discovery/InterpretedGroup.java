package com.vmturbo.topology.processor.group.discovery;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo.SelectionCriteriaCase;
import com.vmturbo.common.protobuf.topology.DiscoveredGroup.DiscoveredGroupInfo;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.topology.processor.rpc.DiscoveredGroupRpcService;

/**
 * The {@link InterpretedGroup} represents the results of an attempt to interpret a
 * {@link CommonDTO.GroupDTO} into a format that can be sent to the Group component.
 *
 * Interpreted groups retain a builder to their group or cluster info so that membership
 * can be mutated to reflect updated identities that result from stitching. See
 * {@link com.vmturbo.topology.processor.stitching.StitchingGroupFixer} for additional details.
 */
public class InterpretedGroup {
    private final CommonDTO.GroupDTO dto;
    private final Optional<GroupInfo.Builder> dtoAsGroup;
    private final Optional<ClusterInfo.Builder> dtoAsCluster;

    public InterpretedGroup(@Nonnull final CommonDTO.GroupDTO dto,
                     @Nonnull final Optional<GroupInfo.Builder> dtoAsGroup,
                     @Nonnull final Optional<ClusterInfo.Builder> dtoAsCluster) {
        this.dto = Objects.requireNonNull(dto);
        this.dtoAsCluster = Objects.requireNonNull(dtoAsCluster);
        this.dtoAsGroup = Objects.requireNonNull(dtoAsGroup);
        if (dtoAsGroup.isPresent() && dtoAsCluster.isPresent()) {
            throw new IllegalArgumentException(
                            "Interpreted group must be a group OR a cluster, not both.");
        }
    }

    /**
     * Get the static members of the group, if the group is a static group.
     *
     * @return A list containing the static members of the group. An empty list if the group is
     *         not a static group/cluster. Note - from the output of this method there is no way
     *         to distinguish an empty static group from a non-static group.
     */
    @Nonnull
    public List<Long> getStaticMembers() {
        if (dtoAsCluster.isPresent()) {
            return dtoAsCluster.get().getMembers().getStaticMemberOidsList();
        } else if (dtoAsGroup.isPresent()) {
            if (dtoAsGroup.get().getSelectionCriteriaCase() == SelectionCriteriaCase.STATIC_GROUP_MEMBERS) {
                return dtoAsGroup.get().getStaticGroupMembers().getStaticMemberOidsList();
            }
        }
        return Collections.emptyList();
    }

    /**
     * Creates a deep copy of Interpreted group.
     *
     * @param source to make copy of it
     * @return deep copy of provided group
     */
    public static InterpretedGroup deepCopy(@Nonnull InterpretedGroup source) {
        final CommonDTO.GroupDTO dto = CommonDTO.GroupDTO.newBuilder(
                        Objects.requireNonNull(source.dto)).build();
        final Optional<GroupInfo.Builder> dtoAsGroup = source.dtoAsGroup.map(group ->
                        GroupInfo.newBuilder(group.build()));
        final Optional<ClusterInfo.Builder> dtoAsCluster = source.dtoAsCluster.map(cluster ->
                        ClusterInfo.newBuilder(cluster.build()));
        return new InterpretedGroup(dto, dtoAsGroup, dtoAsCluster);
    }

    /**
     * Get the result of the interpretation as a group.
     *
     * At most one of {@link InterpretedGroup#getDtoAsGroup()} and
     * {@link InterpretedGroup#getDtoAsCluster()} can return a non-empty optional.
     */
    public Optional<GroupInfo.Builder> getDtoAsGroup() {
        return dtoAsGroup;
    }

    @Nonnull
    public CommonDTO.GroupDTO getOriginalSdkGroup() {
        return dto;
    }

    /**
     * Get the result of the interpretation as a cluster.
     *
     * At most one of {@link InterpretedGroup#getDtoAsGroup()} and
     * {@link InterpretedGroup#getDtoAsCluster()} can return a non-empty optional.
     */
    public Optional<ClusterInfo.Builder> getDtoAsCluster() {
        return dtoAsCluster;
    }

    /**
     * Create a {@link DiscoveredGroupInfo} for the purposes of
     * {@link DiscoveredGroupRpcService}.
     *
     * @return The {@link DiscoveredGroupInfo} representing this DTO.
     */
    @Nonnull
    DiscoveredGroupInfo createDiscoveredGroupInfo() {
        DiscoveredGroupInfo.Builder builder = DiscoveredGroupInfo.newBuilder();
        builder.setDiscoveredGroup(dto);
        dtoAsGroup.ifPresent(builder::setInterpretedGroup);
        dtoAsCluster.ifPresent(builder::setInterpretedCluster);
        return builder.build();
    }

    @Override
    public int hashCode() {
        return com.google.common.base.Objects.hashCode(dto, dtoAsCluster, dtoAsGroup);
    }

    @Override
    public boolean equals(@Nullable Object other) {
        if (!(other instanceof InterpretedGroup)) {
            return false;
        }

        final InterpretedGroup ig = (InterpretedGroup)other;
        return com.google.common.base.Objects.equal(dto, ig.dto) &&
            com.google.common.base.Objects.equal(dtoAsCluster.map(ClusterInfo.Builder::build),
                ig.dtoAsCluster.map(ClusterInfo.Builder::build)) &&
            com.google.common.base.Objects.equal(dtoAsGroup.map(GroupInfo.Builder::build),
                ig.dtoAsGroup.map(GroupInfo.Builder::build));
    }

    @Override
    public String toString() {
        return "InterpretedGroup[" + dto + ']';
    }
}
