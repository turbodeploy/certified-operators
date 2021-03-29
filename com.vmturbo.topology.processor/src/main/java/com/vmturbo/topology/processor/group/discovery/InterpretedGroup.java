package com.vmturbo.topology.processor.group.discovery;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredGroupsPoliciesSettings.UploadedGroup;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.topology.DiscoveredGroup.DiscoveredGroupInfo;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO;
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

    private final CommonDTO.GroupDTO sdkDTO;

    private final Optional<GroupDefinition.Builder> groupDefinition;

    /**
     * Constructor for {@link InterpretedGroup}.
     *
     * @param sdkDTO the original sdk group dto coming from probe
     * @param groupDefinition the interpreted group, which may be empty if any error occurred
     */
    public InterpretedGroup(@Nonnull final CommonDTO.GroupDTO sdkDTO,
                            @Nonnull final Optional<GroupDefinition.Builder> groupDefinition) {
        this.sdkDTO = Objects.requireNonNull(sdkDTO);
        this.groupDefinition = Objects.requireNonNull(groupDefinition);
    }

    /**
     * Get the static members of the group, if the group is a static group.
     *
     * @return A list containing the static members of the group. An empty list if the group is
     *         not a static group/cluster. Note - from the output of this method there is no way
     *         to distinguish an empty static group from a non-static group.
     */
    @Nonnull
    public Map<MemberType, List<Long>> getStaticMembersByType() {
        return groupDefinition.map(group ->
                group.getStaticGroupMembers().getMembersByTypeList().stream()
                        .collect(Collectors.toMap(StaticMembersByType::getType,
                                StaticMembersByType::getMembersList)))
                .orElse(Collections.emptyMap());
    }

    /**
     * Get all the StaticMembersByType for this group.
     *
     * @return list of {@link StaticMembersByType}
     */
    @Nonnull
    public List<StaticMembersByType> getStaticMembers() {
        return groupDefinition.map(GroupDefinition.Builder::getStaticGroupMembers)
                .map(StaticMembers::getMembersByTypeList)
                .orElse(Collections.emptyList());
    }

    /**
     * Get all the static member oids for this group.
     *
     * @return list of oids
     */
    @Nonnull
    public Set<Long> getAllStaticMembers() {
        return groupDefinition.map(GroupProtoUtil::getAllStaticMembers)
                .orElse(Collections.emptySet());
    }

    /**
     * Get the source id of this group from the probe.
     *
     * @return source identification
     */
    public String getSourceId() {
        return GroupProtoUtil.extractId(sdkDTO);
    }

    /**
     * Get the original sdk group.
     *
     * @return {@link GroupDTO}
     */
    public CommonDTO.GroupDTO getOriginalSdkGroup() {
        return sdkDTO;
    }

    /**
     * Get the result of the interpretation as a {@link GroupDefinition.Builder}.
     *
     * @return internal {@link GroupDefinition.Builder} representation of the sdk group dto
     */
    public Optional<GroupDefinition.Builder> getGroupDefinition() {
        return groupDefinition;
    }

    /**
     * Create a {@link DiscoveredGroupInfo} for the purposes of
     * {@link DiscoveredGroupRpcService} and dumping diags.
     *
     * @return The {@link DiscoveredGroupInfo} representing this DTO.
     */
    @Nonnull
    public DiscoveredGroupInfo createDiscoveredGroupInfo() {
        DiscoveredGroupInfo.Builder builder = DiscoveredGroupInfo.newBuilder();
        builder.setDiscoveredGroup(sdkDTO);
        groupDefinition.ifPresent(group ->
                builder.setUploadedGroup(UploadedGroup.newBuilder()
                        .setSourceIdentifier(GroupProtoUtil.extractId(sdkDTO))
                        .setDefinition(group)));
        return builder.build();
    }

    /**
     * Convert this group into a {@link UploadedGroup} which will be used to upload to group
     * component.
     *
     * @return optional of UploadedGroup
     */
    public Optional<UploadedGroup> convertToUploadedGroup() {
        return groupDefinition.map(group -> UploadedGroup.newBuilder()
                .setSourceIdentifier(getSourceId())
                .setDefinition(group)
                .build());
    }

    /**
     * Creates a deep copy of Interpreted group.
     *
     * @param source to make copy of it
     * @return deep copy of provided group
     */
    public static InterpretedGroup deepCopy(@Nonnull InterpretedGroup source) {
        final CommonDTO.GroupDTO dto = GroupDTO.newBuilder(source.sdkDTO).build();
        final Optional<GroupDefinition.Builder> groupDefinition = source.groupDefinition.map(
                group -> GroupDefinition.newBuilder(group.build()));
        return new InterpretedGroup(dto, groupDefinition);
    }

    @Override
    public int hashCode() {
        return com.google.common.base.Objects.hashCode(sdkDTO, groupDefinition);
    }

    @Override
    public boolean equals(@Nullable Object other) {
        if (!(other instanceof InterpretedGroup)) {
            return false;
        }

        final InterpretedGroup ig = (InterpretedGroup)other;
        return com.google.common.base.Objects.equal(sdkDTO, ig.sdkDTO) &&
                com.google.common.base.Objects.equal(
                        groupDefinition.map(GroupDefinition.Builder::build),
                        ig.groupDefinition.map(GroupDefinition.Builder::build));
    }

    @Override
    public String toString() {
        return "InterpretedGroup[\n" + sdkDTO
                + ",\n" + groupDefinition +
                "\n]";
    }
}
