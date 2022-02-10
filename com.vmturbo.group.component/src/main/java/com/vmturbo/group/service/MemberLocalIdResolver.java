package com.vmturbo.group.service;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.group.group.IGroupStore.DiscoveredGroup;
import com.vmturbo.group.stitching.StitchingGroup;
import com.vmturbo.group.stitching.StitchingResult;

/**
 * This class resolves member local (target-specific) IDs to global OIDs.
 */
public class MemberLocalIdResolver {

    private static final Logger logger = LogManager.getLogger();

    private final Table<Long, String, Long> groupIdTable;

    /**
     * Create a {@code MemberLocalIdResolver}.
     *
     * @param stitchingResult Group stitching result.
     */
    public MemberLocalIdResolver(@Nonnull final StitchingResult stitchingResult) {
        Objects.requireNonNull(stitchingResult);

        this.groupIdTable = createGroupIdTable(stitchingResult);
    }

    /**
     * Resolve nested groups local IDs to global OIDs.
     *
     * @param groupList Original (source) list of groups.
     * @return List of groups where all member local IDs are replaced with OIDs.
     * @throws IllegalStateException If one of the groups has no targets.
     */
    public List<DiscoveredGroup> resolveNestedGroupsLocalIds(
            @Nonnull final List<DiscoveredGroup> groupList) throws IllegalStateException {
        Objects.requireNonNull(groupList);

        return groupList.stream()
                .filter(Objects::nonNull)
                .map(this::resolveNestedGroupsLocalIds)
                .collect(Collectors.toList());
    }

    private DiscoveredGroup resolveNestedGroupsLocalIds(@Nonnull final DiscoveredGroup srcGroup) {
        Objects.requireNonNull(srcGroup);

        if (!hasMemberWithLocalId(srcGroup)) {
            // All static members in the group have OIDs, so nothing to resolve
            return srcGroup;
        }

        if (srcGroup.getTargetIds().isEmpty()) {
            throw new IllegalStateException("Group has no associated targets: "
                    + srcGroup.getDefinition().getDisplayName());
        }

        final GroupDefinition.Builder groupDefinition = srcGroup.getDefinition().toBuilder();
        groupDefinition.getStaticGroupMembersBuilder().getMembersByTypeBuilderList()
                .forEach(staticMembersByType -> {
                    staticMembersByType.getMemberLocalIdList().forEach(localId -> {
                        Long oid = null;
                        for (final Long targetId : srcGroup.getTargetIds()) {
                            oid = groupIdTable.get(targetId, localId);
                            if (oid != null) {
                                break;
                            }
                        }
                        if (oid == null) {
                            logger.error("Cannot find OID for member: " + localId);
                        } else {
                            staticMembersByType.addMembers(oid);
                        }
                    });
                    staticMembersByType.clearMemberLocalId();
                });

        return new DiscoveredGroup(
                srcGroup.getOid(),
                groupDefinition.build(),
                srcGroup.getSourceIdentifier(),
                srcGroup.stitchAcrossTargets(),
                srcGroup.getTargetIds(),
                srcGroup.getExpectedMembers(),
                srcGroup.isReverseLookupSupported());
    }

    /**
     * Build mapping from member local ID to member OID.
     *
     * @param stitchingResult Group stitching result.
     * @return Table: [target ID, member local ID] -> OID.
     */
    private static Table<Long, String, Long> createGroupIdTable(
            @Nonnull final StitchingResult stitchingResult) {
        final ImmutableTable.Builder<Long, String, Long> builder = ImmutableTable.builder();
        for (StitchingGroup stitchingGroup : stitchingResult.getGroupsToAddOrUpdate()) {
            for (long targetId : stitchingGroup.getTargetIds()) {
                builder.put(targetId, stitchingGroup.getSourceId(), stitchingGroup.getOid());
            }
        }
        return builder.build();
    }

    private static boolean hasMemberWithLocalId(@Nonnull final DiscoveredGroup group) {
        final GroupDefinition groupDefinition = group.getDefinition();
        if (groupDefinition.hasStaticGroupMembers()) {
            return groupDefinition.getStaticGroupMembers().getMembersByTypeList()
                    .stream()
                    .anyMatch(staticMembers -> !staticMembers.getMemberLocalIdList().isEmpty());
        }
        return false;
    }
}
