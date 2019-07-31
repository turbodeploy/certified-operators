package com.vmturbo.group.setting;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredSettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.Scope;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Map discovered setting policies to instances of {@link com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo}.
 */
public class DiscoveredSettingPoliciesMapper {
    private final Logger logger = LogManager.getLogger();

    /**
     * Discovered setting policies reference groups by their names. We need to map
     * from this name to the group OID. The groupOids map holds this mapping.
     */
    private final Map<String, Long> groupNamesToOids;

    public DiscoveredSettingPoliciesMapper(@Nonnull final Map<String, Long> groupNamesToOids) {
        this.groupNamesToOids = Objects.requireNonNull(groupNamesToOids);
    }

    /**
     * Convert a discovered setting policy to a {@link com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo}.
     *
     * @param info the description discovered setting policy.
     * @param targetId The id of the target whose setting policy is being mapped.
     * @return an equivalent {@link com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo}.
     */
    public Optional<SettingPolicyInfo> mapToSettingPolicyInfo(@Nonnull final DiscoveredSettingPolicyInfo info,
                                                              final long targetId) {
        if (info.getDiscoveredGroupNamesCount() == 0) {
            logger.warn("Invalid setting policy {}. Must be associated with at least one group.", info);
            return Optional.empty();
        }
        if (info.getSettingsCount() == 0) {
            logger.warn("Invalid setting policy {}. Must be associated with at least one setting.", info);
            return Optional.empty();
        }

        final List<Long> groupOids = new ArrayList<>(info.getDiscoveredGroupNamesCount());
        for (String groupName : info.getDiscoveredGroupNamesList()) {
            final Long oid = groupNamesToOids.get(GroupProtoUtil.createGroupCompoundKey(groupName,
                    EntityType.forNumber(info.getEntityType()), targetId));
            if (oid == null) {
                logger.warn("Invalid setting policy {}. Invalid groupName {}.", info, groupName);
                // Valid group names could be large, they are 300+ in BoA environment. Stop printing them
                // out by default
                logger.debug("Valid group names are: {}", groupNamesToOids.keySet());
                return Optional.empty();
            }
            groupOids.add(oid);
        }

        return Optional.of(SettingPolicyInfo.newBuilder()
            .setTargetId(targetId)
            .setEntityType(info.getEntityType())
            .setScope(Scope.newBuilder().addAllGroups(groupOids))
            .setName(info.getName())
            .setDisplayName(info.getDisplayName())
            .addAllSettings(info.getSettingsList())
            .build());
    }
}
