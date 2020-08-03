package com.vmturbo.group.setting;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredSettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.Scope;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;

/**
 * Map discovered setting policies to instances of {@link com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo}.
 */
public class DiscoveredSettingPoliciesMapper {
    private final Logger logger = LogManager.getLogger();

    /**
     * The id of the target whose setting policy is being mapped.
     */
    private final long targetId;
    /**
     * Discovered setting policies reference groups by their identifying key. We need to map
     * from this identifying key to the group OID. The groupOids map holds this mapping.
     */
    private final Map<String, Long> groupNamesToOids;

    /**
     * Cpmstricts setting policy mapper.
     *
     * @param targetId target to operate on
     * @param groupNamesToOids map of srcId -> groupId for the target
     */
    public DiscoveredSettingPoliciesMapper(long targetId, @Nonnull final Map<String, Long> groupNamesToOids) {
        this.targetId = targetId;
        this.groupNamesToOids = Objects.requireNonNull(groupNamesToOids);
    }

    /**
     * Convert a discovered setting policy to a {@link com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo}.
     *
     * @param info the description discovered setting policy.
     * @return an equivalent {@link com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo}.
     */
    public Optional<SettingPolicyInfo> mapToSettingPolicyInfo(@Nonnull final DiscoveredSettingPolicyInfo info) {
        if (info.getDiscoveredGroupNamesCount() == 0) {
            logger.warn("Invalid setting policy (must be associated with at least one group): {}", info);
            return Optional.empty();
        }
        if (info.getSettingsCount() == 0) {
            logger.warn("Invalid setting policy (must be associated with at least one setting): {}", info);
            return Optional.empty();
        }

        final List<Long> groupOids = new ArrayList<>(info.getDiscoveredGroupNamesCount());
        for (String groupIdentifyingKey : info.getDiscoveredGroupNamesList()) {
            final Long oid = groupNamesToOids.get(groupIdentifyingKey);
            if (oid == null) {
                logger.warn("Invalid setting policy (group {} not found in target {}): {}",
                        groupIdentifyingKey, targetId, info);
                // Valid group names could be large, they are 300+ in BoA environment. Stop printing them
                // out by default
                logger.debug("Valid group discovered source ids are: {}", groupNamesToOids.keySet());
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
