package com.vmturbo.topology.processor.group.discovery;

import static com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.Setting.SettingType.HORIZONTAL_SCALE_DOWN_AUTOMATION_MODE;
import static com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.Setting.SettingType.HORIZONTAL_SCALE_UP_AUTOMATION_MODE;
import static com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.Setting.SettingType.MAX_REPLICAS;
import static com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.Setting.SettingType.MIN_REPLICAS;
import static com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.Setting.SettingType.MOVE_AUTOMATION_MODE;
import static com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.Setting.SettingType.RESIZE_AUTOMATION_MODE;
import static com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.Setting.SettingType.RESPONSE_TIME_SLO;
import static com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.Setting.SettingType.TRANSACTION_SLO;
import static com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.Setting.SettingType.VCPU_CORES_MAX_THRESHOLD;
import static com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.Setting.SettingType.VCPU_CORES_MIN_THRESHOLD;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredSettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SortedSetOfOidSettingValue;
import com.vmturbo.components.common.setting.ConfigurableActionSettings;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.ConstraintInfo;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.ConstraintType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.Setting.SettingType;
import com.vmturbo.platform.sdk.common.util.SDKUtil;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Interpreter for converting mediation {@link GroupDTO}s to {@link DiscoveredSettingPolicyInfo}.
 */
public class DiscoveredSettingPolicyInterpreter {

    /**
     * A static map that maps {@link SettingType} to its corresponding setting name.
     */
    private static final Map<SettingType, String> settingType2SettingName =
            ImmutableMap.<SettingType, String>builder()
                    .put(VCPU_CORES_MIN_THRESHOLD,
                            EntitySettingSpecs.ResizeVcpuMinThreshold.getSettingName())
                    .put(VCPU_CORES_MAX_THRESHOLD,
                            EntitySettingSpecs.ResizeVcpuMaxThreshold.getSettingName())
                    .put(RESPONSE_TIME_SLO,
                            EntitySettingSpecs.ResponseTimeSLO.getSettingName())
                    .put(TRANSACTION_SLO,
                            EntitySettingSpecs.TransactionSLO.getSettingName())
                    .put(MIN_REPLICAS,
                            EntitySettingSpecs.MinReplicas.getSettingName())
                    .put(MAX_REPLICAS,
                            EntitySettingSpecs.MaxReplicas.getSettingName())
                    .put(MOVE_AUTOMATION_MODE,
                            ConfigurableActionSettings.Move.getSettingName())
                    .put(RESIZE_AUTOMATION_MODE,
                            ConfigurableActionSettings.Resize.getSettingName())
                    .put(HORIZONTAL_SCALE_UP_AUTOMATION_MODE,
                            ConfigurableActionSettings.HorizontalScaleUp.getSettingName())
                    .put(HORIZONTAL_SCALE_DOWN_AUTOMATION_MODE,
                            ConfigurableActionSettings.HorizontalScaleDown.getSettingName())
                    .build();

    /**
     * A static map that maps SLO {@link SettingType} to its mandatory related setting.
     */
    private static final Map<SettingType, Setting> SLOValue2SLOEnabled =
            ImmutableMap.of(RESPONSE_TIME_SLO,
                    Setting.newBuilder()
                            .setSettingSpecName(
                                    EntitySettingSpecs.ResponseTimeSLOEnabled.getSettingName())
                            .setBooleanSettingValue(
                                    BooleanSettingValue.newBuilder().setValue(true).build())
                            .build(),
                    TRANSACTION_SLO,
                    Setting.newBuilder()
                            .setSettingSpecName(
                                    EntitySettingSpecs.TransactionSLOEnabled.getSettingName())
                            .setBooleanSettingValue(
                                    BooleanSettingValue.newBuilder().setValue(true).build())
                            .build());

    private final Logger logger = LogManager.getLogger();

    private final TargetStore targetStore;

    private final EntityStore entityStore;

    /**
     * Constructs a new {@link DiscoveredSettingPolicyInterpreter}.
     * @param targetStore The target store.
     * @param entityStore The entity store.
     */
    public DiscoveredSettingPolicyInterpreter(@Nonnull TargetStore targetStore,
                                              @Nonnull EntityStore entityStore) {

        this.targetStore = Objects.requireNonNull(targetStore);
        this.entityStore = Objects.requireNonNull(entityStore);
    }

    /**
     * Converts the provided {@link GroupDTO}s to {@link DiscoveredSettingPolicyInfo}.
     * @param targetId The target ID for the target discovering the provided groups.
     * @param groups The groups to convert.
     * @return An immutable list of converted {@link DiscoveredSettingPolicyInfo}s.
     */
    @Nonnull
    public List<DiscoveredSettingPolicyInfo> convertDiscoveredSettingPolicies(final long targetId,
                                                                              @Nonnull final List<CommonDTO.GroupDTO> groups) {

        final Map<String, Long> entityIdMap = entityStore.getTargetEntityIdMap(targetId)
                .orElseGet(() -> {
                    logger.warn("Unable to resolve the target entity ID map for {}", targetId);
                    return Collections.emptyMap();
                });
        final PolicyConversionContext context = PolicyConversionContext.createContext(targetId, entityIdMap);

        final ImmutableList.Builder<DiscoveredSettingPolicyInfo> discoveredSettingPolicyInfos = ImmutableList.<DiscoveredSettingPolicyInfo>builder()
                .addAll(convertTemplateExclusionGroupsToPolicies(groups, context))
                .addAll(convertConsistentScalingGroupsToPolicies(groups, context))
                .addAll(convertSettingPolicies(groups, context));

        return discoveredSettingPolicyInfos.build();
    }

    private List<DiscoveredSettingPolicyInfo> convertTemplateExclusionGroupsToPolicies(
            @Nonnull List<GroupDTO> groups,
            @Nonnull final PolicyConversionContext context) {

        final ImmutableList.Builder<DiscoveredSettingPolicyInfo> result = ImmutableList.builder();
        String targetName = getTargetDisplayName(context.targetId());

        for (GroupDTO group : groups) {
            if (group.getConstraintInfo()
                    .getConstraintType() != ConstraintType.TEMPLATE_EXCLUSION) {
                continue;
            }

            final SortedSetOfOidSettingValue.Builder oids = SortedSetOfOidSettingValue.newBuilder();
            oids.addAllOids(convertTemplateNamesToOids(group, targetName, context));

            final Setting.Builder setting = Setting.newBuilder();
            setting.setSettingSpecName(EntitySettingSpecs.ExcludedTemplates.getSettingName());
            setting.setSortedSetOfOidSettingValue(oids);

            result.add(createPolicy(group, context.targetId(), Collections.singletonList(setting.build()),
                    "Cloud Compute Tier Exclusion Policy", "EXP"));
        }

        return result.build();
    }

    private List<DiscoveredSettingPolicyInfo> convertConsistentScalingGroupsToPolicies(
            final List<GroupDTO> groups,
            @Nonnull final PolicyConversionContext context) {

        final ImmutableList.Builder<DiscoveredSettingPolicyInfo> discoveredPolicyInfos = ImmutableList.builder();
        for (GroupDTO group : groups) {
            if (!group.getIsConsistentResizing()) {
                continue;
            }

            Setting.Builder setting = Setting.newBuilder();
            setting.setSettingSpecName(EntitySettingSpecs.EnableConsistentResizing.getSettingName());
            setting.setBooleanSettingValue(BooleanSettingValue.newBuilder().setValue(true));

            discoveredPolicyInfos.add(createPolicy(group, context.targetId(),
                    Collections.singletonList(setting.build()), "Consistent Scaling Policy", "CSP"));
        }
        return discoveredPolicyInfos.build();
    }

    /**
     * Convert setting policies GroupDTOs to DiscoveredSettingPolicyInfos.
     *
     * @param groups GroupDTOs to process
     * @return the list of DiscoveredSettingPolicyInfos
     */
    @Nonnull
    private List<DiscoveredSettingPolicyInfo> convertSettingPolicies(
            @Nonnull List<CommonDTO.GroupDTO> groups,
            @Nonnull final PolicyConversionContext context) {
        List<DiscoveredSettingPolicyInfo> result = new ArrayList<>();
        String targetName = getTargetDisplayName(context.targetId());

        for (GroupDTO group : groups) {
            if (!group.hasSettingPolicy()) {
                continue;
            }
            List<Setting> settings = new ArrayList<>(group.getSettingPolicy().getSettingsCount());
            group.getSettingPolicy().getSettingsList().forEach(discoveredSetting -> {
                final SettingType settingType = discoveredSetting.getType();
                if (!settingType2SettingName.containsKey(settingType)) {
                    logger.error("The setting \"{}\" discovered from target \"{}\" is unknown.",
                            settingType, targetName);
                    return;
                }
                final String settingName = settingType2SettingName.get(settingType);
                final Setting.Builder setting = Setting.newBuilder().setSettingSpecName(settingName);
                switch (discoveredSetting.getSettingValueTypeCase()) {
                    case NUMERIC_SETTING_VALUE_TYPE:
                        setting.setNumericSettingValue(NumericSettingValue.newBuilder()
                                .setValue(discoveredSetting.getNumericSettingValueType().getValue()));
                        break;
                    case STRING_SETTING_VALUE_TYPE:
                        // Currently, all string setting values from discovered policy settings
                        // map to enum setting values
                        // TODO: Distinguish between String and Enum setting value type if needed
                        //   in the future
                        setting.setEnumSettingValue(SettingProto.EnumSettingValue.newBuilder()
                                .setValue(discoveredSetting.getStringSettingValueType().getValue()));
                        break;
                    default:
                        logger.error("The setting value \"{}\" discovered from target \"{}\" is unknown.",
                                discoveredSetting.getSettingValueTypeCase(), targetName);
                        return;
                }
                settings.add(setting.build());
                // If SLO value exists, enable SLO settings automatically
                if (SLOValue2SLOEnabled.containsKey(settingType)) {
                    settings.add(SLOValue2SLOEnabled.get(settingType));
                }
            });

            if (settings.size() > 0) {
                result.add(createPolicy(
                        group,
                        context.targetId(),
                        settings,
                        "Setting policy",
                        "SET",
                        group.getSettingPolicy().getDisplayName()
                ));
            } else {
                logger.warn("No setting policy were created for \"{}\" discovered from \"{}\"",
                        group.getDisplayName(), targetName);
            }
        }

        return result;
    }

    private DiscoveredSettingPolicyInfo createPolicy(
            GroupDTO group,
            long targetId,
            List<Setting> settings,
            String displayNameId,
            String nameId) {

        return createPolicy(
                group,
                targetId,
                settings,
                displayNameId,
                nameId,
                null
        );
    }

    private DiscoveredSettingPolicyInfo createPolicy(
            GroupDTO group,
            long targetId,
            List<Setting> settings,
            String displayNameId,
            String nameId,
            @Nullable final String displayName) {
        DiscoveredSettingPolicyInfo.Builder policy = DiscoveredSettingPolicyInfo.newBuilder();
        policy.setEntityType(group.getEntityType().getNumber());
        policy.addDiscoveredGroupNames(GroupProtoUtil.createIdentifyingKey(group));
        if (StringUtils.isEmpty(displayName)) {
            String name = String.format("%s - %s (target %d)", group.getDisplayName(), displayNameId, targetId);
            policy.setDisplayName(name);
        } else {
            policy.setDisplayName(String.format("%s (target %d)", displayName, targetId));
        }
        policy.setName(createPolicyName(group, targetId, nameId));
        policy.addAllSettings(settings);
        return policy.build();
    }

    /**
     * Create a policy name based on group name and discovering target oid. It will make the name
     * stays under 255 character.
     *
     * @param group the group which has the policy.
     * @param targetId the id for target discovering the group.
     * @param prefix the prefix distinguishing different type of policies.
     * @return the name created for policy.
     */
    private String createPolicyName(GroupDTO group, long targetId, String prefix) {
        // It is expected that the group name is unique in the scope of that target.
        // Therefore, the concatenation of group name and target id should be unqiue.
        final String name = String.join(":", prefix, extractSettingPolicyGroupName(group),
                String.valueOf(targetId));
        // The name of policy should not be longer than character limit. Fix if that is the case
        return SDKUtil.fixUuid(name, 255, 215);
    }

    @Nonnull
    private String extractSettingPolicyGroupName(@Nonnull GroupDTO group) {
        switch (group.getInfoCase()) {
            case GROUP_NAME:
                return group.getGroupName();
            case CONSTRAINT_INFO:
                return group.getConstraintInfo().getConstraintId();
            case SETTING_POLICY:
                return group.getSettingPolicy().getName();
            default:
                throw new IllegalArgumentException("Unknown group info " + group.getInfoCase()
                        + " for group " + group.getDisplayName());
        }
    }

    @Nonnull
    private String getTargetDisplayName(long targetId) {
        Optional<String> name = targetStore.getTargetDisplayName(targetId);
        return name.orElseGet(() -> String.valueOf(targetId));
    }

    /**
     * Converts template names to OIDs for a template exclusion group.
     *
     * @param templateExclusionGroup template exclusion group
     * @param targetName The target name
     * @return the set of template OIDs
     */
    @Nonnull
    private Set<Long> convertTemplateNamesToOids(@Nonnull CommonDTO.GroupDTO templateExclusionGroup,
                                                 @Nonnull String targetName,
                                                 @Nonnull final PolicyConversionContext context) {

        final Map<String, Long> entityIdMap = context.entityIdMap();

        ConstraintInfo constraint = templateExclusionGroup.getConstraintInfo();

        if (constraint == null) {
            logger.warn("Constraint is null for group '{}', target {}", templateExclusionGroup,
                    targetName);
            return Collections.emptySet();
        }

        if (constraint.getExcludedTemplatesCount() <= 0) {
            logger.warn("No cloud tiers for group '{}', target {}", templateExclusionGroup,
                    targetName);
            return Collections.emptySet();
        }

        // The OIDs should be sorted
        Set<Long> result = new TreeSet<>();

        for (String templateName : constraint.getExcludedTemplatesList()) {
            Long oid = entityIdMap.get(templateName);

            if (oid == null) {
                logger.error("No OID found for cloud tier '{}', target {}", templateName,
                        targetName);
            } else {
                result.add(oid);
            }
        }

        return result;
    }

    /**
     * Conversion context for converting groups to setting policies.
     */
    static class PolicyConversionContext {

        private final long targetId;

        private final Map<String, Long> entityIdMap;

        private PolicyConversionContext(final long targetId,
                                        @Nonnull final Map<String, Long> entityIdMap) {
            this.targetId = targetId;
            this.entityIdMap = ImmutableMap.copyOf(entityIdMap);
        }

        static PolicyConversionContext createContext(final long targetId,
                                                     @Nonnull final Map<String, Long> entityIdMap) {
            return new PolicyConversionContext(targetId, entityIdMap);
        }

        long targetId() {
            return targetId;
        }

        Map<String, Long> entityIdMap() {
            return entityIdMap;
        }
    }
}
