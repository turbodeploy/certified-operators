package com.vmturbo.group.setting;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.utils.ProbeFeature;
import com.vmturbo.components.common.setting.ActionSettingSpecs;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;

/**
 * Setting spec store, based on enum ({@link EntitySettingSpecs}) and
 * ({@link GlobalSettingSpecs}).
 */
@Immutable
public class EnumBasedSettingSpecStore implements SettingSpecStore {

    private final Map<ProbeFeature, Map<String, SettingSpec>> settingSpecMap;
    private final boolean hideExecutionScheduleSetting;
    private final boolean hideExternalApprovalOrAuditSettings;
    private final ThinTargetCache thinTargetCache;

    /**
     * Constructs EnumBasedSettingSpecStore with the provided feature flag.
     *
     * @param hideExecutionScheduleSetting true when we need to hide the execution window settings.
     * @param hideExternalApprovalOrAuditSettings true when we need to hide the external approval
     * @param thinTargetCache a cache for simple target information
     */
    public EnumBasedSettingSpecStore(boolean hideExecutionScheduleSetting,
            boolean hideExternalApprovalOrAuditSettings,
            @Nonnull final ThinTargetCache thinTargetCache) {
        this.hideExecutionScheduleSetting = hideExecutionScheduleSetting;
        this.hideExternalApprovalOrAuditSettings = hideExternalApprovalOrAuditSettings;
        this.thinTargetCache = Objects.requireNonNull(thinTargetCache);

        final Map<ProbeFeature, Map<String, SettingSpec>> specs = new HashMap<>();
        for (EntitySettingSpecs setting : EntitySettingSpecs.values()) {
            SettingSpec settingSpec = setting.getSettingSpec();
            specs.computeIfAbsent(null, e -> new HashMap<>()).put(setting.getSettingName(),
                    settingSpec);
        }
        for (GlobalSettingSpecs setting : GlobalSettingSpecs.values()) {
            specs.computeIfAbsent(null, e -> new HashMap<>()).put(setting.getSettingName(),
                    setting.createSettingSpec());
        }

        final Map<String, ProbeFeature> settingSpecToProbeFeatureMap =
                ActionSettingSpecs.getSettingSpecToProbeFeatureMap();

        for (SettingSpec settingSpec : ActionSettingSpecs.getSettingSpecs()) {
            if (hideExternalApprovalOrAuditSettings && ActionSettingSpecs.isActionModeSetting(settingSpec.getName())) {
                // replace enum setting values with EXTERNAL_APPROVAL filtered
                List<String> filteredEnumValues =
                    settingSpec.getEnumSettingValueType().getEnumValuesList().stream()
                        .filter(enumValue -> !ActionMode.EXTERNAL_APPROVAL.toString().equals(enumValue))
                        .collect(Collectors.toList());
                settingSpec = settingSpec.toBuilder()
                    .setEnumSettingValueType(settingSpec.getEnumSettingValueType().toBuilder()
                        .clearEnumValues()
                        .addAllEnumValues(filteredEnumValues))
                    .build();
            }
            // only keep settings that have not been hidden by a feature flag
            if (isSettingVisible(settingSpec)) {
                final String settingSpecName = settingSpec.getName();
                specs.computeIfAbsent(settingSpecToProbeFeatureMap.get(settingSpecName),
                        e -> new HashMap<>()).put(settingSpecName, settingSpec);
            }
        }

        settingSpecMap = Collections.unmodifiableMap(specs);
    }

    @Nonnull
    @Override
    public Optional<SettingSpec> getSettingSpec(@Nonnull String name) {
        Objects.requireNonNull(name);
        return Optional.ofNullable(getAvailableSettingSpecMap().get(name));
    }

    @Nonnull
    @Override
    public Collection<SettingSpec> getAllSettingSpecs() {
        return getAvailableSettingSpecMap().values();
    }

    @Nonnull
    @Override
    public Collection<SettingSpec> getAllGlobalSettingSpecs() {
        return getAvailableSettingSpecMap()
                .values()
                .stream()
                .filter(SettingSpec::hasGlobalSettingSpec)
                .collect(Collectors.toList());
    }

    private Map<String, SettingSpec> getAvailableSettingSpecMap() {
        final Set<ProbeFeature> availableProbeFeatures = thinTargetCache.getAvailableProbeFeatures();
        final Map<String, SettingSpec> availableSettingSpecs = new HashMap<>();
        for (Entry<ProbeFeature, Map<String, SettingSpec>> entry : settingSpecMap.entrySet()) {
            if (entry.getKey() == null || availableProbeFeatures.contains(entry.getKey())) {
                entry.getValue().forEach(availableSettingSpecs::put);
            }
        }
        return availableSettingSpecs;
    }

    private boolean isSettingVisible(@Nonnull SettingSpec settingSpec) {
        return !isHiddenExecutionScheduleSetting(settingSpec.getName())
            && !isHiddenExternalApprovalOrAuditSetting(settingSpec.getName());
    }

    private boolean isHiddenExecutionScheduleSetting(@Nonnull String settingName) {
        return hideExecutionScheduleSetting
            && ActionSettingSpecs.isExecutionScheduleSetting(settingName);
    }

    private boolean isHiddenExternalApprovalOrAuditSetting(@Nonnull String settingName) {
        return hideExternalApprovalOrAuditSettings
            && ActionSettingSpecs.isExternalApprovalOrAuditSetting(settingName);
    }
}
