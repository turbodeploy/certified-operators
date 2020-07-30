package com.vmturbo.group.setting;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.components.common.setting.ActionSettingSpecs;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;

/**
 * Setting spec store, based on enum ({@link EntitySettingSpecs}) and
 * ({@link GlobalSettingSpecs}).
 */
@Immutable
public class EnumBasedSettingSpecStore implements SettingSpecStore {

    private final Map<String, SettingSpec> settingSpecMap;
    private final boolean hideExecutionScheduleSetting;
    private final boolean hideExternalApprovalOrAuditSettings;

    /**
     * Constructs EnumBasedSettingSpecStore with the provided feature flag.
     *
     * @param hideExecutionScheduleSetting true when we need to hide the execution window settings.
     * @param hideExternalApprovalOrAuditSettings true when we need to hide the external approval
     *                                            related settings.
     */
    public EnumBasedSettingSpecStore(
            boolean hideExecutionScheduleSetting,
            boolean hideExternalApprovalOrAuditSettings) {
        this.hideExecutionScheduleSetting = hideExecutionScheduleSetting;
        this.hideExternalApprovalOrAuditSettings = hideExternalApprovalOrAuditSettings;

        Map<String, SettingSpec> specs = new HashMap<>();
        for (EntitySettingSpecs setting : EntitySettingSpecs.values()) {
            SettingSpec settingSpec = setting.getSettingSpec();
            if (hideExternalApprovalOrAuditSettings && ActionSettingSpecs.isActionModeSetting(setting)) {
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
            specs.put(setting.getSettingName(), settingSpec);
        }
        for (GlobalSettingSpecs setting : GlobalSettingSpecs.values()) {
            specs.put(setting.getSettingName(), setting.createSettingSpec());
        }

        ActionSettingSpecs.getSettingSpecs().stream()
            // only keep settings that have not been hidden by a feature flag
            .filter(this::isSettingVisible)
            .forEach(settingSpec ->
                specs.put(settingSpec.getName(), settingSpec));

        settingSpecMap = Collections.unmodifiableMap(specs);
    }

    @Nonnull
    @Override
    public Optional<SettingSpec> getSettingSpec(@Nonnull String name) {
        Objects.requireNonNull(name);
        return Optional.ofNullable(settingSpecMap.get(name));
    }

    @Nonnull
    @Override
    public Collection<SettingSpec> getAllSettingSpecs() {
        return settingSpecMap.values();
    }

    @Nonnull
    @Override
    public Collection<SettingSpec> getAllGlobalSettingSpecs() {
        return settingSpecMap
                .values()
                .stream()
                .filter(SettingSpec::hasGlobalSettingSpec)
                .collect(Collectors.toList());
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
