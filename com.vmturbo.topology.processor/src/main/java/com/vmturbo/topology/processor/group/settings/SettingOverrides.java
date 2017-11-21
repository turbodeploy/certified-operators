package com.vmturbo.topology.processor.group.settings;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.SettingOverride;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTOOrBuilder;

/**
 * A helper class to capture the {@link SettingOverride}s defined for a scenario and index
 * them in a way that makes application of overrides more straightforward.
 */
public class SettingOverrides {

    /**
     * settingName -> setting
     *
     * These overrides apply to all entities that have a setting matching settingName.
     */
    private Map<String, Setting> globalOverrides = new HashMap<>();

    /**
     * entityType -> settingName -> setting
     *
     * These overrides apply to all entities of a particular type that have a setting matching
     * settingName.
     */
    private Map<Integer, Map<String, Setting>> overridesForEntityType = new HashMap<>();

    public SettingOverrides(@Nonnull final List<ScenarioChange> changes) {
        changes.stream()
            .filter(ScenarioChange::hasSettingOverride)
            .map(ScenarioChange::getSettingOverride)
            .filter(SettingOverride::hasSetting)
            .forEach(settingOverride -> {
                final Map<String, Setting> settingByNameMap;
                if (settingOverride.hasEntityType()) {
                    settingByNameMap = overridesForEntityType.computeIfAbsent(
                            settingOverride.getEntityType(), k -> new HashMap<>());
                } else {
                    settingByNameMap = globalOverrides;
                }
                final Setting overridenSetting = settingOverride.getSetting();
                settingByNameMap.put(overridenSetting.getSettingSpecName(), overridenSetting);
            });
    }

    /**
     * Override the settings in a {@link EntitySettings.Builder} with the settings
     * that apply to the entity.
     *
     * @param entity The entity that the settings apply to.
     * @param settingsBuilder The {@link EntitySettings.Builder}. This builder can be modified
     *                        inside the function. We accept the builder as an
     *                        argument instead of returning a map so that we're not constructing
     *                        a lot of unnecessary map objects just to insert them into the DTO.
     */
    public void overrideSettings(final TopologyEntityDTOOrBuilder entity,
                                 @Nonnull final EntitySettings.Builder settingsBuilder) {
        settingsBuilder.addAllUserSettings(globalOverrides.values());
        settingsBuilder.addAllUserSettings(
                overridesForEntityType.getOrDefault(entity.getEntityType(), Collections.emptyMap())
                        .values());
    }
}
