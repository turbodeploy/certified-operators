package com.vmturbo.topology.processor.group.settings;

import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.common.setting.ConfigurableActionSettings;
import com.vmturbo.components.common.setting.EntitySettingSpecs;

/**
 * Settings applicator that requires multiple settings to be processed.
 */
@FunctionalInterface
interface SettingApplicator {

    /**
     * Applies settings to the specified entity.
     *
     * @param entity entity to apply settings to
     * @param entitySettings the entity related settings to apply
     * @param actionModeSettings related the entity related settings to apply
     */
    void apply(@Nonnull TopologyEntityDTO.Builder entity,
               @Nonnull Map<EntitySettingSpecs, Setting> entitySettings,
               @Nonnull Map<ConfigurableActionSettings, Setting> actionModeSettings);
}
