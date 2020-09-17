package com.vmturbo.api.component.external.api.util.setting;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.api.component.external.api.service.SettingsService;
import com.vmturbo.api.dto.setting.SettingApiDTO;
import com.vmturbo.api.dto.setting.SettingsManagerApiDTO;
import com.vmturbo.api.dto.settingspolicy.SettingsPolicyApiDTO;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.components.common.setting.ActionSettingSpecs;
import com.vmturbo.components.common.setting.ActionSettingType;
import com.vmturbo.components.common.setting.ConfigurableActionSettings;

/**
 * Converts old settings to new settings.
 */
public class LegacySettingsConverter {

    /**
     * Private since this a utility class.
     */
    private LegacySettingsConverter() {

    }

    /**
     * Converts old settings to new settings in place.
     *
     * @param oldSettingsPolicyApiDTO the potentially old version of setting policy that needs to be
     *                             converted to the new version.
     */
    @Nonnull
    public static void convertSettings(
            @Nonnull final SettingsPolicyApiDTO oldSettingsPolicyApiDTO) {
        ApiEntityType entityType = ApiEntityType.fromString(oldSettingsPolicyApiDTO.getEntityType());
        Map<String, ConversionConfig> oldSettingMapping = CONVERSION_CONFIG_MAP.get(entityType);

        // the entity type does not have a setting name that needs to be fixed.
        if (oldSettingMapping == null) {
            return;
        }

        for (SettingsManagerApiDTO settingManager : oldSettingsPolicyApiDTO.getSettingsManagers()) {
            // ignore RI Purchase Profile settings because they do not need backwards compatibility
            // yet and they are handled very different due to JsonSubTypes. Handling them would
            // force us to jump through a bunch of hoops to get the generics working.
            // There might be future settings that do not use SettingApiDTO<String> so we must
            // not impact them.
            // As a result, we check specifically only for the managers we're interested in.
            if (SettingsService.AUTOMATION_MANAGER.equals(settingManager.getUuid())
                    || SettingsService.CONTROL_MANAGER.equals(settingManager.getUuid())) {
                settingManager.setSettings(translateSettings(
                    (List<? extends SettingApiDTO<String>>)settingManager.getSettings(),
                    oldSettingMapping));
            }
        }
    }

    private static List<SettingApiDTO<String>> translateSettings(
            @Nonnull List<? extends SettingApiDTO<String>> oldSettings,
            @Nonnull Map<String, ConversionConfig> oldSettingMapping) {
        List<SettingApiDTO<String>> convertedSettings = new ArrayList<>();
        for (SettingApiDTO<String> oldSettingApiDTO : oldSettings) {
            final ConversionConfig conversionConfig = oldSettingMapping.get(oldSettingApiDTO.getUuid());
            // Use a list because the translation might result in multiple settings like in the case of
            // resize split into resize [Vcpu|Vmem] x [Up|Down|AboveMax|BelowMin]
            List<SettingApiDTO<String>> translatedSetting = Collections.singletonList(oldSettingApiDTO);
            if (conversionConfig != null) {
                translatedSetting = translateSetting(conversionConfig, oldSettingApiDTO);
            }
            convertedSettings.addAll(translatedSetting);
        }

        return convertedSettings;
    }

    @Nonnull
    private static List<SettingApiDTO<String>> translateSetting(
            @Nonnull final ConversionConfig conversionConfig,
            @Nonnull final SettingApiDTO<String> oldSetting) {
        List<SettingApiDTO<String>> result = new ArrayList<>();

        for (ConfigurableActionSettings destinationSetting : conversionConfig.getActionSettingDestinations()) {
            String newSettingName = ActionSettingSpecs.getSubSettingFromActionModeSetting(
                destinationSetting, conversionConfig.getActionSettingTypeDestination());
            SettingApiDTO<String> clone = copy(oldSetting);
            clone.setUuid(newSettingName);
            result.add(clone);
        }

        if (!conversionConfig.mustDeleteLegacySetting()) {
            result.add(oldSetting);
        }

        return result;
    }

    private static SettingApiDTO<String> copy(SettingApiDTO<String> other) {
        SettingApiDTO<String> clone = new SettingApiDTO<>();
        clone.setUuid(other.getUuid());
        clone.setDisplayName(other.getDisplayName());
        clone.setClassName(other.getClassName());
        clone.setValue(other.getValue());
        clone.setOptions(other.getOptions());
        clone.setValueType(other.getValueType());
        clone.setValueObjectType(other.getValueObjectType());
        clone.setMin(other.getMin());
        clone.setMax(other.getMax());
        clone.setDefaultValue(other.getDefaultValue());
        clone.setCategories(other.getCategories());
        clone.setActiveSettingsPolicies(clone.getActiveSettingsPolicies());
        clone.setEntityType(clone.getEntityType());
        clone.setRange(other.getRange());
        clone.setScope(other.getScope());
        clone.setValueDisplayName(other.getValueDisplayName());
        clone.setSourceGroupName(other.getSourceGroupName());
        clone.setSourceGroupUuid(other.getSourceGroupUuid());
        return clone;
    }

    /**
     * Describes how old settings should be to converted to the new refactored
     * {@link ActionSettingSpecs}.
     */
    private static final List<ConversionConfig> CONVERSION_CONFIG = Arrays.asList(
        // BusinessUserMove was renamed to move
        new ConversionConfig("businessUserMove", ApiEntityType.BUSINESS_USER, true,
            ActionSettingType.ACTION_MODE,
            Arrays.asList(ConfigurableActionSettings.Move)),
        // suspendIsDisabled renamed to suspend
        new ConversionConfig("suspendIsDisabled", ApiEntityType.IOMODULE, true,
            ActionSettingType.ACTION_MODE,
            Arrays.asList(ConfigurableActionSettings.Suspend)),
        // provisionIsDisabled renamed to provision
        new ConversionConfig("provisionIsDisabled", ApiEntityType.STORAGECONTROLLER, true,
            ActionSettingType.ACTION_MODE,
            Arrays.asList(ConfigurableActionSettings.Provision)),
        // moveActionWorkflowWithNativeAsDefault did not follow the convention
        // should be moveActionWorkflow. this separate setting was unnecessary because action
        // workflow settings are already native as default.
        // BusinessUserMove already used preMoveActionWorkflow and postMoveActionWorkflow, so no
        // rename was necessary there.
        new ConversionConfig("moveActionWorkflowWithNativeAsDefault", ApiEntityType.BUSINESS_USER, true,
            ActionSettingType.REPLACE,
            Arrays.asList(ConfigurableActionSettings.Move)),

        // From 7.22.7 onwards, VM storageMove (the storage that the VM is hosted on) is no longer
        // // grouped with VM move (the physical machine the VM is hosted on). storageMove only
        // needs to be copied from the database. From 7.22.8 onwards, customer needs to create
        // move for VM  (move) and move for VM storage (storageMove) separately.
        // If the API copied them, we would never be able to separate VM move from storage VM move.

        // VM Resize setting split up by vcpu|vmem x Up|Down|AboveMax|BelowMin.
        new ConversionConfig("resizeActionWorkflow", ApiEntityType.VIRTUAL_MACHINE, true,
            ActionSettingType.REPLACE,
            Arrays.asList(
                ConfigurableActionSettings.ResizeVmemUpInBetweenThresholds,
                ConfigurableActionSettings.ResizeVmemDownInBetweenThresholds,
                ConfigurableActionSettings.ResizeVmemAboveMaxThreshold,
                ConfigurableActionSettings.ResizeVmemBelowMinThreshold,
                ConfigurableActionSettings.ResizeVcpuUpInBetweenThresholds,
                ConfigurableActionSettings.ResizeVcpuDownInBetweenThresholds,
                ConfigurableActionSettings.ResizeVcpuAboveMaxThreshold,
                ConfigurableActionSettings.ResizeVcpuBelowMinThreshold)),
        new ConversionConfig("preResizeActionWorkflow", ApiEntityType.VIRTUAL_MACHINE, true,
            ActionSettingType.PRE,
            Arrays.asList(
                ConfigurableActionSettings.ResizeVmemUpInBetweenThresholds,
                ConfigurableActionSettings.ResizeVmemDownInBetweenThresholds,
                ConfigurableActionSettings.ResizeVmemAboveMaxThreshold,
                ConfigurableActionSettings.ResizeVmemBelowMinThreshold,
                ConfigurableActionSettings.ResizeVcpuUpInBetweenThresholds,
                ConfigurableActionSettings.ResizeVcpuDownInBetweenThresholds,
                ConfigurableActionSettings.ResizeVcpuAboveMaxThreshold,
                ConfigurableActionSettings.ResizeVcpuBelowMinThreshold)),
        new ConversionConfig("postResizeActionWorkflow", ApiEntityType.VIRTUAL_MACHINE, true,
            ActionSettingType.POST,
            Arrays.asList(
                ConfigurableActionSettings.ResizeVmemUpInBetweenThresholds,
                ConfigurableActionSettings.ResizeVmemDownInBetweenThresholds,
                ConfigurableActionSettings.ResizeVmemAboveMaxThreshold,
                ConfigurableActionSettings.ResizeVmemBelowMinThreshold,
                ConfigurableActionSettings.ResizeVcpuUpInBetweenThresholds,
                ConfigurableActionSettings.ResizeVcpuDownInBetweenThresholds,
                ConfigurableActionSettings.ResizeVcpuAboveMaxThreshold,
                ConfigurableActionSettings.ResizeVcpuBelowMinThreshold))
    );

    /**
     * The the conversion configs and index it by EntityType -> OldSettingName -> ConversionConfig
     * for efficient lookup by {@link #convertSettings(SettingsPolicyApiDTO)}.
     */
    private static final Map<ApiEntityType, Map<String, ConversionConfig>> CONVERSION_CONFIG_MAP =
        CONVERSION_CONFIG.stream()
            .collect(Collectors.groupingBy(ConversionConfig::getEntityType,
                Collectors.toMap(ConversionConfig::getOldSettingName, Function.identity())));

    /**
     * Describes how the setting needs to be translated.
     */
    private static class ConversionConfig {
        private final String oldSettingName;
        private final ApiEntityType entityType;
        private final boolean deleteLegacySetting;
        private final ActionSettingType actionSettingTypeDestination;

        /**
         * If rename, this list will have one entry.
         * If split, this entry will have all entries this needs to be copied too.
         */
        private final List<ConfigurableActionSettings> actionSettingDestinations;

        private ConversionConfig(
                final String oldSettingName,
                final ApiEntityType entityType,
                final boolean deleteLegacySetting,
                final ActionSettingType actionSettingTypeDestination,
                final List<ConfigurableActionSettings> actionSettingDestinations) {
            this.oldSettingName = oldSettingName;
            this.entityType = entityType;
            this.deleteLegacySetting = deleteLegacySetting;
            this.actionSettingTypeDestination = actionSettingTypeDestination;
            this.actionSettingDestinations = actionSettingDestinations;
        }

        public String getOldSettingName() {
            return oldSettingName;
        }

        public ApiEntityType getEntityType() {
            return entityType;
        }

        public boolean mustDeleteLegacySetting() {
            return deleteLegacySetting;
        }

        public ActionSettingType getActionSettingTypeDestination() {
            return actionSettingTypeDestination;
        }

        public List<ConfigurableActionSettings> getActionSettingDestinations() {
            return actionSettingDestinations;
        }
    }
}
