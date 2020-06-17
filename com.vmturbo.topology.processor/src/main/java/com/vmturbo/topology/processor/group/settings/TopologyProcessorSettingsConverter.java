package com.vmturbo.topology.processor.group.settings;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting.ValueCase;
import com.vmturbo.common.protobuf.setting.SettingProto.SortedSetOfOidSettingValue;
import com.vmturbo.components.common.setting.ActionSettingSpecs;

/**
 * Converter between proto settings {@link Setting} and topology processor setting representation
 * {@link TopologyProcessorSetting}.
 */
public class TopologyProcessorSettingsConverter {
    /**
     * Convert simple {@link TopologyProcessorSetting} into collection of proto setting with
     * single element.
     */
    private static final Function<TopologyProcessorSetting<?>, Collection<Setting>>
            SIMPLE_SETTING_CONVERTER =
            (setting) -> ImmutableList.of((Setting)setting.getSettingValue());

    /**
     * Used to convert protobuf setting into corresponding topology processor setting.
     */
    private static final Map<ValueCase, Function<Setting, TopologyProcessorSetting<?>>>
            TO_TOPOLOGY_PROCESSOR_SETTINGS_CONVERTERS =
            ImmutableMap.<ValueCase, Function<Setting, TopologyProcessorSetting<?>>>builder()
                    .put(ValueCase.BOOLEAN_SETTING_VALUE, BooleanTopologyProcessorSetting::new)
                    .put(ValueCase.NUMERIC_SETTING_VALUE, NumericTopologyProcessorSetting::new)
                    .put(ValueCase.STRING_SETTING_VALUE, StringTopologyProcessorSetting::new)
                    .put(ValueCase.ENUM_SETTING_VALUE, EnumTopologyProcessorSetting::new)
                    .put(ValueCase.SORTED_SET_OF_OID_SETTING_VALUE, SortedSetOfOidTopologyProcessorSetting::new)
                    .build();

    /**
     * Used to convert topology processor setting into protobuf settings.
     */
    private static final Map<Class<?>, Function<TopologyProcessorSetting<?>, Collection<Setting>>> TO_PROTO_SETTINGS_CONVERTERS =
            ImmutableMap.<Class<?>, Function<TopologyProcessorSetting<?>, Collection<Setting>>>builder()
                    .put(BooleanTopologyProcessorSetting.class, SIMPLE_SETTING_CONVERTER)
                    .put(NumericTopologyProcessorSetting.class, SIMPLE_SETTING_CONVERTER)
                    .put(StringTopologyProcessorSetting.class, SIMPLE_SETTING_CONVERTER)
                    .put(EnumTopologyProcessorSetting.class, SIMPLE_SETTING_CONVERTER)
                    .put(SortedSetOfOidTopologyProcessorSetting.class, SIMPLE_SETTING_CONVERTER)
                    .put(ActionModeExecutionScheduleTopologyProcessorSetting.class, (val) -> transformActionModeExecutionScheduleSettingIntoProtoSettings((ActionModeExecutionScheduleSetting)val.getSettingValue()))
                    .build();

    private TopologyProcessorSettingsConverter() {}

    /**
     * Convert simple protobuf settings into topology processor settings.
     * InputSettingsList could have single settings or several dependent settings
     * (ActionMode and ExecutionSchedule). If input settings list contains action mode setting then
     * we transform them into ActionModeExecutionScheduleSetting regardless does it has
     * corresponding execution schedule setting or not.
     *
     * @param inputSettingsList collection of protobuf settings
     * @return topology processor setting representation
     */
    @Nonnull
    public static TopologyProcessorSetting<?> toTopologyProcessorSetting(
            @Nonnull final Collection<Setting> inputSettingsList) {
        final Optional<String> actionModeSetting = Objects.requireNonNull(inputSettingsList)
                .stream()
                .map(Setting::getSettingSpecName)
                .filter(ActionSettingSpecs::isActionModeSetting)
                .findFirst();

        if (actionModeSetting.isPresent()) {
            return new ActionModeExecutionScheduleTopologyProcessorSetting(
                    toActionModeExecutionScheduleSetting(inputSettingsList));
        } else {
            final Setting protoSetting = inputSettingsList.iterator().next();
            return TO_TOPOLOGY_PROCESSOR_SETTINGS_CONVERTERS.get(protoSetting.getValueCase())
                    .apply(protoSetting);
        }
    }

    /**
     * Transform topology processor setting into simple protobuf settings.
     *
     * @param topologyProcessorSetting topology processor setting
     * @return protobuf settings representation
     */
    @Nonnull
    public static Collection<Setting> toProtoSettings(
            @Nonnull final TopologyProcessorSetting<?> topologyProcessorSetting) {
        return TO_PROTO_SETTINGS_CONVERTERS.get(topologyProcessorSetting.getClass())
                .apply(topologyProcessorSetting);
    }

    /**
     * Transform corresponding settings (required ActionMode and optional ExecutionSchedule) into
     * {@link ActionModeExecutionScheduleSetting}.
     *
     * @param settings input settings
     * @return composite setting
     */
    @Nonnull
    private static ActionModeExecutionScheduleSetting toActionModeExecutionScheduleSetting(@Nonnull Collection<Setting> settings) {
        Setting actionModeSetting = null;
        Setting executionScheduleSetting = null;

        for (Setting setting : settings) {
            final String settingName = setting.getSettingSpecName();
            if (ActionSettingSpecs.isExecutionScheduleSetting(settingName)) {
                executionScheduleSetting = setting;
            } else if (ActionSettingSpecs.isActionModeSetting(settingName)) {
                actionModeSetting = setting;
            }
        }

        return transformToActionModeExecutionScheduleSetting(Objects.requireNonNull(actionModeSetting,
                "ActionMode is required setting for ActionModeExecutionScheduleSetting"), executionScheduleSetting);
    }

    /**
     * Combined several settings into one {@link ActionModeExecutionScheduleSetting}.
     *
     * @param actionModeSetting required action mode setting
     * @param executionScheduleSetting optional execution schedule setting
     * @return composite setting
     */
    @Nonnull
    private static ActionModeExecutionScheduleSetting transformToActionModeExecutionScheduleSetting(@Nonnull Setting actionModeSetting,
            @Nullable Setting executionScheduleSetting) {

        final Collection<Long> executionWindowIds = new ArrayList<>();

        final EnumSettingValue actionMode = actionModeSetting.getEnumSettingValue();
        if (executionScheduleSetting != null) {
            executionWindowIds.addAll(
                    executionScheduleSetting.getSortedSetOfOidSettingValue().getOidsList());
        }

        return new ActionModeExecutionScheduleSetting(actionMode, executionWindowIds,
                actionModeSetting.getSettingSpecName());
    }

    /**
     * Transform {@link ActionModeExecutionScheduleSetting} settings into collection of protobut settings.
     *
     * @param actionModeExecutionScheduleSetting combined setting
     * @return collection of {@link Setting}s
     */
    @Nonnull
    private static Collection<Setting> transformActionModeExecutionScheduleSettingIntoProtoSettings(
            @Nonnull ActionModeExecutionScheduleSetting actionModeExecutionScheduleSetting) {

        final Collection<Setting> resultSettings = new ArrayList<>();

        final Setting actionModeSetting = Setting.newBuilder()
                .setEnumSettingValue(actionModeExecutionScheduleSetting.getActionMode())
                .setSettingSpecName(actionModeExecutionScheduleSetting.getSettingName())
                .build();
        resultSettings.add(actionModeSetting);

        if (!actionModeExecutionScheduleSetting.getExecutionSchedules().isEmpty()) {
            final Setting executionScheduleSetting = Setting.newBuilder()
                    .setSortedSetOfOidSettingValue(SortedSetOfOidSettingValue.newBuilder()
                            .addAllOids(actionModeExecutionScheduleSetting.getExecutionSchedules())
                            .build())
                    .setSettingSpecName(
                            ActionSettingSpecs.getExecutionScheduleSettingFromActionModeSetting(
                                actionModeExecutionScheduleSetting.getSettingName()))
                    .build();
            resultSettings.add(executionScheduleSetting);
        }

        return resultSettings;
    }
}
