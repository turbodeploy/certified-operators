package com.vmturbo.components.common.setting;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.BiMap;
import com.google.common.collect.EnumHashBiMap;
import com.google.common.collect.ImmutableBiMap;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;

/**
 * Each action mode comes with 4 settings. To ensure we do not miss any and to prevent typos, we
 * programmatically generate the 4 settings for each action mode setting. This protects against:
 * <ul>
 *     <li>Forgetting to add the these 4 settings when a new action mode setting is added.</li>
 *     <li>Copying and pasting errors when modifying the displayName or settingName</li>
 * </ul>
 */
public class ActionSettingSpecs {


    /**
     * Contains an ActionSettingSpecs for each action mode setting in {@link EntitySettingSpecs}.
     */
    @Nonnull
    private static final Map<String, ActionSettingSpecs> ACTION_SETTING_SPECS;

    /**
     * All the {@link SettingSpec}s generated from the {@link ActionSettingSpecs}. Each
     * ActionSettingSpecs will have 4 SettingSpecs (External Approval, on generation audit,
     * after exec audit, and execution schedule). As a result, this size of this list will be
     * 4 * size ofACTION_SETTING_SPECS.
     */
    @Nonnull
    private static final Map<ActionSettingType, BiMap<EntitySettingSpecs, SettingSpec>> SETTING_SPECS;

    /**
     * A map of each setting name to it's SettingSpec. There will be one key value in this map for
     * each entry in SETTING_SPECS.
     */
    @Nonnull
    private static final Map<String, SettingSpec> SETTING_NAME_TO_SETTING_SPEC;

    static {
        ACTION_SETTING_SPECS = Arrays.stream(EntitySettingSpecs.values())
            .filter(entitySettingSpecs ->
                entitySettingSpecs.getDataStructure() instanceof EnumSettingDataType)
            .filter(entitySettingSpecs ->
                ActionMode.class.equals(
                    ((EnumSettingDataType)entitySettingSpecs.getDataStructure()).getEnumClass()))
            .map(entitySettingSpecs -> new ActionSettingSpecs(entitySettingSpecs))
            .collect(Collectors.toMap(
                    actionSpec -> actionSpec.actionModeSettingSpec.getSettingName(),
                    Function.identity()));

        final Map<ActionSettingType, BiMap<EntitySettingSpecs, SettingSpec>> settingSpecs =
                new EnumMap<>(ActionSettingType.class);
        for (ActionSettingSpecs actionSettings: ACTION_SETTING_SPECS.values()) {
            final EntitySettingSpecs baseSettingName = actionSettings.actionModeSettingSpec;
            for (Entry<ActionSettingType, SettingSpec> entry: actionSettings.createSettingSpecs().entrySet()) {
                settingSpecs.computeIfAbsent(entry.getKey(), keu -> EnumHashBiMap.create(EntitySettingSpecs.class))
                        .put(baseSettingName, entry.getValue());
            }
        }
        SETTING_SPECS = Collections.unmodifiableMap(settingSpecs);

        SETTING_NAME_TO_SETTING_SPEC = SETTING_SPECS
                .values()
                .stream()
                .map(BiMap::values)
                .flatMap(Collection::stream)
                .collect(Collectors.toMap(SettingSpec::getName, Function.identity()));
    }

    @Nonnull
    private final EntitySettingSpecs actionModeSettingSpec;

    /**
     * Get all the action related settings as {@link SettingSpec} objects.
     *
     * @return all the action related settings as {@link SettingSpec} objects.
     */
    @Nonnull
    public static Collection<SettingSpec> getSettingSpecs() {
        return SETTING_SPECS
                .values()
                .stream()
                .map(BiMap::values)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
    }

    /**
     * Get the {@link SettingSpec} with the provided settingName.
     *
     * @param settingName the settingName to search using.
     * @return the {@link SettingSpec} with the provided settingName.
     */
    @Nullable
    public static SettingSpec getSettingSpec(@Nonnull String settingName) {
        Objects.requireNonNull(settingName);
        return SETTING_NAME_TO_SETTING_SPEC.get(settingName);
    }

    /**
     * Determines if the provided settingName is an action related setting.
     *
     * @param settingName the settingName to check.
     * @return true of the given settingName is an action related setting.
     */
    public static boolean isActionModeSubSetting(@Nonnull String settingName) {
        Objects.requireNonNull(settingName);
        return SETTING_NAME_TO_SETTING_SPEC.containsKey(settingName);
    }

    /**
     * Determines if the provided settingName is related to External Approval or Audit.
     *
     * @param settingName the settingName to check.
     * @return true if the provided settingName is related to External Approval or Audit.
     */
    public static boolean isExternalApprovalOrAuditSetting(@Nonnull String settingName) {
        Objects.requireNonNull(settingName);
        return SETTING_NAME_TO_SETTING_SPEC.containsKey(settingName)
            && !isExecutionScheduleSetting(settingName);
    }

    /**
     * Determines if the provided settingName is related to Execution Schedules.
     *
     * @param settingName the settingName to check.
     * @return true if the provided settingName is related to Execution Schedules.
     */
    public static boolean isExecutionScheduleSetting(@Nonnull String settingName) {
        Objects.requireNonNull(settingName);
        final BiMap<EntitySettingSpecs, SettingSpec> executionSettings = SETTING_SPECS.get(
                ActionSettingType.SCHEDULE);
        return executionSettings
                .values()
                .stream()
                .anyMatch(
                    spec -> spec.getName().equals(settingName));
    }

    /**
     * Returns the action mode setting name for the provided execution schedule setting name.
     *
     * @param executionSetting the execution schedule setting name to search for.
     * @return the action mode setting name for the provided execution schedule setting name.
     *         Returns null if not found.
     */
    @Nullable
    public static String getActionModeSettingFromExecutionScheduleSetting(
            @Nonnull String executionSetting) {
        Objects.requireNonNull(executionSetting);
        final SettingSpec scheduleSettingSpec = SETTING_NAME_TO_SETTING_SPEC.get(executionSetting);
        if (scheduleSettingSpec == null) {
            return null;
        }
        final BiMap<EntitySettingSpecs, SettingSpec> executionSettings = SETTING_SPECS.get(
                ActionSettingType.SCHEDULE);
        return executionSettings.inverse().get(scheduleSettingSpec).getSettingName();
    }

    /**
     * Returns the execution schedule setting name for the provided action mode setting name.
     *
     * @param actionModeSettingName action mode setting name to search for.
     * @param subsettingType subsetting type to retrieve
     * @return the execution schedule setting name for the provided action mode setting name.
     *         Returns null if not found.
     */
    @Nullable
    public static String getSubSettingFromActionModeSetting(
        @Nonnull String actionModeSettingName, @Nonnull ActionSettingType subsettingType) {
        Objects.requireNonNull(actionModeSettingName);
        final ActionSettingSpecs actionModeSettingA = ACTION_SETTING_SPECS.get(
                actionModeSettingName);
        if (actionModeSettingA == null) {
            return null;
        }
        final EntitySettingSpecs actionModeSetting = actionModeSettingA.actionModeSettingSpec;
        return getSubSettingFromActionModeSetting(actionModeSetting, subsettingType);
    }

    /**
     * Returns the execution schedule setting name for the provided entitySettingSpecs.
     *
     * @param entitySettingSpecs entitySettingSpecs to search for.
     * @param subSettingType subsetting type to retrieve
     * @return the execution schedule setting name for the provided entitySettingSpecs.
     *         Returns null if not found.
     */
    @Nullable
    public static String getSubSettingFromActionModeSetting(
            @Nonnull EntitySettingSpecs entitySettingSpecs,
            @Nonnull ActionSettingType subSettingType) {
        Objects.requireNonNull(entitySettingSpecs);
        Objects.requireNonNull(subSettingType);
        final BiMap<EntitySettingSpecs, SettingSpec> bimap = SETTING_SPECS.get(subSettingType);
        Objects.requireNonNull(bimap);
        final SettingSpec schedSpec = bimap.get(entitySettingSpecs);
        if (schedSpec == null) {
            return null;
        }
        return schedSpec.getName();
    }

    /**
     * Determines if the provided setting name is for a setting that sets the action mode.
     *
     * @param settingName the setting name to search using.
     * @return true if the provided setting name is for the setting that sets the action mode.
     */
    public static boolean isActionModeSetting(@Nonnull String settingName) {
        Objects.requireNonNull(settingName);
        return ACTION_SETTING_SPECS.containsKey(settingName);
    }

    /**
     * Determines if the provided entitySettingSpecs is for a setting that sets the action mode.
     *
     * @param entitySettingSpecs the entitySettingSpecs to search using.
     * @return true if the provided entitySettingSpecs is for the setting that sets the action mode.
     */
    public static boolean isActionModeSetting(@Nonnull EntitySettingSpecs entitySettingSpecs) {
        Objects.requireNonNull(entitySettingSpecs);
        return ACTION_SETTING_SPECS.containsKey(
            entitySettingSpecs.getSettingName());
    }

    /**
     * Constructs an ActionSettingSpecs based on the action mode setting.
     *
     * @param actionModeSettingSpec the action mode setting to generate more settings for.
     */
    public ActionSettingSpecs(final @Nonnull EntitySettingSpecs actionModeSettingSpec) {
        Objects.requireNonNull(actionModeSettingSpec);
        this.actionModeSettingSpec = Objects.requireNonNull(actionModeSettingSpec);
    }

    @Nonnull
    private BiMap<ActionSettingType, SettingSpec> createSettingSpecs() {
        final ImmutableBiMap.Builder<ActionSettingType, SettingSpec> settingSpecs =
                ImmutableBiMap.builder();
        for (ActionSettingType actionSettingType : ActionSettingType.values()) {
            settingSpecs.put(actionSettingType,
                    actionSettingType.createEntitySpec(actionModeSettingSpec));
        }
        return settingSpecs.build();
    }

    @Override
    public String toString() {
        return "ActionSettingSpecs{" + actionModeSettingSpec + '}';
    }
}
