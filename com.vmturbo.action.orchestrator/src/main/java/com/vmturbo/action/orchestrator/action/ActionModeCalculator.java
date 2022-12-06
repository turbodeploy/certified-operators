package com.vmturbo.action.orchestrator.action;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;

import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.Deactivate;
import com.vmturbo.common.protobuf.action.ActionDTO.Provision;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.schedule.ScheduleProto;
import com.vmturbo.common.protobuf.setting.SettingProto;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;
import com.vmturbo.commons.Units;
import com.vmturbo.components.common.setting.ActionSettingSpecs;
import com.vmturbo.components.common.setting.ActionSettingType;
import com.vmturbo.components.common.setting.ConfigurableActionSettings;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.components.common.setting.SettingDTOUtil;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * A utility class to capture the logic to calculate an {@link ActionMode} for an action,
 * given some user settings.
 */
public class ActionModeCalculator {
    private static final Map<Integer, ConfigurableActionSettings>
        PROVIDER_ENTITY_TYPE_TO_MOVE_SETTING_SPECS =
            ImmutableMap.of(
                EntityType.STORAGE_VALUE, ConfigurableActionSettings.StorageMove,
                EntityType.DESKTOP_POOL_VALUE, ConfigurableActionSettings.Move);

    private static final Logger logger = LogManager.getLogger();

    private static final EnumSettingValueType CLOUD_SCALE_SETTING_ENUM_TYPE =
            ActionSettingSpecs.getSettingSpec(ActionSettingSpecs.getSubSettingFromActionModeSetting(
                    ConfigurableActionSettings.CloudComputeScaleForPerf,
                    ActionSettingType.ACTION_MODE)).getEnumSettingValueType();

    private static final List<ResizeDirectionConfiguration> RESIZE_DIRECTION_CONFIGURATIONS =
        Arrays.asList(
            new ResizeDirectionConfiguration(EntityType.APPLICATION_COMPONENT, CommodityType.HEAP,
                ConfigurableActionSettings.ResizeUpHeap,
                ConfigurableActionSettings.ResizeDownHeap),
            new ResizeDirectionConfiguration(EntityType.APPLICATION_COMPONENT, CommodityType.THREADS,
                ConfigurableActionSettings.ResizeUpThreadPool,
                ConfigurableActionSettings.ResizeDownThreadPool),
            new ResizeDirectionConfiguration(EntityType.APPLICATION_COMPONENT, CommodityType.CONNECTION,
                ConfigurableActionSettings.ResizeUpConnections,
                ConfigurableActionSettings.ResizeDownConnections),
            new ResizeDirectionConfiguration(EntityType.DATABASE_SERVER, CommodityType.DB_MEM,
                ConfigurableActionSettings.ResizeUpDBMem,
                ConfigurableActionSettings.ResizeDownDBMem),
            new ResizeDirectionConfiguration(EntityType.DATABASE_SERVER, CommodityType.TRANSACTION_LOG,
                ConfigurableActionSettings.ResizeUpTransactionLog,
                ConfigurableActionSettings.ResizeDownTransactionLog),
            new ResizeDirectionConfiguration(EntityType.DATABASE_SERVER, CommodityType.CONNECTION,
                ConfigurableActionSettings.ResizeUpConnections,
                ConfigurableActionSettings.ResizeDownConnections)
        );

    //scale, the number of digits to the right of the decimal point for rounding
    private static final int PRECISION = 7;
    private final RangeAwareSpecCalculator rangeAwareSpecCalculator;
    private final boolean enableCloudScaleEnhancement;

    /**
     * Constructs a ActionModeCalculator with the default RangeAwareSpecCalculator.
     */
    public ActionModeCalculator() {
        this.rangeAwareSpecCalculator = new RangeAwareSpecCalculator();
        this.enableCloudScaleEnhancement = false;
    }

    /**
     * Constructs a ActionModeCalculator with the default RangeAwareSpecCalculator.
     *
     * @param enableCloudScaleEnhancement private preview flag for 'Scale for Performance' and
     * 'Scale for Savings' settings.
     */
    public ActionModeCalculator(boolean enableCloudScaleEnhancement) {
        this.rangeAwareSpecCalculator = new RangeAwareSpecCalculator();
        this.enableCloudScaleEnhancement = enableCloudScaleEnhancement;
    }

    /**
     * Map from commodity attribute and commodity type to the correct entitySettingSpec. This is
     * only used for vms, since we have different type of resize based on the commodity and the
     * attribute.
     */
    private static final Map<CommodityAttribute, Map<Integer, ConfigurableActionSettings>> VMS_ACTION_MODE_SETTINGS =
        new ImmutableMap.Builder<CommodityAttribute, Map<Integer, ConfigurableActionSettings>>()
            .put(CommodityAttribute.LIMIT,
                new ImmutableMap.Builder<Integer, ConfigurableActionSettings>()
                    .put(CommodityType.VCPU.getNumber(), ConfigurableActionSettings.ResizeVcpuUpInBetweenThresholds)
                    .put(CommodityType.VMEM.getNumber(), ConfigurableActionSettings.ResizeVmemUpInBetweenThresholds)
                    .build())
            .put(CommodityAttribute.RESERVED,
                new ImmutableMap.Builder<Integer, ConfigurableActionSettings>()
                    .put(CommodityType.CPU.getNumber(), ConfigurableActionSettings.ResizeVcpuDownInBetweenThresholds)
                    .put(CommodityType.MEM.getNumber(), ConfigurableActionSettings.ResizeVmemDownInBetweenThresholds)
                    .build())
            .build();

    private static final Set<Integer> MEM_COMMODITY_TYPES = ImmutableSet.of(
        CommodityType.VMEM_VALUE,
        CommodityType.VMEM_REQUEST_VALUE
    );

    private static final Map<Integer, Integer> reservedCommodityBoughtSoldConversion
        = ImmutableMap.of(CommodityType.MEM_VALUE, CommodityType.VMEM_VALUE,
            CommodityType.CPU_VALUE, CommodityType.VCPU_VALUE);

    /**
     * Get the action mode and execution schedule for a particular action. The both of these are
     * determined by the settings for the action, or, if the settings are not available, by the
     * system defaults of
     * the relevant automation settings.
     *
     * @param action The action to calculate action mode for.
     * @param entitiesCache The {@link EntitiesAndSettingsSnapshotFactory} to retrieve settings for. May
     *                            be null.
     *                            TODO (roman, Aug 7 2018): Can we make this non-null? The cache
     *                            should exist as a spring object and be injected appropriately.
     * @return The {@link ActionMode} and {@link ActionSchedule} to use for the action.
     */
    @Nonnull
    ModeAndSchedule calculateActionModeAndExecutionSchedule(@Nonnull final ActionView action,
                                   @Nullable final EntitiesAndSettingsSnapshot entitiesCache) {
        final Optional<ModeAndSchedule> workflowModeAndSchedule =
                calculateWorkflowActionModeAndSchedule(action, entitiesCache);

        if (workflowModeAndSchedule.isPresent()) {
            return workflowModeAndSchedule.get();
        } else {
            ModeAndSchedule supportingLevelActionModeAndSchedule =
                calculateActionModeAndScheduleFromSupportingLevel(action, entitiesCache);
            if (action.getRecommendation().getPrerequisiteList().isEmpty()) {
                return supportingLevelActionModeAndSchedule;
            } else {
                return ModeAndSchedule.of(ActionMode.RECOMMEND.compareTo(supportingLevelActionModeAndSchedule.getMode()) < 0
                    ? ActionMode.RECOMMEND : supportingLevelActionModeAndSchedule.getMode());
            }
        }
    }

    /**
     * Calculate action mode and execution schedule based on support level from probe and target
     * entity settings.
     *
     * @param action an action
     * @param entitiesCache a nullable entities and settings snapshot
     * @return the action mode and execution schedule
     */
    @Nonnull
    private ModeAndSchedule calculateActionModeAndScheduleFromSupportingLevel(
            @Nonnull final ActionView action,
            @Nullable final EntitiesAndSettingsSnapshot entitiesCache) {
        switch (action.getRecommendation().getSupportingLevel()) {
            case UNSUPPORTED:
            case UNKNOWN:
                return ModeAndSchedule.of(ActionMode.DISABLED);
            case SHOW_ONLY:
            case SUPPORTED:
                return getNonWorkflowActionModeAndSchedule(action, entitiesCache);
            default:
                throw new IllegalArgumentException("Action SupportLevel is of unrecognized type.");
        }
    }

    @Nonnull
    private ModeAndSchedule getNonWorkflowActionModeAndSchedule(@Nonnull final ActionView action,
              @Nullable final EntitiesAndSettingsSnapshot entitiesCache) {
        ModeAndSchedule result;
        Optional<ActionDTO.Action> translatedRecommendation = action.getActionTranslation()
                .getTranslatedRecommendation();
        if (translatedRecommendation.isPresent()) {
            ActionDTO.Action actionDto = translatedRecommendation.get();
            try {
                final long targetEntityId = ActionDTOUtil.getPrimaryEntityId(actionDto);

                final Map<String, Setting> settingsForTargetEntity = entitiesCache == null
                    ? Collections.emptyMap() : entitiesCache.getSettingsForEntity(targetEntityId);

                final Set<String> defaultSettingsForEntity = entitiesCache == null
                        ? Collections.emptySet() : entitiesCache.getDefaultSettingPoliciesForEntity(targetEntityId);

                result = specsApplicableToAction(actionDto, settingsForTargetEntity, defaultSettingsForEntity)
                    .flatMap(spec -> {
                        List<Pair<ActionMode, List<Long>>> modeResults = new ArrayList<>(2);
                        if (spec.isNonDisruptiveEnforced()) {
                            final Setting nonDisruptiveSetting = settingsForTargetEntity.get(EntitySettingSpecs.EnforceNonDisruptive.getSettingName());
                            // Default is to return most liberal setting because calculateActionMode picks
                            // the minimum ultimately.
                            ActionMode mode = ActionMode.AUTOMATIC;
                            if (nonDisruptiveSetting != null && nonDisruptiveSetting.hasBooleanSettingValue()
                                && nonDisruptiveSetting.getBooleanSettingValue().getValue()) {
                                Optional<ActionPartialEntity> entity = entitiesCache.getEntityFromOid(targetEntityId);
                                if (entity.isPresent()) {
                                    mode = applyNonDisruptiveSetting(entity.get(), action.getRecommendation());
                                } else {
                                    logger.error("Entity with id {} not found for non-disruptive setting.",
                                        targetEntityId);
                                }
                            }
                            modeResults.add(Pair.of(mode, Collections.emptyList()));
                        }
                        final Setting setting = settingsForTargetEntity.get(spec.getConfigurableActionSetting().getSettingName());
                        if (setting == null) {
                            // If there is no setting for this spec that applies to the target
                            // of the action, we use the system default (which comes from the
                            // enum definitions).
                            final ActionMode defaultValue;
                            SettingSpec defaultSpec = ActionSettingSpecs.getSettingSpec(
                                spec.getConfigurableActionSetting().getSettingName());
                            if (defaultSpec == null) {
                                logger.error(spec.getConfigurableActionSetting().getSettingName()
                                    + " did not have a setting spec. As a result, we could not"
                                    + " determine the default action mode.");
                                defaultValue = ActionMode.DISABLED;
                            } else {
                                defaultValue = ActionMode.valueOf(
                                    defaultSpec.getEnumSettingValueType().getDefault());
                            }
                            modeResults.add(Pair.of(defaultValue, Collections.emptyList()));
                        } else {
                            final List<Long> scheduleIds =
                                    getSchedulesForActionSpec(settingsForTargetEntity, spec);

                            // In all other cases, we use the default value from the setting.
                            modeResults.add(Pair.of(ActionMode.valueOf(setting.getEnumSettingValue().getValue()),
                                scheduleIds));
                        }
                        return modeResults.stream();
                    })
                    // make sure the lower mode comes first
                    // if the modes are tied, make sure the one with schedules comes first
                    .min(new ActionModeScheduleComparator())
                    // select the schedule from the list of schedules
                    .map(p -> getModeAndSchedule(action, p.getLeft(), p.getRight(), entitiesCache))
                    .orElse(ModeAndSchedule.of(ActionMode.RECOMMEND));

            } catch (UnsupportedActionException e) {
                logger.error("Unable to calculate action mode.", e);
                result = ModeAndSchedule.of(ActionMode.RECOMMEND);
            }
        } else {
            logger.error("Action {} has no translated recommendation.", action.getId());
            result = ModeAndSchedule.of(ActionMode.RECOMMEND);
        }

        if (action.getRecommendation().getSupportingLevel() == SupportLevel.SHOW_ONLY) {
            ActionMode resultMode = result.getMode();
            if (resultMode == ActionMode.EXTERNAL_APPROVAL) {
                result = ModeAndSchedule.of(ActionMode.EXTERNAL_APPROVAL);
            } else {
                result = ModeAndSchedule.of((resultMode.getNumber() > ActionMode.RECOMMEND_VALUE)
                        ? ActionMode.RECOMMEND : resultMode);
            }
        }

        // If action is not executable, its action mode shouldn't be MANUAL or AUTOMATIC.
        if (result.getMode().compareTo(ActionMode.MANUAL) >= 0 && !action.determineExecutability()) {
            result = ModeAndSchedule.of(ActionMode.RECOMMEND);
        }

        return result;
    }

    /**
     * Comparator to ensure that the lower action mode comes first, but the one with the larger
     * oid list comes first when the action modes are the same.
     */
    private static class ActionModeScheduleComparator implements Comparator<Pair<ActionMode, List<Long>>> {
        @Override
        public int compare(
                final @Nonnull Pair<ActionMode, List<Long>> o1,
                final @Nonnull Pair<ActionMode, List<Long>> o2) {
            // Compare o1 and o2 in the expected order so the lower mode
            // (example: DISABLED < RECOMMEND < AUTO) comes first in the min.
            int comparison = o1.getLeft().compareTo(o2.getLeft());
            if (comparison != 0) {
                return comparison;
            }
            // Reverse o1 and o2 so that the large list comes first in a min
            return Integer.compare(o2.getRight().size(), o1.getRight().size());
        }
    }

    /**
     * This method gets the selected action mode for an action and list of schedules associated
     * with action and selects the mode for the action and its schedule.
     *
     * @param action the action in question.
     * @param chosenMode the mode chosen for action based on settings.
     * @param scheduleIds the schedule ids associates to action.
     * @param entitiesCache the entities cache.
     * @return the result mode and schedule.
     */
    @Nonnull
    private ModeAndSchedule getModeAndSchedule(@Nonnull ActionView action,
                                               @Nonnull ActionMode chosenMode, @Nonnull List<Long> scheduleIds,
                                               @Nullable EntitiesAndSettingsSnapshot entitiesCache) {

        if ((chosenMode != ActionMode.MANUAL && chosenMode != ActionMode.AUTOMATIC
                && chosenMode != ActionMode.EXTERNAL_APPROVAL) || scheduleIds.isEmpty()
                || entitiesCache == null) {
            return ModeAndSchedule.of(chosenMode);
        }

        logger.debug("Schedules with OIDs `{}` are associated with action `{}`",
            () -> scheduleIds.stream().map(String::valueOf).collect(Collectors.joining(", ")),
            () -> action);

        final String acceptedBy =
            entitiesCache.getAcceptingUserForAction(action.getRecommendationOid()).orElse(null);

        // Select the schedule that we use for the action
        final ActionSchedule selectedSchedule = createActionSchedule(scheduleIds, chosenMode,
            entitiesCache, acceptedBy);

        // If we are here it means there are some schedules associated to this action and if
        // the selected schedule is null it means something has gone wrong. Therefore, we go with
        // recommend action mode to be on the safe side.
        if (selectedSchedule == null) {
            logger.warn("Cannot find any schedule to associate to action `{}`. Setting the mode to "
                + "`RECOMMEND`.", action);
            return ModeAndSchedule.of(ActionMode.RECOMMEND);
        }

        logger.debug("Action schedule `{}` was selected for action `{}`.", () -> selectedSchedule,
            () -> action);

        // Determine the action mode based on execution schedule
        final ActionMode selectedMode = selectActionMode(action, chosenMode, selectedSchedule);

        return ModeAndSchedule.of(selectedMode, selectedSchedule);
    }

    @Nonnull
    private ActionMode selectActionMode(@Nonnull ActionView action,
                                        @Nonnull ActionMode chosenMode,
                                        @Nonnull ActionSchedule actionSchedule) {
        if (chosenMode == ActionMode.EXTERNAL_APPROVAL) {
            // we shouldn't change EXTERNAL_APPROVAL mode because it leads to problem with external operations
            logger.debug("Action mode for action `{}` is `EXTERNAL_APPROVAL`.", action);
            return ActionMode.EXTERNAL_APPROVAL;
        }
        final Long scheduleStartTimestamp = actionSchedule.getScheduleStartTimestamp();
        final Long scheduleEndTimestamp = actionSchedule.getScheduleEndTimestamp();
        final ActionMode selectedMode;
        if (scheduleStartTimestamp == null && scheduleEndTimestamp == null) {
            logger.debug("Action mode for action `{}` has been set to recommend as the selected "
                    + "schedule `{}` for the action has next no occurrence.", () -> action,
                () -> actionSchedule);
            selectedMode = ActionMode.RECOMMEND;
        } else if (actionSchedule.isActiveSchedule()) {
            // If the selected schedule is active and the action is accepted or chosen mode is
            // automated accept it.
            if (chosenMode == ActionMode.AUTOMATIC) {
                logger.info("Setting mode for Action `{}` to `AUTOMATED` as it is in its "
                    + "execution window `{}`.", action, actionSchedule);
                selectedMode = ActionMode.AUTOMATIC;
            } else if (chosenMode == ActionMode.MANUAL && actionSchedule.getAcceptingUser() != null) {
                logger.debug("Setting mode for Action `{}` to `MANUAL`. It has been accepted "
                        + "by `{}` and it is in its execution window `{}`.", action,
                        actionSchedule.getAcceptingUser(), actionSchedule);
                selectedMode = ActionMode.MANUAL;
            } else {
                logger.debug("Setting mode for Action `{}` to `MANUAL`. The action has not been "
                        + "accepted by a user and it is in its execution window `{}`.", action,
                        actionSchedule);
                selectedMode = ActionMode.MANUAL;
            }
        } else {
            if (chosenMode == ActionMode.AUTOMATIC) {
                logger.debug("Setting the action mode for action `{}` to `RECOMMEND` as there is "
                    + "an upcoming schedule `{}` for it with the `AUTOMATED` mode.", () -> action,
                    () -> actionSchedule);
                selectedMode = ActionMode.RECOMMEND;
            } else if (actionSchedule.getAcceptingUser() != null)  {
                logger.debug("Setting the action mode for action `{}` to `MANUAL` as there is "
                    + "an upcoming schedule `{}` with `MANUAL` for it and it has been accepted by"
                    + " `{}`.", () -> action, () -> actionSchedule, () -> actionSchedule.getAcceptingUser());
                selectedMode = ActionMode.MANUAL;
            } else {
                logger.debug("Setting the action mode for action `{}` to `MANUAL` as there is an "
                    + "upcoming schedule `{}` with `MANUAL` mode for it.", () -> action,
                    () -> actionSchedule);
                selectedMode = ActionMode.MANUAL;
            }
        }

        return selectedMode;
    }

    @Nullable
    private ActionSchedule createActionSchedule(@Nonnull List<Long> scheduleIds,
                                                @Nonnull ActionMode chosenMode,
                                                @Nonnull EntitiesAndSettingsSnapshot entitiesCache,
                                                @Nullable String acceptingUser) {
        // Select the closest schedule between the schedules associated to action
        ScheduleProto.Schedule selectedSchedule = selectClosestSchedule(scheduleIds,
            entitiesCache.getScheduleMap());

        if (selectedSchedule == null) {
            return null;
        }

        // Find the start and time of closest schedule
        Pair<Long, Long> startAndEndTime = getScheduleStartAndEndTime(selectedSchedule,
            entitiesCache.getPopulationTimestamp());

        return new ActionSchedule(startAndEndTime.getLeft(), startAndEndTime.getRight(),
            selectedSchedule.getTimezoneId(), selectedSchedule.getId(), selectedSchedule.getDisplayName(),
            chosenMode, acceptingUser);
    }

    @Nullable
    private ScheduleProto.Schedule selectClosestSchedule(@Nonnull List<Long> scheduleIds,
                                                         Map<Long, ScheduleProto.Schedule> scheduleMap) {
        ScheduleProto.Schedule selectedSchedule = null;

        for (long scheduleOid : scheduleIds) {
            final ScheduleProto.Schedule currentSchedule = scheduleMap.get(scheduleOid);

            // If the schedule cannot found, we log an error message and ignore this schedule
            if (currentSchedule == null) {
                logger.error("The schedule with OID {} cannot be found. This schedule will be ignored.",
                    scheduleOid);
                continue;
            }

            if (selectedSchedule == null) {
                selectedSchedule = currentSchedule;
                continue;
            }

            // This is the case where there is an existing schedule that is active has been
            // selected.
            if (selectedSchedule.hasActive()) {
                // If there are more than one schedule which are active we go with the one that
                // is active longer
                if (currentSchedule.hasActive()
                    && currentSchedule.getActive().getRemainingActiveTimeMs()
                    > selectedSchedule.getActive().getRemainingActiveTimeMs()) {
                    selectedSchedule = currentSchedule;
                }
                continue;
            }

            // If we are here, it means currently selected schedule is not active and therefore
            // if current schedule is active we use that as the selected schedule
            if (currentSchedule.hasActive()) {
                selectedSchedule = currentSchedule;
                continue;
            }

            // If selected schedule and current schedule both have next occurrence go with the
            // one than it next occurrence is closer.
            if (selectedSchedule.hasNextOccurrence()) {
                if (currentSchedule.hasNextOccurrence()
                    && currentSchedule.getNextOccurrence().getStartTime() < selectedSchedule.getNextOccurrence().getStartTime()) {
                    selectedSchedule = currentSchedule;
                }

                continue;
            }

            // If we are here, it means currently selected schedule does not have next occurrence
            selectedSchedule = currentSchedule;
        }

        return selectedSchedule;
    }

    @Nonnull
    private Pair<Long, Long> getScheduleStartAndEndTime(@Nonnull ScheduleProto.Schedule schedule,
                                                        long populationTimeStamp) {
        final long scheduleOccurrenceDuration =
            schedule.getEndTime() - schedule.getStartTime();

        // Find next occurrence start and end time
        final Long scheduleStartTime;
        final Long scheduleEndTime;
        if (schedule.hasNextOccurrence()) {
            scheduleStartTime = schedule.getNextOccurrence().getStartTime();
        } else {
            scheduleStartTime = null;
        }

        if (schedule.hasActive()) {
            scheduleEndTime =
                populationTimeStamp + schedule.getActive().getRemainingActiveTimeMs();
        } else if (schedule.hasNextOccurrence()) {
            scheduleEndTime = scheduleStartTime + scheduleOccurrenceDuration;
        } else {
            scheduleEndTime = null;
        }

        return Pair.of(scheduleStartTime, scheduleEndTime);
    }

    /**
     * This class holds an action mode and schedule.
     */
    @Immutable
    public static class ModeAndSchedule {
        final ActionMode mode;
        final ActionSchedule schedule;

        private ModeAndSchedule(@Nonnull ActionMode mode, @Nullable ActionSchedule schedule) {
            this.mode = mode;
            this.schedule = schedule;
        }

        /**
         * Factory method for actions without schedule.
         * @param mode the mode for action.
         * @return The new instance of {@link ModeAndSchedule} object.
         */
        @Nonnull
        public static ModeAndSchedule of(@Nonnull ActionMode mode) {
            return new ModeAndSchedule(mode, null);
        }

        /**
         * Factory method for actions without schedule.
         * @param mode the mode for action.
         * @param schedule the schedule for action.
         * @return The new instance of {@link ModeAndSchedule} object.
         */
        @Nonnull
        public static ModeAndSchedule of(@Nonnull ActionMode mode, @Nonnull ActionSchedule schedule) {
            return new ModeAndSchedule(mode, schedule);
        }

        @Nonnull
        public ActionMode getMode() {
            return mode;
        }

        @Nullable
        public ActionSchedule getSchedule() {
            return schedule;
        }

        @Override
        public boolean equals(final Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final ModeAndSchedule that = (ModeAndSchedule)o;
            return mode == that.mode && Objects.equals(schedule, that.schedule);
        }

        @Override
        public int hashCode() {
            return Objects.hash(mode, schedule);
        }
    }

    private ActionMode applyNonDisruptiveSetting(ActionPartialEntity entity, Action action) {
        final ActionTypeCase actionType = action.getInfo().getActionTypeCase();
        // Check for VM Resize.
        if (entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE
                && actionType == ActionTypeCase.RESIZE) {
            Resize resizeAction = action.getInfo().getResize();
            final Integer commType = resizeAction.getCommodityType().getType();

            boolean supportsHotReplace = entity.getCommTypesWithHotReplaceList().stream()
                    .anyMatch(type -> type.equals(commType))
                    //If a VM's cores per socket ratio is changed, it is never hot replace.
                    && !(resizeAction.hasNewCpsr() && resizeAction.hasOldCpsr() && resizeAction.getNewCpsr() != resizeAction.getOldCpsr());

            // Check applicable commodities.
            if (ActionDTOUtil.NON_DISRUPTIVE_SETTING_COMMODITIES.contains(commType)
                    && resizeAction.getCommodityAttribute() == CommodityAttribute.CAPACITY) {
                // Check hot replace setting enabled.
                if (supportsHotReplace) {
                    // Return Automatic for resize up.
                    if (resizeAction.getNewCapacity() >= resizeAction.getOldCapacity()) {
                        return ActionMode.AUTOMATIC;
                    }
                }
            } else if (ActionDTOUtil.NON_DISRUPTIVE_SETTING_COMMODITIES.contains(commType)
                    && resizeAction.getCommodityAttribute() == CommodityAttribute.LIMIT) {
                if (supportsHotReplace) {
                    return ActionMode.AUTOMATIC;
                }
            }
            // Return Recommend and if resize is disabled we will pick the minimum.
            return ActionMode.RECOMMEND;
        }
        // Ideally we should never reach here because currently no other entity other than VMs with resize action
        // have non disruptive setting. But this is a sanity check to return Automatic s.t this setting has
        // no effect on any other entity.
        return ActionMode.AUTOMATIC;
    }

    /**
     * For an action which corresponds to a Workflow Action, e.g. ProvisionActionWorkflow,
     * return the ModeAndSchedule of the policy for the related action, e.g. ProvisionAction.
     *
     * <p>If this is not a Workflow Action, then return Optional.empty()</p>
     *
     * @param action The action to analyze to see if it is a Workflow Action
     * @param entitySettingsCache the EntitySettings look aside for the given action
     * @return an Optional containing the ModeAndSchedule if this is a Workflow Action, or
     * Optional.empty() if this is not a Workflow Action or the type of the ActionDTO is not
     * supported.
     */
    @Nonnull
    private Optional<ModeAndSchedule> calculateWorkflowActionModeAndSchedule(
            @Nonnull final ActionView action,
            @Nullable final EntitiesAndSettingsSnapshot entitySettingsCache) {
        try {
            ActionDTO.Action actionDTO = action.getRecommendation();
            Objects.requireNonNull(actionDTO);
            if (Objects.isNull(entitySettingsCache)) {
                return Optional.empty();
            }

            // find the entity which is the target of this action
            final long actionTargetEntityId = ActionDTOUtil.getPrimaryEntityId(actionDTO);

            // get a map of all the settings (settingName  -> setting) specific to this entity
            final Map<String, Setting> settingsForActionTarget = entitySettingsCache
                    .getSettingsForEntity(actionTargetEntityId);

            final Set<String> defaultSettingsForEntity = entitySettingsCache
                    .getDefaultSettingPoliciesForEntity(actionTargetEntityId);

            Optional<Action> translatedActionOpt = action.getActionTranslation()
                .getTranslatedRecommendation();
            if (!translatedActionOpt.isPresent()) {
                return Optional.empty();
            }

           return specsApplicableToAction(translatedActionOpt.get(), settingsForActionTarget,
    defaultSettingsForEntity)
            // filter actions with REPLACE workflow settings and return values of mode and schedule settings
            .flatMap(spec -> getModeAndScheduleFromReplaceWorkflowActions(spec, settingsForActionTarget))
            .min(new ActionModeScheduleComparator())
            .map(p -> getModeAndSchedule(action, p.getLeft(), p.getRight(),
                    entitySettingsCache));
        } catch (UnsupportedActionException e) {
            logger.error("Unable to calculate complex action mode.", e);
            return Optional.empty();
        }
    }

    /**
     * Get mode and schedules for actions with associated REPLACE workflow settings.
     *
     * @param actionSpec action specification
     * @param settingsForActionTarget map with settings specific to entities
     * @return empty stream if action don't have associated REPLACE workflow settings otherwise
     * return values from action mode and execution schedule settings
     */
    private Stream<Pair<ActionMode, List<Long>>> getModeAndScheduleFromReplaceWorkflowActions(
            @Nonnull ActionSpecifications actionSpec,
            @Nonnull Map<String, Setting> settingsForActionTarget) {
        if (hasReplaceWorkflow(actionSpec.getConfigurableActionSetting(),
                settingsForActionTarget)) {
            final ConfigurableActionSettings configurableActionSetting =
                    actionSpec.getConfigurableActionSetting();
            final String settingName = configurableActionSetting.getSettingName();
            final Setting setting = settingsForActionTarget.get(settingName);
            if (setting != null && setting.hasEnumSettingValue()) {
                final String enumSettingValue = setting.getEnumSettingValue().getValue();
                final ActionMode actionMode = ActionMode.valueOf(enumSettingValue);
                final List<Long> scheduleIds =
                        getSchedulesForActionSpec(settingsForActionTarget, actionSpec);
                return Stream.of(Pair.of(actionMode, scheduleIds));
            }
        }
        return Stream.empty();
    }

    /**
     * Return execution schedules associated with action mode setting if any.
     *
     * @param settingsForActionTarget action settings
     * @param spec action spec (action mode setting)
     * @return if there is execution schedule setting for action, then return execution schedules
     * IDs, otherwise empty collection.
     */
    @Nonnull
    private List<Long> getSchedulesForActionSpec(
            @Nonnull Map<String, Setting> settingsForActionTarget,
            @Nonnull ActionSpecifications spec) {
        List<Long> executionScheduleIds = Collections.emptyList();
        final String scheduleSettingSpecName =
                ActionSettingSpecs.getSubSettingFromActionModeSetting(
                        spec.getConfigurableActionSetting().getSettingName(),
                        ActionSettingType.SCHEDULE);
        if (scheduleSettingSpecName != null && settingsForActionTarget.containsKey(
                scheduleSettingSpecName)) {
            executionScheduleIds = settingsForActionTarget.get(scheduleSettingSpecName)
                    .getSortedSetOfOidSettingValue()
                    .getOidsList();
        }
        return executionScheduleIds;
    }

    private boolean hasReplaceWorkflow(
            final ConfigurableActionSettings setting,
            final Map<String, Setting> settingsForActionTarget) {
        String settingName = ActionSettingSpecs.getSubSettingFromActionModeSetting(
            setting, ActionSettingType.REPLACE);
        Setting workflowSettingSpec = settingsForActionTarget.get(settingName);
        return workflowSettingSpec != null
            && StringUtils.isNotBlank(workflowSettingSpec.getStringSettingValue().getValue());
    }

    /**
     * For an action which corresponds to a Workflow Action, e.g. ProvisionActionWorkflow,
     * return the ActionMode of the policy for the related action, e.g. ProvisionAction.
     *
     * <p>If this is not a Workflow Action, then return an empty map. </p>
     *
     * @param recommendation The action to analyze to see if it is a Workflow Action
     * @param snapshot the entity settings cache to look up the settings.
     * @return A map containing the per-action-state workflow settings is this is a workflow action.
     * Only states that have associated workflow settings will have entries in the map.
     * An empty map otherwise.
     */
    @Nonnull
    public Map<ActionState, SettingProto.Setting> calculateWorkflowSettings(
            @Nonnull final ActionDTO.Action recommendation,
            @Nullable final EntitiesAndSettingsSnapshot snapshot) {
        try {
            Objects.requireNonNull(recommendation);
            if (Objects.isNull(snapshot)) {
                return Collections.emptyMap();
            }

            // find the entity which is the target of this action
            final long actionTargetEntityId = ActionDTOUtil.getPrimaryEntityId(recommendation);
            // get a map of all the settings (settingName  -> setting) specific to this entity
            final Map<String, Setting> settingsForActionTarget =
                snapshot.getSettingsForEntity(actionTargetEntityId);
            final Set<String> defaultSettingsForEntity =
                    snapshot.getDefaultSettingPoliciesForEntity(actionTargetEntityId);
            return getSettingForState(recommendation, settingsForActionTarget, defaultSettingsForEntity);
        } catch (UnsupportedActionException e) {
            logger.error("Unable to calculate complex action mode.", e);
            return Collections.emptyMap();
        }
    }

    @Nonnull
    private Map<ActionState, String> getActionStateSettings(
            @Nonnull final ActionDTO.Action recommendation,
            @Nonnull Map<String, Setting> actionSettings,
            @Nonnull Set<String> defaultSettingsForEntity) {
        final Map<ActionState, String> actionStateSettings = new HashMap<>();
        Optional<ConfigurableActionSettings> configurableActionSettingOptional =
            getActionModeSettingSpec(recommendation, actionSettings, defaultSettingsForEntity);
        if (configurableActionSettingOptional.isPresent()) {
            ConfigurableActionSettings configurableActionSetting = configurableActionSettingOptional.get();
            // action workflows
            addActionModeRelatedSettings(
                configurableActionSetting,
                ActionSettingType.PRE,
                ActionState.PRE_IN_PROGRESS,
                actionStateSettings);
            addActionModeRelatedSettings(
                configurableActionSetting,
                ActionSettingType.REPLACE,
                ActionState.IN_PROGRESS,
                actionStateSettings);
            addActionModeRelatedSettings(
                configurableActionSetting,
                ActionSettingType.POST,
                ActionState.POST_IN_PROGRESS,
                actionStateSettings);
            addActionModeRelatedSettings(
                configurableActionSetting,
                ActionSettingType.POST,
                ActionState.FAILING,
                actionStateSettings);
            // audit work flows
            addActionModeRelatedSettings(
                configurableActionSetting,
                ActionSettingType.AFTER_EXEC,
                Arrays.asList(ActionState.SUCCEEDED, ActionState.FAILED),
                actionStateSettings);
            addActionModeRelatedSettings(
                configurableActionSetting,
                ActionSettingType.ON_GEN,
                ActionState.READY,
                actionStateSettings);
        }

        return Collections.unmodifiableMap(actionStateSettings);
    }

    private void addActionModeRelatedSettings(
            @Nonnull ConfigurableActionSettings configurableActionSetting,
            ActionSettingType actionSettingType,
            ActionState actionStates,
            @Nonnull Map<ActionState, String> actionStateSettings) {
        addActionModeRelatedSettings(configurableActionSetting,
            actionSettingType,
            Arrays.asList(actionStates),
            actionStateSettings);
    }

    private void addActionModeRelatedSettings(
            @Nonnull ConfigurableActionSettings configurableActionSetting,
            ActionSettingType actionSettingType,
            List<ActionState> actionStates,
            @Nonnull Map<ActionState, String> actionStateSettings) {
        final String settingName =
            ActionSettingSpecs.getSubSettingFromActionModeSetting(
                configurableActionSetting,
                actionSettingType);
        for (ActionState actionState : actionStates) {
            actionStateSettings.put(actionState, settingName);
        }
    }

    @Nonnull
    private Optional<ConfigurableActionSettings> getActionModeSettingSpec(
        @Nonnull final ActionDTO.Action action,
        @Nonnull Map<String, Setting> settingsForTargetEntity,
        @Nonnull Set<String> defaultSettingsForEntity) {
        return specsApplicableToAction(action, settingsForTargetEntity, defaultSettingsForEntity)
            // there potentially be setting unrelated to action mode like
            // EntitySettingSpecs.EnforceNonDisruptive. Take a look at specsApplicableToAction
            // for more details. Here we only need action mode settings.
            .map(ActionSpecifications::getConfigurableActionSetting)
            .findAny();
    }

    @Nonnull
    private Map<ActionState, Setting> getSettingForState(
            @Nonnull final ActionDTO.Action recommendation,
            @Nonnull Map<String, Setting> actionSettings,
            @Nonnull Set<String> defaultSettingsForEntity) {
        final Map<ActionState, String> actionStateSettings = getActionStateSettings(
            recommendation, actionSettings, defaultSettingsForEntity);
        final Map<ActionState, Setting> result = new EnumMap<>(ActionState.class);
        for (Entry<ActionState, String> settingEntry: actionStateSettings.entrySet()) {
            final ActionState actionState = settingEntry.getKey();
            final String settingName = settingEntry.getValue();
            final Setting settingValue = actionSettings.get(settingName);
            if (settingValue != null) {
                result.put(actionState, settingValue);
            }
        }
        return Collections.unmodifiableMap(result);
    }

    /**
     * Groups together the settings of an action into a single object.
     */
    @VisibleForTesting
    static class ActionSpecifications {
        private final ConfigurableActionSettings configurableActionSetting;
        private final boolean enforceNonDisruptive;

        ActionSpecifications(ConfigurableActionSettings configurableActionSetting,
                             boolean enforceNonDisruptive) {
            this.configurableActionSetting = configurableActionSetting;
            this.enforceNonDisruptive = enforceNonDisruptive;
        }

        ActionSpecifications(ConfigurableActionSettings configurableActionSetting) {
            this(configurableActionSetting, false);
        }

        ConfigurableActionSettings getConfigurableActionSetting() {
            return configurableActionSetting;
        }

        public boolean isNonDisruptiveEnforced() {
            return enforceNonDisruptive;
        }
    }

    /**
     * Get the setting specs applicable to an action. The applicable setting specs are derived
     * from the type of the action and the entities it involves.
     *
     * @param action The protobuf representation of the action.
     * @param settingsForTargetEntity The settings for the target entity
     * @param defaultSettingsForEntity settings for the target entity defined in default
     *         policies
     * @return The stream of applicable {@link EntitySettingSpecs}. This will be a stream of
     *         size one in most cases.
     */
    @VisibleForTesting
    @Nonnull
    Stream<ActionSpecifications> specsApplicableToAction(
            @Nonnull final ActionDTO.Action action, Map<String, Setting> settingsForTargetEntity,
            @Nonnull Set<String> defaultSettingsForEntity) {
        final ActionTypeCase type = action.getInfo().getActionTypeCase();
        switch (type) {
            case MOVE:
                // This may result in one applicable entity spec (for a host move or storage move),
                // or two applicable entity spec for cases where the host move has to be
                // accompanied by a storage move.
                return action.getInfo().getMove().getChangesList().stream()
                        .map(provider -> provider.getDestination().getType())
                        .map(destinationEntityType -> PROVIDER_ENTITY_TYPE_TO_MOVE_SETTING_SPECS
                                        .getOrDefault(destinationEntityType,
                                            ConfigurableActionSettings.Move))
                        .distinct()
                        .map(ActionSpecifications::new);
            case SCALE:
                final ConfigurableActionSettings cas;
                switch (action.getInfo().getScale().getTarget().getType()) {
                    case EntityType.DATABASE_VALUE:
                        cas = ConfigurableActionSettings.CloudDBScale;
                    break;
                    case EntityType.DATABASE_SERVER_VALUE:
                        cas = getScaleActionSetting(action, ConfigurableActionSettings.CloudDBServerScale);
                        break;
                    case EntityType.VIRTUAL_VOLUME_VALUE:
                        cas = getScaleActionSetting(action, ConfigurableActionSettings.CloudComputeScale);
                    break;
                    default:
                        cas = (enableCloudScaleEnhancement) ? getVmScaleActionSetting(action,
                                settingsForTargetEntity, defaultSettingsForEntity)
                                : ConfigurableActionSettings.CloudComputeScale;
                }
                return Stream.of(new ActionSpecifications(cas));
            case RECONFIGURE:
                return Stream.of(new ActionSpecifications(ConfigurableActionSettings.Reconfigure));
            case PROVISION:
                final String horizontalScaleUp = ConfigurableActionSettings.HorizontalScaleUp.getSettingName();
                final Provision provision = action.getInfo().getProvision();
                if (settingsForTargetEntity.containsKey(horizontalScaleUp)
                        && SettingDTOUtil.isActionEnabled(settingsForTargetEntity.get(horizontalScaleUp))
                        && provision.getEntityToClone().getType() == EntityType.CONTAINER_POD_VALUE) {
                    return Stream.of(new ActionSpecifications(ConfigurableActionSettings.HorizontalScaleUp));
                }
                return Stream.of(new ActionSpecifications(ConfigurableActionSettings.Provision));
            case ATOMICRESIZE:
                return Stream.of(new ActionSpecifications(ConfigurableActionSettings.Resize));
            case RESIZE:
                final Resize resize = action.getInfo().getResize();
                return getResizeActionSpecifications(resize, settingsForTargetEntity);
            case ACTIVATE:
                return Stream.of(new ActionSpecifications(ConfigurableActionSettings.Activate));
            case DEACTIVATE:
                final Deactivate deactivate = action.getInfo().getDeactivate();
                final String horizontalScaleDown = ConfigurableActionSettings.HorizontalScaleDown.getSettingName();
                if (settingsForTargetEntity.containsKey(horizontalScaleDown)
                        && SettingDTOUtil.isActionEnabled(settingsForTargetEntity.get(horizontalScaleDown))
                        && deactivate.getTarget().getType() == EntityType.CONTAINER_POD_VALUE) {
                    return Stream.of(new ActionSpecifications(ConfigurableActionSettings.HorizontalScaleDown));
                }
                return Stream.of(new ActionSpecifications(ConfigurableActionSettings.Suspend));
            case DELETE:
                final EntityType targetType = EntityType.forNumber(
                        action.getInfo().getDelete().getTarget().getType());
                switch (targetType) {
                    // TODO (Cloud PaaS): ASP "legacy" APPLICATION_COMPONENT support, OM-83212
                    //  can remove APPLICATION_COMPONENT case when legacy support not needed
                    case APPLICATION_COMPONENT:
                        return Stream.of(ConfigurableActionSettings.Delete,
                                ConfigurableActionSettings.DeleteAppServicePlan).filter(
                                setting -> setting.getEntityTypeScope().contains(targetType)).map(
                                ActionSpecifications::new);
                    case VIRTUAL_MACHINE_SPEC:
                        return Stream.of(ConfigurableActionSettings.Delete,
                                ConfigurableActionSettings.DeleteVirtualMachineSpec).filter(
                                setting -> setting.getEntityTypeScope().contains(targetType)).map(
                                ActionSpecifications::new);
                    case VIRTUAL_VOLUME:
                    default:
                        // Some form of volume
                        return Stream.of(ConfigurableActionSettings.Delete,
                                ConfigurableActionSettings.DeleteVolume).filter(
                                setting -> setting.getEntityTypeScope().contains(targetType)).map(
                                ActionSpecifications::new);
                }
            case ALLOCATE: // Allocate actions are not executable and are not configurable by the user
            case ACTIONTYPE_NOT_SET:
            default:
                return Stream.empty();
        }
    }

    private static ConfigurableActionSettings getScaleActionSetting(
        @Nonnull final ActionDTO.Action action, @Nonnull final ConfigurableActionSettings defaultSetting) {
        // If probe provided information about disruptiveness/reversibility then use
        // appropriate settings.
        if (action.hasDisruptive() && action.hasReversible()) {
            final ConfigurableActionSettings result;
            if (action.getDisruptive()) {
                result = action.getReversible()
                    ? ConfigurableActionSettings.DisruptiveReversibleScaling
                    : ConfigurableActionSettings.DisruptiveIrreversibleScaling;
            } else {
                result = action.getReversible()
                    ? ConfigurableActionSettings.NonDisruptiveReversibleScaling
                    : ConfigurableActionSettings.NonDisruptiveIrreversibleScaling;
            }

            // Also check that disruptiveness/reversibility settings are applied to the given type.
            final EntityType entityType = EntityType.forNumber(action.getInfo().getScale()
                .getTarget().getType());
            if (result.getEntityTypeScope().contains(entityType)) {
                return result;
            }
        }

        // By default use generic setting for Scale actions
        return defaultSetting;
    }

    /**
     * Describes which settings should be applied resize action for commodity type of an entity must.
     */
    private static final class ResizeDirectionConfiguration {
        private final EntityType entityType;
        private final CommodityType commodityType;
        private final ConfigurableActionSettings upDirectionSetting;
        private final ConfigurableActionSettings downDirectionSetting;

        private ResizeDirectionConfiguration(
                final EntityType entityType,
                final CommodityType commodityType,
                final ConfigurableActionSettings upDirectionSetting,
                final ConfigurableActionSettings downDirectionSetting) {
            this.entityType = entityType;
            this.commodityType = commodityType;
            this.upDirectionSetting = upDirectionSetting;
            this.downDirectionSetting = downDirectionSetting;
        }
    }

    private Stream<ActionSpecifications> getResizeActionSpecifications(
            @Nonnull final Resize resize,
            Map<String, Setting> settingsForTargetEntity) {
        for (ResizeDirectionConfiguration resizeDirectionConfiguration : RESIZE_DIRECTION_CONFIGURATIONS) {
            if (isResizeCommodity(resize, resizeDirectionConfiguration.entityType, resizeDirectionConfiguration.commodityType)) {
                ConfigurableActionSettings spec = resize.getNewCapacity() > resize.getOldCapacity()
                    ? resizeDirectionConfiguration.upDirectionSetting : resizeDirectionConfiguration.downDirectionSetting;
                return Stream.of(new ActionSpecifications(spec));
            }
        }

        Optional<ConfigurableActionSettings> rangeAwareSpec = rangeAwareSpecCalculator
            .getSpecForRangeAwareCommResize(resize, settingsForTargetEntity);
        // Return the range aware spec if present. Otherwise return the regular resize
        // spec, or if it's a vm the default empty stream, since the only resize action
        // that should fall into this logic is for vStorages. Resize Vms for vStorage
        // commodities should always translate to a RECOMMENDED mode .
        if (isVirtualMachine(resize) && !rangeAwareSpec.isPresent()) {
            return Stream.empty();
        }
        return Stream.of(new ActionSpecifications(rangeAwareSpec.orElse(ConfigurableActionSettings.Resize),
            true));
    }

    /**
     * Get the setting specs applicable to a VM scale action.
     *
     * @param action The protobuf representation of the action.
     * @param settingsForTargetEntity The settings for the target entity
     * @param defaultSettingsForEntity settings for the target entity defined in default
     *         policies
     * @return The stream of applicable {@link EntitySettingSpecs}.
     */
    @VisibleForTesting
    @Nonnull
    ConfigurableActionSettings getVmScaleActionSetting(@Nonnull final ActionDTO.Action action,
            @Nonnull Map<String, Setting> settingsForTargetEntity,
            @Nonnull Set<String> defaultSettingsForEntity) {
        final boolean hasCongestion = action.getExplanation()
                .getScale()
                .getChangeProviderExplanationList()
                .stream()
                .anyMatch(c -> c.hasCongestion());

        final boolean hasEfficiency = action.getExplanation()
                .getScale()
                .getChangeProviderExplanationList()
                .stream()
                .anyMatch(c -> c.hasEfficiency());

        final boolean scaleAllisOverridden = !defaultSettingsForEntity.contains(
                ConfigurableActionSettings.CloudComputeScale.getSettingName());
        final ConfigurableActionSettings actionSetting;

        //round savings, sometimes we put extremely small values in savings, because of floating
        //point rounding error, e.g. -1.9054859495826193e-9, in UI it is displayed as 0
        BigDecimal bd = new BigDecimal(action.getSavingsPerHour().getAmount()).setScale(PRECISION,
                RoundingMode.HALF_EVEN);
        final double savings = bd.doubleValue();

        if (hasCongestion) {
            if (Double.compare(savings, 0.0) == 1) {
                // Performance actions with Savings use-case
                // in this case, the "Automation Mode" for performance actions with savings should
                // be auto-configured based on the less conservative automation mode hierarchy
                // between "Scale for Savings" and "Scale for Performance" options.
                actionSetting = getSpecsApplicableToScaleForPerfWithSavings(scaleAllisOverridden,
                        settingsForTargetEntity, defaultSettingsForEntity);
            } else {
                // Performance actions with investment use-case (or with 0 savings)
                actionSetting = scaleAllisOverridden ? ConfigurableActionSettings.CloudComputeScale
                        : ConfigurableActionSettings.CloudComputeScaleForPerf;
            }
        } else if (hasEfficiency) {

            if (Double.compare(savings, 0.0) == -1) {
                // Efficiency actions with an investment use-case
                // in this case, the “Automation Mode” should be auto-configured and matched to the “Scale All” setting
                actionSetting = ConfigurableActionSettings.CloudComputeScale;
            } else {
                // Efficiency actions with savings use-case  (or with 0 savings)
                actionSetting = scaleAllisOverridden ? ConfigurableActionSettings.CloudComputeScale
                        : ConfigurableActionSettings.CloudComputeScaleForSavings;
            }
        } else {
            // actions that are not Performance or Efficiency, e.g. Compliance
            actionSetting = ConfigurableActionSettings.CloudComputeScale;
        }
        return actionSetting;
    }

    /**
     * Get the setting specs applicable to a scale for performance with Savings action.
     *
     * @param scaleAllisOverridden is 'Scale All' is overriden in custom policy.
     * @param settingsForTargetEntity The settings for the target entity
     * @param defaultSettingsForEntity settings for the target entity defined in default
     *         policies
     * @return applicable setting spec
     */
    @Nonnull
    private ConfigurableActionSettings getSpecsApplicableToScaleForPerfWithSavings(
            boolean scaleAllisOverridden, @Nonnull Map<String, Setting> settingsForTargetEntity,
            @Nonnull Set<String> defaultSettingsForEntity) {

        final ConfigurableActionSettings perfActionSetting;
        final ConfigurableActionSettings effActionSetting;

        if (scaleAllisOverridden) {
            perfActionSetting = ConfigurableActionSettings.CloudComputeScale;
            effActionSetting = ConfigurableActionSettings.CloudComputeScale;
        } else {
            final boolean scaleForPerfisOverridden = !defaultSettingsForEntity.contains(
                    ConfigurableActionSettings.CloudComputeScaleForPerf.getSettingName());
            final boolean scaleForSavingsisOverridden = !defaultSettingsForEntity.contains(
                    ConfigurableActionSettings.CloudComputeScaleForSavings.getSettingName());
            if (scaleForPerfisOverridden && !scaleForSavingsisOverridden) {
                perfActionSetting =
                        effActionSetting = ConfigurableActionSettings.CloudComputeScaleForPerf;
            } else if (!scaleForPerfisOverridden && scaleForSavingsisOverridden) {
                perfActionSetting =
                        effActionSetting = ConfigurableActionSettings.CloudComputeScaleForSavings;
            } else {
                perfActionSetting = ConfigurableActionSettings.CloudComputeScaleForPerf;
                effActionSetting = ConfigurableActionSettings.CloudComputeScaleForSavings;
            }
        }

        final Setting perfSetting = settingsForTargetEntity.get(perfActionSetting.getSettingName());
        final Setting effSetting = settingsForTargetEntity.get(effActionSetting.getSettingName());

        if (perfSetting == null || effSetting == null) {
            logger.debug("There is no defined setting for performance/efficiency cloud scale");
            return ConfigurableActionSettings.CloudComputeScale;
        } else {
            final int comparison = SettingDTOUtil.compareEnumSettingValues(
                    perfSetting.getEnumSettingValue(), effSetting.getEnumSettingValue(),
                    CLOUD_SCALE_SETTING_ENUM_TYPE);
            if (comparison >= 0) {
                return perfActionSetting;
            } else {
                return effActionSetting;
            }
        }
    }


    /**
     * It keeps the settings of the range-aware resize.
     */
    @Value.Immutable
    public interface RangeAwareResizeSettings {
        /**
         * Gets the above max automation level.
         *
         * @return The above max automation level
         */
        ConfigurableActionSettings aboveMaxThreshold();

        /**
         * Gets the below min automation level.
         *
         * @return The below min automation level
         */
        ConfigurableActionSettings belowMinThreshold();

        /**
         * Gets the in-range automation level for resize up.
         *
         * @return The in-range automation level for the resize up
         */
        ConfigurableActionSettings upInBetweenThresholds();

        /**
         * Gets the in-range automation level for the resize down.
         *
         * @return The in-range automation level for the resize down
         */
        ConfigurableActionSettings downInBetweenThresholds();

        /**
         * Gets the min threshold of the commodity.
         *
         * @return The min threshold of the commodity
         */
        EntitySettingSpecs minThreshold();

        /**
         * Gets the max threshold of the commodity.
         *
         * @return The max threshold of the commodity
         */
        EntitySettingSpecs maxThreshold();
    }

    /**
     * Resize capacity keeps the old and new capacities of a resize action.
     */
    @Value.Immutable
    interface ResizeCapacity {
        /**
         * Gets the existing capacity.
         *
         * @return The existing capacity
         */
        float oldCapacity();

        /**
         * Gets the new capacity.
         *
         * @return The new capacity
         */
        float newCapacity();
    }

    /**
     * Checks if the Resize action has an EntityType and it's a Virtual Machine .
     * @param resize The {@link Resize} action
     * @return boolean whether is a vm or not
     * */
    private static boolean isVirtualMachine(Resize resize) {
        return resize.getTarget().getType() == EntityType.VIRTUAL_MACHINE_VALUE;
    }

    private boolean isResizeCommodity(Resize resize, EntityType entityType, CommodityType commodityType) {
        return resize.getTarget().getType() == entityType.getNumber()
            && resize.getCommodityType().getType() == commodityType.getNumber();
    }

    /**
     * This class is used to find the spec that applies for a Resize action on a commodity which is
     * range aware.
     * For ex. vmem / vcpu resize of an on-prem VM.
     */
    private class RangeAwareSpecCalculator {
        private static final double CAPACITY_COMPARISON_DELTA = 0.001;
        // This map holds the resizeSettings by commodity type per entity type
        private final Map<Integer, Map<Integer, RangeAwareResizeSettings>> resizeSettingsByEntityType =
                ImmutableMap.of(EntityType.VIRTUAL_MACHINE_VALUE, populateResizeSettingsByCommodityForVM(),
                    EntityType.CONTAINER_VALUE, populateResizeSettingsByCommodityForContainer());

        /**
         * Gets the spec applicable for range aware commodity resize. Currently VMem, VCpu
         * resizes of on-prem VMs and VMem, VMemRequest, VCPU and VCPURequest resizes of containers
         * are considered range aware.
         *
         * <p>There is a minThreshold and a maxThreshold defined for the commodity resizing.
         * There are separate automation modes defined for these cases:
         * 1. The new capacity is greater than the maxThreshold
         * 2. The new capacity is lesser than the minThreshold
         * 3. The new capacity is greater than or equal to minThreshold and less than or equal
         *    to maxThreshold, and it is sizing up
         * 4. The new capacity is greater than or equal to minThreshold and less than or equal
         *    to maxThreshold, and it is sizing down</p>
         *
         * <p>This method will determine if any of these settings apply to the resize action.</p>
         *
         * @param resize the resize action
         * @param settingsForTargetEntity A map of the setting name and the setting for this entity
         * @return Optional of the applicable spec. If nothing applies, Optional.empty.
         */
        @Nonnull
        private Optional<ConfigurableActionSettings> getSpecForRangeAwareCommResize(
                Resize resize, Map<String, Setting> settingsForTargetEntity) {
            Integer entityType = resize.getTarget().getType();
            Integer commType = resize.getCommodityType().getType();
            CommodityAttribute changedAttribute = resize.getCommodityAttribute();
            // Get the resizeSettingsByCommodity for this entity type
            Map<Integer, RangeAwareResizeSettings> resizeSettingsByCommodity = resizeSettingsByEntityType.get(entityType);
            Optional<ConfigurableActionSettings> applicableSpec = Optional.empty();
            // Range aware settings can apply to Resized Commodity attributes such as Capacity and
            // Reservation Resize Down of VM. We generate only resize down actions for Reservation,
            // and since we generate a Remove Limit action for Limits instead of a
            // Limit increase action (Resize Up), we don't want to consider the range awareness for
            // this action.
            if (resizeSettingsByCommodity != null
                && (changedAttribute == CommodityAttribute.CAPACITY
                    || isVMResizeDownReservation(resize))) {
                RangeAwareResizeSettings resizeSettings
                    = resizeSettingsByCommodity.get(resize.getCommodityAttribute()
                        == CommodityAttribute.RESERVED
                            ? reservedCommodityBoughtSoldConversion.get(commType)
                            : commType);
                if (resizeSettings != null) {
                    Optional<Float> minThresholdOpt = getNumericSettingForEntity(
                            settingsForTargetEntity, resizeSettings.minThreshold());
                    Optional<Float> maxThresholdOpt = getNumericSettingForEntity(
                            settingsForTargetEntity, resizeSettings.maxThreshold());
                    if (minThresholdOpt.isPresent() && maxThresholdOpt.isPresent()) {
                        float minThreshold = minThresholdOpt.get();
                        float maxThreshold = maxThresholdOpt.get();
                        if (minThreshold > maxThreshold) {
                            logger.error("Incorrect resize configuration for entity id {} and commodity {} : "
                                + "Min threshold value ({}) is greater than max threshold value ({}).",
                                resize.getTarget().getId(), commType, minThreshold, maxThreshold);
                            return Optional.empty();
                        }
                        ResizeCapacity resizeCapacity = getCapacityForModeCalculation(resize);
                        float oldCapacity = resizeCapacity.oldCapacity();
                        float newCapacity = resizeCapacity.newCapacity();
                        // Recognize if this is a resize up or down
                        if (Math.abs(newCapacity - oldCapacity) <= CAPACITY_COMPARISON_DELTA) {
                            // -Delta <= new capacity - old capacity <= +Delta
                            logger.error("{}  has a resize action on commodity {}  with same "
                                    + "old and new capacity -> {}", resize.getTarget().getId(), commType,
                                resize.getNewCapacity());
                        } else if (newCapacity > oldCapacity) {
                            // A resize up action
                            // initialize applicableSpec to the actionMode for scaling up inbetween thresholds
                            applicableSpec = Optional.of(resizeSettings.upInBetweenThresholds());
                            if (oldCapacity >= maxThreshold && newCapacity > maxThreshold) {
                                // if the currentCapacity is over max and we are scaling up, use setting aboveMax
                                applicableSpec = Optional.of(resizeSettings.aboveMaxThreshold());
                            }
                        } else {
                            // A resize down action
                            // initialize applicableSpec to the actionMode for scaling down inbetween thresholds
                            applicableSpec = Optional.of(resizeSettings.downInBetweenThresholds());
                            if (oldCapacity <= minThreshold && newCapacity < minThreshold) {
                                // if the currentCapacity is below min and we are scaling down, use setting belowMin
                                applicableSpec = Optional.of(resizeSettings.belowMinThreshold());
                            }
                        }
                    }
                }
            } else if (entityType == EntityType.VIRTUAL_MACHINE.getNumber()) {
                applicableSpec = Optional.ofNullable(VMS_ACTION_MODE_SETTINGS.get(changedAttribute) != null
                    ? VMS_ACTION_MODE_SETTINGS.get(changedAttribute).get(commType) : null );
            }
            logger.debug("Range aware spec for resizing {} of commodity {} of entity {} is {} ",
                    changedAttribute, commType, resize.getTarget().getId(),
                    applicableSpec.map(ConfigurableActionSettings::getSettingName).orElse("empty"));
            return applicableSpec;
        }

        private boolean isVMResizeDownReservation(Resize resize) {
            return resize.getTarget().getType() == EntityType.VIRTUAL_MACHINE.getNumber()
                    && resize.getCommodityAttribute() == CommodityAttribute.RESERVED
                    && resize.getNewCapacity() < resize.getOldCapacity();
        }

        /**
         * Gets the numeric setting defined by the spec from the settings map. If it is not present in
         * the settings map, then it returns the default defined in the EntitySettingSpecs enum.
         * @param settings The settings for an entity
         * @param spec The spec to look for in the settings map
         * @return the numeric setting defined by spec if it is a numeric spec. Optional.empty() otherwise.
         */
        private Optional<Float> getNumericSettingForEntity(
                @Nonnull final Map<String, Setting> settings, @Nonnull final EntitySettingSpecs spec) {
            final Setting setting = settings.get(spec.getSettingName());
            if (spec.getSettingSpec().hasNumericSettingValueType()) {
                if (setting == null) {
                    // If there is no setting for this spec that applies to the target
                    // of the action, we use the system default (which comes from the
                    // enum definitions).
                    return Optional.of(spec.getSettingSpec().getNumericSettingValueType().getDefault());
                } else {
                    // In all other cases, we use the value from the setting.
                    return Optional.of(setting.getNumericSettingValue().getValue());
                }
            } else {
                return Optional.empty();
            }
        }

        /**
         * Gets the old capacity and the new capacity from this resize action.
         * In case of Vmem commodity resize, the old and new capacities of the action are in terms
         * of Kilo bytes. We convert this to MB because the ResizeVmemMinThreshold and
         * ResizeVmemMaxThreshold are defined in MB.
         *
         * @param resize the resize action to get the old capacity and new capacity from
         * @return An array containing 2 floats - oldCapacity and newCapacity
         */
        private ResizeCapacity getCapacityForModeCalculation(Resize resize) {
            float oldCapacityForMode = resize.getOldCapacity();
            float newCapacityForMode = resize.getNewCapacity();
            if (MEM_COMMODITY_TYPES.contains(resize.getCommodityType().getType())
                    || (isVMResizeDownReservation(resize)
                        && resize.getCommodityType().getType() == CommodityType.MEM_VALUE)) {
                oldCapacityForMode /= Units.NUM_OF_KB_IN_MB;
                newCapacityForMode /= Units.NUM_OF_KB_IN_MB;
            }
            return ImmutableResizeCapacity.builder()
                    .newCapacity(newCapacityForMode)
                    .oldCapacity(oldCapacityForMode).build();
        }

        /**
         * Returns a map of the commodity type to the range aware resize settings applicable to it
         * for VM.
         *
         * @return returns a map of the commodity type to the range aware resize settings applicable
         * to it.
         */
        private Map<Integer, RangeAwareResizeSettings> populateResizeSettingsByCommodityForVM() {
            RangeAwareResizeSettings vCpuSettings = ImmutableRangeAwareResizeSettings.builder()
                    .aboveMaxThreshold(ConfigurableActionSettings.ResizeVcpuAboveMaxThreshold)
                    .belowMinThreshold(ConfigurableActionSettings.ResizeVcpuBelowMinThreshold)
                    .upInBetweenThresholds(ConfigurableActionSettings.ResizeVcpuUpInBetweenThresholds)
                    .downInBetweenThresholds(ConfigurableActionSettings.ResizeVcpuDownInBetweenThresholds)
                    .maxThreshold(EntitySettingSpecs.ResizeVcpuMaxThreshold)
                    .minThreshold(EntitySettingSpecs.ResizeVcpuMinThreshold).build();
            RangeAwareResizeSettings vMemSettings = ImmutableRangeAwareResizeSettings.builder()
                    .aboveMaxThreshold(ConfigurableActionSettings.ResizeVmemAboveMaxThreshold)
                    .belowMinThreshold(ConfigurableActionSettings.ResizeVmemBelowMinThreshold)
                    .upInBetweenThresholds(ConfigurableActionSettings.ResizeVmemUpInBetweenThresholds)
                    .downInBetweenThresholds(ConfigurableActionSettings.ResizeVmemDownInBetweenThresholds)
                    .maxThreshold(EntitySettingSpecs.ResizeVmemMaxThreshold)
                    .minThreshold(EntitySettingSpecs.ResizeVmemMinThreshold).build();
            return ImmutableMap.of(CommodityDTO.CommodityType.VCPU_VALUE, vCpuSettings,
                CommodityDTO.CommodityType.VMEM_VALUE, vMemSettings);
        }

        /**
         * Returns a map of the commodity type to the range aware resize settings applicable to it
         * for Container.
         *
         * @return A map of the commodity type to the range aware resize settings applicable to it.
         */
        private Map<Integer, RangeAwareResizeSettings> populateResizeSettingsByCommodityForContainer() {
            RangeAwareResizeSettings vCpuSettings = ImmutableRangeAwareResizeSettings.builder()
                .aboveMaxThreshold(ConfigurableActionSettings.ResizeVcpuLimitAboveMaxThreshold)
                .belowMinThreshold(ConfigurableActionSettings.ResizeVcpuLimitBelowMinThreshold)
                .upInBetweenThresholds(ConfigurableActionSettings.Resize)
                .downInBetweenThresholds(ConfigurableActionSettings.Resize)
                .maxThreshold(EntitySettingSpecs.ResizeVcpuLimitMaxThreshold)
                .minThreshold(EntitySettingSpecs.ResizeVcpuLimitMinThreshold).build();
            RangeAwareResizeSettings vMemSettings = ImmutableRangeAwareResizeSettings.builder()
                .aboveMaxThreshold(ConfigurableActionSettings.ResizeVmemLimitAboveMaxThreshold)
                .belowMinThreshold(ConfigurableActionSettings.ResizeVmemLimitBelowMinThreshold)
                .upInBetweenThresholds(ConfigurableActionSettings.Resize)
                .downInBetweenThresholds(ConfigurableActionSettings.Resize)
                .maxThreshold(EntitySettingSpecs.ResizeVmemLimitMaxThreshold)
                .minThreshold(EntitySettingSpecs.ResizeVmemLimitMinThreshold).build();
            RangeAwareResizeSettings vCpuRequestSettings = ImmutableRangeAwareResizeSettings.builder()
                // AboveMaxThreshold is a required field, so use "ResizeVcpuLimitAboveMaxThreshold" for
                // request resize settings. In fact, market only generates resizing down request, so
                // aboveMaxThreshold configured here does not matter.
                .aboveMaxThreshold(ConfigurableActionSettings.ResizeVcpuLimitAboveMaxThreshold)
                .belowMinThreshold(ConfigurableActionSettings.ResizeVcpuRequestBelowMinThreshold)
                .upInBetweenThresholds(ConfigurableActionSettings.Resize)
                .downInBetweenThresholds(ConfigurableActionSettings.Resize)
                .maxThreshold(EntitySettingSpecs.ResizeVcpuLimitMaxThreshold)
                .minThreshold(EntitySettingSpecs.ResizeVcpuRequestMinThreshold).build();
            RangeAwareResizeSettings vMemRequestSettings = ImmutableRangeAwareResizeSettings.builder()
                .aboveMaxThreshold(ConfigurableActionSettings.ResizeVmemLimitAboveMaxThreshold)
                .belowMinThreshold(ConfigurableActionSettings.ResizeVmemRequestBelowMinThreshold)
                .upInBetweenThresholds(ConfigurableActionSettings.Resize)
                .downInBetweenThresholds(ConfigurableActionSettings.Resize)
                .maxThreshold(EntitySettingSpecs.ResizeVmemLimitMaxThreshold)
                .minThreshold(EntitySettingSpecs.ResizeVmemRequestMinThreshold).build();
            return ImmutableMap.of(CommodityDTO.CommodityType.VCPU_VALUE, vCpuSettings,
                CommodityDTO.CommodityType.VMEM_VALUE, vMemSettings,
                CommodityDTO.CommodityType.VCPU_REQUEST_VALUE, vCpuRequestSettings,
                CommodityDTO.CommodityType.VMEM_REQUEST_VALUE, vMemRequestSettings);
        }
    }
}
