package com.vmturbo.action.orchestrator.action;

import static com.vmturbo.components.common.setting.EntitySettingSpecs.ActivateActionWorkflow;
import static com.vmturbo.components.common.setting.EntitySettingSpecs.DeleteActionWorkflow;
import static com.vmturbo.components.common.setting.EntitySettingSpecs.MoveActionWorkflow;
import static com.vmturbo.components.common.setting.EntitySettingSpecs.PostActivateActionWorkflow;
import static com.vmturbo.components.common.setting.EntitySettingSpecs.PostDeleteActionWorkflow;
import static com.vmturbo.components.common.setting.EntitySettingSpecs.PostMoveActionWorkflow;
import static com.vmturbo.components.common.setting.EntitySettingSpecs.PostProvisionActionWorkflow;
import static com.vmturbo.components.common.setting.EntitySettingSpecs.PostResizeActionWorkflow;
import static com.vmturbo.components.common.setting.EntitySettingSpecs.PostSuspendActionWorkflow;
import static com.vmturbo.components.common.setting.EntitySettingSpecs.PreActivateActionWorkflow;
import static com.vmturbo.components.common.setting.EntitySettingSpecs.PreDeleteActionWorkflow;
import static com.vmturbo.components.common.setting.EntitySettingSpecs.PreMoveActionWorkflow;
import static com.vmturbo.components.common.setting.EntitySettingSpecs.PreProvisionActionWorkflow;
import static com.vmturbo.components.common.setting.EntitySettingSpecs.PreResizeActionWorkflow;
import static com.vmturbo.components.common.setting.EntitySettingSpecs.PreSuspendActionWorkflow;
import static com.vmturbo.components.common.setting.EntitySettingSpecs.ProvisionActionWorkflow;
import static com.vmturbo.components.common.setting.EntitySettingSpecs.ResizeActionWorkflow;
import static com.vmturbo.components.common.setting.EntitySettingSpecs.ResizeVcpuDownInBetweenThresholds;
import static com.vmturbo.components.common.setting.EntitySettingSpecs.ResizeVcpuUpInBetweenThresholds;
import static com.vmturbo.components.common.setting.EntitySettingSpecs.ResizeVmemDownInBetweenThresholds;
import static com.vmturbo.components.common.setting.EntitySettingSpecs.ResizeVmemUpInBetweenThresholds;
import static com.vmturbo.components.common.setting.EntitySettingSpecs.SuspendActionWorkflow;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;

import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.schedule.ScheduleProto;
import com.vmturbo.common.protobuf.setting.SettingProto;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;
import com.vmturbo.commons.Units;
import com.vmturbo.components.api.SetOnce;
import com.vmturbo.components.common.setting.ActionSettingSpecs;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * A utility class to capture the logic to calculate an {@link ActionMode} for an action,
 * given some user settings.
 */
public class ActionModeCalculator {
    private static final Map<Integer, EntitySettingSpecs>
                    PROVIDER_ENTITY_TYPE_TO_MOVE_SETTING_SPECS =
                    ImmutableMap.of(EntityType.STORAGE_VALUE, EntitySettingSpecs.StorageMove,
                                    EntityType.DESKTOP_POOL_VALUE,
                                    EntitySettingSpecs.BusinessUserMove);

    private static final Logger logger = LogManager.getLogger();

    private final RangeAwareSpecCalculator rangeAwareSpecCalculator;

    public ActionModeCalculator() {
        this.rangeAwareSpecCalculator = new RangeAwareSpecCalculator();
    }

    // This is present in case we want to test just ActionModeCalculator without
    // rangeAwareSpecCalculator. In that case, it cane be mocked if needed.
    @VisibleForTesting
    ActionModeCalculator(@Nonnull RangeAwareSpecCalculator rangeAwareSpecCalculator) {
        this.rangeAwareSpecCalculator = rangeAwareSpecCalculator;
    }

    /**
     * Map from commodity attribute and commodity type to the correct entitySettingSpec. This is
     * only used for vms, since we have different type of resize based on the commodity and the
     * attribute.
     */
    private static final Map<CommodityAttribute, Map<Integer, EntitySettingSpecs>> VMS_ACTION_MODE_SETTINGS =
        new ImmutableMap.Builder<CommodityAttribute, Map<Integer, EntitySettingSpecs>>()
            .put(CommodityAttribute.LIMIT,
                new ImmutableMap.Builder<Integer, EntitySettingSpecs>()
                    .put(CommodityType.VCPU.getNumber(), ResizeVcpuUpInBetweenThresholds)
                    .put(CommodityType.VMEM.getNumber(), ResizeVmemUpInBetweenThresholds)
                    .build())
            .put(CommodityAttribute.RESERVED,
                new ImmutableMap.Builder<Integer, EntitySettingSpecs>()
                    .put(CommodityType.CPU.getNumber(), ResizeVcpuDownInBetweenThresholds)
                    .put(CommodityType.MEM.getNumber(), ResizeVmemDownInBetweenThresholds)
                    .build())
            .build();
    /**
     * Map from an actionType -> corresponding Workflow Action EntitySettingsSpec if
     * the actionType may be overridden. Used to calculate the action mode for Workflow
     * policy applications, if any.
     * TODO: We should define a dynamic process for Orchestration probe types to add dynamically
     * to the supported workflow types.
     */
    private static final Map<ActionType, EntitySettingSpecs> WORKFLOW_ACTION_TYPE_MAP =
            new ImmutableMap.Builder<ActionType, EntitySettingSpecs>()
                .put(ActionType.ACTIVATE, ActivateActionWorkflow)
                .put(ActionType.START, ActivateActionWorkflow)
                .put(ActionType.DEACTIVATE, SuspendActionWorkflow)
                .put(ActionType.SUSPEND, SuspendActionWorkflow)
                .put(ActionType.MOVE, MoveActionWorkflow)
                .put(ActionType.PROVISION, ProvisionActionWorkflow)
                .put(ActionType.RESIZE, ResizeActionWorkflow)
                .put(ActionType.DELETE, DeleteActionWorkflow)
                .build();

    /**
     * Map from an actionType -> corresponding PRE Workflow Action EntitySettingsSpec if
     * the actionType may be overridden. Used to calculate the action mode for PRE Workflow
     * policy applications, if any, to the supported workflow types.
     */
    private static final Map<ActionType, EntitySettingSpecs> PREP_WORKFLOW_ACTION_TYPE_MAP =
        new ImmutableMap.Builder<ActionType, EntitySettingSpecs>()
            .put(ActionType.ACTIVATE, PreActivateActionWorkflow)
            .put(ActionType.START, PreActivateActionWorkflow)
            .put(ActionType.DEACTIVATE, PreSuspendActionWorkflow)
            .put(ActionType.SUSPEND, PreSuspendActionWorkflow)
            .put(ActionType.MOVE, PreMoveActionWorkflow)
            .put(ActionType.PROVISION, PreProvisionActionWorkflow)
            .put(ActionType.RESIZE, PreResizeActionWorkflow)
            .put(ActionType.DELETE, PreDeleteActionWorkflow)
            .build();

    /**
     * Map from an actionType -> corresponding POST Workflow Action EntitySettingsSpec if
     * the actionType may be overridden. Used to calculate the action mode for POST Workflow
     * policy applications, if any, to the supported workflow types.
     */
    private static final Map<ActionType, EntitySettingSpecs> POST_WORKFLOW_ACTION_TYPE_MAP =
        new ImmutableMap.Builder<ActionType, EntitySettingSpecs>()
            .put(ActionType.ACTIVATE, PostActivateActionWorkflow)
            .put(ActionType.START, PostActivateActionWorkflow)
            .put(ActionType.DEACTIVATE, PostSuspendActionWorkflow)
            .put(ActionType.SUSPEND, PostSuspendActionWorkflow)
            .put(ActionType.MOVE, PostMoveActionWorkflow)
            .put(ActionType.PROVISION, PostProvisionActionWorkflow)
            .put(ActionType.RESIZE, PostResizeActionWorkflow)
            .put(ActionType.DELETE, PostDeleteActionWorkflow)
            .build();

    /**
     * Map from an {@link EntitySettingSpecs} for each Workflow to the corresponding "base"
     * EntitySettingSpecs to fetch the {@link ActionMode} for the policy. In other words, the
     * ActionMode (DISABLED, RECOMMEND, MANUAL, AUTOMATIC) for ProvisionActionWorkflow is
     * taken from the Provision setting.
     */
    private static final Map<EntitySettingSpecs, EntitySettingSpecs> WORKFLOW_ACTION_BASE_MAP =
            new ImmutableMap.Builder<EntitySettingSpecs, EntitySettingSpecs>()
                    .put(ActivateActionWorkflow, EntitySettingSpecs.Activate)
                    .put(MoveActionWorkflow, EntitySettingSpecs.Move)
                    .put(ProvisionActionWorkflow, EntitySettingSpecs.Provision)
                    .put(ResizeActionWorkflow, EntitySettingSpecs.Resize)
                    .put(SuspendActionWorkflow, EntitySettingSpecs.Suspend)
                    .put(DeleteActionWorkflow, EntitySettingSpecs.Delete)
                    .build();

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
        Optional<ActionMode> workflowMode = calculateWorkflowActionMode(action, entitiesCache);

        if (workflowMode.isPresent()) {
            return ModeAndSchedule.of(workflowMode.get());
        } else {
            ModeAndSchedule supportingLevelActionModeAndSchedule =
                calculateActionModeFromSupportingLevel(action, entitiesCache);
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
     * @param entitiesCache a nullable entitis and settings snapshot
     * @return the action mode and execution schedule
     */
    @Nonnull
    private ModeAndSchedule calculateActionModeFromSupportingLevel(
            @Nonnull final ActionView action,
            @Nullable final EntitiesAndSettingsSnapshot entitiesCache) {
        switch (action.getRecommendation().getSupportingLevel()) {
            case UNSUPPORTED:
            case UNKNOWN:
                return ModeAndSchedule.of(ActionMode.DISABLED);
            case SHOW_ONLY:
                final ActionMode mode = getNonWorkflowActionMode(
                    action, entitiesCache).getMode();
                return ModeAndSchedule.of((mode.getNumber() > ActionMode.RECOMMEND_VALUE)
                    ? ActionMode.RECOMMEND : mode);
            case SUPPORTED:
                return getNonWorkflowActionMode(action, entitiesCache);
            default:
                throw new IllegalArgumentException("Action SupportLevel is of unrecognized type.");
        }
    }

    @Nonnull
    private ModeAndSchedule getNonWorkflowActionMode(@Nonnull final ActionView action,
              @Nullable final EntitiesAndSettingsSnapshot entitiesCache) {
        Optional<ActionDTO.Action> translatedRecommendation = action.getActionTranslation()
                .getTranslatedRecommendation();
        if (translatedRecommendation.isPresent()) {
            ActionDTO.Action actionDto = translatedRecommendation.get();
            try {
                final long targetEntityId = ActionDTOUtil.getPrimaryEntityId(actionDto);

                final Map<String, Setting> settingsForTargetEntity = entitiesCache == null ?
                        Collections.emptyMap() : entitiesCache.getSettingsForEntity(targetEntityId);

                return specsApplicableToAction(actionDto, settingsForTargetEntity)
                    .map(spec -> {
                        final Setting setting = settingsForTargetEntity.get(spec.getSettingName());
                        if (spec == EntitySettingSpecs.EnforceNonDisruptive) {
                            // Default is to return most liberal setting because calculateActionMode picks
                            // the minimum ultimately.
                            ActionMode mode = ActionMode.AUTOMATIC;
                            if (setting != null && setting.hasBooleanSettingValue()
                                && setting.getBooleanSettingValue().getValue()) {
                                Optional<ActionPartialEntity> entity = entitiesCache.getEntityFromOid(targetEntityId);
                                if (entity.isPresent()) {
                                    mode = applyNonDisruptiveSetting(entity.get(), action.getRecommendation());
                                } else {
                                    logger.error("Entity with id {} not found for non-disruptive setting.",
                                        targetEntityId);
                                }
                            }
                            return Pair.of(mode, Collections.<Long>emptyList());
                        } else {
                            if (setting == null) {
                                // If there is no setting for this spec that applies to the target
                                // of the action, we use the system default (which comes from the
                                // enum definitions).
                                return Pair.of(ActionMode.valueOf(spec.getSettingSpec().getEnumSettingValueType().getDefault()),
                                    Collections.<Long>emptyList());
                            } else {
                                List<Long> scheduleIds = Collections.emptyList();
                                final String scheduleSettingSpecName =
                                    getExecutionWindowSettingSpec(spec);

                                if (scheduleSettingSpecName != null
                                    && settingsForTargetEntity.containsKey(scheduleSettingSpecName)) {
                                    scheduleIds = settingsForTargetEntity.get(scheduleSettingSpecName)
                                            .getSortedSetOfOidSettingValue().getOidsList();
                                }

                                // In all other cases, we use the default value from the setting.
                                return Pair.of(ActionMode.valueOf(setting.getEnumSettingValue().getValue()),
                                    scheduleIds);
                            }
                        }
                    })
                    // We're not using a proper tiebreaker because we're comparing across setting specs.
                    .min(Comparator.comparing(Pair::getLeft))
                    // select the schedule from the list of schedules
                    .map(p -> getModeAndSchedule(action, p.getLeft(), p.getRight(), entitiesCache))
                    .orElse(ModeAndSchedule.of(ActionMode.RECOMMEND));

            } catch (UnsupportedActionException e) {
                logger.error("Unable to calculate action mode.", e);
                return ModeAndSchedule.of(ActionMode.RECOMMEND);
            }
        } else {
            logger.error("Action {} has no translated recommendation.", action.getId());
            return ModeAndSchedule.of(ActionMode.RECOMMEND);
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
                logger.debug("Setting the action mode for action `{}` to `RECOMMEND` as there is "
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

    @Nullable
    private String getExecutionWindowSettingSpec(@Nonnull EntitySettingSpecs spec) {
        return ActionSettingSpecs.getExecutionScheduleSettingFromActionModeSetting(
            spec.getSettingName());
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

            // Check applicable commodities.
            if (ActionDTOUtil.NON_DISRUPTIVE_SETTING_COMMODITIES.contains(commType)
                    && resizeAction.getCommodityAttribute() == CommodityAttribute.CAPACITY) {
                Optional<CommoditySoldDTO> commoditySold = entity.getCommoditySoldList().stream()
                    .filter(commSold -> commType == commSold.getCommodityType().getType())
                    .findFirst();
                boolean supportsHotReplace = false;
                if (commoditySold.isPresent()) {
                    CommoditySoldDTO commSold = commoditySold.get();
                    supportsHotReplace = commSold.getHotResizeInfo().getHotReplaceSupported();
                }

                // Check hot replace setting enabled.
                if (supportsHotReplace) {
                    // Return Automatic for resize up.
                    if (resizeAction.getNewCapacity() >= resizeAction.getOldCapacity()) {
                        return ActionMode.AUTOMATIC;
                    }
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
     * return the ActionMode of the policy for the related action, e.g. ProvisionAction.
     *
     * If this is not a Workflow Action, then return Optional.empty()
     *
     * @param action The action to analyze to see if it is a Workflow Action
     * @param entitySettingsCache the EntitySettings lookaside for the given action
     * @return an Optional containing the ActionMode if this is a Workflow Action, or
     * Optional.empty() if this is not a Workflow Action or the type of the ActionDTO is not
     * supported.
     */
    @Nonnull
    private Optional<ActionMode> calculateWorkflowActionMode(
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

            // Are there ever workflow overrides defined for this action?
            final ActionType actionType = ActionDTOUtil.getActionInfoActionType(actionDTO);
            final EntitySettingSpecs workflowOverride = WORKFLOW_ACTION_TYPE_MAP.get(actionType);
            if (workflowOverride == null) {
                return Optional.empty();
            }
            // Is there a setting for this Workflow override for the current entity?
            // note: the value of the workflowSettingSpec is the OID of the workflow, only used during
            // execution
            Setting workflowSettingSpec = settingsForActionTarget.get(workflowOverride.getSettingName());
            if (workflowSettingSpec == null ||
                    StringUtils.isEmpty(workflowSettingSpec.getStringSettingValue().getValue())) {
                return Optional.empty();
            }

            // look up the value of the base action spec, i.e. provisionWorkflow -> provision
            EntitySettingSpecs baseSettingSpec = WORKFLOW_ACTION_BASE_MAP.get(workflowOverride);
            Setting baseSetting = settingsForActionTarget.get(baseSettingSpec.getSettingName());
            if (baseSetting == null) {
                return Optional.empty();
            }

            // extract the setting value as a string and return it
            final String actionModeString = baseSetting.getEnumSettingValue().getValue();
            return Optional.of(ActionMode.valueOf(actionModeString));
        } catch (UnsupportedActionException e) {
            logger.error("Unable to calculate complex action mode.", e);
            return Optional.empty();
        }
    }

    /**
     * For an action which corresponds to a Workflow Action, e.g. ProvisionActionWorkflow,
     * return the ActionMode of the policy for the related action, e.g. ProvisionAction.
     *
     * If this is not a Workflow Action, then return an empty map.
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
            final ActionType actionType = ActionDTOUtil.getActionInfoActionType(recommendation);
            // get a map of all the settings (settingName  -> setting) specific to this entity
            final Map<String, Setting> settingsForActionTarget =
                snapshot.getSettingsForEntity(actionTargetEntityId);

            // Use a "set-once" so that we can avoid the overhead of constructing a new HashMap
            // if there are no workflow settings for the action.
            final SetOnce<Map<ActionState, Setting>> retMap = new SetOnce<>();
            Stream.of(ActionState.PRE_IN_PROGRESS, ActionState.IN_PROGRESS, ActionState.POST_IN_PROGRESS)
                .forEach(state -> {
                    // Determine which override use based on the state.
                    final EntitySettingSpecs workflowOverride;
                    switch (state) {
                        case PRE_IN_PROGRESS:
                            workflowOverride = PREP_WORKFLOW_ACTION_TYPE_MAP.get(actionType);
                            break;
                        case IN_PROGRESS:
                            workflowOverride = WORKFLOW_ACTION_TYPE_MAP.get(actionType);
                            break;
                        case POST_IN_PROGRESS:
                            // POST runs after success or failure
                            workflowOverride = POST_WORKFLOW_ACTION_TYPE_MAP.get(actionType);
                            break;
                        default:
                            logger.warn("Tried to retrieve workflow setting in an unexpected action "
                                + "state {}", state);
                            return;
                    }

                    // Is there a corresponding setting for this Workflow override for the current entity?
                    // Note: the value of the workflowSettingSpec is the OID of the workflow, only used during
                    // execution.
                    Optional.ofNullable(workflowOverride)
                        .map(EntitySettingSpecs::getSettingName)
                        .map(settingsForActionTarget::get)
                        .ifPresent(setting -> {
                            // Initialize the return map if necessary.
                            retMap.ensureSet(HashMap::new).put(state, setting);
                        });
                });
            return retMap.getValue().orElse(Collections.emptyMap());
        } catch (UnsupportedActionException e) {
            logger.error("Unable to calculate complex action mode.", e);
            return Collections.emptyMap();
        }
    }

    /**
     * Get the setting specs applicable to an action. The applicable setting specs are derived
     * from the type of the action and the entities it involves.
     *
     * @param action The protobuf representation of the action.
     * @param settingsForTargetEntity The settings for the target entity
     * @return The stream of applicable {@link EntitySettingSpecs}. This will be a stream of
     *         size one in most cases.
     */
    @VisibleForTesting
    @Nonnull
    Stream<EntitySettingSpecs> specsApplicableToAction(
            @Nonnull final ActionDTO.Action action, Map<String, Setting> settingsForTargetEntity) {
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
                                                        EntitySettingSpecs.Move))
                        .distinct();
            case SCALE:
                // For now we use Move policy for SCALE actions
                return Stream.of(EntitySettingSpecs.Move);
            case ALLOCATE:
                // Allocate actions are not executable and are not configurable by the user
                return Stream.empty();
            case RECONFIGURE:
                return Stream.of(EntitySettingSpecs.Reconfigure);
            case PROVISION:
                return Stream.of(EntitySettingSpecs.Provision);
            case RESIZE:
                final Resize resize = action.getInfo().getResize();
                if (isApplicationComponentHeapCommodity(resize)) {
                    EntitySettingSpecs spec = resize.getNewCapacity() > resize.getOldCapacity()
                            ? EntitySettingSpecs.ResizeUpHeap : EntitySettingSpecs.ResizeDownHeap;
                    return Stream.of(spec);
                } else if (isDatabaseServerDBMemCommodity(resize)) {
                    EntitySettingSpecs spec = resize.getNewCapacity() > resize.getOldCapacity()
                            ? EntitySettingSpecs.ResizeUpDBMem : EntitySettingSpecs.ResizeDownDBMem;
                    return Stream.of(spec);
                }
                Optional<EntitySettingSpecs> rangeAwareSpec = rangeAwareSpecCalculator
                        .getSpecForRangeAwareCommResize(resize, settingsForTargetEntity);
                // Return the range aware spec if present. Otherwise return the regular resize
                // spec, or if it's a vm the default empty stream, since the only resize action
                // that should fall into this logic is for vStorages. Resize Vms for vStorage
                // commodities should always translate to a RECOMMENDED mode .
                if (isVirtualMachine(resize) && !rangeAwareSpec.isPresent()) {
                    return Stream.empty();
                }
                return Stream.of(rangeAwareSpec.orElse(EntitySettingSpecs.Resize),
                                EntitySettingSpecs.EnforceNonDisruptive);
            case ACTIVATE:
                return Stream.of(EntitySettingSpecs.Activate);
            case DEACTIVATE:
                return Stream.of(EntitySettingSpecs.Suspend);
            case DELETE:
                return Stream.of(EntitySettingSpecs.Delete);
            case ACTIONTYPE_NOT_SET:
                return Stream.empty();
        }
        return Stream.empty();
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
        EntitySettingSpecs aboveMaxThreshold();

        /**
         * Gets the below min automation level.
         *
         * @return The below min automation level
         */
        EntitySettingSpecs belowMinThreshold();

        /**
         * Gets the in-range automation level for resize up.
         *
         * @return The in-range automation level for the resize up
         */
        EntitySettingSpecs upInBetweenThresholds();

        /**
         * Gets the in-range automation level for the resize down.
         *
         * @return The in-range automation level for the resize down
         */
        EntitySettingSpecs downInBetweenThresholds();

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
    private boolean isVirtualMachine(Resize resize) {
        return resize.getTarget().getType() == EntityType.VIRTUAL_MACHINE_VALUE;
    }

    /**
     * Checks if the Resize action has an EntityType, it's an Application Component and Heap commodity.
     * @param resize the {@link Resize} action
     * @return Returns {@code true} if the action has Application Component as a target and Heap commodity
     */
    private boolean isApplicationComponentHeapCommodity(Resize resize) {
        return resize.getTarget().getType() == EntityType.APPLICATION_COMPONENT_VALUE &&
                resize.getCommodityType().getType() == CommodityType.HEAP.getNumber();
    }

    /**
     * Checks if the Resize action has an EntityType, it's an Database Server and DBMem commodity.
     * @param resize The {@link Resize} action
     * @return Returns {@code true} if the action has Database Server as a target and DBMem commodity
     * */
    private boolean isDatabaseServerDBMemCommodity(Resize resize) {
        return resize.getTarget().getType() == EntityType.DATABASE_SERVER_VALUE &&
                resize.getCommodityType().getType() == CommodityType.DB_MEM.getNumber();
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
                ImmutableMap.of(EntityType.VIRTUAL_MACHINE_VALUE, populateResizeSettingsByCommodityForVM());
        /**
         * Gets the spec applicable for range aware commodity resize. Currently VMem, VCpu
         * resizes of on-prem VMs are considered range aware.
         *
         * There is a minThreshold and a maxThreshold defined for the commodity resizing.
         * There are separate automation modes defined for these cases:
         * 1. The new capacity is greater than the maxThreshold
         * 2. The new capacity is lesser than the minThreshold
         * 3. The new capacity is greater than or equal to minThreshold and less than or equal
         *    to maxThreshold, and it is sizing up
         * 4. The new capacity is greater than or equal to minThreshold and less than or equal
         *    to maxThreshold, and it is sizing down
         *
         * This method will determine if any of these settings apply to the resize action.
         *
         * @param resize the resize action
         * @param settingsForTargetEntity A map of the setting name and the setting for this entity
         * @return Optional of the applicable spec. If nothing applies, Optional.empty.
         */
        @Nonnull
        private Optional<EntitySettingSpecs> getSpecForRangeAwareCommResize(
                Resize resize, Map<String, Setting> settingsForTargetEntity) {
            Integer entityType = resize.getTarget().getType();
            Integer commType = resize.getCommodityType().getType();
            CommodityAttribute changedAttribute = resize.getCommodityAttribute();
            // Get the resizeSettingsByCommodity for this entity type
            Map<Integer, RangeAwareResizeSettings> resizeSettingsByCommodity = resizeSettingsByEntityType.get(entityType);
            Optional<EntitySettingSpecs> applicableSpec = Optional.empty();
            // Range aware settings should only apply if the changed attribute is capacity and if
            // it applies to this entity and commodity
            if (changedAttribute == CommodityAttribute.CAPACITY
                    && resizeSettingsByCommodity != null) {
                RangeAwareResizeSettings resizeSettings = resizeSettingsByCommodity.get(commType);
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
                            logger.error("{}  has a resize action on commodity {}  with same " +
                                            "old and new capacity -> {}", resize.getTarget().getId(), commType,
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
                applicableSpec = Optional.ofNullable(VMS_ACTION_MODE_SETTINGS.get(changedAttribute) != null ?
                    VMS_ACTION_MODE_SETTINGS.get(changedAttribute).get(commType) : null );
            }
            logger.debug("Range aware spec for resizing {} of commodity {} of entity {} is {} ",
                    changedAttribute, commType, resize.getTarget().getId(),
                    applicableSpec.map(EntitySettingSpecs::getSettingName).orElse("empty"));
            return applicableSpec;
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
            if (resize.getCommodityType().getType() == CommodityDTO.CommodityType.VMEM_VALUE) {
                oldCapacityForMode /= Units.NUM_OF_KB_IN_MB;
                newCapacityForMode /= Units.NUM_OF_KB_IN_MB;
            }
            return ImmutableResizeCapacity.builder()
                    .newCapacity(newCapacityForMode)
                    .oldCapacity(oldCapacityForMode).build();
        }

        /**
         * Returns a map of the commodity type to the range aware resize settings applicable to it.
         * @return
         */
        private Map<Integer, RangeAwareResizeSettings> populateResizeSettingsByCommodityForVM() {
            RangeAwareResizeSettings vCpuSettings = ImmutableRangeAwareResizeSettings.builder()
                    .aboveMaxThreshold(EntitySettingSpecs.ResizeVcpuAboveMaxThreshold)
                    .belowMinThreshold(EntitySettingSpecs.ResizeVcpuBelowMinThreshold)
                    .upInBetweenThresholds(ResizeVcpuUpInBetweenThresholds)
                    .downInBetweenThresholds(EntitySettingSpecs.ResizeVcpuDownInBetweenThresholds)
                    .maxThreshold(EntitySettingSpecs.ResizeVcpuMaxThreshold)
                    .minThreshold(EntitySettingSpecs.ResizeVcpuMinThreshold).build();
            RangeAwareResizeSettings vMemSettings = ImmutableRangeAwareResizeSettings.builder()
                    .aboveMaxThreshold(EntitySettingSpecs.ResizeVmemAboveMaxThreshold)
                    .belowMinThreshold(EntitySettingSpecs.ResizeVmemBelowMinThreshold)
                    .upInBetweenThresholds(ResizeVmemUpInBetweenThresholds)
                    .downInBetweenThresholds(ResizeVmemDownInBetweenThresholds)
                    .maxThreshold(EntitySettingSpecs.ResizeVmemMaxThreshold)
                    .minThreshold(EntitySettingSpecs.ResizeVmemMinThreshold).build();
            return ImmutableMap.of(CommodityDTO.CommodityType.VCPU_VALUE, vCpuSettings,
                CommodityDTO.CommodityType.VMEM_VALUE, vMemSettings);
        }
    }
}
