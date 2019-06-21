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
import static com.vmturbo.components.common.setting.EntitySettingSpecs.SuspendActionWorkflow;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.setting.SettingProto;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.commons.Units;
import com.vmturbo.components.api.SetOnce;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * A utility class to capture the logic to calculate an {@link ActionMode} for an action,
 * given some user settings.
 */
public class ActionModeCalculator {

    private static final Logger logger = LogManager.getLogger();

    private final ActionTranslator actionTranslator;

    private final RangeAwareSpecCalculator rangeAwareSpecCalculator;

    public ActionModeCalculator(@Nonnull ActionTranslator actionTranslator) {
        this.actionTranslator = actionTranslator;
        this.rangeAwareSpecCalculator = new RangeAwareSpecCalculator();
    }

    // This is present in case we want to test just ActionModeCalculator without
    // rangeAwareSpecCalculator. In that case, it cane be mocked if needed.
    @VisibleForTesting
    ActionModeCalculator(@Nonnull ActionTranslator actionTranslator,
                                @Nonnull RangeAwareSpecCalculator rangeAwareSpecCalculator) {
        this.actionTranslator = actionTranslator;
        this.rangeAwareSpecCalculator = rangeAwareSpecCalculator;
    }

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
     * Get the action mode for a particular action. The action mode is determined by the
     * settings for the action, or, if the settings are not available, by the system defaults of
     * the relevant automation settings.
     *
     * @param action The action to calculate action mode for.
     * @param entitiesCache The {@link EntitiesAndSettingsSnapshotFactory} to retrieve settings for. May
     *                            be null.
     *                            TODO (roman, Aug 7 2018): Can we make this non-null? The cache
     *                            should exist as a spring object and be injected appropriately.
     * @return The {@link ActionMode} to use for the action.
     */
    @Nonnull
    public ActionMode calculateActionMode(@Nonnull final ActionView action,
                @Nullable final EntitiesAndSettingsSnapshot entitiesCache) {
        return calculateWorkflowActionMode(action, entitiesCache)
            .orElseGet(() -> {
                switch (action.getRecommendation().getSupportingLevel()) {
                    case UNSUPPORTED:
                    case UNKNOWN:
                        return ActionMode.DISABLED;
                    case SHOW_ONLY:
                        final ActionMode mode = getNonWorkflowActionMode(
                            action, entitiesCache);
                        return (mode.getNumber() > ActionMode.RECOMMEND_VALUE)
                            ? ActionMode.RECOMMEND
                            : mode;
                    case SUPPORTED:
                        return getNonWorkflowActionMode(
                            action, entitiesCache);
                    default:
                        throw new IllegalArgumentException("Action SupportLevel is of unrecognized type.");
                }
            });
    }

    @Nonnull
    private ActionMode getNonWorkflowActionMode(@Nonnull final ActionView action,
                @Nullable final EntitiesAndSettingsSnapshot entitiesCache) {
        final boolean translationSuccess = actionTranslator.translate(action);
        if (!translationSuccess){
            return ActionMode.RECOMMEND;
        }
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
                                return mode.name();
                            } else {
                                if (setting == null) {
                                    // If there is no setting for this spec that applies to the target
                                    // of the action, we use the system default (which comes from the
                                    // enum definitions).
                                    return spec.getSettingSpec().getEnumSettingValueType().getDefault();
                                } else {
                                    // In all other cases, we use the default value from the setting.
                                    return setting.getEnumSettingValue().getValue();
                                }
                            }
                        })
                        .map(ActionMode::valueOf)
                        // We're not using a proper tiebreaker because we're comparing across setting specs.
                        .min(ActionMode::compareTo)
                        .orElse(ActionMode.RECOMMEND);
            } catch (UnsupportedActionException e) {
                logger.error("Unable to calculate action mode.", e);
                return ActionMode.RECOMMEND;
            }
        } else {
            logger.error("Action {} has no translated recommendation despite successful translation.", action.getId());
            return ActionMode.RECOMMEND;
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
    @Nonnull
    private Stream<EntitySettingSpecs> specsApplicableToAction(
            @Nonnull final ActionDTO.Action action, Map<String, Setting> settingsForTargetEntity) {
        final ActionTypeCase type = action.getInfo().getActionTypeCase();
        switch (type) {
            case MOVE:
                // This may result in one applicable entity spec (for a host move or storage move),
                // or two applicable entity spec for cases where the host move has to be
                // accompanied by a storage move.
                return action.getInfo().getMove().getChangesList().stream()
                        .map(provider -> provider.getDestination().getType())
                        .map(destinationEntityType -> {
                            if (TopologyDTOUtil.isStorageEntityType(destinationEntityType)) {
                                return EntitySettingSpecs.StorageMove;
                            } else {
                                // TODO (roman, Aug 6 2018): Should we check explicitly for
                                // physical machine, or are there other valid non-storage destination
                                // types for move actions?
                                return EntitySettingSpecs.Move;
                            }
                        })
                        .distinct();
            case RECONFIGURE:
                return Stream.of(EntitySettingSpecs.Reconfigure);
            case PROVISION:
                return Stream.of(EntitySettingSpecs.Provision);
            case RESIZE:
                Optional<EntitySettingSpecs> rangeAwareSpec = rangeAwareSpecCalculator
                        .getSpecForRangeAwareCommResize(action.getInfo().getResize(), settingsForTargetEntity);
                // Return the range aware spec if present. Otherwise return the regular resize spec.
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

    @Value.Immutable
    public interface RangeAwareResizeSettings {
        EntitySettingSpecs aboveMaxThreshold();
        EntitySettingSpecs belowMinThrewshold();
        EntitySettingSpecs upInBetweenThresholds();
        EntitySettingSpecs downInBetweenThresholds();
        EntitySettingSpecs minThreshold();
        EntitySettingSpecs maxThreshold();
    }

    @Value.Immutable
    interface ResizeCapacity {
        float oldCapacity();
        float newCapacity();
    }

    /**
     * This class is used to find the spec that applies for a Resize action on a commodity which is
     * range aware.
     * For ex. vmem / vcpu resize of an on-prem VM.
     */
    private class RangeAwareSpecCalculator {
        // This map holds the resizeSettings by commodity type per entity type
        private final Map<Integer, Map<Integer, RangeAwareResizeSettings>> resizeSettingsByEntityType =
                ImmutableMap.of(EntityType.VIRTUAL_MACHINE_VALUE, populateResizeSettingsByCommodityForVM());
        /**
         * Gets the spec applicable for range aware commodity resize. Currently VMem and VCpu
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
         * @throws Exception
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
                        // The new capacity is greater than the maxThreshold
                        if (newCapacity > maxThreshold) {
                            applicableSpec = Optional.of(resizeSettings.aboveMaxThreshold());
                        } else if (newCapacity < minThreshold) {
                            // The new capacity is lesser than the minThreshold
                            applicableSpec = Optional.of(resizeSettings.belowMinThrewshold());
                        } else {
                            if (newCapacity > oldCapacity) {
                                applicableSpec = Optional.of(resizeSettings.upInBetweenThresholds());
                            } else if (newCapacity < oldCapacity) {
                                applicableSpec = Optional.of(resizeSettings.downInBetweenThresholds());
                            } else {
                                // new capacity == old capacity
                                logger.error("{}  has a resize action on commodity {}  with same " +
                                                "old and new capacity -> {}", resize.getTarget().getId(), commType,
                                        resize.getNewCapacity());
                            }
                        }
                    }
                }
            }
            logger.debug("Range aware spec for resizing {} of commodity {} of entity {} is {} ",
                    changedAttribute, commType, resize.getTarget().getId(),
                    applicableSpec.map(spec -> spec.getSettingName()).orElse("empty"));
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
                    .belowMinThrewshold(EntitySettingSpecs.ResizeVcpuBelowMinThreshold)
                    .upInBetweenThresholds(EntitySettingSpecs.ResizeVcpuUpInBetweenThresholds)
                    .downInBetweenThresholds(EntitySettingSpecs.ResizeVcpuDownInBetweenThresholds)
                    .maxThreshold(EntitySettingSpecs.ResizeVcpuMaxThreshold)
                    .minThreshold(EntitySettingSpecs.ResizeVcpuMinThreshold).build();
            RangeAwareResizeSettings vMemSettings = ImmutableRangeAwareResizeSettings.builder()
                    .aboveMaxThreshold(EntitySettingSpecs.ResizeVmemAboveMaxThreshold)
                    .belowMinThrewshold(EntitySettingSpecs.ResizeVmemBelowMinThreshold)
                    .upInBetweenThresholds(EntitySettingSpecs.ResizeVmemUpInBetweenThresholds)
                    .downInBetweenThresholds(EntitySettingSpecs.ResizeVmemDownInBetweenThresholds)
                    .maxThreshold(EntitySettingSpecs.ResizeVmemMaxThreshold)
                    .minThreshold(EntitySettingSpecs.ResizeVmemMinThreshold).build();
            return ImmutableMap.of(CommodityDTO.CommodityType.VCPU_VALUE, vCpuSettings,
                    CommodityDTO.CommodityType.VMEM_VALUE, vMemSettings);
        }
    }
}
