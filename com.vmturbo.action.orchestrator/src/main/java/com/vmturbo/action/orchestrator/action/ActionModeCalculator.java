package com.vmturbo.action.orchestrator.action;

import static com.vmturbo.components.common.setting.EntitySettingSpecs.ProvisionActionWorkflow;
import static com.vmturbo.components.common.setting.EntitySettingSpecs.ResizeActionWorkflow;
import static com.vmturbo.components.common.setting.EntitySettingSpecs.SuspendActionWorkflow;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.action.orchestrator.store.EntitySettingsCache;
import com.vmturbo.common.protobuf.ActionDTOUtil;
import com.vmturbo.common.protobuf.TopologyDTOUtil;
import com.vmturbo.common.protobuf.UnsupportedActionException;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.setting.SettingProto;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * A utility class to capture the logic to calculate an {@link ActionMode} for an action,
 * given some user settings.
 */
public class ActionModeCalculator {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Map from an actionType -> corresponding Workflow Action EntitySettingsSpec if
     * the actionType may be overridden. Used to calculate the action mode for Workflow
     * policy applications, if any.
     * TODO: We should define a dynamic process for Orchestration probe types to add dynamically
     * to the supported workflow types.
     */
    private static final Map<ActionTypeCase, EntitySettingSpecs> WORKFLOW_ACTION_TYPE_MAP =
            new ImmutableMap.Builder<ActionTypeCase, EntitySettingSpecs>()
                    .put(ActionTypeCase.PROVISION, ProvisionActionWorkflow)
                    .put(ActionTypeCase.DEACTIVATE, SuspendActionWorkflow)
                    .put(ActionTypeCase.RESIZE, ResizeActionWorkflow)
                    .build();

    /**
     * Map from an {@link EntitySettingSpecs} for each Workflow to the corresponding "base"
     * EntitySettingSpecs to fetch the {@link ActionMode} for the policy. In other words, the
     * ActionMode (DISABLED, RECOMMEND, MANUAL, AUTOMATIC) for ProvisionActionWorkflow is
     * taken from the Provision setting.
     */
    private static final Map<EntitySettingSpecs, EntitySettingSpecs> WORKFLOW_ACTION_BASE_MAP =
            new ImmutableMap.Builder<EntitySettingSpecs, EntitySettingSpecs>()
                    .put(ProvisionActionWorkflow, EntitySettingSpecs.Provision)
                    .put(SuspendActionWorkflow, EntitySettingSpecs.Suspend)
                    .put(ResizeActionWorkflow, EntitySettingSpecs.Resize)
                    .build();

    /**
     * Get the action mode for a particular action. The action mode is determined by the
     * settings for the action, or, if the settings are not available, by the system defaults of
     * the relevant automation settings.
     *
     * @param actionDto The protobuf representation of the action to calculate action mode for.
     * @param entitySettingsCache The {@link EntitySettingsCache} to retrieve settings for. May
     *                            be null.
     *                            TODO (roman, Aug 7 2018): Can we make this non-null? The cache
     *                            should exist as a spring object and be injected appropriately.
     * @return The {@link ActionMode} to use for the action.
     */
    @Nonnull
    public static ActionMode calculateActionMode(@Nonnull final ActionDTO.Action actionDto,
                @Nullable final EntitySettingsCache entitySettingsCache) {
        try {
            final long targetEntityId = ActionDTOUtil.getPrimaryEntityId(actionDto);

            final Map<String, Setting> settingsForTargetEntity = entitySettingsCache == null ?
                    Collections.emptyMap() : entitySettingsCache.getSettingsForEntity(targetEntityId);

            return specsApplicableToAction(actionDto)
                    .map(spec -> {
                        final Setting setting = settingsForTargetEntity.get(spec.getSettingName());
                        if (setting == null) {
                            // If there is no setting for this spec that applies to the target
                            // of the action, we use the system default (which comes from the
                            // enum definitions).
                            return spec.getSettingSpec().getEnumSettingValueType().getDefault();
                        } else {
                            // In all other cases, we use the default value from the setting.
                            return setting.getEnumSettingValue().getValue();
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
    }


    /**
     * For an action which corresponds to a Workflow Action, e.g. ProvisionActionWorkflow,
     * return the ActionMode of the policy for the related action, e.g. ProvisionAction.
     *
     * If this is not a Workflow Action, then return Optional.empty()
     *
     * @param actionDTO The action to analyze to see if it is a Workflow Action
     * @param entitySettingsCache the EntitySettings lookaside for the given action
     * @return an Optional containing the ActionMode if this is a Workflow Action, or
     * Optional.empty() if this is not a Workflow Action or the type of the ActionDTO is not
     * supported.
     */
    @Nonnull
    public static Optional<ActionMode> calculateWorkflowActionMode(
            @Nonnull final ActionDTO.Action actionDTO,
            @Nullable final EntitySettingsCache entitySettingsCache) {
        try {
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
            final ActionTypeCase actionTypeCase = actionDTO.getInfo().getActionTypeCase();
            final EntitySettingSpecs workflowOverride = WORKFLOW_ACTION_TYPE_MAP.get(actionTypeCase);
            if (workflowOverride == null) {
                return Optional.empty();
            }
            // Is there a setting for this Workflow override for the current entity?
            // note: the value of the workflowSettingSpec is the OID of the workflow, only used during
            // execution
            Setting workflowSettingSpec = settingsForActionTarget.get(workflowOverride.getSettingName());
            if (workflowSettingSpec == null) {
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
     * If this is not a Workflow Action, then return Optional.empty()
     *
     * @param actionDTO The action to analyze to see if it is a Workflow Action
     * @param entitySettingsCache the EntitySettings lookaside for the given action
     * @return an Optional containing the ActionMode if this is a Workflow Action, or
     * Optional.empty() if this is not a Workflow Action or the type of the ActionDTO is not
     * supported.
     */
    @Nonnull
    public static Optional<SettingProto.Setting> calculateWorkflowSetting(
            @Nonnull final ActionDTO.Action actionDTO,
            @Nullable final EntitySettingsCache entitySettingsCache) {
        try {
            Objects.requireNonNull(actionDTO);
            if (Objects.isNull(entitySettingsCache)) {
                return Optional.empty();
            }

            // find the entity which is the target of this action
            final long actionTargetEntityId = ActionDTOUtil.getPrimaryEntityId(actionDTO);

            // get a map of all the settings (settingName  -> setting) specific to this entity
            final Map<String, Setting> settingsForActionTarget = entitySettingsCache
                    .getSettingsForEntity(actionTargetEntityId);

            // Are there any workflow override settings allowed for this action type?
            EntitySettingSpecs workflowOverride = WORKFLOW_ACTION_TYPE_MAP.get(
                    actionDTO.getInfo().getActionTypeCase());
            if (workflowOverride == null) {
                // workflow overrides are not allowed
                return Optional.empty();
            }
            // Is there a corresponding setting for this Workflow override for the current entity?
            // note: the value of the workflowSettingSpec is the OID of the workflow, only used during
            // execution
            return Optional.ofNullable(settingsForActionTarget.get(
                    workflowOverride.getSettingName()));
        } catch (UnsupportedActionException e) {
            logger.error("Unable to calculate complex action mode.", e);
            return Optional.empty();
        }
    }

    /**
     * Get the setting specs applicable to an action. The applicable setting specs are derived
     * from the type of the action and the entities it involves.
     *
     * @param action The protobuf representation of the action.
     * @return The stream of applicable {@link EntitySettingSpecs}. This will be a stream of
     *         size one in most cases.
     */
    @Nonnull
    private static Stream<EntitySettingSpecs> specsApplicableToAction(
            @Nonnull final ActionDTO.Action action) {
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
                return Stream.of(EntitySettingSpecs.Resize);
            case ACTIVATE:
                return Stream.of(EntitySettingSpecs.Activate);
            case DEACTIVATE:
                return Stream.of(EntitySettingSpecs.Suspend);
            case ACTIONTYPE_NOT_SET:
                return Stream.empty();
        }
        return Stream.empty();
    }
}