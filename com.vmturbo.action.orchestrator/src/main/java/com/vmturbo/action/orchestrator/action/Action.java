package com.vmturbo.action.orchestrator.action;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.util.StringUtils;

import com.vmturbo.action.orchestrator.action.ActionEvent.AcceptanceEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.AcceptanceRemovalEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.AuthorizedActionEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.BeginExecutionEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.ClearingEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.FailureEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.ProgressEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.QueuedEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.RejectionEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.RejectionRemovalEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.RollBackToAcceptedEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.SuccessEvent;
import com.vmturbo.action.orchestrator.action.ActionModeCalculator.ActionSpecifications;
import com.vmturbo.action.orchestrator.action.ActionTranslation.TranslationStatus;
import com.vmturbo.action.orchestrator.state.machine.StateMachine;
import com.vmturbo.action.orchestrator.state.machine.Transition.TransitionResult;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStore;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStoreException;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision.ExecutionDecision;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.BuyRI;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutionStep;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutionStep.Status;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.setting.SettingProto;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.EntityWithConnections;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.Workflow;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowInfo;
import com.vmturbo.components.common.setting.ActionSettingSpecs;
import com.vmturbo.components.common.setting.ActionSettingType;
import com.vmturbo.components.common.setting.ConfigurableActionSettings;
import com.vmturbo.proactivesupport.DataMetricGauge;
import com.vmturbo.proactivesupport.DataMetricSummary;

/**
 * The representation of an action in the system.
 *
 * Actions permit mutation through event reception via the {@link #receive(ActionEvent)} method
 * which updates their internal state via a {@link StateMachine}.
 */
@ThreadSafe
public class Action implements ActionView {

    private static final Logger logger = LogManager.getLogger();

    /**
     * The recommended steps to take in the environment.
     *
     * Not final - if the same recommendation with different non-identifying properties (e.g.
     * different savings or explanation) comes in from the market we want to be able to capture
     * the updated information.
     */
    @GuardedBy("recommendationLock")
    private ActionDTO.Action recommendation;

    /**
     * The translation for this action.
     *
     * Actions are translated from the market's domain-agnostic action recommendations into
     * domain-specific actions that make sense in the real world. See
     * {@link ActionTranslator} for more details.
     */
    @Nonnull
    @GuardedBy("recommendationLock")
    private ActionTranslation actionTranslation;

    /**
     * A lock used to control re-assignments of the recommendation, and the associated
     * action translation.
     *
     * Because the recommendation is immutable, you don't need to hold the lock when interacting
     * with it if you save it to a local variable. i.e:
     *
     * <code>
     * final ActionDTO.Action curRecommendation;
     * synchronized (recommendationLock) {
     *     curRecommendation = recommendation;
     * }
     * </code>
     */
    private final Object recommendationLock = new Object();

    /**
     * The cached action mode for the action. This is not final, because for "live" actions it
     * can change during the lifetime of the action. We save it because action mode computation
     * is always necessary, rarely changes, and fairly expensive.
     * <p>
     * Plans/BuyRI:
     * In plans, and for Buy RI actions, this action mode is always the same.
     * <p>
     * Live:
     * Every broadcast the Topology Processor updates the per-entity settings in the group
     * component according to the group and setting policies defined by the user. When the
     * Action Orchestrator processes an action plan it gets these per-entity settings, and
     * can then determine the new action mode. The processing code is responsible for calling
     * {@link Action#refreshAction(EntitiesAndSettingsSnapshot)} for all live actions when the per-entity settings
     * are updated.
     * <p>
     */
    private volatile ActionMode actionMode = ActionMode.RECOMMEND;

    /**
     * The cached workflow settings for the actions, broken down by state.
     * Each action state may have a different workflow setting.
     * Not final, because the settings can change during the lifetime of the action.
     * <p>
     * Plans/BuyRI:
     * In plans, and for Buy RI actions, this should always be empty because we don't allow
     * action execution.
     * <p>
     * Live:
     * Updated every broadcast via {@link Action#refreshAction(EntitiesAndSettingsSnapshot)} for all live actions.
     */
    private volatile Map<ActionState, SettingProto.Setting> workflowSettingsForState =
        Collections.emptyMap();

    /**
     * The time at which this action was recommended.
     */
    private final LocalDateTime recommendationTime;

    private final ActionModeCalculator actionModeCalculator;

    /**
     * The decision applied to this action. Initially there is no decision.
     * Once a decision is made, it cannot be overwritten.
     */
    private final Decision decision;

    private ExecutionDecision.Reason acceptanceReason;

    private String executionAuthorizerId;

    private String description = null;

    private Set<String> relatedRisks = Collections.emptySet();

    private Optional<Long> associatedAccountId = Optional.empty();

    private Optional<Long> associatedResourceGroupId = Optional.empty();

    private Collection<Long> associatedSettingsPolicies;

    /**
     * The state of the action. The state of an action transitions due to certain system events.
     * The initial state is determined by the action mode.
     */
    private final StateMachine<ActionState, ActionEvent> stateMachine;

    /**
     * The current executable step for actions that the system decides should be executed.
     * Tracks the progress of an action as it is executed by the appropriate target(s).
     */
    private Optional<ExecutableStep> currentExecutableStep;

    /**
     * Stores previous executable steps for this action that have already been completed.
     */
    private final Map<ActionState, ExecutableStep> executableSteps = Maps.newHashMap();

    /**
     * The ID of the action plan to which this action's recommendation belongs.
     */
    private final long actionPlanId;

    private final long recommendationOid;

    private static final DataMetricGauge IN_PROGRESS_ACTION_COUNTS_GAUGE = DataMetricGauge.builder()
        .withName("ao_in_progress_action_counts")
        .withHelp("Number of actions currently in progress.")
        .withLabelNames("action_type")
        .build()
        .register();

    private static final DataMetricSummary ACTION_DURATION_SUMMARY = DataMetricSummary.builder()
        .withName("ao_action_duration_seconds")
        .withHelp("Duration in seconds it takes actions to execute.")
        .withLabelNames("action_type")
        .build()
        .register();

    /**
     * The category of the action, extracted from the explanation in the {@link ActionDTO.Action}
     * and saved here for efficiency.
     * <p>
     * The category of an action is determined from explanations, which are affected by translation.
     * However, the translation shouldn't affect the category (i.e. running an action through
     * translation shouldn't change the purpose of doing the action), so we should be safe
     * saving the {@link ActionCategory} regardless of the translation status.
     */
    private ActionCategory actionCategory;

    @Nullable
    private ActionSchedule schedule;

    @Nullable
    private String externalActionName;

    @Nullable
    private String externalActionUrl;

    /**
     * Create an action from a state object that was used to serialize the state of the action.
     *
     * @param savedState A state object that was used to serialize the state of the action.
     * @param actionModeCalculator used to calculate the automation mode of the action
     */
    public Action(@Nonnull final SerializationState savedState,
            @Nonnull final ActionModeCalculator actionModeCalculator) {
        this.recommendation = savedState.recommendation;
        this.actionPlanId = savedState.actionPlanId;
        this.currentExecutableStep = Optional.ofNullable(ExecutableStep.fromExecutionStep(savedState.executionStep));
        this.recommendationTime = savedState.recommendationTime;
        this.decision = new Decision();
        if (savedState.actionDecision != null) {
            this.decision.setDecision(savedState.actionDecision);
        }
        this.stateMachine = ActionStateMachine.newInstance(this, savedState.currentState);
        this.actionTranslation = savedState.actionTranslation;
        this.actionCategory = savedState.actionCategory;
        this.actionModeCalculator = actionModeCalculator;
        this.schedule = savedState.schedule;
        if (savedState.getActionDetailData() != null) {
            // TODO: OM-48679 Initially actionDetailData will contain the action description.
            //  It was named this way instead of description since eventually we should store the
            //  the data used to build the action description instead of the description itself.
            this.description = new String(savedState.getActionDetailData());
        }
        if (savedState.getRecommendationOid() != null) {
            this.recommendationOid = savedState.getRecommendationOid();
        } else {
            // The recommendation oid will be null when the action was saved to the database before
            // actions had recommendation oids (7.22.1 and earlier). However, we still need some
            // sort of identifier because this.getRecommendationOid() is used to populate data
            // structures in other objects. The Action.ActionDTO is good enough. It would be
            // expensive to recalculate the oid. We do not need a stable oid for these database
            // actions because they existed before external approval and audit probes like
            // ServiceNOW.
            this.recommendationOid = recommendation.getId();
        }
    }

    public Action(@Nonnull final ActionDTO.Action recommendation,
                  @Nonnull final LocalDateTime recommendationTime,
                  final long actionPlanId,
                  @Nonnull final ActionModeCalculator actionModeCalculator,
                  @Nullable final String description,
                  @Nullable final Long associatedAccountId,
                  @Nullable final Long associatedResourceGroupId,
            long recommendationOid) {
        this.recommendation = recommendation;
        this.actionTranslation = new ActionTranslation(this.recommendation);
        this.actionPlanId = actionPlanId;
        this.recommendationTime = Objects.requireNonNull(recommendationTime);
        this.stateMachine = ActionStateMachine.newInstance(this);
        this.currentExecutableStep = Optional.empty();
        this.decision = new Decision();
        this.actionCategory = ActionCategoryExtractor.assignActionCategory(
                recommendation);
        this.actionModeCalculator = actionModeCalculator;
        this.description = description;
        this.associatedAccountId = Optional.ofNullable(associatedAccountId);
        this.associatedResourceGroupId = Optional.ofNullable(associatedResourceGroupId);
        this.recommendationOid = recommendationOid;
    }

    public Action(@Nonnull final ActionDTO.Action recommendation,
                  final long actionPlanId, @Nonnull final ActionModeCalculator actionModeCalculator,
            long recommendationOid) {
        // This constructor is used by LiveActionStore, so passing null to 'description' argument
        // or the 'associatedAccountId' or to "associatedResourceGroup" is not hurtful since the
        // description will be formed at a later stage during refreshAction.
        this(recommendation, LocalDateTime.now(), actionPlanId, actionModeCalculator, null, null,
                null, recommendationOid);
    }

    /**
     * Deliver an event to the action. This may trigger an update to the action's state.
     *
     * @param event The event the action should receive.
     * @return The state of this action after processing the event.
     */
    public TransitionResult<ActionState> receive(@Nonnull final ActionEvent event) {
        return stateMachine.receive(event);
    }

    /**
     * Update the {@link ActionDTO.Action} generated by the market to recommend this action.
     * This is necessary when the market re-recommends the same action with a different set
     * of non-identifying fields (e.g. the importance changes, or the savings amount).
     * Also updates the action category because the updated action may belong to a different category.
     *
     * @param newRecommendation The new recommendation from the market.
     */
    public void updateRecommendationAndCategory(@Nonnull final ActionDTO.Action newRecommendation) {
        synchronized (recommendationLock) {
            if (recommendation == null) {
                throw new IllegalStateException("Action has no recommendation.");
            }
            this.recommendation = newRecommendation.toBuilder()
                    // It's important to keep the same ID - that's the whole point of
                    // updating the recommendation instead of creating a new action.
                    .setId(this.recommendation.getId())
                    .build();
            this.actionTranslation = new ActionTranslation(this.recommendation);
        }
        this.actionCategory = ActionCategoryExtractor.assignActionCategory(recommendation);
    }

    @Override
    public String toString() {
        synchronized (recommendationLock) {
            return "Action Id=" + getId()
                    + ", Type=" + recommendation.getInfo().getActionTypeCase()
                    + ", Mode=" + getMode()
                    + ", State=" + getState()
                    + ", Recommendation=" + recommendation
                    + ", Schedule=" + schedule;
        }
    }

    /**
     * Get the recommendation associated with this action.
     *
     * @return The recommendation associated with this action.
     */
    @Override
    public ActionDTO.Action getRecommendation() {
        synchronized (recommendationLock) {
            return recommendation;
        }
    }

    /**
     * Get the time at which the action was originally recommended.
     *
     * @return Get the time at which the action was originally recommended.
     */
    @Override
    public LocalDateTime getRecommendationTime() {
        return recommendationTime;
    }

    /**
     * Get the ID of the action plan that originally recommended this action.
     *
     * @return the ID of the action plan that originally recommended this action.
     */
    @Override
    public long getActionPlanId() {
        return actionPlanId;
    }

    /**
     * Get the action's current state as governed by its state machine.
     *
     * @return The action's current state.
     */
    @Override
    public ActionState getState() {
        return stateMachine.getState();
    }

    public boolean isFinished() {
        return stateMachine.isInFinalState();
    }

    /**
     * Check if the action is in the READY state.
     *
     * @return true if in the READY state, false otherwise.
     */
    @Override
    public boolean isReady() {
        return getState() == ActionState.READY;
    }

    /**
     * Returns OID of recommendation for this action.
     *
     * @return recommendation OID
     */
    public long getRecommendationOid() {
        return recommendationOid;
    }

    /**
     * Refreshes the currently saved action mode and sets the action description
     *
     * <p>This should only be called during live action plan processing, after the
     * {@link EntitiesAndSettingsSnapshotFactory} is updated with the new snapshot of the settings
     * and entities needed to resolve action modes and form the action description.
     */
    public void refreshAction(@Nonnull final EntitiesAndSettingsSnapshot entitiesSnapshot)
            throws UnsupportedActionException {
        synchronized (recommendationLock) {
            final ActionModeCalculator.ModeAndSchedule actionModeAndSchedule = actionModeCalculator
                .calculateActionModeAndExecutionSchedule(this, entitiesSnapshot);
            setActionMode(actionModeAndSchedule.getMode());
            schedule = actionModeAndSchedule.getSchedule();
            workflowSettingsForState = getWorkflowSettingsForState(entitiesSnapshot);

            try {
                final long primaryEntity = ActionDTOUtil.getPrimaryEntityId(recommendation);
                associatedSettingsPolicies =
                        getAssociatedPolicies(entitiesSnapshot, primaryEntity);
                associatedAccountId = Action.getAssociatedAccountId(recommendation, entitiesSnapshot, primaryEntity);
                associatedResourceGroupId = entitiesSnapshot.getResourceGroupForEntity(primaryEntity);
            } catch (UnsupportedActionException e) {
                // Shouldn't ever happen here, because we would have rejected this action
                // if it was unsupported.
                logger.error("Unexpected unsupported action exception while refreshing action:" +
                    " {}. Error: {}", recommendation, e.getMessage());
            }
            // in real-time, entitiesSnapshot has information on all action types, hence the second
            // parameter specific to detached volumes is not needed as in plans.
            setDescription(ActionDescriptionBuilder.buildActionDescription(entitiesSnapshot,
               actionTranslation.getTranslationResultOrOriginal()));

            setRelatedRisks(ExplanationComposer.composeRelatedRisks(actionTranslation.getTranslationResultOrOriginal()));
        }
    }

    private Map<ActionState, SettingProto.Setting> getWorkflowSettingsForState(
            @Nonnull final EntitiesAndSettingsSnapshot entitiesSnapshot) {
        final Optional<ActionDTO.Action> translatedRecommendation =
                getActionTranslation().getTranslatedRecommendation();
        final Map<ActionState, Setting> workflowSettingsForActionStates;
        if (translatedRecommendation.isPresent()) {
            workflowSettingsForActionStates =
                    actionModeCalculator.calculateWorkflowSettings(translatedRecommendation.get(),
                            entitiesSnapshot);
        } else {
            logger.error("Workflow settings weren't calculated for action with {} ID because "
                    + "recommendation wasn't translated.", this::getId);
            workflowSettingsForActionStates = Collections.emptyMap();
        }
        return workflowSettingsForActionStates;
    }

    /**
     * Set action mode and associated properties if need it.
     *
     * @param calculatedActionMode action mode after its calculation
     */
    private void setActionMode(@Nonnull final ActionMode calculatedActionMode) {
        actionMode = calculatedActionMode;
        if (calculatedActionMode != ActionMode.EXTERNAL_APPROVAL) {
            externalActionName = null;
            externalActionUrl = null;
        }
    }

    private static Optional<Long> getAssociatedAccountId(@Nonnull final ActionDTO.Action action,
                                                         @Nonnull final EntitiesAndSettingsSnapshot entitiesSnapshot,
                                                         long primaryEntity) {
        final ActionInfo actionInfo = action.getInfo();
        switch (actionInfo.getActionTypeCase()) {
            case BUYRI:
                final BuyRI buyRi = actionInfo.getBuyRi();
                return Optional.of(buyRi.getMasterAccount().getId());
            default:
                return entitiesSnapshot.getOwnerAccountOfEntity(primaryEntity)
                        .map(EntityWithConnections::getOid);
        }
    }

    @Nonnull
    private Set<Long> getAssociatedPolicies(@Nonnull EntitiesAndSettingsSnapshot entitiesSnapshot,
            long primaryEntity) {
        final Optional<ActionDTO.Action> translatedRecommendation = getActionTranslation()
            .getTranslatedRecommendation();
        if (!translatedRecommendation.isPresent()) {
            logger.error("Action translation for action {} is not ready. Cannot find associated "
                    + "policies", this::getId);
            return Collections.emptySet();
        }
        final Map<String, Collection<Long>> settingPoliciesForEntity =
                entitiesSnapshot.getSettingPoliciesForEntity(primaryEntity);
        final Set<String> defaultSettingsForEntity =
                entitiesSnapshot.getDefaultSettingPoliciesForEntity(primaryEntity);
        final Set<Long> policiesAssociatedWithAction = new HashSet<>();
        final List<String> specApplicableToAction =
                actionModeCalculator.specsApplicableToAction(translatedRecommendation.get(),
                        entitiesSnapshot.getSettingsForEntity(primaryEntity), defaultSettingsForEntity)
                        .map(ActionSpecifications::getConfigurableActionSetting)
                        .map(ConfigurableActionSettings::getSettingName)
                        .collect(Collectors.toList());
        for (String settingName : specApplicableToAction) {
            final Collection<Long> policies = settingPoliciesForEntity.get(settingName);
            if (policies != null) {
                policiesAssociatedWithAction.addAll(policies);
            }
            // also check execution schedule settings because specsApplicableToAction don't have
            // information about them
            final String executionScheduleSetting =
                    ActionSettingSpecs.getSubSettingFromActionModeSetting(settingName,
                            ActionSettingType.SCHEDULE);
            if (executionScheduleSetting != null) {
                final Collection<Long> executionSchedulePolicies =
                        settingPoliciesForEntity.get(executionScheduleSetting);
                if (executionSchedulePolicies != null) {
                    policiesAssociatedWithAction.addAll(executionSchedulePolicies);
                }
            }
        }
        return policiesAssociatedWithAction;
    }

    /**
     * Return the {@link ActionMode} of the action.
     *
     * @return The {@link ActionMode} that currently applies to the action.
     */
    @Override
    public ActionMode getMode() {
        return actionMode;
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public VisibilityLevel getVisibilityLevel() {
        if (getMode() == ActionMode.DISABLED) {
            // Disabled actions are hidden by default.
            return VisibilityLevel.HIDDEN_BY_DEFAULT;
        }
        return VisibilityLevel.ALWAYS_VISIBLE;
    }

    /**
     * Gets the action description.
     *
     * @return The action description string.
     */
    @Nonnull
    @Override
    public String getDescription() {
        return description == null ? "" : description;
    }

    /**
     * Sets action description.
     *
     * This method is exposed to permit setting action description in tests without the need to
     * go through Action Store logic.
     *
     * The action description is being built by
     * {@link ActionDescriptionBuilder#buildActionDescription(EntitiesAndSettingsSnapshot, ActionDTO.Action)}
     *
     * @param description The action description that will be set.
     */
    @VisibleForTesting
    public void setDescription(@Nonnull final String description) {
        this.description =  description;

    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public Optional<WorkflowDTO.Workflow> getWorkflow(@Nonnull WorkflowStore workflowStore,
            @Nonnull ActionState actionState)
            throws WorkflowStoreException {
        // Fetch the setting, if any, that defines whether a Workflow should be applied in the *next*
        // step of this action's execution. This may be a PRE, REPLACE or POST workflow, depending
        // on the policies defined and the current state of the action.
        final Optional<SettingProto.Setting> workflowSettingOpt = getWorkflowSetting(actionState);
        // An empty value indicates no workflow has been set
        if (hasNonEmptyStringSetting(workflowSettingOpt)) {
            final String workflowIdString = workflowSettingOpt.get().getStringSettingValue()
                .getValue();
            try {
                // the value of the Workflow Setting denotes the ID of the Workflow to apply
                final long workflowId = Long.valueOf(workflowIdString);
                return Optional.of(workflowStore.fetchWorkflow(workflowId)
                        .orElseThrow(() -> new WorkflowStoreException(
                                "Workflow not found, id: " + workflowIdString)));
            } catch (NumberFormatException e) {
                throw new WorkflowStoreException(
                        "Invalid workflow ID: " + workflowIdString + " for action " + getId(), e);
            }
        }
        return Optional.empty();
    }

    /**
     * Return id of the target discovered REPLACE workflow for action if any.
     * This target will be used as execution target for action.
     *
     * @param workflowStore the store for all the known {@link WorkflowDTO.Workflow} items
     * @return an Optional of the target discovered REPLACE workflow, otherwise {@link Optional#empty()}
     */
    public Optional<Long> getWorkflowExecutionTarget(@Nonnull WorkflowStore workflowStore) {
        try {
            final Optional<Workflow> replaceWorkflow =
                    getWorkflow(workflowStore, ActionState.IN_PROGRESS);
            return replaceWorkflow.map(Workflow::getWorkflowInfo).map(WorkflowInfo::getTargetId);
        } catch (WorkflowStoreException ex) {
            logger.error("Failed to get REPLACE workflow for action {}.", getId(), ex);
        }
        return Optional.empty();
    }

    private boolean hasWorkflowForCurrentState() {
        return hasNonEmptyStringSetting(getWorkflowSetting(stateMachine.getState()));
    }

    private boolean hasNonEmptyStringSetting(Optional<SettingProto.Setting> settingOpt) {
        return settingOpt.map(this::hasNonEmptyStringValue).orElse(false);
    }

    private boolean hasNonEmptyStringValue(Setting setting) {
        return setting.hasStringSettingValue()
            && !StringUtils.isEmpty(setting.getStringSettingValue().getValue());
    }

    /**
     * Get the ID of the action. This ID is the same as the one provided by the market in its
     * recommendation.
     *
     * @return The ID of the action.
     */
    @Override
    public long getId() {
        synchronized (recommendationLock) {
            return recommendation.getId();
        }
    }

    /**
     * Get the decision associated with the action. Actions in the READY state do not have a decision.
     *
     * @return The decision associated with the action.
     */
    @Override
    public Optional<ActionDecision> getDecision() {
        return decision.getDecision();
    }

    /**
     * Get the executable step associated with the action. An action must be accepted before it will
     * have an executable step.
     *
     * @return An optional of the executable step. Empty if the action has not been accepted.
     */
    @Override
    public Optional<ExecutableStep> getCurrentExecutableStep() {
        return currentExecutableStep;
    }

    /**
     * Get the {@link ActionCategory} associated with the action.
     *
     * @return The {@link ActionCategory}.
     */
    @Override
    @Nonnull
    public ActionCategory getActionCategory() {
        return actionCategory;
    }

    @Override
    @Nonnull
    public Optional<Long> getAssociatedAccount() {
        return associatedAccountId;
    }

    @Override
    public Optional<Long> getAssociatedResourceGroupId() {
        return associatedResourceGroupId;
    }

    @Nonnull
    @Override
    public Optional<ActionSchedule> getSchedule() {
        return Optional.ofNullable(schedule);
    }

    @Override
    public void setSchedule(@Nonnull ActionSchedule actionSchedule) {
        this.schedule = actionSchedule;
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Optional<String> getExternalActionName() {
        return Optional.ofNullable(externalActionName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setExternalActionName(@Nonnull String externalActionName) {
        this.externalActionName = Objects.requireNonNull(externalActionName);
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Optional<String> getExternalActionUrl() {
        return Optional.ofNullable(externalActionUrl);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setExternalActionUrl(@Nonnull String externalActionUrl) {
        this.externalActionUrl = Objects.requireNonNull(externalActionUrl);
    }

    @Nonnull
    @Override
    public Collection<Long> getAssociatedSettingsPolicies() {
        return associatedSettingsPolicies != null ? associatedSettingsPolicies : Collections.emptyList();
    }

    /**
     * Get the translation of the action from the market's domain-agnostic representation into
     * the domain-specific real-world representation. See {@link ActionTranslation} for more details.
     *
     * @return The {@link ActionTranslation} associated with this action.
     */
    @Override
    @Nonnull
    public ActionTranslation getActionTranslation() {
        synchronized (recommendationLock) {
            return actionTranslation;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public ActionDTO.Action getTranslationResultOrOriginal() {
        return getActionTranslation().getTranslationResultOrOriginal();
    }

    /**
     * Get the status of the translation associated with this action. See
     * {@link ActionTranslation} for more details.
     *
     * @return the status of the translation associated with this action.
     */
    @Override
    public TranslationStatus getTranslationStatus() {
        synchronized (recommendationLock) {
            return actionTranslation.getTranslationStatus();
        }
    }

    /**
     * Find the Setting for a Workflow Orchestration Policy, if there is one, that applies to
     * the current Action in the given state (e.g. PRE, POST).
     * The calculation uses the ActionDTO.Action and the EntitySettingsCache.
     *
     * @param actionState the state for which to retrieve the workflow
     * @return an Optional containing the Setting for a Workflow Orchestration Policy, if there
     * is one defined for this action, or Optional.empty() otherwise
     */
    @Nonnull
    private Optional<SettingProto.Setting> getWorkflowSetting(ActionState actionState) {
        return Optional.ofNullable(workflowSettingsForState.get(actionState));
    }


    /**
     * Determine if this Action may be executed directly by Turbonomic, i.e. the mode
     * is either AUTOMATIC or MANUAL.
     *
     * @return true if this Action may be executed, i.e. mode is AUTOMATIC, MANUAL, or
     *         EXTERNAL_APPROVAL.
     */
    private boolean modePermitsExecution() {
        final ActionMode mode = getMode();
        return mode == ActionMode.AUTOMATIC
            || mode == ActionMode.MANUAL
            || mode == ActionMode.EXTERNAL_APPROVAL;
    }

    /**
     * Guard against illegal attempts to accept the action.
     * If it returns true, the action may be accepted for the given event.
     * If it returns false, the action may NOT be accepted for the given event.
     *
     * @param event The event that caused the acceptance.
     * @return true if the action may be accepted, false if not.
     */
    boolean acceptanceGuard(@Nonnull final AuthorizedActionEvent event) {
        return modePermitsExecution() && hasPermissionToAccept(event.getAuthorizerUuid());
    }

    /**
     * Check if a user has permission to accept this action.
     *
     * @param userUuid The user who should be checked for permission.
     * @return true if the user has permission, false otherwise.
     */
    private boolean hasPermissionToAccept(String userUuid) {
        // Until we implement users and authorization, always grant permission.
        return true;
    }

    @Override
    public boolean hasPendingExecution() {
        return currentExecutableStep.map(this::hasPendingExecution).orElse(false);
    }

    private boolean hasPendingExecution(@Nonnull ExecutableStep step) {
        switch (step.getStatus()) {
            case QUEUED:
            case IN_PROGRESS:
                return true;
            case FAILED:
            case SUCCESS:
            default:
                return false;
        }
    }

    public Map<ActionState, ExecutableStep> getExecutableSteps() {
        return executableSteps;
    }

    /**
     * Called when an action is accepted and queued for execution.
     *
     * @param event The event that caused the acceptance/queueing.
     */
    void onActionAccepted(@Nonnull final AcceptanceEvent event) {
        acceptanceReason = event.getReason();
        executionAuthorizerId = event.getAuthorizerUuid();
        createExecutionStep(event.getTargetId());
    }

    /**
     * Called when an action is rejected.
     *
     * @param event The event that caused the rejecting.
     */
    void onActionRejected(@Nonnull final RejectionEvent event) {
    }

    /**
     * Called when acceptance is removed for action.
     *
     * @param event The event that caused the removing acceptance.
     */
    void onAcceptanceRemoved(@Nonnull final AcceptanceRemovalEvent event) {
        currentExecutableStep = Optional.empty();
    }

    /**
     * Called when rejection is removed for action.
     *
     * @param event The event that caused the removing rejection.
     */
    void onRejectionRemoved(@Nonnull final RejectionRemovalEvent event) {
    }

    /**
     * Called when accepted action is sent to the queue.
     *
     * @param event The event that caused the sending action to the queue.
     */
    void onActionQueued(@Nonnull final QueuedEvent event) {
    }

    /**
     * Returns a state which will be a start of action execution. It may be {@link
     * ActionState#PRE_IN_PROGRESS} if there is a PRE workflow for this action. Alternatively it
     * will be {@link ActionState#IN_PROGRESS}.
     *
     * @return destination action state
     */
    ActionState getExecutionState() {
        final Optional<Setting> preExecSetting = getWorkflowSetting(ActionState.PRE_IN_PROGRESS);
        if (preExecSetting.isPresent()) {
            return ActionState.PRE_IN_PROGRESS;
        } else {
            return ActionState.IN_PROGRESS;
        }
    }

    /**
     * Returns a state which will be after action execution. It may be {@link
     * ActionState#POST_IN_PROGRESS} if there is a POST workflow for this action. Alternatively it
     * will be {@link ActionState#SUCCEEDED} or {@link ActionState#FAILED}.
     *
     * @param nextWithWorkflowState next state to go to if there is a workflow.
     * @param noPostWorkflowState next state when there is no work.
     *          Should be either {@link ActionState#SUCCEEDED} or {@link ActionState#FAILED}
     * @return destination action state
     */
    ActionState getPostExecutionStep(
            @Nonnull ActionState nextWithWorkflowState,
            @Nonnull ActionState noPostWorkflowState) {
        final Optional<Setting> preExecSetting = getWorkflowSetting(ActionState.POST_IN_PROGRESS);
        if (preExecSetting.isPresent()) {
            return nextWithWorkflowState;
        } else {
            return noPostWorkflowState;
        }
    }

    /**
     * Called when an action enters the PRE state, preparing to execute an action.
     *
     * @param event The event that caused the execution preparation.
     */
    void onActionPrepare(@Nonnull final BeginExecutionEvent event) {
        // Store the fact that this action was accepted and why (manual/automated)
        decide(builder -> builder.setExecutionDecision(
            ExecutionDecision.newBuilder()
                .setUserUuid(executionAuthorizerId)
                .setReason(acceptanceReason)
                .build()));
            // Add the current executable step to the map of all steps with the PRE_IN_PROGRESS key
            executableSteps.put(getState(), currentExecutableStep.get());
            // Mark this action as in-progress
            // (PRE_IN_PROGRESS, IN_PROGRESS, POST_IN_PROGRESS, and FAILING states all count as
            // in-progress)
            updateExecutionStatus(step -> {
                    step.execute();
                    IN_PROGRESS_ACTION_COUNTS_GAUGE
                        .labels(getActionType().name())
                        .increment();
                }, event.getEventName()
            );
    }

    /**
     * Called when an action begins execution (when no PRE workflow is configured).
     *
     * @param event The event that caused the execution.
     */
    void onActionStart(@Nonnull final BeginExecutionEvent event) {
        // A PRE workflow was not run, so the current executionStep is still waiting to be used
        // Add the current executable step to the map of all steps with the IN_PROGRESS key
        executableSteps.put(getState(), currentExecutableStep.get());
        // Mark the main execution step of this action as in-progress
        updateExecutionStatus(ExecutableStep::execute, event.getEventName());
    }

    /**
     * Called when an action begins execution (after the PRE state has completed successfully).
     *
     * @param event The event that caused the execution.
     */
    void onPreExecuted(@Nonnull final SuccessEvent event) {
        // A PRE workflow was run, and we need to replace the executionStep with a fresh one for
        // tracking the primary action execution
        replaceExecutionStep(event.getEventName());
        // Mark the main execution step of this action as in-progress
        updateExecutionStatus(ExecutableStep::execute, event.getEventName());
    }

    void onActionProgress(@Nonnull final ProgressEvent event) {
        updateExecutionStatus(step ->
            step.updateProgress(event.getProgressPercentage(), event.getProgressDescription()),
            event.getEventName()
        );
    }

    void onActionSuccess(@Nonnull final SuccessEvent event) {
        updateExecutionStatus(step -> {
            step.success();
            onActionComplete(step);
        }, event.getEventName());
    }

    void onActionFailure(@Nonnull final FailureEvent event) {
        updateExecutionStatus(step -> {
            step.fail().addError(event.getErrorDescription());
            onActionComplete(step);
        }, event.getEventName());
    }

    /**
     * When action was removed from queue because of non active status of execution window.
     *
     * @param rollBackEvent the rollback event signaling the changing action state from QUEUED to
     * previous state
     */
    void onActionRemovedFromQueue(@Nonnull final RollBackToAcceptedEvent rollBackEvent) {
        currentExecutableStep = Optional.empty();
    }

    /**
     * Called when an action enters the POST state, after executing an action successfully.
     *
     * @param event The success event signaling the end of action execution
     */
    void onActionPostSuccess(@Nonnull final SuccessEvent event) {
        // Store the success
        onActionSuccess(event);
        // Progress to the Post phase, preparing to possibly run a POST execution workflow
        onActionPost(event);
    }

    /**
     * Called when an action enters the POST state, after executing an action unsuccessfully
     *
     * @param event The failure event signaling the end of action execution
     */
    void onActionPostFailure(@Nonnull final FailureEvent event) {
        // Store the failure, including error message
        onActionFailure(event);
        // Progress to the Post phase, preparing to possibly run a POST execution workflow
        onActionPost(event);
    }

    /**
     * Called when an action enters the POST state, after executing an action.
     *
     * Helper method for onActionPostSuccess and onActionPostFailure.
     *
     * @param event The event signaling the end of action execution
     */
    private void onActionPost(@Nonnull final ActionEvent event) {
        if (hasWorkflowForCurrentState() && isPostState()) {
            final String outcomeDescription = currentExecutableStep.map(ExecutableStep::getStatus)
                .map(status -> Status.SUCCESS == status)
                .orElse(false) ? "succeeded" : "failed";
            logger.debug("Running POST workflow for action {} after execution {}.",
                getId(), outcomeDescription);
            // An action execution was run, and we need to replace the executionStep with a fresh one for
            // tracking the POST action execution
            replaceExecutionStep(event.getEventName());
            // Mark the main execution step of this action as in-progress
            updateExecutionStatus(ExecutableStep::execute, event.getEventName());
            // ActionStateUpdater will kick off the actual execution of the POST workflow
        }
    }

    private boolean isPostState() {
        ActionState currentState = getState();
        return currentState == ActionState.POST_IN_PROGRESS
            || currentState == ActionState.FAILING;
    }

    /**
     * Called when an action is cleared.
     *
     * @param event The event that caused the clearing.
     */
    void onActionCleared(@Nonnull final ClearingEvent event) {
        decide(builder -> builder.setClearingDecision(event.getDecision()));
    }

    /**
     * Returnes false if action has NOT_SUPPORTED capability and shouldn't be shown in the UI.
     *
     * @return should be the shown or not.
     */
    public SupportLevel getSupportLevel() {
        synchronized (recommendationLock) {
            return recommendation.getSupportingLevel();
        }
    }

    public Set<String> getRelatedRisks() {
        return relatedRisks;
    }

    public void setRelatedRisks(@Nonnull final Set<String> relatedRisks) {
        this.relatedRisks = relatedRisks;
    }

    /**
     * Method to update the status of an {@link ExecutionStep}.
     */
    @FunctionalInterface
    private interface ExecutionStatusUpdater {
        void updateExecutionStep(@Nonnull final ExecutableStep stepToUpdate);
    }

    /**
     * Update the execution step via the given update method.
     *
     * @param updateMethod The method to call to update the step.
     * @param eventName The name of the event that triggered the update.
     * @throws IllegalArgumentException if called when the execution step is not present.
     */
    private void updateExecutionStatus(@Nonnull final ExecutionStatusUpdater updateMethod,
                                       @Nonnull final String eventName) {
        if (currentExecutableStep.isPresent()) {
            updateMethod.updateExecutionStep(currentExecutableStep.get());
        } else {
            throw new IllegalStateException(eventName + " received with no execution step available.");
        }
    }

    private void createExecutionStep(final long targetId) {
        if (currentExecutableStep.isPresent()) {
            throw new IllegalStateException("createExecutionStep for target " + targetId +
                " called when execution step already present");
        }

        final ExecutableStep newExecutableStep = new ExecutableStep(targetId);
        // Set the current executable step to the newly created one
        currentExecutableStep = Optional.of(newExecutableStep);
    }

    private void replaceExecutionStep(@Nonnull final String eventName) {
        if (currentExecutableStep.isPresent()) {
            // A previous execution step has completed running
            final ExecutableStep previousExecutableStep = currentExecutableStep.get();

            // Currently, all executable steps hold the same targetId, namely the targetId which
            // was provided when originally invoking action execution. A workflow will actually
            // execute on the target that discovered the workflow, regardless of what is set here.
            // TODO: Consider whether it is worth the effort to insert the targetId of the workflow
            //  target instead (if different), when running a workflow
            final long targetId = previousExecutableStep.getTargetId();

            // Create a fresh executable step for tracking the next step of action execution
            final ExecutableStep newExecutableStep = new ExecutableStep(targetId);
            // Add the new executable step to the map of all steps with the current state as the key
            executableSteps.put(getState(), newExecutableStep);
            // Set the current executable step to the newly created one
            currentExecutableStep = Optional.of(newExecutableStep);

        } else {
            throw new IllegalStateException(eventName + " received with no execution step available.");
        }


    }

    /**
     * Apply a decision to this action.
     *
     * @param decisionGenerator An operator that, given a builder, generates a decision that it
     *                          applies to the builder.
     */
    private void decide(UnaryOperator<ActionDecision.Builder> decisionGenerator) {
        ActionDecision.Builder builder = ActionDecision.newBuilder()
            .setDecisionTime(System.currentTimeMillis());

        decision.setDecision(decisionGenerator.apply(builder).build());
    }

    private ActionTypeCase getActionType() {
        return getRecommendation().getInfo().getActionTypeCase();
    }

    private void onActionComplete(@Nonnull final ExecutableStep executableStep) {
        IN_PROGRESS_ACTION_COUNTS_GAUGE
            .labels(getActionType().name())
            .decrement();

        executableStep.getExecutionTimeSeconds().ifPresent(executionTime ->
                ACTION_DURATION_SUMMARY
                        .labels(getActionType().name())
                        .observe((double)executionTime));
    }

    /**
     * The Decision provides information about why OperationsManager
     * decided to execute or not to execute a particular action.
     * Not provided if no decision has been made.
     *
     * Tracked using an internal class so that the variable can be final and
     * all attempts to write to it, even those from internal to the Action model,
     * can be checked for legality before being carried out.
     */
    private class Decision {
        private Optional<ActionDecision> decision;

        private Decision() {
            decision = Optional.empty(); // Initialize to empty
        }

        Optional<ActionDecision> getDecision() {
            return decision;
        }

        void setDecision(@Nonnull final ActionDecision actionDecision) {
            if (decision.isPresent()) {
                // The state machine makes this condition impossible
                throw new IllegalStateException("Illegal attempt to overwrite decision for action " + getId() + "."
                    + "Existing: " + decision.get()
                    + "New: " + actionDecision
                );
            }

            decision = Optional.of(actionDecision);
        }
    }

    /**
     * Compose a state object for serialization the state of this action.
     *
     * @return A state object for serialization purposes..
     */
    public SerializationState toSerializationState() {
        return new SerializationState(this);
    }

    /**
     * The essential state required to dump and restore {@link Action} objects.
     * Used for easy integration with GSON.
     */
    @Immutable
    public static class SerializationState {

        final long actionPlanId;

        final ActionDTO.Action recommendation;

        public long getActionPlanId() {
            return actionPlanId;
        }

        public ActionDTO.Action getRecommendation() {
            return recommendation;
        }

        public LocalDateTime getRecommendationTime() {
            return recommendationTime;
        }

        public ActionDecision getActionDecision() {
            return actionDecision;
        }

        public ExecutionStep getExecutionStep() {
            return executionStep;
        }

        public ActionState getCurrentState() {
            return currentState;
        }

        public ActionCategory getActionCategory() {
            return actionCategory;
        }

        @Nullable
        public Long getAssociatedAccountId() {
            return associatedAccountId;
        }

        @Nullable
        public Long getAssociatedResourceGroupId() {
            return associatedResourceGroupId;
        }

        @Nullable
        public byte[] getActionDetailData() {
            return actionDetailData;
        }

        @Nullable
        public ActionSchedule getSchedule() {
            return schedule;
        }

        private final LocalDateTime recommendationTime;

        private final ActionDecision actionDecision;

        private final ExecutionStep executionStep;

        private final ActionState currentState;

        private final ActionTranslation actionTranslation;

        private final Long associatedAccountId;

        private final Long associatedResourceGroupId;

        private final byte[] actionDetailData;

        private final ActionSchedule schedule;

        @Nullable
        private final Long recommendationOid;

        /**
         * We don't really need to save the category because it can be extracted from the
         * recommendation, but saving it anyway so that we know what got extracted - and
         * so that we can restore properly even if the extraction code changes.
         */
        final ActionCategory actionCategory;

        public SerializationState(@Nonnull final Action action) {
            this.actionPlanId = action.actionPlanId;
            this.recommendation = action.recommendation;
            this.recommendationTime = action.recommendationTime;
            this.actionDecision = action.decision.getDecision().orElse(null);
            this.executionStep = action.currentExecutableStep.map(ExecutableStep::getExecutionStep).orElse(null);
            this.currentState = action.stateMachine.getState();
            this.actionTranslation = action.actionTranslation;
            this.actionCategory = action.getActionCategory();
            this.associatedAccountId = action.getAssociatedAccount().orElse(null);
            this.associatedResourceGroupId = action.getAssociatedResourceGroupId().orElse(null);
            this.actionDetailData = action.getDescription().getBytes();
            this.schedule = action.getSchedule().orElse(null);
            this.recommendationOid = action.getRecommendationOid();
        }

        public SerializationState(final long actionPlanId,
                                  @Nonnull ActionDTO.Action recommendation,
                                  @Nonnull LocalDateTime recommendationTime,
                                  @Nullable ActionDecision decision,
                                  @Nullable ExecutionStep executableStep,
                                  @Nonnull ActionState actionState,
                                  @Nonnull ActionTranslation actionTranslation,
                                  @Nullable Long associatedAccountId,
                                  @Nullable Long associatedResourceGroupId,
                                  @Nullable byte[] actionDetailData,
                                  @Nullable Long recommendationOid) {
            this.actionPlanId = actionPlanId;
            this.recommendation = recommendation;
            this.recommendationTime = recommendationTime;
            this.actionDecision = decision;
            this.executionStep = executableStep;
            this.currentState = actionState;
            this.actionTranslation = actionTranslation;
            this.actionCategory =
                    ActionCategoryExtractor.assignActionCategory(recommendation);
            this.associatedAccountId = associatedAccountId;
            this.associatedResourceGroupId = associatedResourceGroupId;
            this.actionDetailData = actionDetailData;
            this.schedule = null;
            this.recommendationOid = recommendationOid;
        }

        /**
         * Return the stable oid that identifies the action, even between market cycles and
         * restarts.
         *
         * @return the stable oid that identifies the action, even between market cycles and
         * restarts. This value is null when the data was saved when the stable oid did not exist.
         * This includes XL versions 7.22.1 and earlier.
         */
        @Nullable
        public Long getRecommendationOid() {
            return recommendationOid;
        }
    }

}
