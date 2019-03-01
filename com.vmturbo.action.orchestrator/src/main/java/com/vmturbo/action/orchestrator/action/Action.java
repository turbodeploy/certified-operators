package com.vmturbo.action.orchestrator.action;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.Optional;
import java.util.function.UnaryOperator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.action.ActionEvent.AcceptanceEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.AuthorizedActionEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.BeginExecutionEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.ClearingEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.FailureEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.ProgressEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.SuccessEvent;
import com.vmturbo.action.orchestrator.action.ActionTranslation.TranslationStatus;
import com.vmturbo.action.orchestrator.state.machine.StateMachine;
import com.vmturbo.action.orchestrator.state.machine.Transition.TransitionResult;
import com.vmturbo.action.orchestrator.store.EntitySettingsCache;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStore;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStoreException;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision.ExecutionDecision;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutionStep;
import com.vmturbo.common.protobuf.setting.SettingProto;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
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
     * The time at which this action was recommended.
     */
    private final LocalDateTime recommendationTime;

    /**
     * The decision applied to this action. Initially there is no decision.
     * Once a decision is made, it cannot be overwritten.
     */
    private final Decision decision;

    private ExecutionDecision.Reason acceptanceReason;

    private String executionAuthorizerId;


    /**
     * The state of the action. The state of an action transitions due to certain system events.
     * The initial state is determined by the action mode.
     */
    private final StateMachine<ActionState, ActionEvent> stateMachine;

    /**
     * The executable step for actions that the system decides should be executed.
     * Tracks the progress of an action as it is executed by the appropriate target(s).
     * TODO: DavidBlinn 8/26/2016 Support for multiple execution steps
     */
    private Optional<ExecutableStep> executableStep;

    /**
     * Cached settings for entities.
     */
    private final EntitySettingsCache entitySettings;


    /**
     * The ID of the action plan to which this action's recommendation belongs.
     */
    private final long actionPlanId;

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
    private final ActionCategory actionCategory;

    /**
     * Create an action from a state object that was used to serialize the state of the action.
     *
     * @param savedState A state object that was used to serialize the state of the action.
     */
    public Action(@Nonnull final SerializationState savedState) {
        this.recommendation = savedState.recommendation;
        this.actionPlanId = savedState.actionPlanId;
        this.executableStep = Optional.ofNullable(ExecutableStep.fromExecutionStep(savedState.executionStep));
        this.recommendationTime = savedState.recommendationTime;
        this.decision = new Decision();
        if (savedState.actionDecision != null) {
            this.decision.setDecision(savedState.actionDecision);
        }
        this.stateMachine = ActionStateMachine.newInstance(this, savedState.currentState);
        this.actionTranslation = savedState.actionTranslation;
        this.entitySettings = null;
        this.actionCategory = savedState.actionCategory;
    }

    public Action(@Nonnull final ActionDTO.Action recommendation,
                  @Nonnull final LocalDateTime recommendationTime,
                  final long actionPlanId) {
        this.recommendation = recommendation;
        this.actionTranslation = new ActionTranslation(this.recommendation);
        this.actionPlanId = actionPlanId;
        this.recommendationTime = Objects.requireNonNull(recommendationTime);
        this.stateMachine = ActionStateMachine.newInstance(this);
        this.executableStep = Optional.empty();
        this.decision = new Decision();
        this.entitySettings = null;
        this.actionCategory = ActionCategoryExtractor.assignActionCategory(
                recommendation.getExplanation());
    }

    public Action(@Nonnull final ActionDTO.Action recommendation,
                  @Nonnull final LocalDateTime recommendationTime,
                  @Nonnull final EntitySettingsCache entitySettings,
                  final long actionPlanId) {
        this.recommendation = recommendation;
        this.actionTranslation = new ActionTranslation(this.recommendation);
        this.actionPlanId = actionPlanId;
        this.recommendationTime = Objects.requireNonNull(recommendationTime);
        this.stateMachine = ActionStateMachine.newInstance(this);
        this.executableStep = Optional.empty();
        this.decision = new Decision();
        this.entitySettings = Objects.requireNonNull(entitySettings);
        this.actionCategory = ActionCategoryExtractor.assignActionCategory(
                recommendation.getExplanation());
    }

    public Action(Action prototype, SupportLevel supportLevel) {
        this.recommendation = prototype.recommendation;
        this.actionTranslation = prototype.actionTranslation;
        this.actionPlanId = prototype.actionPlanId;
        this.decision = prototype.decision;
        this.executableStep = prototype.executableStep;
        this.recommendationTime = prototype.recommendationTime;
        this.recommendation = ActionDTO.Action.newBuilder(prototype.recommendation)
                .setSupportingLevel(supportLevel).build();
        this.entitySettings = prototype.entitySettings;
        this.stateMachine = ActionStateMachine.newInstance(this, prototype.getState());
        this.actionCategory = prototype.actionCategory;
    }

    public Action(@Nonnull final ActionDTO.Action recommendation,
                  final long actionPlanId) {
        this(recommendation, LocalDateTime.now(), actionPlanId);
    }

    public Action(@Nonnull final ActionDTO.Action recommendation,
                  @Nonnull final EntitySettingsCache entitySettings,
                  final long actionPlanId) {
        this(recommendation, LocalDateTime.now(), entitySettings, actionPlanId);
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
     *
     * @param newRecommendation The new recommendation from the market.
     */
    public void updateRecommendation(@Nonnull final ActionDTO.Action newRecommendation) {
        synchronized (recommendationLock) {
            if (recommendation == null) {
                throw new IllegalStateException("Action has no recommendation.");
            }

            if (!newRecommendation.getInfo().equals(this.recommendation.getInfo())) {
                throw new IllegalArgumentException(
                    "Updated recommendation must have the same ActionInfo!\n" +
                        "Before:\n" + this.recommendation.getInfo() + "\nAfter:\n" +
                        newRecommendation.getInfo());
            }
            this.recommendation = newRecommendation.toBuilder()
                    // It's important to keep the same ID - that's the whole point of
                    // updating the recommendation instead of creating a new action.
                    .setId(this.recommendation.getId())
                    .build();
            this.actionTranslation = new ActionTranslation(this.recommendation);
        }
    }


    @Override
    public String toString() {
        synchronized (recommendationLock) {
            return "Action Id=" + getId() +
                    ", Type=" + recommendation.getInfo().getActionTypeCase() +
                    ", Mode=" + getMode() +
                    ", State=" + getState() +
                    ", Recommendation=" + recommendation;
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
     * Calculate the {@link ActionMode} of the action. Check first for a Workflow Action, where
     * the Action Mode is determined by the Workflow Policy. If not a Workflow Action,
     * then the mode is determined by the  strictest policy that applies to the entity
     * involved in the action.
     *
     * @return The {@link ActionMode} that currently applies to the action.
     */
    @Override
    public ActionMode getMode() {
        // To avoid holding the lock for a long time, save the reference to the recommendation at
        // the time the method is called. Note that the underlying protobuf is immutable.
        final ActionDTO.Action curRecommendation;
        synchronized (recommendationLock) {
            curRecommendation = recommendation;
        }
        return ActionModeCalculator.calculateWorkflowActionMode(curRecommendation, entitySettings)
                .orElseGet(() -> getClippedActionMode(curRecommendation));
    }

    /**
     * Calculate the {@link ActionMode} for this action based on the mode default for this action,
     * limited by (clipped by) the action modes supported by the probe for this action.
     *
     * The mode is determined by the strictest policy that applies to any entity involved in the action.
     * The default mode is RECOMMEND for actions of unrecognized type, and MANUAL for actions of
     * recognized type with sufficient SupportLevel.
     *
     * @return The {@link ActionMode} that currently applies to the action.
     * @throws IllegalArgumentException if the Action SupportLevel is not supported.
     */
    private ActionMode getClippedActionMode(@Nonnull final ActionDTO.Action recommendation) {
        switch (recommendation.getSupportingLevel()) {
            case UNSUPPORTED:
                return ActionMode.DISABLED;
            case SHOW_ONLY:
                final ActionMode mode = ActionModeCalculator.calculateActionMode(
                        recommendation, entitySettings);
                return (mode.getNumber() > ActionMode.RECOMMEND_VALUE)
                        ? ActionMode.RECOMMEND
                        : mode;
            case SUPPORTED:
                return ActionModeCalculator.calculateActionMode(
                        recommendation, entitySettings);
            default:
                throw new IllegalArgumentException("Action SupportLevel is of unrecognized type.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public Optional<WorkflowDTO.Workflow> getWorkflow(WorkflowStore workflowStore) {
        Optional<WorkflowDTO.Workflow> workflowOpt = Optional.empty();
        // fetch the setting, if any, that defines whether a Workflow should be applied
        final Optional<SettingProto.Setting> workflowSettingOpt = getWorkflowSetting();
        if (workflowSettingOpt.isPresent()) {
            final String workflowIdString = workflowSettingOpt.get().getStringSettingValue()
                    .getValue();
            try {
                // the value of the Workflow Setting denotes the ID of the Workflow to apply
                final long workflowId = Long.valueOf(workflowIdString);
                workflowOpt = Optional.of(workflowStore.fetchWorkflow(workflowId)
                        .orElseThrow(() -> new IllegalStateException("Workflow not found, id: " +
                                workflowIdString)));
            } catch (NumberFormatException e) {
                logger.error("Invalid workflow ID: " + workflowIdString, e);
                return Optional.empty();
            } catch (WorkflowStoreException e) {
                logger.error("Error accessing Workflow, id: " + workflowIdString, e);
                return Optional.empty();
            }
        }
        return workflowOpt;
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
    public Optional<ExecutableStep> getExecutableStep() {
        return executableStep;
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

    /**
     * Get the translation of the action from the market's domain-agnostic representation into
     * the domain-specific real-world representaiton. See {@link ActionTranslation} for more details.
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
     * the current Action. The calculation uses the ActionDTO.Action and the EntitySettingsCache.
     *
     * @return an Optional containing the Setting for a Workflow Orchestration Policy, if there
     * is one defined for this action, or Optional.empty() otherwise
     */
    @Nonnull
    private Optional<SettingProto.Setting> getWorkflowSetting() {
        // To avoid holding the lock for a long time, save the reference to the recommendation at
        // the time the method is called. Note that the underlying protobuf is immutable.
        final ActionDTO.Action curRecommendation;
        synchronized (recommendationLock) {
            curRecommendation = recommendation;
        }
        return ActionModeCalculator.calculateWorkflowSetting(curRecommendation, entitySettings);
    }


    /**
     * Determine if this Action may be executed directly by Turbonomic, i.e. the mode
     * is either AUTOMATIC or MANUAL.
     *
     * @return true if this Action may be executed, i.e. mode is either AUTOMATIC or MANUAL
     */
    private boolean modePermitsExecution() {
        final ActionMode mode = getMode();
        return mode == ActionMode.AUTOMATIC || mode == ActionMode.MANUAL;
    }

    /**
     * Guard against illegal attempts to accept the action.
     * If it returns true, the action may be accepted for the given event.
     * If it returns false, the action may NOT be accepted for the given event.
     *
     * @param event The event that caused the failure.
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
     * Called when an action begins execution.
     *
     * @param event The event that caused the execution.
     */
    void onActionExecuted(@Nonnull final BeginExecutionEvent event) {
        decide(builder -> builder.setExecutionDecision(
                ExecutionDecision.newBuilder()
                        .setUserUuid(executionAuthorizerId)
                        .setReason(acceptanceReason)
                        .build()));
        updateExecutionStatus(step -> {
                step.execute();
                IN_PROGRESS_ACTION_COUNTS_GAUGE
                    .labels(getActionType().name())
                    .increment();
            }, event.getEventName()
        );
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
        if (executableStep.isPresent()) {
            updateMethod.updateExecutionStep(executableStep.get());
        } else {
            throw new IllegalStateException(eventName + " received with no execution step available.");
        }
    }

    private void createExecutionStep(final long targetId) {
        if (executableStep.isPresent()) {
            throw new IllegalStateException("createExecutionStep for target " + targetId +
                " called when execution step already present");
        }

        executableStep = Optional.of(new ExecutableStep(targetId));
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

        final LocalDateTime recommendationTime;

        final ActionDecision actionDecision;

        final ExecutionStep executionStep;

        final ActionState currentState;

        final ActionTranslation actionTranslation;

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
            this.executionStep = action.executableStep.map(ExecutableStep::getExecutionStep).orElse(null);
            this.currentState = action.stateMachine.getState();
            this.actionTranslation = action.actionTranslation;
            this.actionCategory = action.getActionCategory();
        }

        public SerializationState(final long actionPlanId,
                                  @Nonnull ActionDTO.Action recommendation,
                                  @Nonnull LocalDateTime recommendationTime,
                                  @Nullable ActionDecision decision,
                                  @Nullable ExecutionStep executableStep,
                                  @Nonnull ActionState actionState,
                                  @Nonnull ActionTranslation actionTranslation) {
            this.actionPlanId = actionPlanId;
            this.recommendation = recommendation;
            this.recommendationTime = recommendationTime;
            this.actionDecision = decision;
            this.executionStep = executableStep;
            this.currentState = actionState;
            this.actionTranslation = actionTranslation;
            this.actionCategory =
                    ActionCategoryExtractor.assignActionCategory(recommendation.getExplanation());
        }
    }

}
