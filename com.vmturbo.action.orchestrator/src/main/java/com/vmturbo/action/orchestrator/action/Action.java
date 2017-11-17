package com.vmturbo.action.orchestrator.action;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

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
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutionStep;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
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
    /**
     * The recommended steps to take in the environment.
     */
    private final ActionDTO.Action recommendation;

    /**
     * The time at which this action was recommended.
     */
    private final LocalDateTime recommendationTime;

    /**
     * The decision applied to this action. Initially there is no decision.
     * Once a decision is made, it cannot be overwritten.
     */
    private final Decision decision;

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
     * The translation for this action.
     *
     * Actions are translated from the market's domain-agnostic action recommendations into
     * domain-specific actions that make sense in the real world. See
     * {@link com.vmturbo.action.orchestrator.execution.ActionTranslator} for more details.
     */
    @Nonnull
    private final ActionTranslation actionTranslation;

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
     * Create an action from a state object that was used to serialize the state of the action.
     *
     * @param savedState A state object that was used to serialize the state of the action.
     */
    public Action(@Nonnull final SerializationState savedState) {
        this.actionPlanId = savedState.actionPlanId;
        this.executableStep = Optional.ofNullable(ExecutableStep.fromExecutionStep(savedState.executionStep));
        this.recommendation = savedState.recommendation;
        this.recommendationTime = savedState.recommendationTime;
        this.decision = new Decision();
        if (savedState.actionDecision != null) {
            this.decision.setDecision(savedState.actionDecision);
        }
        this.stateMachine = ActionStateMachine.newInstance(this, savedState.currentState);
        this.actionTranslation = savedState.actionTranslation;
        this.entitySettings = null;
    }

    public Action(@Nonnull final ActionDTO.Action recommendation,
                  @Nonnull final LocalDateTime recommendationTime,
                  final long actionPlanId) {
        this.recommendation = Objects.requireNonNull(recommendation);
        this.actionPlanId = actionPlanId;
        this.recommendationTime = Objects.requireNonNull(recommendationTime);
        this.stateMachine = ActionStateMachine.newInstance(this);
        this.executableStep = Optional.empty();
        this.decision = new Decision();
        this.actionTranslation = new ActionTranslation(this.recommendation);
        this.entitySettings = null;
    }

    public Action(@Nonnull final ActionDTO.Action recommendation,
                  @Nonnull final LocalDateTime recommendationTime,
                  @Nonnull final EntitySettingsCache entitySettings,
                  final long actionPlanId) {
        this.recommendation = Objects.requireNonNull(recommendation);
        this.actionPlanId = actionPlanId;
        this.recommendationTime = Objects.requireNonNull(recommendationTime);
        this.stateMachine = ActionStateMachine.newInstance(this);
        this.executableStep = Optional.empty();
        this.decision = new Decision();
        this.actionTranslation = new ActionTranslation(this.recommendation);
        this.entitySettings = Objects.requireNonNull(entitySettings);
    }

    public Action(Action prototype, ActionDTO.Action.SupportLevel supportLevel) {
        this.actionPlanId = prototype.actionPlanId;
        this.actionTranslation = prototype.actionTranslation;
        this.decision = prototype.decision;
        this.executableStep = prototype.executableStep;
        this.recommendationTime = prototype.recommendationTime;
        this.stateMachine = prototype.stateMachine;
        this.recommendation = ActionDTO.Action.newBuilder(prototype.recommendation)
                .setSupportingLevel(supportLevel).build();
        this.entitySettings = prototype.entitySettings;
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


    @Override
    public String toString() {
        return "Action " + getId() + " " + recommendation.getInfo().getActionTypeCase() + " " +
            getMode() + " " + getState();
    }

    /**
     * Get the recommendation associated with this action.
     *
     * @return The recommendation associated with this action.
     */
    @Override
    public ActionDTO.Action getRecommendation() {
        return recommendation;
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
     * Get the mode of the action. The action mode is established by checking the policy for the action
     * when it is received by the action orchestrator.
     *
     * The mode is determined by the strictest policy that applies to any entity involved in the action.
     * The default mode is RECOMMEND for actions of unrecognized type, and MANUAL for actions of
     * recognized type with sufficient SupportLevel.
     * @return The {@link ActionMode} that currently applies to the action.
     */
    @Override
    public ActionMode getMode() {
        switch (recommendation.getSupportingLevel()) {
            case UNSUPPORTED:
                return ActionMode.DISABLED;
            case SHOW_ONLY:
                ActionMode mode = calculateActionMode();
                return (mode.getNumber() > ActionMode.RECOMMEND_VALUE) ? ActionMode.RECOMMEND : mode;
            case SUPPORTED:
                return calculateActionMode();
            default:
                throw new IllegalArgumentException("Action SupportLevel is of unrecognized type.");
        }
    }

    private ActionMode calculateActionMode() {
        // TODO: (Michelle Neuburger, 2017-10-23). Support other action types besides Move.
        // TODO: Determine which settings apply when action involves more than one entity.
        final long targetId;
        final Predicate<Setting> targetSettingFilter;
        switch (recommendation.getInfo().getActionTypeCase()) {
            case MOVE:
                targetId = recommendation.getInfo().getMove().getTargetId();
                targetSettingFilter = s -> s.getSettingSpecName().toLowerCase().equals("move");
                break;
            case RESIZE: case ACTIVATE: case DEACTIVATE:
                return ActionMode.MANUAL;
            default:
                return ActionMode.RECOMMEND;
        }

        List<Setting> targetSettings = entitySettings == null ?
                Collections.emptyList() : entitySettings.getSettingsForEntity(targetId);
        return targetSettings.stream()
                .filter(targetSettingFilter.and(Setting::hasEnumSettingValue))
                .map(s -> ActionMode.valueOf(s.getEnumSettingValue().getValue()).getNumber())
                .min(Integer::compareTo)
                .map(ActionMode::forNumber)
                .orElse(ActionMode.MANUAL);
    }

    /**
     * Get the ID of the action. This ID is the same as the one provided by the market in its
     * recommendation.
     *
     * @return The ID of the action.
     */
    @Override
    public long getId() {
        return recommendation.getId();
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
     * Get the translation of the action from the market's domain-agnostic representation into
     * the domain-specific real-world representaiton. See {@link ActionTranslation} for more details.
     *
     * @return The {@link ActionTranslation} associated with this action.
     */
    @Override
    @Nonnull
    public ActionTranslation getActionTranslation() {
        return actionTranslation;
    }

    /**
     * Get the status of the translation associated with this action. See
     * {@link ActionTranslation} for more details.
     *
     * @return the status of the translation associated with this action.
     */
    @Override
    public TranslationStatus getTranslationStatus() {
        return actionTranslation.getTranslationStatus();
    }

    private boolean modePermitsExecution() {
        return getMode() == ActionMode.AUTOMATIC || getMode() == ActionMode.MANUAL;
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
        return modePermitsExecution() && hasPermissionToAccept(event.getAuthorizerId());

    }

    /**
     * Check if a user has permission to accept this action.
     *
     * @param userId The user who should be checked for permission.
     * @return true if the user has permission, false otherwise.
     */
    private boolean hasPermissionToAccept(long userId) {
        // Until we implement users and authorization, always grant permission.
        return true;
    }

    /**
     * Called when an action is accepted and queued for execution.
     *
     * @param event The event that caused the acceptance/queueing.
     */
    void onActionAccepted(@Nonnull final AcceptanceEvent event) {
        decide(builder -> builder.setExecutionDecision(event.getDecision()));
        createExecutionStep(event.getTargetId());
    }

    /**
     * Called when an action begins execution.
     *
     * @param event The event that caused the execution.
     */
    void onActionExecuted(@Nonnull final BeginExecutionEvent event) {
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
    public ActionDTO.Action.SupportLevel getSupportLevel() {
        return recommendation.getSupportingLevel();
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

        executableStep.getExecutionTimeSeconds().ifPresent(executionTime -> {
            ACTION_DURATION_SUMMARY
                .labels(getActionType().name())
                .observe((double)executionTime);
        });
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

        final LocalDateTime recommendationTime;

        final ActionDecision actionDecision;

        final ExecutionStep executionStep;

        final ActionState currentState;

        final ActionTranslation actionTranslation;

        SerializationState(@Nonnull final Action action) {
            this.actionPlanId = action.actionPlanId;
            this.recommendation = action.recommendation;
            this.recommendationTime = action.recommendationTime;
            this.actionDecision = action.decision.getDecision().orElse(null);
            this.executionStep = action.executableStep.map(ExecutableStep::getExecutionStep).orElse(null);
            this.currentState = action.stateMachine.getState();
            this.actionTranslation = action.actionTranslation;
        }
    }
}
