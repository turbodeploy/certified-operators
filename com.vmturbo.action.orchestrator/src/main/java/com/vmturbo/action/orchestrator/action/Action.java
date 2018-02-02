package com.vmturbo.action.orchestrator.action;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.collect.ImmutableMap;

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
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutionStep;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
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
     * Map of action type to filter for relevant settings.
     * Set in the constructor depending on whether entity settings are available
     * to distinguish Move from Storage Move.
     */
    private final ImmutableMap<ActionTypeCase, Predicate<Setting>> actionTypeSettingFilter;

    /**
     * Map of action type to setting default.
     * Set in the constructor depending on whether entity settings are available
     * to distinguish Move from Storage Move.
     */
    private final ImmutableMap<ActionTypeCase, ActionMode> actionTypeSettingDefault;

    /**
     * Map of action type to target ID getter method
     */
    private static final ImmutableMap<ActionTypeCase, Function<ActionInfo, Long>>
            ACTION_TYPE_TARGET_ID_GETTER =
            ImmutableMap.<ActionTypeCase, Function<ActionInfo,Long>>builder()
                    .put(ActionTypeCase.RESIZE, (info) -> info.getResize().getTargetId())
                    .put(ActionTypeCase.ACTIVATE, (info) -> info.getActivate().getTargetId())
                    .put(ActionTypeCase.DEACTIVATE, (info) -> info.getDeactivate().getTargetId())
                    .put(ActionTypeCase.MOVE, (info) -> info.getMove().getTargetId())
                    .put(ActionTypeCase.RECONFIGURE, (info) -> info.getReconfigure().getTargetId())
                    .build();

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
        this.actionTypeSettingFilter = loadSettingFilterMap(false);
        this.actionTypeSettingDefault = loadSettingDefaultMap(false);
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
        this.actionTypeSettingFilter = loadSettingFilterMap(false);
        this.actionTypeSettingDefault = loadSettingDefaultMap(false);
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
        this.actionTypeSettingFilter = loadSettingFilterMap(true);
        this.actionTypeSettingDefault = loadSettingDefaultMap(true);
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
        this.actionTypeSettingFilter = loadSettingFilterMap(this.entitySettings != null);
        this.actionTypeSettingDefault = loadSettingDefaultMap(this.entitySettings != null);
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
     * Create an immutable map of action type to the predicate needed to filter relevant
     * entity settings. Move actions' predicates may determine whether to use move or
     * storage move settings.
     *
     * @param hasEntitySettings whether the entitySettingsCache has been initialized. This is
     *                          the case for actions from a LiveActionStore, which may be executed
     *                          if entity settings allow. A PlanActionStore does not load the
     *                          entity settings cache.
     * @return ImmutableMap containing mappings for all supported action types
     */
    private ImmutableMap<ActionTypeCase, Predicate<Setting>> loadSettingFilterMap(
            boolean hasEntitySettings) {
        ImmutableMap.Builder<ActionTypeCase, Predicate<Setting>> builder =
                new ImmutableMap.Builder<>();
        builder.put(ActionTypeCase.RESIZE,
                        (s -> s.getSettingSpecName()
                                .equals(EntitySettingSpecs.Resize.getSettingName())))
                .put(ActionTypeCase.ACTIVATE,
                        (s -> s.getSettingSpecName()
                                .equals(EntitySettingSpecs.Activate.getSettingName())))
                .put(ActionTypeCase.RECONFIGURE,
                        (s -> (s.getSettingSpecName()
                                .equals(EntitySettingSpecs.Reconfigure.getSettingName()) &&
                                s.hasEnumSettingValue() &&
                                ActionMode.valueOf(s.getEnumSettingValue().getValue())
                                        .getNumber() < ActionMode.MANUAL_VALUE)))
                .put(ActionTypeCase.DEACTIVATE,
                        (s -> s.getSettingSpecName()
                                .equals(EntitySettingSpecs.Suspend.getSettingName())));

        // todo: currently if there are no entity settings, there is no way to determine
        // if a move action is a move storage action.
        final EntitySettingSpecs moveType =
                hasEntitySettings ? determineMoveType() : EntitySettingSpecs.Move;

        builder.put(ActionTypeCase.MOVE,
                (s -> s.getSettingSpecName().equals(moveType.getSettingName())));
        return builder.build();
    }

    /**
     * Create an immutable map from action type to the default ActionMode for that type.
     * Move actions' predicates may determine whether to use move or storage move settings.
     *
     * @param hasEntitySettings whether the entitySettingsCache has been initialized. This is
     *                          the case for actions from a LiveActionStore, which may be executed
     *                          if entity settings allow. A PlanActionStore does not load the
     *                          entity settings cache.
     * @return ImmutableMap containing mappings for all supported action types
     */
    private ImmutableMap<ActionTypeCase, ActionMode>
    loadSettingDefaultMap(boolean hasEntitySettings) {
        ImmutableMap.Builder<ActionTypeCase, ActionMode> builder = new ImmutableMap.Builder<>();
        builder.put(ActionTypeCase.RESIZE,
                        getDefaultActionModeFromSetting(EntitySettingSpecs.Resize))
                .put(ActionTypeCase.ACTIVATE,
                        getDefaultActionModeFromSetting(EntitySettingSpecs.Activate))
                .put(ActionTypeCase.RECONFIGURE,
                        getDefaultActionModeFromSetting(EntitySettingSpecs.Reconfigure))
                .put(ActionTypeCase.DEACTIVATE,
                        getDefaultActionModeFromSetting(EntitySettingSpecs.Suspend))
                .put(ActionTypeCase.PROVISION,
                        getDefaultActionModeFromSetting(EntitySettingSpecs.Provision));

        // todo: currently if there are no entity settings, there is no way to determine
        // if a move action is a move storage action.
        final EntitySettingSpecs moveType =
                hasEntitySettings ? determineMoveType() : EntitySettingSpecs.Move;

        builder.put(ActionTypeCase.MOVE, getDefaultActionModeFromSetting(moveType));
        return builder.build();
    }

    /**
     * Find the default value of a EntitySettingSpecs by using the associated spec.
     *
     * @param setting The EntitySettingSpecs to get the default value of
     * @return an ActionMode representing the default value of {@param setting}
     */
    private ActionMode getDefaultActionModeFromSetting(EntitySettingSpecs setting) {
        return ActionMode.valueOf(setting.createSettingSpec().getEnumSettingValueType().getDefault());
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
                final ActionMode mode = calculateActionMode();
                return (mode.getNumber() > ActionMode.RECOMMEND_VALUE) ? ActionMode.RECOMMEND : mode;
            case SUPPORTED:
                return calculateActionMode();
            default:
                throw new IllegalArgumentException("Action SupportLevel is of unrecognized type.");
        }
    }

    /**
     * Based on the automation settings for the entities involved in an action,
     * determine which ActionMode the action should have.
     * @return the applicable ActionMode
     */
    private ActionMode calculateActionMode() {
        // TODO: Determine which settings apply when action involves more than one entity.
        final ActionTypeCase type = recommendation.getInfo().getActionTypeCase();
        final Function<ActionInfo, Long> targetIdGetter = ACTION_TYPE_TARGET_ID_GETTER.get(type);
        final Predicate<Setting> targetSettingFilter = actionTypeSettingFilter.get(type);
        if (targetIdGetter == null || targetSettingFilter == null) {
            // if the action is of a type not supported yet,
            return ActionMode.RECOMMEND;
        }

        final long targetId = targetIdGetter.apply(recommendation.getInfo());
        final List<Setting> targetSettings = entitySettings == null ?
                Collections.emptyList() : entitySettings.getSettingsForEntity(targetId);
        return targetSettings.stream()
                .filter(targetSettingFilter.and(Setting::hasEnumSettingValue))
                .map(s -> ActionMode.valueOf(s.getEnumSettingValue().getValue()).getNumber())
                .min(Integer::compareTo)
                .map(ActionMode::forNumber)
                .orElse(actionTypeSettingDefault.get(type));
    }

    /**
     * Determines if a move action refers to moving between physical machines or storages
     * @return a predicate for filtering settings based on move type
     */
    @Nonnull
    private EntitySettingSpecs determineMoveType() {
        final Optional<EntityType> sourceType = entitySettings
                .getTypeForEntity(recommendation.getInfo().getMove().getSourceId());
        final Optional<EntityType> destType = entitySettings
                .getTypeForEntity(recommendation.getInfo().getMove().getDestinationId());
        if (sourceType.isPresent() && sourceType.get().equals(EntityType.STORAGE) &&
                destType.isPresent() && destType.get().equals(EntityType.STORAGE)) {
            return EntitySettingSpecs.StorageMove;
        } else {
            return EntitySettingSpecs.Move;
        }
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

        final LocalDateTime recommendationTime;

        final ActionDecision actionDecision;

        final ExecutionStep executionStep;

        final ActionState currentState;

        final ActionTranslation actionTranslation;

        public SerializationState(@Nonnull final Action action) {
            this.actionPlanId = action.actionPlanId;
            this.recommendation = action.recommendation;
            this.recommendationTime = action.recommendationTime;
            this.actionDecision = action.decision.getDecision().orElse(null);
            this.executionStep = action.executableStep.map(ExecutableStep::getExecutionStep).orElse(null);
            this.currentState = action.stateMachine.getState();
            this.actionTranslation = action.actionTranslation;
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
        }
    }
}
