package com.vmturbo.action.orchestrator.action;

import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.action.orchestrator.state.machine.StateMachineEvent;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision.ClearingDecision;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision.ExecutionDecision;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision.ExecutionDecision.Reason;

/**
 * Action events cause {@link Action} state transitions.
 */
public abstract class ActionEvent implements StateMachineEvent {

    protected ActionEvent() {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return getEventName();
    }

    /**
     * An event in the system initiated with authorization from a specific user or
     * other element in the system. Contains the ID of the element in the system
     * that authorized the action. When the event is processed, this ID can be used
     * to verify that the event source actually has authorization for the
     * {@link ActionStateMachine} transition that should result from the event.
     */
    public abstract static class AuthorizedActionEvent extends ActionEvent {
        private final String authorizerUuid;

        protected AuthorizedActionEvent(final String authorizerUuid) {
            this.authorizerUuid = authorizerUuid;
        }

        /**
         * Get the ID of the element in the system that authorized the event.
         * The meaning of the ID should be inferred by the context of the event.
         *
         * @return The ID of the element in the system that initiated/authorized the event.
         *         {@link Optional#empty()} if not associated with a specific authorizer.
         */
        public String getAuthorizerUuid() {
            return authorizerUuid;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString() {
            return getEventName() + " [authorizer: " + authorizerUuid + "]";
        }
    }

    /**
     * An event indicating the acceptance of an action.
     */
    public abstract static class AcceptanceEvent extends AuthorizedActionEvent {
        private final long targetId;

        public AcceptanceEvent(String authorizerId, long targetId) {
            super(authorizerId);
            this.targetId = targetId;
        }

        /**
         * Get a decision describing why the action should be accepted.
         *
         * @return a decision describing why the action should be accepted.
         */
        public ExecutionDecision getDecision() {
            return ExecutionDecision.newBuilder()
                .setUserUuid(getAuthorizerUuid())
                .setReason(getReason())
                .build();
        }

        /**
         * Get the ID of target where the action should be executed.
         *
         * @return the ID of target where the action should be executed.
         */
        public long getTargetId() {
            return targetId;
        }

        /**
         * Get the reason the action was accepted.
         *
         * @return The reason the action was accepted.
         */
        public abstract ExecutionDecision.Reason getReason();
    }

    /**
     * An event generated when a user manually accepts an action.
     * For a manually accepted action, the authorizer ID will be the ID of the user who accepted the action.
     */
    public static class ManualAcceptanceEvent extends AcceptanceEvent {
        public ManualAcceptanceEvent(String acceptingUserUuid, long targetId) {
            super(acceptingUserUuid, targetId);
        }

        @Override
        public ExecutionDecision.Reason getReason() {
            return Reason.MANUALLY_ACCEPTED;
        }
    }

    /**
     * An event generated when a user automatically accepts an action.
     */
    public static class AutomaticAcceptanceEvent extends AcceptanceEvent {
        public AutomaticAcceptanceEvent(String policyCreatingUserUuid, long targetId) {
            super(policyCreatingUserUuid, targetId);
        }

        public ExecutionDecision.Reason getReason() {
            return Reason.AUTOMATICALLY_ACCEPTED;
        }
    }

    /**
     * Indicates that an action was rejected.
     */
    public static class RejectionEvent extends ActionEvent {
        /**
         * Constructor of {@link RejectionEvent}.
         */
        public RejectionEvent() {
            super();
        }
    }

    /**
     * An event generated when an action is ready for execution and it is sent to queue.
     */
    public static class QueuedEvent extends ActionEvent {
        /**
         * Constructor of {@link QueuedEvent}.
         */
        public QueuedEvent() {
            super();
        }
    }

    /**
     * An event generated when acceptance was removed for an action. For instance, when policy
     * associated with action was deleted.
     */
    public static class AcceptanceRemovalEvent extends ActionEvent {
        /**
         * Constructor of {@link AcceptanceRemovalEvent}.
         */
        public AcceptanceRemovalEvent() {
            super();
        }
    }

    /**
     * An event generated when rejection was removed for action. For instance, when policy
     * associated with action was deleted.
     */
    public static class RejectionRemovalEvent extends ActionEvent {
        /**
         * Constructor of {@link AcceptanceRemovalEvent}.
         */
        public RejectionRemovalEvent() {
            super();
        }
    }

    /**
     * An event generated when the action begins to be executed.
     */
    public static class BeginExecutionEvent extends ActionEvent {
        public BeginExecutionEvent() {
            super();
        }
    }

    /**
     * An event generated when the action begins to be prepared for execution.
     * This may involve executing a workflow, if a policy defines a PRE workflow for the action
     */
    public static class PrepareExecutionEvent extends ActionEvent {
        public PrepareExecutionEvent() {
            super();
        }
    }

    /**
     * An event generated when the Action Orchestrator receives a progress
     * report about an action currently being executed.
     */
    public static class ProgressEvent extends ActionEvent {
        private final int progressPercentage;
        private final String progressDescription;

        public ProgressEvent(final int progressPercentage, @Nonnull final String progressDescription) {
            super();
            this.progressPercentage = progressPercentage;
            this.progressDescription = progressDescription;
        }

        public int getProgressPercentage() {
            return progressPercentage;
        }

        public String getProgressDescription() {
            return progressDescription;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString() {
            return getEventName() + " Progress: " + progressPercentage + "% (" + progressDescription + ")";
        }
    }

    /**
     * An event generated when action execution succeeds.
     */
    public static class SuccessEvent extends ActionEvent {
        public SuccessEvent() {
            super();
        }
    }

    /**
     * An event generated when action execution fails.
     *
     * Includes a description of the error that caused the failure.
     */
    public static class FailureEvent extends ActionEvent {
        private final String errorDescription;

        public FailureEvent(String errorDescription) {
            super();

            this.errorDescription = errorDescription;
        }

        public String getErrorDescription() {
            return errorDescription;
        }
    }

    /**
     * An event generated when the action execution has completed successfully.
     * This event is intended to follow the SuccessEvent and allow for cleanup.
     * This may involve executing a workflow, if a policy defines a POST workflow for the action.
     */
    public static class AfterSuccessEvent extends SuccessEvent {

        /**
         * Create an event that signals the action to run a POST workflow, if one is configured
         */
        public AfterSuccessEvent() {
            super();
        }

    }

    /**
     * An event generated when the action execution has failed.
     * This event is intended to follow the FailureEvent and allow for cleanup.
     * This may involve executing a workflow, if a policy defines a POST workflow for the action.
     */
    public static class AfterFailureEvent extends FailureEvent {

        /**
         * Create an event that signals the action to run a POST workflow, if one is configured
         */
        public AfterFailureEvent(String errorDescription) {
            super(errorDescription);
        }

    }

    /**
     * Indicates that an action can't start because there is no
     * target that can execute the action on the entities involved.
     */
    public static class NoTargetResolvedEvent extends FailureEvent {

        public NoTargetResolvedEvent() {
            super("Failed to find a target to execute the action on.");
        }
    }

    /**
     * Indicates that an action is no longer recommended.
     */
    public abstract static class ClearingEvent extends AuthorizedActionEvent {
        public ClearingEvent(String authorizerUuid) {
            super(authorizerUuid);
        }

        /**
         * Get a decision describing why the action should be cleared.
         *
         * @return a decision describing why the action should be cleared.
         */
        public abstract ClearingDecision getDecision();
    }


    /**
     * An event generated when the action executor receives an action
     * plan in which an action in a READY state is no longer recommended
     * after being previously recommended.
     * <p/>
     * The authorizer id is the ID of the actionPlan that no longer recommended
     * the action.
     */
    public static class NotRecommendedEvent extends ClearingEvent {
        /**
         * Create a new NotRecommendedEvent.
         *
         * @param actionPlanId The ID of the action plan that no longer recommends the action.
         */
        public NotRecommendedEvent(final long actionPlanId) {
            super(String.valueOf(actionPlanId));
        }

        @Override
        public ClearingDecision getDecision() {
            return ClearingDecision.newBuilder()
                .setActionPlanId(authorizedUuidToId(getAuthorizerUuid()))
                .setReason(ClearingDecision.Reason.NO_LONGER_RECOMMENDED)
                .build();
        }
    }

    /**
     * An event generated when the action executor receives an action
     * plan in which an action in a READY state is no longer recommended.
     * <p/>
     * The authorizer id will be the ID of the probe that was expected to
     * execute the action but lacks the capability to do so.
     */
    public static class CannotExecuteEvent extends ClearingEvent {
        /**
         * Create a new CannotExecuteEvent.
         *
         * @param probeId The ID of the probe that was expected to execute the action
         *                but lacks the capability to do so.
         */
        public CannotExecuteEvent(final long probeId) {
            super(String.valueOf(probeId));
        }


        @Override
        public ClearingDecision getDecision() {
            return ClearingDecision.newBuilder()
                .setProbeId(authorizedUuidToId(getAuthorizerUuid()))
                .setReason(ClearingDecision.Reason.PROBE_UNABLE_TO_EXECUTE)
                .build();
        }
    }

    /**
     *  Try to convert uuid to id.
     *
     *  actionPlanId: When NO_LONGER_RECOMMENDED, this will be the ID of the first action plan
     *  after the initial recommendation that did not recommend the action anymore.
     *  probeId: When PROBE_UNABLE_TO_EXECUTE, this will be the ID of that probe.
     *
     *  Consideration:
     *  1. From the explanations, both probeId or actionPlanId should be an id.
     *  2. Not sure why both were set with authorizedId. If we want to store who authorized the
     *  decision, should we better to have an optional field, e.g. authorizedId in ActionDTO.proto?
     *  3. Currently authorizerUuid is casted from long (see com.vmturbo.auth.component.store.AuthProvider).
     *  So now it is safe to parse it back to long.
     */
    private static long authorizedUuidToId(String uuid) {
        return Long.parseLong(uuid);
    }
}
