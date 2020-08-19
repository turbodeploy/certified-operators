package com.vmturbo.action.orchestrator.approval;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import io.grpc.Status;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.action.AcceptedActionsDAO;
import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.BeginExecutionEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.FailureEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.PrepareExecutionEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.QueuedEvent;
import com.vmturbo.action.orchestrator.action.ActionSchedule;
import com.vmturbo.action.orchestrator.exception.ActionStoreOperationException;
import com.vmturbo.action.orchestrator.exception.ExecutionInitiationException;
import com.vmturbo.action.orchestrator.execution.ActionExecutor;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector.ActionTargetInfo;
import com.vmturbo.action.orchestrator.execution.ExecutionStartException;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStore;
import com.vmturbo.auth.api.auditing.AuditAction;
import com.vmturbo.auth.api.auditing.AuditLog;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.AcceptActionResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;

/**
 * Action approval manager is responsible for accepting actions that are pending acceptance.
 */
public class ActionApprovalManager {

    private final Logger logger = LogManager.getLogger(getClass());

    /**
     * To execute actions (by sending them to Topology Processor).
     */
    private final ActionExecutor actionExecutor;

    /**
     * For selecting which target/probe to execute each action against.
     */
    private final ActionTargetSelector actionTargetSelector;

    /**
     * An entity snapshot factory used for creating entity snapshot.
     * It is now only used for creating empty entity snapshot.
     */
    private final EntitiesAndSettingsSnapshotFactory entitySettingsCache;

    /**
     * To translate an action from the market's domain-agnostic form to the domain-specific form
     * relevant for execution and display in the real world.
     */
    private final ActionTranslator actionTranslator;

    /**
     * the store for all the known {@link WorkflowDTO.Workflow} items.
     */
    private final WorkflowStore workflowStore;
    private final AcceptedActionsDAO acceptedActionsStore;

    /**
     * Constructs action approval manager.
     *
     * @param actionExecutor action executor to run actions
     * @param actionTargetSelector selector to detect target to execute actions on
     * @param entitySettingsCache cache of entity settings
     * @param actionTranslator action translator
     * @param workflowStore workflow store
     * @param acceptedActionsStore accepted actions store
     */
    public ActionApprovalManager(@Nonnull ActionExecutor actionExecutor,
            @Nonnull ActionTargetSelector actionTargetSelector,
            @Nonnull EntitiesAndSettingsSnapshotFactory entitySettingsCache,
            @Nonnull ActionTranslator actionTranslator, @Nonnull WorkflowStore workflowStore,
            @Nonnull AcceptedActionsDAO acceptedActionsStore) {
        this.actionExecutor = Objects.requireNonNull(actionExecutor);
        this.actionTargetSelector = Objects.requireNonNull(actionTargetSelector);
        this.entitySettingsCache = Objects.requireNonNull(entitySettingsCache);
        this.actionTranslator = Objects.requireNonNull(actionTranslator);
        this.workflowStore = Objects.requireNonNull(workflowStore);
        this.acceptedActionsStore = Objects.requireNonNull(acceptedActionsStore);
    }

    /**
     * Attempts executing the specified action. It is implied that actions is in MANUAL
     * action execution mode, so this method will trigger execution (perform manual approval of
     * the action).
     *
     * @param store action store
     * @param userNameAndUuid ID of a user accepting the action
     * @param action the action to accept
     * @return action acceptance response
     * @throws ExecutionInitiationException if something goes wrong in the process of starting
     * the execution of the action.
     */
    @Nonnull
    public AcceptActionResponse attemptAndExecute(@Nonnull ActionStore store,
            @Nonnull String userNameAndUuid, @Nonnull Action action) throws ExecutionInitiationException {
        final ActionState actionState = action.getState();
        if (actionState != ActionState.READY) {
            throw new ExecutionInitiationException(
                "Only action with READY state can be accepted. Action " + action.getId()
                + " has " + actionState + " state.", Status.Code.INVALID_ARGUMENT);
        }

        final AcceptActionResponse attemptResponse = attemptAcceptAndExecute(action,
                userNameAndUuid);
        if (!action.isReady()) {
            store.getEntitySeverityCache().ifPresent(severityCache ->
                severityCache.refresh(action.getTranslationResultOrOriginal(), store));
        }
        AuditLog.newEntry(AuditAction.EXECUTE_ACTION, action.getDescription(), true)
                .targetName(String.valueOf(action.getId()))
                .audit();
        return attemptResponse;
    }

    /**
     * Attempt to accept and execute the action.
     *
     * @param action action to accept
     * @param userUuid user trying to accept
     * @return The result of attempting to accept and execute the action.
     * @throws ExecutionInitiationException if cannot initiate the action execution.
     */
    @Nonnull
    private AcceptActionResponse attemptAcceptAndExecute(@Nonnull final Action action,
            @Nonnull final String userUuid) throws ExecutionInitiationException {
        long actionTargetId = -1;
        Optional<FailureEvent> failure = Optional.empty();
        if (action.getSchedule().isPresent()) {
            final Optional<AcceptActionResponse> errors =
                    persistAcceptanceForActionWithSchedule(action, userUuid);
            if (errors.isPresent()) {
                return errors.get();
            }
        }

        ActionTargetInfo actionTargetInfo = actionTargetSelector.getTargetForAction(
                action.getTranslationResultOrOriginal(), entitySettingsCache);
        if (actionTargetInfo.supportingLevel() == SupportLevel.SUPPORTED && action.getMode()
                .getNumber() >= ActionMode.MANUAL_VALUE) {
            // Target should be set if support level is "supported".
            actionTargetId = actionTargetInfo.targetId().get();
        } else {
            failure = Optional.of(new FailureEvent(
                    "Action cannot be executed by any target. Support level: "
                            + actionTargetInfo.supportingLevel() + "Action mode: "
                            + action.getMode()));
        }

        failure.ifPresent(failureEvent -> logger.error("Failed to accept action: {}",
                failureEvent.getErrorDescription()));

        if (action.receive(new ActionEvent.ManualAcceptanceEvent(userUuid, actionTargetId))
                .transitionNotTaken()) {
            throw new ExecutionInitiationException("Unauthorized to accept action in mode " + action.getMode(), Status.Code.PERMISSION_DENIED);
        }

        if (action.getSchedule().isPresent() && !action.getSchedule().get().isActiveScheduleNow()) {
            // postpone action execution, because action has related execution window
            return AcceptActionResponse.newBuilder()
                    .setActionSpec(actionTranslator.translateToSpec(action))
                    .build();
        } else {
            action.receive(new QueuedEvent());
        }

        return handleTargetResolution(action, actionTargetId, failure);
    }

    private AcceptActionResponse handleTargetResolution(@Nonnull final Action action,
            final long targetId, @Nonnull final Optional<FailureEvent> failure) throws ExecutionInitiationException {
        if (failure.isPresent()) {
            action.receive(failure.get());
            throw new ExecutionInitiationException(failure.get().getErrorDescription(), Status.Code.INTERNAL);
        } else {
            return attemptActionExecution(action, targetId);
        }
    }

    private AcceptActionResponse attemptActionExecution(@Nonnull final Action action,
            final long targetId) throws ExecutionInitiationException {
        try {
            // A prepare event prepares the action for execution, and initiates a PRE
            // workflow if one is associated with this action.
            action.receive(new PrepareExecutionEvent());
            // Allows the action to begin execution, if a PRE workflow is not running
            action.receive(new BeginExecutionEvent());
            final Optional<ActionDTO.Action> translatedRecommendation =
                    action.getActionTranslation()
                            .getTranslatedRecommendation();
            if (translatedRecommendation.isPresent()) {
                // execute the action, passing the workflow override (if any)
                actionExecutor.execute(targetId, translatedRecommendation.get(),
                        action.getWorkflow(workflowStore));
                AuditLog.newEntry(AuditAction.EXECUTE_ACTION, action.getDescription(), true)
                        .targetName(String.valueOf(action.getId()))
                        .audit();
                return AcceptActionResponse.newBuilder()
                        .setActionSpec(actionTranslator.translateToSpec(action))
                        .build();
            } else {
                final String errorMsg = String.format(
                        "Failed to translate action %d for execution.", action.getId());
                logger.error(errorMsg);
                action.receive(new FailureEvent(errorMsg));
                throw new ExecutionInitiationException(errorMsg, Status.Code.INTERNAL);
            }
        } catch (ExecutionStartException e) {
            logger.error("Failed to start action {} due to error {}.", action.getId(), e);
            action.receive(new FailureEvent(e.getMessage()));
            throw new ExecutionInitiationException(e.toString(), e, Status.Code.INTERNAL);
        }
    }

    private Optional<AcceptActionResponse> persistAcceptanceForActionWithSchedule(
            @Nonnull Action action, @Nonnull String acceptingUser) throws ExecutionInitiationException {
        boolean isFailedPersisting = false;
        try {
            if (!action.getAssociatedSettingsPolicies().isEmpty()) {
                final LocalDateTime currentTime = LocalDateTime.now();
                final String acceptingUserType;
                if (action.getMode() == ActionMode.EXTERNAL_APPROVAL) {
                    acceptingUserType = StringConstants.EXTERNAL_ORCHESTRATOR_USER_TYPE;
                } else {
                    acceptingUserType = StringConstants.TURBO_USER_TYPE;
                }
                acceptedActionsStore.persistAcceptedAction(action.getRecommendationOid(),
                        currentTime, acceptingUser, currentTime, acceptingUserType,
                        action.getAssociatedSettingsPolicies());
                logger.info("Successfully persisted acceptance for action `{}` accepted by {}({}) "
                                + "at {}", action.getId(), acceptingUser, acceptingUserType,
                        currentTime);
                // we should update accepting user in action schedule otherwise we
                // couldn't see in UI that this action is accepted
                updateAcceptingUserForSchedule(action, acceptingUser);
            } else {
                logger.error(
                        "There are no associated policies for action {} . Acceptance for action"
                                + " was not persisted.", action.getId());
                isFailedPersisting = true;
            }
        } catch (ActionStoreOperationException e) {
            logger.error("Failed to persist acceptance for action {}", action.getId(), e);
            isFailedPersisting = true;
        }
        if (isFailedPersisting) {
            throw new ExecutionInitiationException("Failed to persist acceptance for action " + action.getId(), Status.Code.INTERNAL);
        } else {
            return Optional.empty();
        }
    }

    private void updateAcceptingUserForSchedule(@Nonnull Action action,
            @Nonnull String acceptingUser) {
        final Optional<ActionSchedule> currentSchedule = action.getSchedule();
        if (currentSchedule.isPresent()) {
            final ActionSchedule schedule = currentSchedule.get();
            final ActionSchedule updatedSchedule =
                    new ActionSchedule(schedule.getScheduleStartTimestamp(),
                            schedule.getScheduleEndTimestamp(), schedule.getScheduleTimeZoneId(),
                            schedule.getScheduleId(), schedule.getScheduleDisplayName(),
                            schedule.getExecutionWindowActionMode(), acceptingUser);
            action.setSchedule(updatedSchedule);
        } else {
            logger.warn("Failed to update accepting user for action `{}` which doesn't have "
                    + "associated schedule.", action.toString());
        }
    }
}
