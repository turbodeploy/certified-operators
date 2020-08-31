package com.vmturbo.action.orchestrator.approval;

import java.time.LocalDateTime;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import io.opentracing.SpanContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionEvent.RejectionEvent;
import com.vmturbo.action.orchestrator.action.RejectedActionsDAO;
import com.vmturbo.action.orchestrator.exception.ActionStoreOperationException;
import com.vmturbo.action.orchestrator.exception.ExecutionInitiationException;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.platform.common.dto.ActionExecution.ActionResponseState;
import com.vmturbo.platform.common.dto.ActionExecution.ExternalActionInfo;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionApprovalResponse;
import com.vmturbo.platform.sdk.common.MediationMessage.GetActionStateResponse;

/**
 * Manager to handle approvals from external action approval backends.
 */
public class ExternalActionApprovalManager {
    protected static final String USER_ID = "External action approval";

    private final ActionApprovalManager actionApprovalManager;
    private final Logger logger = LogManager.getLogger(getClass());
    private final ActionStorehouse actionStoreHouse;
    private final long topologyContextId;
    private final RejectedActionsDAO rejectedActionsStore;

    /**
     * Constructs the manager.
     *
     * @param actionApprovalManager action approval manager
     * @param actionStoreHouse actions store house
     * @param getActionStateResponseReceiver message receiver to track external accept actions
     * @param actionApprovalResponseReceiver message receiver to track creation of approval in
     *                                       external system like ServiceNOW.
     * @param topologyContextId context id to retrieve action store from action store house
     * @param rejectedActionsDAO for persisting rejected actions
     */
    public ExternalActionApprovalManager(@Nonnull ActionApprovalManager actionApprovalManager,
            @Nonnull ActionStorehouse actionStoreHouse,
            @Nonnull IMessageReceiver<GetActionStateResponse> getActionStateResponseReceiver,
            @Nonnull IMessageReceiver<ActionApprovalResponse> actionApprovalResponseReceiver,
            long topologyContextId,
            @Nonnull RejectedActionsDAO rejectedActionsDAO) {
        this.actionApprovalManager = Objects.requireNonNull(actionApprovalManager);
        this.actionStoreHouse = Objects.requireNonNull(actionStoreHouse);
        this.topologyContextId = topologyContextId;
        this.rejectedActionsStore = Objects.requireNonNull(rejectedActionsDAO);
        getActionStateResponseReceiver.addListener(this::externalActionStates);
        actionApprovalResponseReceiver.addListener(this::externalActionsCreated);
    }

    private void externalActionStates(@Nonnull GetActionStateResponse externalStates,
            @Nonnull Runnable commit, @Nonnull SpanContext tracingContext) {
        logger.debug("Received the following states from external action approval: {}",
                externalStates.getActionStateMap()
                        .entrySet()
                        .stream()
                        .collect(Collectors.groupingBy(Entry::getValue)));
        if (externalStates.getActionStateCount() == 0) {
            commit.run();
            return;
        }
        final Optional<ActionStore> liveActionStore = actionStoreHouse.getStore(topologyContextId);
        if (!liveActionStore.isPresent()) {
            logger.info("There is no live action store yet. Skipping action approval procedure");
            // Do not commit.run() because we did not process the response yet and are not ready
            // to process it yet.
            return;
        }
        for (Entry<Long, ActionResponseState> entry : externalStates.getActionStateMap()
                .entrySet()) {
            final long recommendationId = entry.getKey();
            final ActionResponseState actionState = entry.getValue();
            switch (actionState) {
                case ACCEPTED:
                    processAcceptedAction(liveActionStore.get(), recommendationId);
                    break;
                case REJECTED:
                    processRejectedAction(liveActionStore.get(), recommendationId);
                    break;
            }
        }
        commit.run();
    }

    private void processAcceptedAction(@Nonnull ActionStore liveActionStore,
            long recommendationId) {
        final Optional<Action> actionOpt =
                liveActionStore.getActionByRecommendationId(recommendationId);
        if (!actionOpt.isPresent()) {
            logger.error("Action with recommendation id ({}) doesn't exist, so external "
                    + "approval is not applied.", recommendationId);
            return;
        }
        final Action action = actionOpt.get();
        if (action.getMode() != ActionDTO.ActionMode.EXTERNAL_APPROVAL) {
            logger.info("Action with recommendation id ({}) and mode of {} and state of {} is"
                + " returned from approval backened as approved but will not get executed.",
                recommendationId, action.getMode(), action.getState());
            return;
        }
        try {
            if (action.getState() != ActionState.ACCEPTED) {
                actionApprovalManager.attemptAndExecute(liveActionStore, USER_ID, action);
            }
        } catch (ExecutionInitiationException ex) {
            logger.info("Failed accepting action {}: {}", recommendationId,
                ex.toString(), ex);
        }
    }

    private void processRejectedAction(@Nonnull final ActionStore actionStore,
            long recommendationOid) {
        final Optional<Action> rejectedActionOpt =
                actionStore.getActionByRecommendationId(recommendationOid);
        if (rejectedActionOpt.isPresent()) {
            final Action rejectedAction = rejectedActionOpt.get();
            if (rejectedAction.getState() != ActionState.REJECTED) {
                if (rejectedAction.receive(new RejectionEvent()).transitionNotTaken()) {
                    logger.error("Failed to transit action {} to REJECTED state.",
                            rejectedAction.getId());
                } else {
                    try {
                        rejectedActionsStore.persistRejectedAction(
                                rejectedAction.getRecommendationOid(), USER_ID, LocalDateTime.now(),
                                StringConstants.EXTERNAL_ORCHESTRATOR_USER_TYPE,
                                rejectedAction.getAssociatedSettingsPolicies());
                    } catch (ActionStoreOperationException e) {
                        logger.error("Failed to persist rejection for action {}.",
                                rejectedAction.getId(), e);
                    }
                }
            } else {
                logger.warn("Action {} was already rejected.", rejectedAction.getId());
            }
        } else {
            logger.error("Action with recommendation id ({}) doesn't exist, so external "
                    + "rejection is not applied.", recommendationOid);
        }
    }

    private void externalActionsCreated(@Nonnull ActionApprovalResponse createdApprovalsResponse,
                                       @Nonnull Runnable commit, @Nonnull SpanContext tracingContext) {
        // none of the approvals were created
        if (createdApprovalsResponse.getActionStateMap().isEmpty()) {
            commit.run();
            return;
        }

        final Optional<ActionStore> liveActionStoreOpt = actionStoreHouse.getStore(topologyContextId);
        if (!liveActionStoreOpt.isPresent()) {
            logger.info("There is no live action store yet. Skipping recording creating actions.");
            // Do not commit.run() because we did not process the response yet and are not ready
            // to process it yet.
            return;
        }
        final ActionStore liveActionStore = liveActionStoreOpt.get();

        for (Entry<Long, ExternalActionInfo> entry : createdApprovalsResponse.getActionStateMap()
                .entrySet()) {
            final long recommendationOid = entry.getKey();
            final ExternalActionInfo externalActionInfo = entry.getValue();
            final Optional<Action> actionOptional = liveActionStore.getActionByRecommendationId(recommendationOid);
            // The action might not be recommended by the market anymore
            if (actionOptional.isPresent()) {
                Action action = actionOptional.get();
                action.setExternalActionName(externalActionInfo.getShortName());
                action.setExternalActionUrl(externalActionInfo.getUrl());
            } else {
                logger.debug("Action approval created in external approval with,"
                        + " but the action was not found in the live actions store."
                        + " oid={},"
                        + " shortName={},"
                        + " url={}",
                    recommendationOid,
                    externalActionInfo.getShortName(),
                    externalActionInfo.getUrl());
            }
        }

        commit.run();
    }

}
