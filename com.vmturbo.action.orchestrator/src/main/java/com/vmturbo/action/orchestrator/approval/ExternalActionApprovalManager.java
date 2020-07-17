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
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.common.protobuf.action.ActionDTO.AcceptActionResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.platform.common.dto.ActionExecution.ActionResponseState;
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
     * @param messageReceiver message receiver to track external accept actions
     * @param topologyContextId context id to retrieve action store from action store house
     * @param rejectedActionsDAO for persisting rejected actions
     */
    public ExternalActionApprovalManager(@Nonnull ActionApprovalManager actionApprovalManager,
            @Nonnull ActionStorehouse actionStoreHouse,
            @Nonnull IMessageReceiver<GetActionStateResponse> messageReceiver,
            long topologyContextId, @Nonnull RejectedActionsDAO rejectedActionsDAO) {
        this.actionApprovalManager = Objects.requireNonNull(actionApprovalManager);
        this.actionStoreHouse = Objects.requireNonNull(actionStoreHouse);
        this.topologyContextId = topologyContextId;
        this.rejectedActionsStore = Objects.requireNonNull(rejectedActionsDAO);
        messageReceiver.addListener(this::externalActionStates);
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
        final Optional<Action> action =
                liveActionStore.getActionByRecommendationId(recommendationId);
        if (!action.isPresent()) {
            logger.error("Action with recommendation id ({}) doesn't exist, so external "
                    + "approval is not applied.", recommendationId);
            return;
        }
        final AcceptActionResponse acceptResult =
                actionApprovalManager.attemptAndExecute(liveActionStore, USER_ID, action.get());
        if (acceptResult.hasError()) {
            logger.info("Failed accepting action {}: {}", recommendationId,
                    acceptResult.getError());
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
}
