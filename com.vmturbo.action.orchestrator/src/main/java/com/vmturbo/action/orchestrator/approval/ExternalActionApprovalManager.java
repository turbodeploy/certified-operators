package com.vmturbo.action.orchestrator.approval;

import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.common.protobuf.action.ActionDTO.AcceptActionResponse;
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

    /**
     * Constructs the manager.
     *
     * @param actionApprovalManager action approval manager
     * @param actionStoreHouse actions store house
     * @param messageReceiver message receiver to track external accept actions
     * @param topologyContextId context id to retrieve action store from action store house
     */
    public ExternalActionApprovalManager(@Nonnull ActionApprovalManager actionApprovalManager,
            @Nonnull ActionStorehouse actionStoreHouse,
            @Nonnull IMessageReceiver<GetActionStateResponse> messageReceiver,
            long topologyContextId) {
        this.actionApprovalManager = Objects.requireNonNull(actionApprovalManager);
        this.actionStoreHouse = Objects.requireNonNull(actionStoreHouse);
        this.topologyContextId = topologyContextId;
        messageReceiver.addListener(this::externalActionStates);
    }

    private void externalActionStates(@Nonnull GetActionStateResponse externalStates,
            @Nonnull Runnable commit) {
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
            final long actionId = entry.getKey();
            if (entry.getValue() == ActionResponseState.ACCEPTED) {
                final AcceptActionResponse acceptResult = actionApprovalManager.attemptAndExecute(
                        liveActionStore.get(), USER_ID, actionId);
                if (acceptResult.hasError()) {
                    logger.info("Failed accepting action {}: {}", actionId,
                            acceptResult.getError());
                }
            }
        }
        commit.run();
    }
}
