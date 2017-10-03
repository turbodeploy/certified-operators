package com.vmturbo.plan.orchestrator.api.impl;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import com.vmturbo.action.orchestrator.api.PlanOrchestratorDTO.PlanNotification;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.components.api.client.ApiClientException;
import com.vmturbo.components.api.client.ComponentNotificationReceiver;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.plan.orchestrator.api.PlanListener;
import com.vmturbo.plan.orchestrator.api.PlanOrchestrator;

/**
 * Implementation of plan orchestrator client.
 */
public class PlanOrchestratorClientImpl extends
        ComponentNotificationReceiver<PlanNotification> implements PlanOrchestrator {

    public static final String WEBSOCKET_PATH = "/planOperations";

    private final Set<PlanListener> listeners =
            Collections.newSetFromMap(new ConcurrentHashMap<>());

    public PlanOrchestratorClientImpl(@Nonnull final IMessageReceiver<PlanNotification> messageReceiver,
            @Nonnull final ExecutorService executorService) {
        super(messageReceiver, executorService);
    }

    @Override
    protected void processMessage(@Nonnull PlanNotification message) throws ApiClientException {
        getLogger().debug("Received message {} of type {}", message.getBroadcastId(),
                message.getTypeCase());
        switch (message.getTypeCase()) {
            case STATUS_CHANGED:
                onStatusChanged(message.getStatusChanged());
                break;
            default:
                getLogger().error("Unknown message type received: {}",
                        message.getTypeCase());
        }
    }

    private void onStatusChanged(PlanInstance planInstance) {
        for (PlanListener listener : listeners) {
            getExecutorService().submit(() -> listener.onPlanStatusChanged(planInstance));
        }
    }

    @Override
    public void addPlanListener(@Nonnull final PlanListener listener) {
        listeners.add(Objects.requireNonNull(listener, "Listener should not be null"));
    }
}
