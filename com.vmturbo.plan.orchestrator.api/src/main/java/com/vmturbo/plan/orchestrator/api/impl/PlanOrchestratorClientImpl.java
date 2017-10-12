package com.vmturbo.plan.orchestrator.api.impl;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

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
        ComponentNotificationReceiver<PlanInstance> implements PlanOrchestrator {

    public static final String STATUS_CHANGED_TOPIC = "plan-orchestrator-status-changed";

    private final Set<PlanListener> listeners =
            Collections.newSetFromMap(new ConcurrentHashMap<>());

    public PlanOrchestratorClientImpl(@Nonnull final IMessageReceiver<PlanInstance> messageReceiver,
            @Nonnull final ExecutorService executorService) {
        super(messageReceiver, executorService);
    }

    @Override
    protected void processMessage(@Nonnull PlanInstance message) throws ApiClientException {
        getLogger().debug("Received plan instance {}", message.hasPlanId());
        for (PlanListener listener : listeners) {
            getExecutorService().submit(() -> listener.onPlanStatusChanged(message));
        }
    }

    @Override
    public void addPlanListener(@Nonnull final PlanListener listener) {
        listeners.add(Objects.requireNonNull(listener, "Listener should not be null"));
    }
}
