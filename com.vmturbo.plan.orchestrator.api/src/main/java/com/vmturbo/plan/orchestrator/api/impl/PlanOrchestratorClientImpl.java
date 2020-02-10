package com.vmturbo.plan.orchestrator.api.impl;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.components.api.client.ApiClientException;
import com.vmturbo.components.api.client.ComponentNotificationReceiver;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.client.MulticastNotificationReceiver;
import com.vmturbo.plan.orchestrator.api.PlanListener;
import com.vmturbo.plan.orchestrator.api.PlanOrchestrator;

/**
 * Implementation of plan orchestrator client.
 */
public class PlanOrchestratorClientImpl extends
        MulticastNotificationReceiver<PlanInstance, PlanListener> implements PlanOrchestrator {

    private static final Logger logger = LogManager.getLogger();

    /**
     * The topic name for plan orchestrator statuses
     */
    public static final String STATUS_CHANGED_TOPIC = "plan-orchestrator-status-changed";

    public PlanOrchestratorClientImpl(@Nonnull final IMessageReceiver<PlanInstance> messageReceiver,
            @Nonnull final ExecutorService executorService, int kafkaReceiverTimeoutSeconds) {
        super(messageReceiver, executorService, kafkaReceiverTimeoutSeconds, message -> {
            logger.debug("Received plan instance {}", message.hasPlanId());
            return listener -> listener.onPlanStatusChanged(message);
        });
    }

    @Override
    public void addPlanListener(@Nonnull final PlanListener planListener) {
        addListener(planListener);
    }
}
