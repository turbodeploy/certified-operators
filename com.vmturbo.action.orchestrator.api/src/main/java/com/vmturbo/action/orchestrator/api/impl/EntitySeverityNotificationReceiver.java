package com.vmturbo.action.orchestrator.api.impl;

import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import io.opentracing.SpanContext;

import com.vmturbo.action.orchestrator.api.EntitySeverityListener;
import com.vmturbo.common.protobuf.action.EntitySeverityNotificationOuterClass.EntitySeverityNotification;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.client.MulticastNotificationReceiver;

/**
 * Receives notifications about entity severities and forwards them to listeners.
 */
public class EntitySeverityNotificationReceiver extends MulticastNotificationReceiver<EntitySeverityNotification, EntitySeverityListener> {

    protected EntitySeverityNotificationReceiver(
            @Nonnull IMessageReceiver<EntitySeverityNotification> messageReceiver,
            @Nonnull ExecutorService executorService, int listenerTimeoutSecs) {
        super(messageReceiver, executorService, listenerTimeoutSecs);
    }

    @Override
    protected void processMessage(@Nonnull final EntitySeverityNotification message,
            @Nonnull final SpanContext tracingContext) {
        if (message.getFullRefresh()) {
            invokeListeners(listener -> {
                listener.entitySeveritiesRefresh(message.getEntitiesWithSeverityList(),
                        message.getSeverityBreakdownsMap());
            });
        } else {
            invokeListeners(listener -> {
                listener.entitySeveritiesUpdate(message.getEntitiesWithSeverityList(),
                        message.getSeverityBreakdownsMap());
            });
        }
    }
}
