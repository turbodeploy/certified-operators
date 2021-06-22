package com.vmturbo.action.orchestrator.api.impl;

import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import com.vmturbo.action.orchestrator.api.export.ActionRollupExport.ActionRollupNotification;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.client.MulticastNotificationReceiver;

/**
 * Listens to action rollup notifications and tracks historical pending action counts.
 */
public class ActionRollupNotificationReceiver extends MulticastNotificationReceiver<ActionRollupNotification, Consumer<ActionRollupNotification>> {

    /**
     * Create an instance of the ActionRollupNotificationReceiver to track historical pending action counts.
     *
     * @param messageReceiver the receiver to process the incoming message
     * @param executorService a service to schedule the processing
     * @param listenerTimeoutSecs timeout in seconds for processing a single message
     */
    public ActionRollupNotificationReceiver(
            @Nullable final IMessageReceiver<ActionRollupNotification> messageReceiver,
            @NotNull final ExecutorService executorService, final int listenerTimeoutSecs) {
        super(messageReceiver, executorService, listenerTimeoutSecs);
    }
}
