package com.vmturbo.history.component.api.impl;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import com.google.protobuf.CodedInputStream;

import com.vmturbo.components.api.client.ApiClientException;
import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.components.api.client.ComponentNotificationReceiver;
import com.vmturbo.history.component.api.HistoryComponent;
import com.vmturbo.history.component.api.HistoryComponentNotifications.HistoryComponentNotification;
import com.vmturbo.history.component.api.StatsListener;

public class HistoryComponentNotificationReceiver
        extends ComponentNotificationReceiver<HistoryComponentNotification>
        implements HistoryComponent {

    public static final String WEBSOCKET_PATH = "/history-api";

    private final Set<StatsListener> statsListeners =
            Collections.synchronizedSet(new HashSet<>());

    /**
     * {@inheritDoc}
     */
    public HistoryComponentNotificationReceiver(
            @Nonnull final ComponentApiConnectionConfig connectionConfig,
            @Nonnull final ExecutorService executorService) {
        super(connectionConfig, executorService);
    }

    @Nonnull
    @Override
    protected String addWebsocketPath(@Nonnull final String serverAddress) {
        return serverAddress + WEBSOCKET_PATH;
    }

    @Nonnull
    @Override
    protected HistoryComponentNotification parseMessage(@Nonnull final CodedInputStream bytes) throws IOException {
        return HistoryComponentNotification.parseFrom(bytes);
    }

    @Override
    protected void processMessage(@Nonnull final HistoryComponentNotification message) throws ApiClientException {
        switch (message.getTypeCase()) {
            case STATS_AVAILABLE:
                processNotification((listener) -> listener.onStatsAvailable(message.getStatsAvailable()),
                    "stats available");
                break;
        }
    }

    private void processNotification(@Nonnull final Consumer<StatsListener> listenerConsumer,
                                     @Nonnull final String notificationDescription) {
        statsListeners.forEach(listener -> executorService.submit(() -> {
            try {
                listenerConsumer.accept(listener);
            } catch (RuntimeException e) {
                logger.error("Error executing " + notificationDescription + " notification listener.", e);
            }
        }));
    }

    @Override
    public void addStatsListener(@Nonnull final StatsListener listener) {
        statsListeners.add(listener);
    }
}
