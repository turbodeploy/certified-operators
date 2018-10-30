package com.vmturbo.components.api.server;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import com.google.protobuf.AbstractMessage;
import com.google.protobuf.Empty;

import com.vmturbo.communication.ITransport;
import com.vmturbo.communication.WebsocketServerTransportManager;

/**
 * Utility class to create wbeosket transports.
 */
public class BroadcastWebsocketTransportManager {

    /**
     * Constructs trabsport manager, that will inform all the senders of a new transport as soon
     * as it appears.
     *
     * @param threadPool thread pool to use
     * @param senders senders to inform
     * @param <T> type of a message to send
     * @return websocket endpoint
     */
    public static <T extends AbstractMessage> WebsocketServerTransportManager createTransportManager(
            @Nonnull ExecutorService threadPool,
            @Nonnull Collection<IByteTransferNotificationSender<T>> senders) {
        return new WebsocketServerTransportManager((transport) -> {
            final ITransport<T, Empty> endpoint = new NotificationProtobufEndpoint<>(transport);
            for (IByteTransferNotificationSender<T> sender : senders) {
                sender.addTransport(endpoint);
            }
        }, threadPool, 30L);
    }

    /**
     * Constructs trabsport manager, that will inform all the senders of a new transport as soon
     * as it appears.
     *
     * @param threadPool thread pool to use
     * @param senders senders to inform
     * @param <T> type of a message to send
     * @return websocket endpoint
     */
    public static <T extends AbstractMessage> WebsocketServerTransportManager createTransportManager(
            @Nonnull ExecutorService threadPool,
            @Nonnull IByteTransferNotificationSender<T>... senders) {
        return createTransportManager(threadPool, Arrays.asList(senders));
    }
}
