package com.vmturbo.components.api.test;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

import javax.annotation.Nonnull;

import com.google.protobuf.AbstractMessage;

import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.server.IMessageSender;

/**
 * A pair of notification sender and receiver, short-cut between. Use for tests, but in
 * production/integration tests should be substituted with real implementations.
 *
 * @param <T> type of messages to be sent/received
 */
public class SenderReceiverPair<T extends AbstractMessage> implements IMessageSender<T>,
        IMessageReceiver<T> {

    private final Set<BiConsumer<T, Runnable>> consumers =
            Collections.newSetFromMap(new ConcurrentHashMap<>());

    @Override
    public void addListener(@Nonnull BiConsumer<T, Runnable> listener) {
        consumers.add(listener);
    }

    @Override
    public void sendMessage(@Nonnull T serverMsg) {
        for (BiConsumer<T, Runnable> consumer : consumers) {
            consumer.accept(serverMsg, () -> {});
        }
    }

    @Override
    public int getMaxRequestSizeBytes() {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getRecommendedRequestSizeBytes() {
        return Integer.MAX_VALUE;
    }
}
