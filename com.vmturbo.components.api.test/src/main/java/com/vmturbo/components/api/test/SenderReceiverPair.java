package com.vmturbo.components.api.test;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nonnull;

import com.google.protobuf.AbstractMessage;

import io.opentracing.SpanContext;

import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.client.TriConsumer;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.components.api.tracing.Tracing;

/**
 * A pair of notification sender and receiver, short-cut between. Use for tests, but in
 * production/integration tests should be substituted with real implementations.
 *
 * @param <T> type of messages to be sent/received
 */
public class SenderReceiverPair<T extends AbstractMessage> implements IMessageSender<T>,
        IMessageReceiver<T> {

    private final Set<TriConsumer<T, Runnable, SpanContext>> consumers =
            Collections.newSetFromMap(new ConcurrentHashMap<>());

    @Override
    public void addListener(@Nonnull TriConsumer<T, Runnable, SpanContext> listener) {
        consumers.add(listener);
    }

    @Override
    public void sendMessage(@Nonnull T serverMsg) {
        for (TriConsumer<T, Runnable, SpanContext> consumer : consumers) {
            consumer.accept(serverMsg, () -> { }, Tracing.trace("send_msg").spanContext());
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
