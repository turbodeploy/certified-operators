package com.vmturbo.components.api.localbus;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import io.opentracing.SpanContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.components.api.client.TriConsumer;
import com.vmturbo.components.api.tracing.Tracing;
import com.vmturbo.components.api.tracing.Tracing.TracingScope;

/**
 * A single topic on a {@link LocalBus}. Deals with a particular message type. Can have any
 * number of consumers.
 *
 * <p/>The implementation is basically a single concurrent queue per consumer, and a single consumer
 * thread that listens polls that queue and forwards messages to the consumer. No frills, no back
 * massages.
 *
 * @param <T> The message type.
 */
@ThreadSafe
class LocalBusTopic<T> {
    private static final Logger logger = LogManager.getLogger();

    /**
     * Executor for consumer threads that are listening on the topics.
     */
    private final ExecutorService executorService;

    /**
     * Controls access to the consumers map.
     */
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    @GuardedBy("rwLock")
    private final Map<TriConsumer<T, Runnable, SpanContext>, TopicListener<T>> listeners = new HashMap<>();

    /**
     * Name of the topic.
     */
    private final String topicName;

    LocalBusTopic(@Nonnull final ExecutorService executorService,
                  @Nonnull final String topicName) {
        this.executorService = executorService;
        this.topicName = Objects.requireNonNull(topicName);
    }

    /**
     * Send a message to this topic.
     *
     * @param serverMsg The message to send.
     */
    void sendMessage(final T serverMsg) {
        final Lock lock = rwLock.readLock();
        try {
            lock.lock();
            // Forward the message to each consumer.
            listeners.values().forEach(consumer -> consumer.sendMessage(serverMsg));
        } finally {
            lock.unlock();
        }
    }

    /**
     * Add a listener to the topic.
     *
     * @param listener The listener.
     */
    public void addListener(@Nonnull final TriConsumer<T, Runnable, SpanContext> listener) {
        rwLock.writeLock().lock();
        try {
            listeners.computeIfAbsent(listener, k -> {
                final TopicListener<T> topicListener = new TopicListener<>(listener,
                    topicName + "/listener-" + listeners.size());
                executorService.submit(topicListener);
                return topicListener;
            });
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Information kept for every consumer/listener of this topic.
     *
     * @param <T> The message type of the topic.
     */
    private static class TopicListener<T> implements Runnable {

        private final TriConsumer<T, Runnable, SpanContext> consumerRunnable;

        private final String topicName;

        private final AtomicLong messageIndex = new AtomicLong();

        /**
         * Sender enqueues messages here. Receiver dequeues them. No backpressure, and no trimming
         * of old data - we assume receiver always keeps up (or drains unnecessary data).
         */
        private final LinkedBlockingDeque<MessageContext<T>> queue = new LinkedBlockingDeque<>();

        private TopicListener(@Nonnull final TriConsumer<T, Runnable, SpanContext> consumerRunnable,
                              @Nonnull final String topicName) {
            this.consumerRunnable = consumerRunnable;
            this.topicName = Objects.requireNonNull(topicName);
        }

        /**
         * Send a message to this listener.
         *
         * @param serverMsg The message.
         */
        public void sendMessage(final T serverMsg) {
            try (TracingScope tracingScope = Tracing.trace(topicName + "/message-" + messageIndex.getAndIncrement())) {
                final MessageContext<T> msg = new MessageContext<>(serverMsg, tracingScope.spanContext());
                queue.add(msg);
            }
        }

        /**
         * Poll the queue indefinitely, forwarding messages to the consumer runnable.
         * Meant to be run on a different thread.
         */
        @Override
        public void run() {
            while (true) {
                try {
                    MessageContext<T> polledObj = queue.poll(1, TimeUnit.MINUTES);
                    if (polledObj == null) {
                        continue;
                    }
                    try {
                        consumerRunnable.accept(polledObj.message, () -> { }, polledObj.tracingContext);
                    } catch (RuntimeException e) {
                        logger.warn("Listener failed to process message.", e);
                    }
                } catch (InterruptedException e) {
                    return;
                }
            }
        }

        /**
         * Tiny internal helper class to bundle a message with a distributed tracing context.
         *
         * @param <MessageT> The type of the message.
         */
        private static class MessageContext<MessageT> {
            private final MessageT message;
            private final SpanContext tracingContext;

            /**
             * Create a new message context.
             *
             * @param message The message.
             * @param tracingContext The distributed tracing context for the message.
             */
            private MessageContext(@Nonnull final MessageT message,
                                  @Nonnull final SpanContext tracingContext) {
                this.message = Objects.requireNonNull(message);
                this.tracingContext = Objects.requireNonNull(tracingContext);
            }
        }
    }
}
