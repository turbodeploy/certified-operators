package com.vmturbo.components.api.localbus;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
    private final Map<BiConsumer<T, Runnable>, TopicListener<T>> listeners = new HashMap<>();

    LocalBusTopic(@Nonnull final ExecutorService executorService) {
        this.executorService = executorService;
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
    public void addListener(@Nonnull final BiConsumer<T, Runnable> listener) {
        rwLock.writeLock().lock();
        try {
            listeners.computeIfAbsent(listener, k -> {
                final TopicListener<T> topicListener = new TopicListener<>(listener);
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

        private final BiConsumer<T, Runnable> consumerRunnable;

        /**
         * Sender enqueues messages here. Receiver dequeues them. No backpressure, and no trimming
         * of old data - we assume receiver always keeps up (or drains unnecessary data).
         */
        private final LinkedBlockingDeque<T> queue = new LinkedBlockingDeque<>();

        private TopicListener(@Nonnull final BiConsumer<T, Runnable> consumerRunnable) {
            this.consumerRunnable = consumerRunnable;
        }

        /**
         * Send a message to this listener.
         *
         * @param serverMsg The message.
         */
        public void sendMessage(final T serverMsg) {
            queue.add(serverMsg);
        }

        /**
         * Poll the queue indefinitely, forwarding messages to the consumer runnable.
         * Meant to be run on a different thread.
         */
        @Override
        public void run() {
            while (true) {
                try {
                    T polledObj = queue.poll(1, TimeUnit.MINUTES);
                    if (polledObj == null) {
                        continue;
                    }
                    try {
                        consumerRunnable.accept(polledObj, () -> {
                        });
                    } catch (RuntimeException e) {
                        logger.warn("Listener failed to process message.", e);
                    }
                } catch (InterruptedException e) {
                    return;
                }
            }
        }
    }
}
