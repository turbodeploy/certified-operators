package com.vmturbo.components.api.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.AbstractMessage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A {@link ComponentNotificationReceiver} that supports parallel distribution to a set of listeners.
 *
 * <p>If only one listener is attached, then the listener will be called on the current thread. If there
 * are multiple listeners, then the listeners will be called using a thread scheduler.
 *
 * @param <MSG_TYPE> The protobuf message type that the receiver will be handling.
 * @param <LISTENER> The class of listener objects that will be registering with the receiver..
 *
 */
public class MulticastNotificationReceiver<MSG_TYPE extends AbstractMessage, LISTENER>
        extends ComponentNotificationReceiver<MSG_TYPE> {
    private static final Logger logger = LogManager.getLogger();

    // the max number of seconds to wait for concurrent listeners to finish processing.
    private int listenerTimeoutSecs;

    // the set of consumers of these messages
    protected final List<LISTENER> listeners = new ArrayList<>();

    // the default routing method assumes the listeners accept the message with no transformations.
    protected Function<MSG_TYPE, Consumer<LISTENER>> messageMappingFunction = msg ->
            listener -> ((Consumer<MSG_TYPE>)listener).accept(msg);

    @VisibleForTesting
    protected MulticastNotificationReceiver(@Nonnull final ExecutorService executorService, int listenerTimeoutSecs) {
        super(null, executorService);
        setListenerTimeout(listenerTimeoutSecs);
    }

    /**
     * Create a simple MulticastNotificationReceiver that uses a default message routing method to
     * trigger on the listeners. (The default method treats listeners as an implementation of
     * Consumer&lt;MSG_TYPE&gt;)
     *
     * @param messageReceiver the {@link IMessageReceiver} providing the messages to distribute.
     * @param executorService The thread scheduler to use when distributing concurrently.
     * @param listenerTimeoutSecs The amount of time to wait for listeners to finish processing a
     *                            message before timing out.
     */
    public MulticastNotificationReceiver(@Nullable final IMessageReceiver<MSG_TYPE> messageReceiver,
                                         @Nonnull final ExecutorService executorService, int listenerTimeoutSecs) {
        super(messageReceiver, executorService);
        setListenerTimeout(listenerTimeoutSecs);
    }

    /**
     * Variation that takes a custom message handling method. This method needs to accept an incoming
     * MSG_TYPE and return a Consumer&lt;LISTENER&gt;, that will then be invoked on all of the listeners.
     *
     * @param messageReceiver the {@link IMessageReceiver} providing the messages to distribute.
     * @param executorService The thread scheduler to use when distributing concurrently.
     * @param listenerTimeoutSecs The amount of time to wait for listeners to finish processing a
     *                            message before timing out.
     * @param messageMapper The custom message mapping function to use.
     */
    public MulticastNotificationReceiver(@Nullable final IMessageReceiver<MSG_TYPE> messageReceiver,
                                         @Nonnull final ExecutorService executorService, int listenerTimeoutSecs,
                                         @Nonnull final Function<MSG_TYPE, Consumer<LISTENER>> messageMapper) {
        super(messageReceiver, executorService);
        messageMappingFunction = messageMapper;
        setListenerTimeout(listenerTimeoutSecs);
    }

    /**
     * Set the listener timeout. This is only used when calling listeners asynchronously, such as in
     * the case when we have multiple listeners to call.
     *
     * @param newValue the new value to use.
     */
    public void setListenerTimeout(int newValue) {
        logger.info("Setting notification receiver timeout to {} seconds.", newValue);
        listenerTimeoutSecs = newValue;
    }

    /**
     * Adds a new listener to this receiver.
     *
     * @param listener the listener to add.
     * @return true, if the listener was added. false if the listener already existed.
     */
    public synchronized boolean addListener(@Nonnull LISTENER listener) {
        Objects.requireNonNull(listener);
        // don't re-add if it already exists.
        if (listeners.contains(listener)) {
            return false;
        }
        return listeners.add(listener);
    }

    /**
     * Remove the specified listener from this receiver.
     *
     * @param listener the LISTENER to remove.
     * @return true, if the listener was found and removed. false otherwise.
     */
    public synchronized boolean removeListener(LISTENER listener) {
        return listeners.remove(listener);
    }

    @Override
    protected void processMessage(@Nonnull final MSG_TYPE message) throws ApiClientException, InterruptedException {
        // the default implementation consumes the base message type. Override this if you want to
        // do anything more sophisticated.
        invokeListeners(messageMappingFunction.apply(message));
    }

    /**
     * Utility method for triggering a method on a collection of "listener" objects, then blocking on
     * the invocations to finish.
     *
     * @param invocation the method to invoke on the listeners
     */
    protected void invokeListeners(Consumer<LISTENER> invocation) {
        // if there are no listeners, there's nothing to do.
        if (listeners.size() == 0) {
            return;
        }
        // if there is only one listener, we'll run it on the current thread.
        if (listeners.size() == 1) {
            try {
                invocation.accept(listeners.get(0));
            } catch (Exception e) {
                // don't allow the current processing thread to be killed
                getLogger().error("Error calling notification listener ", e);
            }
            return;
        }
        // If there are multiple listeners, we'll run it on the thread pool and wait for all listeners
        // to process it before resuming the current thread.
        CountDownLatch latch = new CountDownLatch(listeners.size());
        for (final LISTENER consumer : listeners) {
            getExecutorService().submit(() -> {
                try {
                    invocation.accept(consumer);
                } catch (RuntimeException e) {
                    getLogger().error("Error processing listener " + consumer, e);
                } finally {
                    latch.countDown();
                }
            });
        }

        // wait for all the listeners to finish.
        try {
            if (listenerTimeoutSecs <= 0) {
                // don't set a timeout.
                latch.await();
            } else {
                latch.await(listenerTimeoutSecs, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            logger.warn("Thread interrupted while waiting for message listeners to process.");
            Thread.currentThread().interrupt();
        }
    }
}
