package com.vmturbo.components.api.client;

import java.util.Collection;
import java.util.function.BiConsumer;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

/**
 * This meta-message receiver is used to hold several underlying message receivers. This class is
 * used internally to wrap several message receivers if they can hold no more then 1 topic onboard.
 * All that this class does is just propagate {@link #addListener(BiConsumer)} call to the
 * underlying message receivers.
 *
 * @param <T> type of message to receive
 */
public class UmbrellaMessageReceiver<T> implements IMessageReceiver<T> {

    private final Collection<IMessageReceiver<T>> underlyingReceivers;

    /**
     * Constructs umbrella message receiver upon the specified underlying message receivers.
     *
     * @param underlyingReceivers underlying message receivers to propagate calls to.
     */
    public UmbrellaMessageReceiver(
            @Nonnull Collection<? extends IMessageReceiver<T>> underlyingReceivers) {
        this.underlyingReceivers = ImmutableList.copyOf(underlyingReceivers);
    }

    @Override
    public void addListener(@Nonnull BiConsumer<T, Runnable> listener) {
        for (IMessageReceiver<T> underlyingReceiver : underlyingReceivers) {
            underlyingReceiver.addListener(listener);
        }
    }
}
