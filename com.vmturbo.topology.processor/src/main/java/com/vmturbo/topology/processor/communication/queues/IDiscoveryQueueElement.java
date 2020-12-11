package com.vmturbo.topology.processor.communication.queues;

import java.time.LocalDateTime;
import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.discovery.DiscoveryBundle;
import com.vmturbo.topology.processor.targets.Target;

/**
 * Interface implemented by entries into an {@link DiscoveryQueue}.
 *
 */
public interface IDiscoveryQueueElement extends Comparable<IDiscoveryQueueElement> {

    /**
     * Get the operation contained in this IOperationQueueElement.
     *
     * @return operation contained in this IOperationQueueElement.
     */
    Target getTarget();

    /**
     * Get the DiscoveryType for the discovery represented by this element.
     *
     * @return DiscoveryType of the discovery resulting from this element.
     */
    DiscoveryType getDiscoveryType();

    /**
     * Get the time this element was added to the queue.
     *
     * @return {@link LocalDateTime} giving the time that this element was queued.
     */
    LocalDateTime getQueuedTime();

    /**
     * Perform a discovery using the given method and calling the given Runnable
     * when the discovery completes.
     *
     * @param discoveryMethod method to run the discovery and return the Discovery object
     * representing it.
     * @param returnPermit {@link Runnable} function to call after discovery is done.
     * @return Discovery object representing the discovery.
     */
    Discovery performDiscovery(
            @Nonnull Function<DiscoveryBundle, Discovery> discoveryMethod,
            @Nonnull Runnable returnPermit);

    /**
     * Get the discovery associated with this element blocking if the Discovery has not yet been
     * created.
     *
     * @param timeoutMillis how long to wait before giving up and returning a null response.
     * @return Discovery if it has been created successfully or null if the Discovery failed to be
     * created.
     * @throws InterruptedException if the thread gets interrupted while waiting.
     */
    @Nullable
    Discovery getDiscovery(long timeoutMillis) throws InterruptedException;

    /**
     * Return true if this discovery should be run immediately instead of queueing because it was
     * requested by a user action.
     *
     * @return true if this discovery should be run immediately or false otherwise.
     */
    boolean runImmediately();

    /**
     * Set the new value of runImmediately.
     *
     * @param newValue new value of runImmediately.
     */
    void setRunImmediately(boolean newValue);
}
