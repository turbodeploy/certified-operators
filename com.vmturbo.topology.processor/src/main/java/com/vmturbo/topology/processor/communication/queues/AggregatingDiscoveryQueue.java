package com.vmturbo.topology.processor.communication.queues;

import java.util.Collection;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.vmturbo.communication.ITransport;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.discovery.DiscoveryBundle;
import com.vmturbo.topology.processor.targets.Target;

/**
 * Interface for a queue of discovery items that will be run in the order they are queued up. This
 * queue guarantees that a particular discovery type and target will appear only once in the queue.
 * Subsequent attempts to add the same discovery will be noops.
 * A worker that actually runs the discoveries can pull entries off the queue by specifying the
 * probe types it supports and the discovery type it is performing.  It will get the earliest queued
 * discovery that meets those requirements.  It can call takeNextQueuedDiscovery to block until a
 * discovery is available or pollNextQueuedDiscovery to get back and empty discovery if none is
 * available.
 * Implementations of this class must be thread safe as multiple workers will be servicing it while
 * the OperationManager is feeding it.
 */
@ThreadSafe
public interface AggregatingDiscoveryQueue {

    /**
     * Queue a discovery for processing. Processing involves choosing an
     * {@link com.vmturbo.communication.ITransport} to send it to.
     *
     * @param target the Target to be discovered.
     * @param discoveryType the DiscoveryType of the discovery.
     * @param prepareDiscoveryInformation a callback method to be invoked once there is an ITransport available
     * to carry it out. The callback method takes an ITransport, a method to be called when the
     * discovery is done, and returns a Discovery object representing the discovery that it is
     * running.
     * @param errorHandler method to call if discovery attempt leads to an exception.
     * @param runImmediately true if the discovery being queued should run ahead of anything else
     * in the queue.  This is called for user initiated discoveries.  If there is already a
     * discovery queued for the target, it is moved to the head of the queue if runImmediately is
     * true.
     * @return {@link IDiscoveryQueueElement} representing the just queued discovery if it was added
     * or else the existing queued element if there was already a discovery queued for this target.
     */
    IDiscoveryQueueElement offerDiscovery(
            @Nonnull Target target,
            @Nonnull DiscoveryType discoveryType,
            @Nonnull Function<Runnable, DiscoveryBundle> prepareDiscoveryInformation,
            @Nonnull BiConsumer<Discovery, Exception> errorHandler,
            boolean runImmediately);

    /**
     * Get the next discovery suitable for the transport in question. If no discovery is available,
     * block until one is added.
     *
     * @param transport the transport that will carry out the discovery.
     * @param probeTypes the collection of probe types this transport can service.
     * @param discoveryType the {@link DiscoveryType} to get.
     * @return IDiscoveryQueueElement that represents the details of the
     * discovery to be carried out.
     * @throws InterruptedException if thread is interrupted while waiting.
     */
    Optional<IDiscoveryQueueElement> takeNextQueuedDiscovery(
            @Nonnull ITransport<MediationServerMessage, MediationClientMessage> transport,
            @Nonnull Collection<Long> probeTypes,
            @Nonnull DiscoveryType discoveryType)
            throws InterruptedException;

    /**
     * Register that the given transport will handle the given target.
     *
     *  @param transport the transport that should exclusively discovery the target.
     * @param targetIdentifiers String made up of target identifiers that identify the target.
     */
    void assignTargetToTransport(
            @Nonnull ITransport<MediationServerMessage, MediationClientMessage> transport,
            @Nonnull String targetIdentifiers);

    /**
     * Called when a target is deleted.  Queue should remove all queued discoveries for the target.
     *
     * @param probeId long giving the ID of the Probe that is associated with the delted Target.
     * @param targetId long giving the targetId of the Target that was removed.
     */
    void handleTargetRemoval(long probeId, long targetId);

    /**
     * Called when transport is removed.  Any queues associated with the transport should be flushed
     * and their contents added to the appropriate ProbeType queues.
     *
     * @param transport ITransport that has been removed.
     */
    void handleTransportRemoval(
            @Nonnull ITransport<MediationServerMessage, MediationClientMessage> transport);
}
