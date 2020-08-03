package com.vmturbo.topology.processor.api.util;

import java.util.Collection;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;

/**
 * A gating mechanism to control the number of concurrent topologies being processed - for example,
 * to control the number of concurrent analyses in the Market.
 *
 * <p>Processing a topology may be expensive - memory and CPU-wise. Processing too many concurrently,
 * especially on large topologies, can slow down or even crash the component.
 *
 * <p>Intended use: call {@link TopologyProcessingGate#enter(TopologyInfo, Collection)} before starting
 * processing, and call {@link Ticket#close()} on the returned ticket when processing completes to
 * release resources.
 *
 * <p>The purpose of this layer of abstraction is to allow us to swap out the gating logic in
 * the future - e.g. limit by number of entities, by size of entities (in bytes), by settings,
 * or anything else.
 */
public interface TopologyProcessingGate {

    /**
     * Enter the gate. Do this before starting the analysis to make sure the analysis has the
     * resources to run.
     *
     * @param topologyInfo Information about the topology being analyzed.
     * @param entities Information about the entities in the topology.
     * @return A {@link Ticket} that needs to be closed when the analysis is done.
     *
     * @throws InterruptedException If the thread is interrupted while waiting for a ticket to
     *                              become available.
     * @throws TimeoutException If no ticket is available for the timeout period specified in the
     *                          gate. Since we only really use the gate in one place, we opt to
     *                          configure the timeout on a per-gate level instead of on a
     *                          per-method-call level.
     */
    @Nonnull
    Ticket enter(@Nonnull TopologyInfo topologyInfo,
                 @Nonnull Collection<TopologyEntityDTO> entities)
        throws InterruptedException, TimeoutException;

    /**
     * The ticket returned by a {@link TopologyProcessingGate#enter(TopologyInfo, Collection)}. Needs
     * to be "closed" when the analysis is done.
     */
    interface Ticket extends AutoCloseable {
        /**
         * Override the close() method to disallow throwing exceptions.
         */
        @Override
        void close();
    }

}
