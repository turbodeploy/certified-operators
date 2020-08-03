package com.vmturbo.topology.processor.api.util;

import java.util.Collection;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;

/**
 * An {@link TopologyProcessingGate} that limits processing based on the number of concurrent
 * topologies.
 *
 * <p>For live topologies, It acts as a passthrough for live topologies, and allows a configurable number of concurrent
 * plan topologies.
 */
public class ConcurrentLimitProcessingGate implements TopologyProcessingGate {

    private static final Logger logger = LogManager.getLogger();

    private final Semaphore planSemaphore;

    /**
     * Only one concurrent realtime topology allowed.
     */
    private final Semaphore realtimeSemaphore = new Semaphore(1);

    private final long timeout;
    private final TimeUnit timeoutUnit;

    /**
     * Create a new instance of the gate.
     *
     * @param concurrentPlans The number of concurrent plan analyses that will be allowed.
     * @param timeout The timeout for callers waiting for tickets.
     * @param timeoutUnit The unit for the timeout.
     */
    public ConcurrentLimitProcessingGate(final int concurrentPlans,
                                         final long timeout,
                                         @Nonnull final TimeUnit timeoutUnit) {
        if (concurrentPlans < 1) {
            logger.warn("Ignoring invalid concurrent plan count: {}. Using 1.", concurrentPlans);
        }
        // We take the max of the input and 1 to avoid creating a semaphore that can never be
        // acquired in the case of illegal input.
        this.planSemaphore = new Semaphore(Math.max(concurrentPlans, 1));
        this.timeout = timeout;
        this.timeoutUnit = timeoutUnit;
    }

    @Override
    @Nonnull
    public Ticket enter(@Nonnull final TopologyInfo topologyInfo,
                        @Nonnull final Collection<TopologyEntityDTO> entities)
            throws InterruptedException, TimeoutException {
        final boolean isPlan = topologyInfo.getTopologyType() == TopologyType.PLAN;
        final Semaphore semaphoreToUse = isPlan ? planSemaphore : realtimeSemaphore;

        final String logTitle = (isPlan ? "Plan " + topologyInfo.getTopologyContextId() : " Realtime ")  +
            " (topology " + topologyInfo.getTopologyId() + ") ";
        logger.info("{} waiting for free ticket...", logTitle);
        final boolean success = semaphoreToUse.tryAcquire(timeout, timeoutUnit);
        if (!success) {
            throw new TimeoutException("Failed to acquire " + logTitle +
                " semaphore after " + timeout + " " + timeoutUnit + " for topology " + topologyInfo);
        }
        logger.info("{} got free ticket.", logTitle);
        return () -> {
            logger.info("{} done with ticket.", logTitle);
            semaphoreToUse.release();
        };
    }
}
