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
 * A {@link TopologyProcessingGate} that limits processing to a single topology at a time.
 *
 */
public class SingleTopologyProcessingGate implements TopologyProcessingGate {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Analyse only one topology at a time.
     */
    private final Semaphore analysisSemaphore = new Semaphore(1);

    private final long timeout;
    private final TimeUnit timeoutUnit;

    /**
     * Create a new instance of the gate.
     *
     * @param timeout The timeout for callers waiting for tickets.
     * @param timeoutUnit The unit for the timeout.
     */
    public SingleTopologyProcessingGate(final long timeout,
                                        @Nonnull final TimeUnit timeoutUnit) {
        this.timeout = timeout;
        this.timeoutUnit = timeoutUnit;
    }

    @Override
    @Nonnull
    public Ticket enter(@Nonnull final TopologyInfo topologyInfo,
                        @Nonnull final Collection<TopologyEntityDTO> entities)
            throws InterruptedException, TimeoutException {
        final boolean isPlan = topologyInfo.getTopologyType() == TopologyType.PLAN;

        final String logTitle = (isPlan ? "Plan " + topologyInfo.getTopologyContextId() : " Realtime ")
                + " (topology " + topologyInfo.getTopologyId() + ") ";
        logger.info("{} waiting for free ticket...", logTitle);
        final boolean success = analysisSemaphore.tryAcquire(timeout, timeoutUnit);
        if (!success) {
            throw new TimeoutException("Failed to acquire " + logTitle
                + " semaphore after " + timeout + " " + timeoutUnit + " for topology " + topologyInfo);
        }
        logger.info("{} got free ticket.", logTitle);
        return () -> {
            logger.info("{} done with ticket.", logTitle);
            analysisSemaphore.release();
        };
    }
}
