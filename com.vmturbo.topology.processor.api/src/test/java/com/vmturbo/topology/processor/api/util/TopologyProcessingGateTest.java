package com.vmturbo.topology.processor.api.util;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.topology.processor.api.util.TopologyProcessingGate.Ticket;

/**
 * Unit tests for {@link TopologyProcessingGate}.
 */
public class TopologyProcessingGateTest {
    private static final TopologyInfo PLAN_INFO = TopologyInfo.newBuilder()
        .setTopologyType(TopologyType.PLAN)
        .build();

    private static final TopologyInfo REALTIME_INFO = TopologyInfo.newBuilder()
        .setTopologyType(TopologyType.REALTIME)
        .build();

    /**
     * Test that the {@link ConcurrentLimitProcessingGate} acts as a pass-through for live topologies.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testConcurrentLimitGateLiveEnforcement() throws Exception {
        ConcurrentLimitProcessingGate gate = new ConcurrentLimitProcessingGate(1, 1, TimeUnit.MILLISECONDS);
        Ticket firstTicket = gate.enter(REALTIME_INFO, Collections.emptyList());
        // Got first ticket no problem.
        try {
            gate.enter(REALTIME_INFO, Collections.emptyList());
            Assert.fail("Unexpectedly got ticket for second enter() call.");
        } catch (TimeoutException e) {
            // This is expected - we can't get a ticket because the first ticket is still open.
        }

        firstTicket.close();

        // This should succeed.
        Ticket secondTicket = gate.enter(REALTIME_INFO, Collections.emptyList());
        secondTicket.close();
    }

    /**
     * Test that the {@link ConcurrentLimitProcessingGate} enforces the provided concurrent plan limit for plan
     * topologies.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testConcurrentLimitGatePlanEnforcement() throws Exception {
        ConcurrentLimitProcessingGate gate = new ConcurrentLimitProcessingGate(1, 1, TimeUnit.MILLISECONDS);
        Ticket firstTicket = gate.enter(PLAN_INFO, Collections.emptyList());
        // Got first ticket no problem.
        try {
            gate.enter(PLAN_INFO, Collections.emptyList());
            Assert.fail("Unexpectedly got ticket for second enter() call.");
        } catch (TimeoutException e) {
            // This is expected - we can't get a ticket because the first ticket is still open.
        }

        firstTicket.close();

        // This should succeed.
        Ticket secondTicket = gate.enter(PLAN_INFO, Collections.emptyList());
        secondTicket.close();
    }

}