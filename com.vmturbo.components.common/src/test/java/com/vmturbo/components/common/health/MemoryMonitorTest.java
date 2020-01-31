package com.vmturbo.components.common.health;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.time.Duration;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests for the Memory Monitor
 */
public class MemoryMonitorTest {

    /**
     * Test Memory Monitor
     */
    @Ignore // ignoring due to periodic timing issues with this test.
    @Test
    public void testMemoryMonitor() {
        // create a test monitor that triggers when old gen used exceeds 25%.
        double targetRatio = 0.50;
        MemoryMonitor memoryMonitor = new MemoryMonitor(targetRatio);

        // verify initial check is healthy.
        HealthStatus status = memoryMonitor.getHealthStatus();
        Assert.assertTrue(status.isHealthy());

        // now eat up enough memory to trigger the unhealthy state and check it again.
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage memUsage = memoryMXBean.getHeapMemoryUsage();
        // to trigger old gen usage, we will to allocate a "humongous object" that should go right into old gen.
        //
        // To trigger the unhealthy state, the humongous object should also be over the threshold size.
        int humongousObjectGuestimate = (int)(Math.ceil(memUsage.getMax() * targetRatio));
        System.out.println("MONGO is here!! Allocating humongous object of "+ humongousObjectGuestimate +" bytes.");
        // allocate a humongo, then trigger a GC that should result in a memory health check update
        byte[] mongo = new byte[humongousObjectGuestimate];
        System.gc();
        // verify that the next health status reports the problem
        status = memoryMonitor.getStatusStream().blockFirst();
        Assert.assertFalse("We should now be over the threshold after allocating the large object.", status.isHealthy());

        // release mongo and we should go back to normal
        mongo = null;
        System.gc();
        // the collection process sometimes involves a few GC cycles, so we need to give the memory
        // monitor a few updates to resolve into a clean health status for us. We'll do this by
        // asking for a Flux of health checks over the next 100 ms, then we'll take the last() status,
        // or the latest memory monitor status if we don't get one from last(). (due to timing
        // issues we could totally miss the status update in the time it takes to set up the flux)
        status = memoryMonitor.getStatusStream().take(Duration.ofMillis(100)).last(memoryMonitor.getHealthStatus()).block();
        Assert.assertTrue("We should now be back under the threshold. " + status.getDetails(), status.isHealthy());
    }

}
