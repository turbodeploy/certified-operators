package com.vmturbo.components.common.health;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class CompositeHealthTest {

    /**
     * Test a health monitor with multiple dependencies in it.
     */
    @Test
    public void testHealthMonitorWithDependencies() {
        CompositeHealthMonitor monitor = new CompositeHealthMonitor("Test");
        SimpleHealthStatusProvider subcomponent1 = new SimpleHealthStatusProvider("subcomponent1");
        SimpleHealthStatusProvider subcomponent2 = new SimpleHealthStatusProvider("subcomponent2");
        monitor.addHealthCheck(subcomponent1);
        monitor.addHealthCheck(subcomponent2);
        // initial call -- all subcomponents should be unhealthy
        Assert.assertFalse(monitor.getHealthStatus().isHealthy());

        // set one subcomponent to healthy -- the component should still be unhealhty
        subcomponent1.reportHealthy();
        Assert.assertFalse("One dependency unhealthy --> component should be unhealthy.",
                monitor.getHealthStatus().isHealthy());

        // now set the other subcomponent to healthy -- the component should now be healthy
        subcomponent2.reportHealthy();
        Assert.assertTrue(monitor.getHealthStatus().isHealthy());
    }


}
