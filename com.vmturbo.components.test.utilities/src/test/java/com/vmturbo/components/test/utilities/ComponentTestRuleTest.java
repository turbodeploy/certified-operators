package com.vmturbo.components.test.utilities;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import com.vmturbo.components.test.utilities.communication.ComponentStubHost;
import com.vmturbo.components.test.utilities.component.ComponentCluster;
import com.vmturbo.components.test.utilities.metric.MetricsWarehouse;

/**
 * Unit tests for {@link ComponentTestRule}.
 */
public class ComponentTestRuleTest {

    private static MetricsWarehouse.Builder metricsWarehouseBuilder;
    private static MetricsWarehouse metricsWarehouse;

    private static ComponentCluster.Builder clusterBuilder;
    private static ComponentCluster cluster;
    private static ComponentStubHost.Builder componentStubHostBuilder;
    private static ComponentStubHost componentStubHost;

    private static ComponentCluster.Builder clusterBldrNoMetrics;
    private static ComponentCluster clusterNoMetrics;
    private static ComponentStubHost.Builder componentStubHostBuilderNoMetrics;
    private static ComponentStubHost componentStubHostNoMetrics;


    @Rule
    public ComponentTestRule withMetricsRule = ComponentTestRule.newBuilder()
            .withComponentCluster(clusterBuilder)
            .withStubs(componentStubHostBuilder)
            .withMetricsWarehouse(metricsWarehouseBuilder);

    @Rule
    public ComponentTestRule noMetricsRule = ComponentTestRule.newBuilder()
            .withComponentCluster(clusterBldrNoMetrics)
            .withStubs(componentStubHostBuilderNoMetrics)
            .noMetricsCollection();

    /**
     * Do setup of mocks in the @BeforeClass annotation, so they are ready when
     * the @Rule-annotated variables try to use them.
     */
    @BeforeClass
    public static void setupClass() {
        metricsWarehouseBuilder = Mockito.mock(MetricsWarehouse.Builder.class);
        metricsWarehouse = Mockito.mock(MetricsWarehouse.class);

        clusterBuilder = Mockito.mock(ComponentCluster.Builder.class);
        cluster = Mockito.mock(ComponentCluster.class);

        componentStubHostBuilder = Mockito.mock(ComponentStubHost.Builder.class);
        componentStubHost = Mockito.mock(ComponentStubHost.class);

        clusterBldrNoMetrics = Mockito.mock(ComponentCluster.Builder.class);
        clusterNoMetrics = Mockito.mock(ComponentCluster.class);

        componentStubHostBuilderNoMetrics = Mockito.mock(ComponentStubHost.Builder.class);
        componentStubHostNoMetrics = Mockito.mock(ComponentStubHost.class);

        Mockito.when(metricsWarehouseBuilder.build()).thenReturn(metricsWarehouse);
        Mockito.when(componentStubHostBuilder.build()).thenReturn(componentStubHost);
        Mockito.when(clusterBuilder.build()).thenReturn(cluster);

        Mockito.when(clusterBldrNoMetrics.build()).thenReturn(clusterNoMetrics);
        Mockito.when(componentStubHostBuilderNoMetrics.build()).thenReturn(componentStubHostNoMetrics);
    }

    /**
     * Verify that the setup in the rules called the expected methods.
     */
    @Test
    public void testSetup() {
        // This should have been called by the rule with metrics.
        final InOrder inOrder = Mockito.inOrder(componentStubHost, cluster, metricsWarehouse);
        inOrder.verify(componentStubHost).start();
        inOrder.verify(cluster).up();
        inOrder.verify(metricsWarehouse).initialize(Mockito.eq(cluster));

        // This should have been called by the rule without metrics.
        final InOrder inOrderNoMetrics = Mockito.inOrder(componentStubHostNoMetrics, clusterNoMetrics);
        inOrderNoMetrics.verify(componentStubHostNoMetrics).start();
        inOrderNoMetrics.verify(clusterNoMetrics).up();

        Assert.assertEquals(cluster, withMetricsRule.getCluster());
        Assert.assertEquals(clusterNoMetrics, noMetricsRule.getCluster());
    }

    /**
     * Verify that the the teardown in the rules called the expected methods.
     */
    @AfterClass
    public static void afterClass() {
        // This should have been called by the rule with metrics.
        final InOrder inOrder = Mockito.inOrder(metricsWarehouse, cluster, componentStubHost);
        inOrder.verify(metricsWarehouse).stop(Mockito.eq("testSetup"));
        inOrder.verify(cluster).down();
        inOrder.verify(componentStubHost).close();

        // This should have been called by the rule without metrics.
        final InOrder inOrderNoMetrics = Mockito.inOrder(clusterNoMetrics, componentStubHostNoMetrics);
        inOrderNoMetrics.verify(clusterNoMetrics).down();
        inOrderNoMetrics.verify(componentStubHostNoMetrics).close();
    }
}
