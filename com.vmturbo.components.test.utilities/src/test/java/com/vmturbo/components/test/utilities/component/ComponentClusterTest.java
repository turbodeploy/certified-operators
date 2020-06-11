package com.vmturbo.components.test.utilities.component;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.Cluster;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InOrder;
import org.mockito.Mockito;

import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.components.test.utilities.component.ComponentCluster.Component;

public class ComponentClusterTest {

    private final Component mockComponent = mock(Component.class);
    private final DockerComposeRule composeRule = mock(DockerComposeRule.class);
    private final DockerPort dockerPort = mock(DockerPort.class);
    private final ServiceLogger serviceLogger = mock(ServiceLogger.class);
    private final Container clusterMgr = mock(Container.class);
    private final Cluster mockCluster = mock(Cluster.class);
    private final ComponentHealthCheck healthCheck = mock(ComponentHealthCheck.class);

    private final LinkedHashMap<String, Component> components = new LinkedHashMap<>(
        Collections.singletonMap("mock-component", mockComponent)
    );

    private final ComponentCluster componentRule = new ComponentCluster(
        components, false, composeRule, serviceLogger, healthCheck);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() {
        setupMockComponent(mockComponent, "mock-component");
        when(composeRule.containers()).thenReturn(mockCluster);

        when(clusterMgr.port(eq(ComponentUtils.GLOBAL_HTTP_PORT))).thenReturn(dockerPort);
        when(mockCluster.container("clustermgr")).thenReturn(clusterMgr);
    }

    @Test
    public void testUpBringsUpClusterMgrAndComponent() throws Exception {
        final InOrder inOrder = inOrder(clusterMgr, mockComponent);

        componentRule.up();
        inOrder.verify(clusterMgr).up();
        inOrder.verify(mockComponent).up(eq(clusterMgr), eq(composeRule), eq(serviceLogger));
    }

    @Test
    public void testExceptionDuringStartupCleansUp() throws Exception {
        doThrow(new RuntimeException("runtime error")).when(clusterMgr).up();
        expectedException.expect(RuntimeException.class);

        try {
            componentRule.up();
        } finally {
            verify(mockComponent).close();
            verify(composeRule, Mockito.times(2)).after(); // Once before the exception, once after.
        }
    }

    @Test
    public void testDown() throws Exception {
        componentRule.down();

        verify(mockComponent).close();
        verify(composeRule).after();
    }

    @Test
    public void testUpOrder() throws Exception {
        final Component componentA = mock(Component.class);
        final Component componentB = mock(Component.class);
        final LinkedHashMap<String, Component> components = new LinkedHashMap<>(
            ImmutableMap.of(
                "component-A", componentA,
                "component-B", componentB
            )
        );
        setupMockComponent(componentA, "component-A");
        setupMockComponent(componentB, "component-B");

        final ComponentCluster componentRule = new ComponentCluster(
            components, false, composeRule, serviceLogger, healthCheck);
        componentRule.up();

        final InOrder inOrder = inOrder(componentA, componentB);
        inOrder.verify(componentA).up(eq(clusterMgr), eq(composeRule), eq(serviceLogger));
        inOrder.verify(componentB).up(eq(clusterMgr), eq(composeRule), eq(serviceLogger));
    }

    @Test
    public void testDownOrderReversed() throws Exception {
        final Component componentA = mock(Component.class);
        final Component componentB = mock(Component.class);
        final LinkedHashMap<String, Component> components = new LinkedHashMap<>(
            ImmutableMap.of(
                "component-A", componentA,
                "component-B", componentB
                )
        );
        setupMockComponent(componentA, "component-A");
        setupMockComponent(componentB, "component-B");

        final ComponentCluster componentRule = new ComponentCluster(
            components, false, composeRule, serviceLogger, healthCheck);
        componentRule.down();

        // Components should be brought down in the reverse order that they are brought up.
        final InOrder inOrder = inOrder(componentA, componentB);
        inOrder.verify(componentB).close();
        inOrder.verify(componentA).close();
    }

    @Test
    public void testGetConnectionConfig() throws Exception {
        when(dockerPort.getIp()).thenReturn("localhost");
        when(dockerPort.getExternalPort()).thenReturn(9999);

        ComponentApiConnectionConfig config = componentRule.getConnectionConfig("mock-component");
        assertEquals("localhost", config.getHost());
        assertEquals(9999, config.getPort());
    }

    @Test
    public void testGetMetricsURI() throws Exception {
        when(dockerPort.getIp()).thenReturn("localhost");
        when(dockerPort.getExternalPort()).thenReturn(9999);

        final URI uri = componentRule.getMetricsURI("mock-component");
        assertEquals("http://localhost:9999/metrics", uri.toString());
    }

    private void setupMockComponent(@Nonnull final Component mockComponent,
                                    @Nonnull final String componentName) {
        when(mockComponent.getName()).thenReturn(componentName);
        when(mockComponent.getMemoryLimitMb()).thenReturn(Optional.empty());
        when(mockComponent.getHealthCheck()).thenReturn(healthCheck);
        when(mockComponent.getHttpPort()).thenReturn(dockerPort);
    }
}