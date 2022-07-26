package com.vmturbo.components.test.utilities.component;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.OutputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.Cluster;
import com.palantir.docker.compose.connection.Container;

import com.vmturbo.components.test.utilities.component.ComponentCluster.Component;

public class ComponentClusterComponentTest {
    private final DockerComposeRule composeRule = mock(DockerComposeRule.class);
    private final ServiceLogger serviceLogger = mock(ServiceLogger.class);
    private final OutputStream outputStream = mock(OutputStream.class);

    private final ComponentHealthCheck healthCheck = mock(ComponentHealthCheck.class);
    private final Container clusterMgr = mock(Container.class);
    private final Container componentContainer = mock(Container.class);

    private final Cluster mockCluster = mock(Cluster.class);
    private final ServiceConfiguration serviceConfiguration = mock(ServiceConfiguration.class);

    private static final String COMPONENT_NAME = "component-under-test";

    @Before
    public void setup() {
        when(composeRule.containers()).thenReturn(mockCluster);
        when(mockCluster.container(COMPONENT_NAME)).thenReturn(componentContainer);
    }

    @Test
    public void testComponentUpCallsContainerUp() throws Exception {
        final Component component = new Component(COMPONENT_NAME,
            Collections.emptyMap(), outputStream, Optional.empty(), healthCheck, Collections.emptyMap(),
            ComponentCluster.DEFAULT_HEALTH_CHECK_WAIT_MINUTES);

        component.up(clusterMgr, composeRule, serviceLogger, serviceConfiguration);
        verify(componentContainer).up();
    }

    @Test
    public void testComponentUpSetsConfiguration() throws Exception {
        final Component component = new Component(COMPONENT_NAME,
            Collections.singletonMap("foo", "bar"), outputStream, Optional.empty(), healthCheck, Collections.emptyMap(),
            ComponentCluster.DEFAULT_HEALTH_CHECK_WAIT_MINUTES);

        component.up(clusterMgr, composeRule, serviceLogger, serviceConfiguration);
        verify(serviceConfiguration).withConfiguration("foo", "bar");
        verify(serviceConfiguration).apply(clusterMgr);
    }

    @Test
    public void testComponentUpUsesServiceLogger() throws Exception {
        final Component component = new Component(COMPONENT_NAME,
            Collections.emptyMap(), outputStream, Optional.empty(), healthCheck, Collections.emptyMap(),
            ComponentCluster.DEFAULT_HEALTH_CHECK_WAIT_MINUTES);

        component.up(clusterMgr, composeRule, serviceLogger, serviceConfiguration);
        verify(serviceLogger).startCollecting(componentContainer, outputStream);
    }

    @Test
    public void testComponentUpWaitsForComponentToBeReady() throws Exception {
        final Component component = new Component(COMPONENT_NAME,
            Collections.emptyMap(), outputStream, Optional.empty(), healthCheck, Collections.emptyMap(),
            ComponentCluster.DEFAULT_HEALTH_CHECK_WAIT_MINUTES);

        component.up(clusterMgr, composeRule, serviceLogger, serviceConfiguration);
        verify(healthCheck).waitUntilReady(eq(componentContainer), any(Duration.class));
    }

    @Test
    public void testComponentUpUsesSpecifiedHealthCheck() throws Exception {
        final ComponentHealthCheck otherHealthCheck = mock(ComponentHealthCheck.class);
        final Component component = new Component(COMPONENT_NAME,
            Collections.emptyMap(), outputStream, Optional.empty(), otherHealthCheck, Collections.emptyMap(),
            ComponentCluster.DEFAULT_HEALTH_CHECK_WAIT_MINUTES);

        component.up(clusterMgr, composeRule, serviceLogger, serviceConfiguration);
        verify(otherHealthCheck).waitUntilReady(eq(componentContainer), any(Duration.class));
        verify(healthCheck, never()).waitUntilReady(eq(componentContainer), any(Duration.class));
    }

    @Test
    public void testComponentUpUsesTimeout() throws Exception {
        final ComponentHealthCheck otherHealthCheck = mock(ComponentHealthCheck.class);
        final Component component = new Component(COMPONENT_NAME,
            Collections.emptyMap(), outputStream, Optional.empty(), otherHealthCheck, Collections.emptyMap(),
            10);

        component.up(clusterMgr, composeRule, serviceLogger, serviceConfiguration);
        verify(otherHealthCheck).waitUntilReady(eq(componentContainer), eq(Duration.ofMinutes(10)));
    }

    @Test
    public void testComponentClose() throws Exception {
        final Component component = new Component(COMPONENT_NAME,
            Collections.emptyMap(), outputStream, Optional.empty(), healthCheck, Collections.emptyMap(),
            ComponentCluster.DEFAULT_HEALTH_CHECK_WAIT_MINUTES);

        component.up(clusterMgr, composeRule, serviceLogger, serviceConfiguration);
        component.close();

        verify(componentContainer).stop();
    }

    @Test
    public void testComponentDoesNotCloseIfNotUp() throws Exception {
        final Component component = new Component(COMPONENT_NAME,
            Collections.emptyMap(), outputStream, Optional.empty(), healthCheck, Collections.emptyMap(),
            ComponentCluster.DEFAULT_HEALTH_CHECK_WAIT_MINUTES);

        component.close();

        verify(componentContainer, never()).stop();
    }
}
