package com.vmturbo.components.test.utilities.component;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.URI;

import org.apache.http.client.utils.URIBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.connection.State;

import com.vmturbo.components.test.utilities.component.ServiceHealthCheck.ContainerUnreadyException;

public class ComponentHealthCheckTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testIsHealthyIllegalIp() throws Exception {
        final Container container = mock(Container.class);
        final DockerPort port = mock(DockerPort.class);

        // The state returned by container.state() shouldn't matter as long as it's not down.
        when(container.state()).thenReturn(State.HEALTHY);

        when(container.port(Mockito.anyInt())).thenReturn(port);
        when(port.getIp()).thenReturn("illegal ip");
        when(port.getExternalPort()).thenReturn(12345);

        assertFalse(new ComponentHealthCheck().isHealthy(container).succeeded());
    }

    @Test
    public void testIsHealthyIllegalPort() throws Exception {
        final Container container = mock(Container.class);
        final DockerPort port = mock(DockerPort.class);

        // The state returned by container.state() shouldn't matter as long as it's not down.
        when(container.state()).thenReturn(State.HEALTHY);

        when(container.port(Mockito.anyInt())).thenReturn(port);
        when(port.getIp()).thenReturn("123.123.123.123");
        when(port.getExternalPort()).thenReturn(Integer.MAX_VALUE);

        assertFalse(new ComponentHealthCheck().isHealthy(container).succeeded());
    }

    @Test
    public void testIsHealthyComponentDown() throws Exception {
        final Container container = mock(Container.class);
        when(container.state()).thenReturn(State.DOWN);

        expectedException.expect(ContainerUnreadyException.class);
        new ComponentHealthCheck().isHealthy(container);
    }

    @Test
    public void testIsHealthySuccess() throws Exception {
        final URI uri = new URIBuilder().setScheme("http")
            .setHost("localhost")
            .build();
        final RestTemplate restTemplate = mock(RestTemplate.class);

        when(restTemplate.getForEntity(eq(uri), eq(String.class))).thenReturn(ResponseEntity.ok("Success"));

        assertTrue(new ComponentHealthCheck().isHealthy(uri, restTemplate).succeeded());
    }

    @Test
    public void testIsHealthyFailure() throws Exception {
        final URI uri = new URIBuilder().setScheme("http")
            .setHost("localhost")
            .build();
        final RestTemplate restTemplate = mock(RestTemplate.class);

        when(restTemplate.getForEntity(eq(uri), eq(String.class))).thenReturn(ResponseEntity.badRequest().body("failed"));

        assertFalse(new ComponentHealthCheck().isHealthy(uri, restTemplate).succeeded());
    }
}