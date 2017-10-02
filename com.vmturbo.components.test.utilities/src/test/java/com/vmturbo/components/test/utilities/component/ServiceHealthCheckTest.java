package com.vmturbo.components.test.utilities.component;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.time.Duration;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.waiting.SuccessOrFailure;

import com.vmturbo.components.test.utilities.component.ServiceHealthCheck.BasicServiceHealthCheck;
import com.vmturbo.components.test.utilities.component.ServiceHealthCheck.ContainerUnreadyException;

public class ServiceHealthCheckTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testWaitUntilReady() throws Exception {
        final Container container = mock(Container.class);
        final ServiceHealthCheck healthCheck = spy(new BasicServiceHealthCheck());
        doReturn(SuccessOrFailure.success()).when(healthCheck).isHealthy(container);

        healthCheck.waitUntilReady(container, Duration.ofMinutes(1));

        // Success if no exception occurs
    }

    @Test
    public void testWaitUntilReadyTimesOut() throws Exception {
        final Container container = mock(Container.class);
        final ServiceHealthCheck healthCheck = spy(new BasicServiceHealthCheck());
        doReturn(SuccessOrFailure.failure("not ready")).when(healthCheck).isHealthy(container);

        expectedException.expect(ContainerUnreadyException.class);
        healthCheck.waitUntilReady(container, Duration.ofMillis(1));
    }
}