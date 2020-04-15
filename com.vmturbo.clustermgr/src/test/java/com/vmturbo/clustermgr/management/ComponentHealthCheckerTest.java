package com.vmturbo.clustermgr.management;

import static org.mockito.Matchers.contains;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.client.AsyncRestTemplate;

import com.vmturbo.common.protobuf.cluster.ComponentStatus.ComponentInfo;
import com.vmturbo.common.protobuf.cluster.ComponentStatus.UriInfo;

/**
 * Unit tests for the {@link ComponentHealthChecker}.
 */
public class ComponentHealthCheckerTest {

    private final ComponentRegistry registry = mock(ComponentRegistry.class);

    private AsyncRestTemplate asyncRestTemplate = mock(AsyncRestTemplate.class);

    private final long healthCheckMs = 10_000;

    private final ScheduledExecutorService scheduledExecutorService = mock(ScheduledExecutorService.class);

    @Captor
    ArgumentCaptor<ListenableFutureCallback<ResponseEntity<String>>> callbackCaptor;

    /**
     * Common code before every test.
     */
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    /**
     * Test the health check loop.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testHealthCheckTriggers() throws Exception {
        // Arrange
        Table<String, String, RegisteredComponent> registeredComponents = HashBasedTable.create();
        RegisteredComponent component = new RegisteredComponent(ComponentInfo.newBuilder()
            .setUriInfo(UriInfo.newBuilder()
                .setPort(123)
                .setIpAddress("localhost"))
            .build(), ComponentHealth.HEALTHY);
        registeredComponents.put("foo", "instance1", component);
        when(registry.getRegisteredComponents()).thenReturn(registeredComponents);

        ListenableFuture<ResponseEntity<String>> future = mock(ListenableFuture.class);
        when(asyncRestTemplate.getForEntity(component.getUri("health"), String.class))
            .thenReturn(future);

        // Act - the checker pings asynchronously, so we give it a mock executor service and capture
        // the runnable. This lets us test that it's scheduling the checks at the correct interval,
        // and also lets us trigger the check manually.
        new ComponentHealthChecker(registry, scheduledExecutorService, asyncRestTemplate, healthCheckMs, TimeUnit.MILLISECONDS);
        ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(scheduledExecutorService).scheduleAtFixedRate(runnableCaptor.capture(), eq(healthCheckMs), eq(healthCheckMs), eq(TimeUnit.MILLISECONDS));

        runnableCaptor.getValue().run();

        // Assert
        verify(future).addCallback(callbackCaptor.capture());

        ListenableFutureCallback<ResponseEntity<String>> callback = callbackCaptor.getValue();

        final String healthyStatus = "Healthy now.";
        callback.onSuccess(ResponseEntity.ok(healthyStatus));
        verify(registry).updateComponentHealthStatus(component.getComponentInfo().getId(), ComponentHealth.HEALTHY, healthyStatus);

        final String warningStatus = "Not doing too well.";
        callback.onSuccess(ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(warningStatus));
        verify(registry).updateComponentHealthStatus(component.getComponentInfo().getId(), ComponentHealth.WARNING, warningStatus);

        final String criticalStatus = "Doing pretty bad now.";
        callback.onSuccess(ResponseEntity.status(HttpStatus.NOT_FOUND).body(criticalStatus));
        verify(registry).updateComponentHealthStatus(component.getComponentInfo().getId(), ComponentHealth.CRITICAL, criticalStatus);

        final RuntimeException failure = new RuntimeException("Some fatal error.");
        callback.onFailure(failure);
        verify(registry).updateComponentHealthStatus(eq(component.getComponentInfo().getId()), eq(ComponentHealth.CRITICAL), contains(failure.getMessage()));
    }
}