package com.vmturbo.clustermgr.management;

import static com.vmturbo.common.protobuf.cluster.ComponentStatusProtoUtil.getComponentLogId;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Table;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.core.task.AsyncListenableTaskExecutor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.scheduling.concurrent.ConcurrentTaskExecutor;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.client.AsyncRestTemplate;

import com.vmturbo.clustermgr.management.ComponentRegistry.RegistryUpdateException;
import com.vmturbo.common.protobuf.cluster.ComponentStatus.ComponentInfo;
import com.vmturbo.components.common.ComponentController;

/**
 * Utility class to periodically go through the components in the {@link ComponentRegistry}
 * and ping their health endpoints, updating their status with the results.
 */
public class ComponentHealthChecker {
    private static final Logger logger = LogManager.getLogger();

    private final ComponentRegistry componentRegistry;

    private final AsyncRestTemplate asyncRestTemplate;

    /**
     * Public constructor.
     *
     * @param componentRegistry Provides information about registered components.
     * @param scheduledExecutorService Executor used to run the health check loop.
     * @param executorService Executor used to run the actual health checks concurrently.
     * @param healthCheckInterval Time between health checks.
     * @param healthCheckConnectTimeout Connection timeout when sending health check request.
     * @param healthCheckReadTimeout Socket read timeout when sending health check request.
     * @param healthCheckIntervalUnit Time unit for the health check interval.
     */
    public ComponentHealthChecker(@Nonnull final ComponentRegistry componentRegistry,
                                  @Nonnull final ScheduledExecutorService scheduledExecutorService,
                                  @Nonnull final ExecutorService executorService,
                                  final long healthCheckInterval,
                                  final long healthCheckConnectTimeout,
                                  final long healthCheckReadTimeout,
                                  @Nonnull final TimeUnit healthCheckIntervalUnit) {
        this(componentRegistry, scheduledExecutorService, makeAsyncRestTemplate(executorService, healthCheckConnectTimeout, healthCheckReadTimeout, healthCheckIntervalUnit), healthCheckInterval,
            healthCheckIntervalUnit);
    }

    @VisibleForTesting
    ComponentHealthChecker(@Nonnull final ComponentRegistry componentRegistry,
                           @Nonnull final ScheduledExecutorService scheduledExecutorService,
                           @Nonnull final AsyncRestTemplate asyncRestTemplate,
                           final long healthCheckInterval,
                           @Nonnull final TimeUnit healthCheckIntervalUnit) {
        this.componentRegistry = componentRegistry;
        this.asyncRestTemplate = asyncRestTemplate;

        scheduledExecutorService.scheduleAtFixedRate(this::checkComponentHealth,
            healthCheckInterval, healthCheckInterval, healthCheckIntervalUnit);
    }

    @Nonnull
    private static AsyncRestTemplate makeAsyncRestTemplate(@Nonnull final ExecutorService executorService,
                                                           final long healthCheckConnectTimeout,
                                                           final long healthCheckReadTimeout,
                                                           @Nonnull final TimeUnit healthCheckIntervalUnit) {
        final SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout((int)healthCheckIntervalUnit.toSeconds(healthCheckConnectTimeout));
        requestFactory.setReadTimeout((int)healthCheckIntervalUnit.toSeconds(healthCheckReadTimeout));
        final AsyncListenableTaskExecutor taskExecutor = new ConcurrentTaskExecutor(executorService);
        requestFactory.setTaskExecutor(taskExecutor);

        return new AsyncRestTemplate(requestFactory);
    }

    private void checkComponentHealth() {
        final Table<String, String, RegisteredComponent> components =
            componentRegistry.getRegisteredComponents();
        final MutableInt scheduledCounter = new MutableInt(0);
        components.rowMap().forEach((componentType, instancesById) -> {
            instancesById.forEach((instanceId, registeredComponent) -> {
                try {
                    // The processing here is closely tied to the implementation of the health endpoint
                    // in ComponentController.
                    final URI uri = registeredComponent.getUri(ComponentController.HEALTH_PATH);
                    logger.debug("Checking health of {} {}", componentType, instanceId);
                    final ListenableFuture<ResponseEntity<String>> future =
                        asyncRestTemplate.getForEntity(uri, String.class);
                    // We don't wait for the callbacks to finish.
                    future.addCallback(new HealthCheckCallback(registeredComponent.getComponentInfo(), componentRegistry));
                    scheduledCounter.increment();
                } catch (URISyntaxException e) {
                    logger.error("Failed to format URI for health check of component {} instance {}. Error: {}",
                        componentType, instanceId, e.getMessage());
                }
            });
        });

        if (scheduledCounter.intValue() > 0) {
            logger.debug("Scheduled {} health checks across {} instances.", scheduledCounter.intValue(), components.size());
        }
    }

    /**
     * Handler for the health check responses returned by components.
     */
    private static class HealthCheckCallback implements ListenableFutureCallback<ResponseEntity<String>> {

        private final ComponentRegistry componentRegistry;
        private final ComponentInfo componentInfo;

        private HealthCheckCallback(@Nonnull final ComponentInfo componentInfo,
                            @Nonnull final ComponentRegistry componentRegistry) {
            this.componentInfo = componentInfo;
            this.componentRegistry = componentRegistry;
        }

        @Override
        public void onFailure(final Throwable ex) {
            String newDescription = "Health check failed: " + ex.getMessage();
            try {
                componentRegistry.updateComponentHealthStatus(componentInfo.getId(), ComponentHealth.CRITICAL, newDescription);
            } catch (RegistryUpdateException e) {
                logger.error("Failed to update component health status of component " +
                    getComponentLogId(componentInfo.getId()), e);
            }
        }

        @Override
        public void onSuccess(final ResponseEntity<String> result) {
            final ComponentHealth newStatus;
            final String statusDescription;
            if (result.getStatusCode() == HttpStatus.OK) {
                newStatus = ComponentHealth.HEALTHY;
            } else {
                // This is the status returned in ComponentController when the health check
                // is not healthy.
                if (result.getStatusCode() == HttpStatus.SERVICE_UNAVAILABLE) {
                    newStatus = ComponentHealth.WARNING;
                } else {
                    newStatus = ComponentHealth.CRITICAL;
                }
            }

            if (result.hasBody()) {
                statusDescription = result.getBody();
            } else {
                statusDescription = "No details returned by component health check.";
            }

            try {
                componentRegistry.updateComponentHealthStatus(componentInfo.getId(), newStatus, statusDescription);
            } catch (RegistryUpdateException e) {
                logger.error("Failed to update component health status of component " +
                    getComponentLogId(componentInfo.getId()), e);
            }
        }
    }
}
