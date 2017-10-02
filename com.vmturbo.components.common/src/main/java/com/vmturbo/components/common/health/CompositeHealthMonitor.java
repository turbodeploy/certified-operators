package com.vmturbo.components.common.health;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

/**
 * The CompositeHealthMonitor is intended to provide an aggregate health status based on the
 * health of a set of dependencies. If all dependencies report a healthy status, then the composite
 * health is "healthy". If any dependency is unhealthy, the composite is "unhealthy".
 */
@ThreadSafe
public class CompositeHealthMonitor implements HealthStatusProvider {

    private final Map<String,HealthStatusProvider> dependencies = new ConcurrentHashMap<String,HealthStatusProvider>();

    /**
     * Constructs a HealthMonitor
     */
    public CompositeHealthMonitor() {
    }

    /**
     * Add (or replace) a subcomponent health status provider to this health monitor.
     *
     * @param name the name of the subcomponent
     * @param provider the provider of the subcomponent's health status information
     */
    public void addHealthCheck(String name, HealthStatusProvider provider) {
        dependencies.put(name,provider);
    }

    /**
     * Get the map of registered health checks.
     *
     * @return the map of registered health checks
     */
    @Nonnull
    public Map<String,HealthStatusProvider> getDependencies() {
        return dependencies;
    }

    /**
     * Get the current health status
     * @return the composite health status
     */
    @Nonnull
    public CompositeHealthStatus getHealthStatus() {
        // create a new map of dependency name -> health status
        Map<String,HealthStatus> dependencyHealthStatuses
                = dependencies.entrySet().stream().collect(Collectors.toMap(
                        e -> e.getKey(),
                        e -> e.getValue().getHealthStatus()
                ));
        // use the map to create a new composite health status
        return new CompositeHealthStatus(dependencyHealthStatuses);
    }

    /**
     * Composite Health Status represents an aggregate health description, broken down into constituents.

     * A composite status is healthy if all dependencies are healthy. If there are no
     * dependencies, we are considered not healthy. This behavior is to help prevent
     * CompositeHealthMonitor instances from reporting false "healthy" results while the composite
     * is still being assembled.
     *
     * The details for an unhealthy composite will contain a list of the dependencies that are
     * unhealthy.
     */
    public static class CompositeHealthStatus extends SimpleHealthStatus {
        public final Map<String,HealthStatus> dependencies;

        public CompositeHealthStatus(Map<String,HealthStatus> healthStatuses) {
            super(false, "No dependencies.");
            dependencies = healthStatuses;

            if (!dependencies.isEmpty()) {
                // if at least one dependency, determine composite health based on the dependencies
                currentHealthStatus = true;
                StringBuilder detailBuffer = new StringBuilder("All dependencies healthy.");
                dependencies.forEach((name,status) -> {
                    if (!status.isHealthy()) {
                        // unhealthy dependency -- mark the aggregate as unhealthy and add the
                        // dependency to the list of unhealthy dependencies.
                        if (currentHealthStatus) {
                            // the first unhealthy result adds a prefix explanation for an "unhealthy" message
                            detailBuffer.replace(0, detailBuffer.length(), "The following dependencies are unhealthy:");
                        }
                        currentHealthStatus = false;
                        detailBuffer.append(" ").append(name);
                    }
                });
                details = detailBuffer.toString();
            }
            checkTime = Instant.now();
        }
    }
}
