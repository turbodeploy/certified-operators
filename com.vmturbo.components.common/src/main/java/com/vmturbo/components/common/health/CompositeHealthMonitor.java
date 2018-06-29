package com.vmturbo.components.common.health;

import java.time.Instant;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The CompositeHealthMonitor is intended to provide an aggregate health status based on the
 * health of a set of dependencies. If all dependencies report a healthy status, then the composite
 * health is "healthy". If any dependency is unhealthy, the composite is "unhealthy".
 */
@ThreadSafe
public class CompositeHealthMonitor implements HealthStatusProvider {

    private Logger logger = LogManager.getLogger();

    // default health details when all composite health checks are healthy
    private static final String ALL_CLEAR_MESSAGE = "All dependencies healthy.";
    // health details when there are no dependencies
    private static final String NO_DEPENDENCIES_MESSAGE = "No dependencies.";
    // header appended to the details when at least one dependency is unhealthy
    private static final String UNHEALTHY_DEPENDENCIES_HEADER = "The following dependencies are unhealthy:";

    private final String name;

    private final Map<String,HealthStatusProvider> dependencies = new ConcurrentHashMap<String,HealthStatusProvider>();

    // remember the last status so we can correctly determine the "since" time.
    private CompositeHealthStatus lastStatus;

    /**
     * Constructs a HealthMonitor
     */
    public CompositeHealthMonitor(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    /**
     * Add (or replace) a subcomponent health status provider to this health monitor.
     *
     * @param provider the provider of the subcomponent's health status information.
     * @return this -- for fluent usage.
     */
    public CompositeHealthMonitor addHealthCheck(HealthStatusProvider provider) {
        if (dependencies.containsKey(provider.getName())) {
            // log a warning that we are replacing a health check
            logger.warn("Health checked named {} was already registered and is being replaced.", provider.getName());
        }
        dependencies.put(provider.getName(), provider);
        return this;
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
        // create a new map of dependency name -> health status that are the current results
        final Map<String,HealthStatus> dependencyHealthStatuses
                = dependencies.entrySet().stream().collect(Collectors.toMap(
                        e -> e.getKey(),
                        e -> e.getValue().getHealthStatus()
                ));
        // use the map to create a new composite health status
        final boolean isHealthy = deriveCompositeHealth(dependencyHealthStatuses);
        final String details = deriveCompositeDetails(dependencyHealthStatuses);
        lastStatus = new CompositeHealthStatus(dependencyHealthStatuses, isHealthy, details, lastStatus);
        return lastStatus;
    }

    /**
     * Give a list of health checks, determine if the composite result is healthy (all checks pass)
     * or unhealthy (at least one failed)
     * @param healthStatuses the list of checks to derive the composite status from
     * @return true if all checks report healthy, false if at least one reports unhealthy
     */
    private boolean deriveCompositeHealth(@Nonnull  Map<String,HealthStatus> healthStatuses) {
        // special case if there are no dependencies
        if ((null == healthStatuses) || (healthStatuses.isEmpty())) return false;

        for (HealthStatus status : healthStatuses.values()) {
            if (! status.isHealthy()) return false;
        }
        return true;
    }

    /**
     * Assemble the composite health details based on the set of constituent health results.
     * @param healthStatuses the set of health results to use
     * @return a string containing the summary details of all the constituent checks.
     */
    private String deriveCompositeDetails(@Nonnull Map<String,HealthStatus> healthStatuses) {
        // special case if there are no dependencies
        if ((null == healthStatuses) || (healthStatuses.isEmpty())) return NO_DEPENDENCIES_MESSAGE;

        boolean isHealthy = deriveCompositeHealth(healthStatuses);
        if (isHealthy) return ALL_CLEAR_MESSAGE;

        // we are unhealthy and have failing checks -- consolidate the details.
        StringBuilder detailBuffer = new StringBuilder(UNHEALTHY_DEPENDENCIES_HEADER);
        for (Entry<String,HealthStatus> healthStatusEntry : healthStatuses.entrySet()) {
            if (! healthStatusEntry.getValue().isHealthy()) detailBuffer.append(" ").append(healthStatusEntry.getKey());
        }

        return detailBuffer.toString();
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

        public CompositeHealthStatus(@Nonnull Map<String,HealthStatus> healthStatuses,
                                     boolean isHealthy, String details,
                                     @Nullable CompositeHealthStatus lastStatus) {
            super(isHealthy, details, lastStatus);
            dependencies = healthStatuses;
        }

    }
}
