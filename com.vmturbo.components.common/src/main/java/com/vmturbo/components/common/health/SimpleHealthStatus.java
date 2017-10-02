package com.vmturbo.components.common.health;

import java.time.Instant;
import java.util.Objects;

import javax.annotation.concurrent.Immutable;

/**
 * A simple health status. Represents whether something is healthy or not, stores a description
 * associated with the health status, and a status recorded time.
 */
@Immutable
public class SimpleHealthStatus implements HealthStatus {
    /**
     * The health status this object represents. (true = is healthy)
     */
    protected boolean currentHealthStatus;
    /**
     * optional string that may provide additional info about the current status
     */
    protected String details;

    /**
     * the time this check was performed
     */
    protected Instant checkTime;

    /**
     * Constructs a health status with specified health state and details string
     * @param currentHealthStatus the health state the status should represent
     * @param details a string description associated with the current status
     */
    public SimpleHealthStatus(boolean currentHealthStatus, String details) {
        this.currentHealthStatus = currentHealthStatus;
        this.details = details;
        checkTime = Instant.now(); // set the check time to now
    }

    /**
     * Does the status represent a healthy state?
     * @return true if so, false otherwise
     */
    public boolean isHealthy() { return currentHealthStatus; }

    /**
     * Get the details associated with the current state of health
     * @return a string containing any additional details associated with the current health status
     */
    public String getDetails() { return details; }

    /**
     * Get the time the last health status check was performed.
     *
     * @return the time the last health status check was performed.
     */
    public Instant getCheckTime() {
        return checkTime;
    }

    /**
     * Two SimpleHealthStatus objects are equal if they have the same field values.
     * @param obj
     * @return
     */
    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof SimpleHealthStatus)) {
            return false;
        }

        // compare all of the fields and return true if all match
        SimpleHealthStatus other = (SimpleHealthStatus) obj;
        if (other.isHealthy() != this.isHealthy()) {
            return false;
        }
        if (!other.checkTime.equals(this.checkTime)) {
            return false;
        }
        return other.getDetails().equals(this.getDetails());
    }

    /**
     * Generate a hash code for this object.
     * @return the hash code
     */
    @Override
    public int hashCode() {
        return Objects.hash(currentHealthStatus, checkTime, details);
    }
}
