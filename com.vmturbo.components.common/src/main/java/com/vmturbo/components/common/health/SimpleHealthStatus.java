package com.vmturbo.components.common.health;

import java.time.Instant;
import java.util.Objects;

import javax.annotation.Nullable;
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
     * The time the current health status was first detected. This differs from check, since the current
     * health status could have been first detected several checks ago.
     */
    protected Instant since;

    /**
     * Constructs a health status with specified health state and details string
     * @param currentHealthStatus the health state the status should represent
     * @param details a string description associated with the current status
     */
    public SimpleHealthStatus(boolean currentHealthStatus, String details) {
        this(currentHealthStatus,details,null);
    }

    /**
     * Construct a health status with specified health state and details, as well as a last check
     * that will be used to determine if the "since" time should be kept the same.
     * @param currentHealthStatus
     * @param details
     * @param lastCheck
     */
    public SimpleHealthStatus(boolean currentHealthStatus, String details, @Nullable HealthStatus lastCheck) {
        this.currentHealthStatus = currentHealthStatus;
        this.details = details;
        checkTime = Instant.now(); // set the check time to now
        // if the status is the same as the one in lastCheck, keep the previous "since" time, even
        // if the details are different. Otherwise, this is the first time this status is observed,
        // so set "since" to the current check time.
        since = ((lastCheck != null) && (lastCheck.isHealthy() == currentHealthStatus)) ? lastCheck.getSince() : checkTime;
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
     * Get the time the current status was first detected. Note that the details are allowed to
     * change. "Since" only cares about when the current healthy or not-healthy state was first
     * entered.
     * @return the time when the current status was first detected.
     */
    public Instant getSince() { return since; }

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
        if (!other.since.equals(this.since)) {
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
        return Objects.hash(currentHealthStatus, checkTime, since, details);
    }
}
