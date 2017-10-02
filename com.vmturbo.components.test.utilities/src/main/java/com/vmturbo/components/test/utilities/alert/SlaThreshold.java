package com.vmturbo.components.test.utilities.alert;

import javax.annotation.Nonnull;

/**
 * An SLA_VIOLATION Threshold describes a Service Level Agreement that a metric must abide by or an alert will be triggered.
 */
public class SlaThreshold {
    /**
     * The raw value for the SLA_VIOLATION, suitable for direct comparison against
     * measured values for metrics.
     */
    private final double rawValue;

    /**
     * A description of the raw value.
     * For example, a rawValue of 1_000_000_000 may correspond to a description of "1Gb".
     */
    private final String description;

    /**
     * Create an SLA_VIOLATION threshold.
     *
     * @param rawValue The raw value for the SLA_VIOLATION. This raw value should be in a units system appropriate
     *                 for comparison against values measured for metrics.
     * @param description A description of the SLA_VIOLATION value. ie "3m", "4.1d", "120Mb", etc.
     */
    public SlaThreshold(final double rawValue, @Nonnull final String description) {
        this.rawValue = rawValue;
        this.description = description;
    }

    public double getRawValue() {
        return rawValue;
    }

    public String getDescription() {
        return description;
    }
}
