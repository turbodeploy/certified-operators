package com.vmturbo.proactivesupport;

import java.io.Serializable;
import javax.annotation.Nonnull;

/**
 * The DataMetric implements a data collector message.
 */
public interface DataMetric<M> extends Serializable {
    /**
     * The severity.
     */
    enum Severity {
        INFO,
        WARN,
        ERROR,
        FATAL;

        /**
         * Returns the numerical severity.
         * At the moment, it just returns the ordinal(), but that may change.
         *
         * @return The numerical severity.
         */
        public int severity() {
            return ordinal();
        }
    }

    /**
     * The message type.
     */
    enum MetricType {
        COUNTER,
        GAUGE,
        HISTOGRAM,
        SUMMARY,
        LOB
    }

    /**
     * Returns the flag specifying whether this is an urgent metric.
     *
     * @return The flag specifying whether this is an urgent metric.
     */
    boolean isUrgent();

    /**
     * Returns the metric type.
     *
     * @return The metric type.
     */
    @Nonnull MetricType getType();

    /**
     * Returns the metric label.
     *
     * @return The metric label.
     */
    @Nonnull String[] getLabelNames();

    /**
     * Returns the metric help.
     *
     * @return The metric help.
     */
    @Nonnull String getHelp();

    /**
     * Returns the data collector id.
     * For example, it could be "heap", "disk_space", etc.
     */
    @Nonnull String getName();

    /**
     * Returns the severity.
     */
    @Nonnull Severity getSeverity();

    /**
     * Register with the LDCF.
     */
    M register();
}
