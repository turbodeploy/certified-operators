package com.vmturbo.proactivesupport;

import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * The DataMetricBuilder implements the data metric builder.
 */
public abstract class DataMetricBuilder<T extends DataMetricBuilder> {
    /**
     * The metric name.
     */
    protected String name_;

    /**
     * The metric labels.
     */
    protected String[] labelNames_ = new String[]{}; // default to no labels

    /**
     * The help.
     */
    protected String help_;

    /**
     * The metric severity. Defaults to INFO
     */
    protected DataMetric.Severity severity_ = DataMetric.Severity.INFO;

    /**
     * The urgency flag. Defaults to false.
     */
    protected boolean urgent_ = false;

    /**
     * Don't allow to initialize the builder using a constructor directly.
     */
    protected DataMetricBuilder() {
    }

    /**
     * Sets the metric name.
     *
     * @param name The metric name.
     * @return This instance.
     */
    public T withName(final @Nonnull String name) {
        name_ = Objects.requireNonNull(name);
        return (T)this;
    }

    /**
     * Sets the metric label names.
     *
     * @param labels The metric label names.
     * @return This instance.
     */
    public T withLabelNames(final @Nonnull String... labels) {
        labelNames_ = Objects.requireNonNull(labels);
        return (T)this;
    }

    /**
     * Sets the metric jelp.
     *
     * @param help The metric help.
     * @return This instance.
     */
    public T withHelp(final @Nonnull String help) {
        help_ = Objects.requireNonNull(help);
        return (T)this;
    }

    /**
     * Sets the metric severity.
     *
     * @param severity The metric severity.
     * @return This instance.
     */
    public T withSeverity(final @Nonnull DataMetric.Severity severity) {
        severity_ = Objects.requireNonNull(severity);
        return (T)this;
    }

    /**
     * Sets the scalar metric.
     *
     * @return The metric builder.
     */
    public T withUrgent() {
        urgent_ = true;
        return (T)this;
    }
}
