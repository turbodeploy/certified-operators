package com.vmturbo.proactivesupport;

import javax.annotation.Nonnull;

/**
 * DataMetricCollectionListener is used to follow updates to a collection of DataMetrics
 */
public interface DataMetricCollectionListener {
    /**
     * Fired when a new data metric has been registered.
     * @param metric
     */
    void onDataMetricRegistered(@Nonnull DataMetric metric);

    /**
     * Fired when a data metric has been unregistered.
     * @param metric
     */
    void onDataMetricUnregistered(@Nonnull DataMetric metric);
}
