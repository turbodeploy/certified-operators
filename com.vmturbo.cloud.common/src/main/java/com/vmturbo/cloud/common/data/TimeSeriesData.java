package com.vmturbo.cloud.common.data;

import javax.annotation.Nonnull;

/**
 * An interface for data that is associated with a {@link TimeInterval}.
 */
public interface TimeSeriesData {

    /**
     * The {@link TimeInterval} associated with the data encapsulated in implementations
     * of {@link TimeSeriesData}.
     * @return The {@link TimeInterval}.
     */
    @Nonnull
    TimeInterval timeInterval();
}
