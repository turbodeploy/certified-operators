package com.vmturbo.cloud.common.data;

import java.util.Comparator;
import java.util.NavigableSet;

/**
 * A {@link NavigableSet}, with the underlying data containing a time-series data. Iterating on the
 * underlying data is guaranteed to return the data sorted by the data's timestamp. {@link TimeSeriesData}
 * items will first be sorted by the start timestamp. If two entries have the same start time, end time
 * will be used as a tie breaker. And entries with the same time interval will be sorted by hash code.
 * @param <T> The type of the contained data. The type must have a time attribute.
 */
public interface TimeSeries<T extends TimeSeriesData> extends NavigableSet<T> {

    /**
     * A comparator for sorting {@link TimeInterval} instances.
     */
    Comparator<TimeInterval> TIME_INTERVAL_COMPARATOR =
            Comparator.comparing(TimeInterval::startTime)
                    .thenComparing(TimeInterval::endTime);

    /**
     * A comparator for sorting {@link TimeSeriesData} instances. Sorts by the time interval and then
     * hashcode as a tie breaker
     */
    Comparator<TimeSeriesData> TIME_SERIES_COMPARATOR =
            Comparator.comparing(TimeSeriesData::timeInterval, TIME_INTERVAL_COMPARATOR)
                    .thenComparing(TimeSeriesData::hashCode);
}
