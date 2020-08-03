package com.vmturbo.cloud.commitment.analysis.demand;

import java.util.Collection;
import java.util.Comparator;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.stream.Collector;
import java.util.stream.Collector.Characteristics;

import javax.annotation.Nonnull;

/**
 * A {@link NavigableSet}, with the underlying data containing a time-series data. Iterating on the
 * underlying data is guaranteed to return the data sorted by the data's timestamp.
 * @param <T> The type of the contained data. The type must have a time attribute.
 */
public interface TimeSeries<T> extends NavigableSet<T> {

    /**
     * Creates a {@link TimeSeries} from a collection of {@link TimeSeriesData}.
     * @param c The collection to populate the returned {@link TimeSeries}.
     * @param <T> The type of the collected data. The type must extend {@link TimeSeriesData}.
     * @return The newly created {@link TimeSeries}.
     */
    @Nonnull
    static <T extends TimeSeriesData> TimeSeries<T> newTimeSeries(Collection<T> c) {
        return new ModifiableTimeSeries<>(c,
                Comparator.comparing(series -> series.timeInterval().startTime()));
    }

    /**
     * Creates a {@link TimeSeries}, representing a timeline of {@link TimeInterval} instances. The
     * time intervals will be sorted by {@link TimeInterval#startTime()}.
     * @param c An collection of {@link TimeInterval} instances.
     * @return The newly created {@link TimeSeries}
     */
    @Nonnull
    static TimeSeries<TimeInterval> newTimeline(Collection<TimeInterval> c) {
        return new ModifiableTimeSeries<>(c, Comparator.comparing(TimeInterval::startTime));
    }

    /**
     * Returns a {@link Collector} for generating a time series from {@link TimeSeriesData}.
     * @param <T> The type of the collected data, which must extend {@link TimeSeriesData}.
     * @return A {@link Collector} for generating a time series from {@link TimeSeriesData}.
     */
    @Nonnull
    static <T extends TimeSeriesData> Collector<T, ?, TimeSeries<T>> toTimeSeries() {
        return Collector.<T, ModifiableTimeSeries<T>, TimeSeries<T>>of(
                () -> new ModifiableTimeSeries<T>(
                        Comparator.comparing(series -> series.timeInterval().startTime())),
                ModifiableTimeSeries::add,
                (left, right) -> {
                    left.addAll(right);
                    return left;
                },
                TimeSeries.class::cast,
                Characteristics.UNORDERED);
    }

    /**
     * A modifiable implementation of {@link TimeSeries}.
     * @param <T> The type of the contained data.
     */
    class ModifiableTimeSeries<T> extends TreeSet<T> implements TimeSeries<T> {

        private ModifiableTimeSeries(Comparator<T> comparator) {
            super(comparator);
        }

        private ModifiableTimeSeries(Collection<T> c, Comparator<T> comparator) {
            super(comparator);
            addAll(c);
        }
    }
}
