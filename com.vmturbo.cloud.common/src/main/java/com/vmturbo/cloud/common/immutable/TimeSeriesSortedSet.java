package com.vmturbo.cloud.common.immutable;

import java.util.Collection;
import java.util.Comparator;
import java.util.NavigableSet;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSortedSet;

import org.immutables.encode.Encoding;

import com.vmturbo.cloud.common.data.TimeSeriesData;

/**
 * This is an immutable encoder to support utility methods for adding {@link TimeSeriesData} instances
 * to a {@link NavigableSet}, with sorting done on a time series basis.
 *
 * <p>NOTE: THIS ENCODER MUST BE IN A SEPARATE PROJECT FROM ITS TARGET CLASS.
 *
 * <p>NOTE: While the {@link TimeSeriesDataTypeT} is bounded by {@link TimeSeriesData}, the immutable
 * framework currently doesn't respect the bounds in matching to attributes. Therefore, it will match
 * to any {@link NavigableSet} attributes within the data class. If the attribute does not wrap
 * a {@link TimeSeriesData} type, this will result in a compilation error. There is no ability to
 * enable on the encoder on individual attributes
 * @param <TimeSeriesDataTypeT> The data type of the {@link NavigableSet} collection.
 */
@Encoding
final class TimeSeriesSortedSet<TimeSeriesDataTypeT extends TimeSeriesData> {

    @Encoding.Impl
    private NavigableSet<TimeSeriesDataTypeT> timeSeriesSortedSet;

    @Encoding.Expose
    public NavigableSet<TimeSeriesDataTypeT> get() {
        return timeSeriesSortedSet;
    }

    @Encoding.Of
    static <TimeSeriesDataTypeT extends TimeSeriesData> NavigableSet<TimeSeriesDataTypeT> init(
            @Nonnull Collection<TimeSeriesDataTypeT> inputSeries) {

        return ImmutableSortedSet
                .orderedBy(Comparator.comparing((TimeSeriesDataTypeT t) -> t.timeInterval().startTime()))
                .addAll(inputSeries)
                .build();
    }

    /**
     * A builder template for a time-series sorted set.
     * @param <TimeSeriesDataTypeT> The {@link TimeSeriesData} subtype.
     */
    @Encoding.Builder
    static class Builder<TimeSeriesDataTypeT extends TimeSeriesData> {

        private final Comparator<TimeSeriesDataTypeT> timeSeriesComparator =
                Comparator.comparing(series -> series.timeInterval().startTime());

        private ImmutableSortedSet.Builder<TimeSeriesDataTypeT> timeSeriesSet =
                ImmutableSortedSet.orderedBy(timeSeriesComparator);

        @Encoding.Naming(value = "add*", depluralize = true)
        @Encoding.Init
        void add(TimeSeriesDataTypeT element) {
            timeSeriesSet.add(element);
        }

        @Encoding.Naming("addAll*")
        @Encoding.Init
        void addAll(Collection<? extends TimeSeriesDataTypeT> elements) {
            timeSeriesSet.addAll(elements);
        }

        @Encoding.Init
        @Encoding.Copy
        public void set(@Nonnull Collection<TimeSeriesDataTypeT> timeSeresData) {
            this.timeSeriesSet = ImmutableSortedSet.orderedBy(timeSeriesComparator);
            this.timeSeriesSet.addAll(timeSeresData);
        }

        @Encoding.Build
        NavigableSet<TimeSeriesDataTypeT> build() {
            return timeSeriesSet.build();
        }
    }
}
