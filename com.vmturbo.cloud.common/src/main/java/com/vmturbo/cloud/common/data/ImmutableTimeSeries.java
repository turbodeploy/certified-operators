package com.vmturbo.cloud.common.data;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.stream.Collector;

import javax.annotation.Nonnull;

import com.google.common.collect.ForwardingNavigableSet;
import com.google.common.collect.ImmutableSortedSet;

/**
 * An immutable implementaion of {@link TimeSeries}, which wraps an {@link ImmutableSortedSet} through
 * {@link ForwardingNavigableSet}.
 * @param <T> The {@link TimeSeriesData} type.
 */
public class ImmutableTimeSeries<T extends TimeSeriesData> extends ForwardingNavigableSet<T> implements TimeSeries<T> {

    private final NavigableSet<T> delegate;

    private ImmutableTimeSeries(NavigableSet<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    protected NavigableSet<T> delegate() {
        return delegate;
    }

    /**
     * Creates a new {@link ImmutableTimeSeries}, populated with the provided {@code elements}.
     * @param elements The {@link TimeSeriesData} elements, used to populate the series.
     * @param <T> The {@link TimeSeriesData} type.
     * @return The newly constructed {@link ImmutableTimeSeries} instance.
     */
    @Nonnull
    public static <T extends TimeSeriesData> ImmutableTimeSeries<T> of(T... elements) {
        return ImmutableTimeSeries.<T>builder()
                .add(elements)
                .build();
    }

    /**
     * Constructs and returns a new {@link Builder} instance.
     * @param <T> The {@link TimeSeriesData} type.
     * @return The newly constructed {@link Builder} instance.
     */
    @Nonnull
    public static <T extends TimeSeriesData> Builder<T> builder() {
        return new Builder<T>();
    }

    /**
     * Returns a {@link Collector} for generating a time series from {@link TimeSeriesData}.
     * @param <T> The type of the collected data, which must extend {@link TimeSeriesData}.
     * @return A {@link Collector} for generating a time series from {@link TimeSeriesData}.
     */
    @Nonnull
    public static <T extends TimeSeriesData> Collector<T, ?, TimeSeries<T>> toImmutableTimeSeries() {

        // ImmutableSortedSet does not expose its combine method. In order to use it as part
        // of the collection, it would require building (and therefore sorting/deduping) one of
        // the builders on each combine operation. Therefore, we collect into an array list and
        // only build the immutable set on finalization.
        return Collector.of(
                () -> new ArrayList<T>(),
                ArrayList::add,
                (a1, a2) -> {
                    a1.addAll(a2);
                    return a1;
                },
                (elementList) -> ImmutableTimeSeries.<T>builder()
                        .addAll(elementList)
                        .build());
    }

    /**
     * A builder class for {@link ImmutableTimeSeries}.
     * @param <T> The {@link TimeSeriesData} type.
     */
    public static class Builder<T extends TimeSeriesData> {

        private final ImmutableSortedSet.Builder<T> timeSeriesSet =
                ImmutableSortedSet.orderedBy((Comparator<T>)TimeSeries.TIME_SERIES_COMPARATOR);

        /**
         * Adds the elements to the {@link ImmutableTimeSeries} under construction.
         * @param elements The {@link TimeSeriesData} elements to add.
         * @return This builder for method chaining.
         */
        @Nonnull
        public Builder<T> add(T... elements) {
            timeSeriesSet.add(elements);
            return this;
        }

        /**
         * Adds the element to the {@link ImmutableTimeSeries} under construction.
         * @param element The {@link TimeSeriesData} elements to add.
         * @return This builder for method chaining.
         */
        @Nonnull
        public Builder<T> add(T element) {
            timeSeriesSet.add(element);
            return this;
        }

        /**
         * Adds the elements to the {@link ImmutableTimeSeries} under construction.
         * @param elements The {@link TimeSeriesData} elements to add.
         * @return This builder for method chaining.
         */
        @Nonnull
        public Builder<T> addAll(Iterable<? extends T> elements) {
            timeSeriesSet.addAll(elements);
            return this;
        }

        /**
         * Adds the elements to the {@link ImmutableTimeSeries} under construction.
         * @param elements The {@link TimeSeriesData} elements to add.
         * @return This builder for method chaining.
         */
        @Nonnull
        public Builder<T> addAll(Iterator<? extends T> elements) {
            timeSeriesSet.addAll(elements);
            return this;
        }

        /**
         * Constructs a new {@link ImmutableTimeSeries} instance.
         * @return The newly constructed {@link ImmutableTimeSeries} instance.
         */
        @Nonnull
        public ImmutableTimeSeries<T> build() {
            return new ImmutableTimeSeries<T>(timeSeriesSet.build());
        }
    }
}
