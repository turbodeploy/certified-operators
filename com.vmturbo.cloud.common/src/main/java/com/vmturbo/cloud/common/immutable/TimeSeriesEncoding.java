package com.vmturbo.cloud.common.immutable;

import java.util.Collection;

import javax.annotation.Nonnull;

import org.immutables.encode.Encoding;

import com.vmturbo.cloud.common.data.ImmutableTimeSeries;
import com.vmturbo.cloud.common.data.TimeSeries;
import com.vmturbo.cloud.common.data.TimeSeriesData;

/**
 * This is an immutable encoder to support utility methods for adding {@link TimeSeriesData} instances
 * to a {@link TimeSeries}, with sorting done on a time series basis.
 *
 * <p>NOTE: THIS ENCODER MUST BE IN A SEPARATE PROJECT FROM ITS TARGET CLASS.
 *
 * @param <TimeSeriesDataTypeT> The {@link TimeSeriesData} type.
 */
@Encoding
final class TimeSeriesEncoding<TimeSeriesDataTypeT extends TimeSeriesData> {

    @Encoding.Impl
    private TimeSeries<TimeSeriesDataTypeT> timeSeries;

    @Encoding.Expose
    public TimeSeries<TimeSeriesDataTypeT> get() {
        return timeSeries;
    }

    @Encoding.Of
    static <TimeSeriesDataTypeT extends TimeSeriesData> TimeSeries<TimeSeriesDataTypeT> init(
            @Nonnull Collection<TimeSeriesDataTypeT> inputSeries) {

        if (inputSeries instanceof ImmutableTimeSeries) {
            return (TimeSeries<TimeSeriesDataTypeT>)inputSeries;
        } else {
            return ImmutableTimeSeries.<TimeSeriesDataTypeT>builder()
                    .addAll(inputSeries)
                    .build();
        }
    }

    /**
     * A builder template for a time-series sorted set.
     * @param <TimeSeriesDataTypeT> The {@link TimeSeriesData} subtype.
     */
    @Encoding.Builder
    static class Builder<TimeSeriesDataTypeT extends TimeSeriesData> {


        private ImmutableTimeSeries.Builder<TimeSeriesDataTypeT> timeSeries =
                ImmutableTimeSeries.builder();

        @Encoding.Naming(value = "add*", depluralize = true)
        @Encoding.Init
        void add(TimeSeriesDataTypeT element) {
            timeSeries.add(element);
        }

        @Encoding.Naming(value = "add*", depluralize = false)
        @Encoding.Init
        void addArray(TimeSeriesDataTypeT... elements) {
            timeSeries.add(elements);
        }

        @Encoding.Naming("addAll*")
        @Encoding.Init
        void addAll(Collection<? extends TimeSeriesDataTypeT> elements) {
            timeSeries.addAll(elements);
        }

        @Encoding.Init
        @Encoding.Copy
        public void set(@Nonnull Collection<TimeSeriesDataTypeT> timeSeresData) {
            this.timeSeries = ImmutableTimeSeries.builder();
            this.timeSeries.addAll(timeSeresData);
        }

        @Encoding.Build
        TimeSeries<TimeSeriesDataTypeT> build() {
            return timeSeries.build();
        }
    }
}
