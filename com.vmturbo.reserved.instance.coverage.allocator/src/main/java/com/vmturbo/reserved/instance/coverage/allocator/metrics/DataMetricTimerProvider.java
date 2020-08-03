package com.vmturbo.reserved.instance.coverage.allocator.metrics;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricSummary.SummaryData;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * Provides an interface for producing a {@link DataMetricTimer} from both {@link DataMetricSummary}
 * and {@link SummaryData} instances. The consumer of {@link DataMetricTimerProvider} does not need
 * to know whether labeled data is being collected.
 */
@FunctionalInterface
public interface DataMetricTimerProvider {

    /**
     * Provides a {@link DataMetricTimer} from a source metric type
     * @return The {@link DataMetricTimer} or null if no timer is being monitored
     */
    @Nullable
    DataMetricTimer startTimer();


    /**
     * Creates a {@link DataMetricTimerProvider} from a {@link DataMetricSummary} instance
     * @param dataMetricSummary The {@link DataMetricSummary} instance to convert to a {@link DataMetricTimerProvider}
     * @return The newly created {@link DataMetricTimerProvider} instance
     */
    static DataMetricTimerProvider createProvider(@Nonnull DataMetricSummary dataMetricSummary) {
        return () -> dataMetricSummary.startTimer();
    }

    /**
     * Creates a {@link DataMetricTimerProvider} from a {@link SummaryData} instance
     * @param summaryData The {@link SummaryData} instance to convert to a {@link DataMetricTimerProvider}
     * @return The newly created {@link DataMetricTimerProvider} instance
     */
    static DataMetricTimerProvider createProvider(@Nonnull SummaryData summaryData) {
        return () -> summaryData.startTimer();
    }

    /**
     * A {@link DataMetricTimerProvider} that returns null, indicating the duration of an operation
     * should not be monitored.
     */
    DataMetricTimerProvider EMPTY_PROVIDER = () -> null;
}
