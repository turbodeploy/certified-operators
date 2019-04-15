package com.vmturbo.market.topology.conversions;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import com.vmturbo.proactivesupport.DataMetricCounter;

/**
 * A utility for tracking frequently occurring errors during topology conversion to and from
 * market entities.
 * <p>
 * We need something like this because when certain types of errors occur, simply logging them leads
 * to enormous amounts of redundant logs (for example, every commodity of a particular type in
 * a 30k topology might have the same issue, which results in ~30k lines of almost identical logs).
 * We shouldn't be printing those at the error level, but we should still log that the errors
 * occurred.
 * <p>
 * This class is intended to be embedded inside the {@link TopologyConverter}, and used at
 * different phases of conversion (see: {@link Phase}). The intended usage is as follows:
 * 1) Mark the beginning of a "phase" of conversion (e.g. converting to market traders) with
 *    {@link ConversionErrorCounts#startPhase(Phase)}.
 * 2) When logging an error that might spam the logs, use
 *    {@link ConversionErrorCounts#recordError(ErrorCategory, String)}, and log the details
 *    at the debug or trace level.
 * 3) When the "phase" is over, mark the end of the phase. The {@link ConversionErrorCounts}
 *    will print a single log line with the different errors and their counts.
 */
public class ConversionErrorCounts {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Enumeration of the phases of conversion that use this utility object.
     * The errors encountered during each phase get mapped to different prometheus time series
     * (via labels), so it's important to not change the values.
     *
     * The user of the {@link ConversionErrorCounts} methods is responsible for marking the
     * beginnings and ends of phases via {@link ConversionErrorCounts#startPhase(Phase)} and
     * {@link ConversionErrorCounts#endPhase()} respectively.
     */
    public enum Phase {
        NONE,
        CONVERT_TO_MARKET,
        CONVERT_FROM_MARKET;
    }

    /**
     * Enum to represent possible categories.
     * <p>
     * Used to avoid indexing categories by strings, which can be unreliable if more than one
     * place in the code needs to record the same category of error.
     */
    public enum ErrorCategory {
        USED_GT_CAPACITY_MEDIATION,
        USED_GT_CAPACITY,
        MAX_QUANTITY_NEGATIVE,
        PEAK_NEGATIVE;
    }

    @GuardedBy("this")
    private Phase curPhase = Phase.NONE;

    private final Table<ErrorCategory, String, MutableInt> errorCountsByCategoryAndSubCategory =
        HashBasedTable.create();

    /**
     * Record an error encountered during the current phase of the conversion process.
     *
     * @param category The category. This can be any string.
     * @param subcategory The subcategory. This can be any string.
     */
    synchronized void recordError(@Nonnull final ErrorCategory category, @Nonnull final String subcategory) {
        MutableInt errorCount = errorCountsByCategoryAndSubCategory.get(category, subcategory);
        if (errorCount == null) {
            errorCount = new MutableInt(0);
            errorCountsByCategoryAndSubCategory.put(category, subcategory, errorCount);
        }
        errorCount.increment();
    }

    @VisibleForTesting
    @Nonnull
    synchronized Table<ErrorCategory, String, MutableInt> getErrorCountsDebug() {
        return errorCountsByCategoryAndSubCategory;
    }

    @VisibleForTesting
    synchronized Phase getCurPhaseDebug() {
        return curPhase;
    }

    /**
     * Mark the beginning of a new phase of conversion.
     * Each phase has its own error counts.
     *
     * @param phase The phase to start. Note - we use an enum instead of a free-form string because
     *              this enum value gets mapped to a prometheus label, so we want the naming
     *              to be more consistent.
     */
    synchronized void startPhase(@Nonnull final Phase phase) {
        // Since we don't support nested phases right now, we want to make sure to end the current
        // phase before starting the next one.
        endPhase();

        curPhase = phase;
    }

    /**
     * End the current phase.
     */
    synchronized void endPhase() {
        // Print the errors encountered so far.
        if (!errorCountsByCategoryAndSubCategory.isEmpty()) {
            final MutableInt totalErrorCount = new MutableInt(0);
            final StringBuilder errorBuilder = new StringBuilder();
            errorBuilder.append("Phase ").append(curPhase).append(" encountered errors!\n");
            errorCountsByCategoryAndSubCategory.rowMap().forEach((category, subCategoryCounts) -> {
                subCategoryCounts.forEach((subCategory, errorCount) -> {
                    errorBuilder.append(category.name()).append(":").append(subCategory)
                        .append(" - encountered ").append(errorCount.getValue()).append(" times.\n");
                    totalErrorCount.add(errorCount);
                });
            });

            logger.error(errorBuilder.toString());

            Metrics.MKT_CONVERSION_ERROR_COUNT.labels(curPhase.name().toLowerCase())
                .increment(totalErrorCount.doubleValue());

            // Clear the errors now that we've recorded them.
            errorCountsByCategoryAndSubCategory.clear();
        }

        curPhase = Phase.NONE;
    }

    private static class Metrics {

        private static final DataMetricCounter MKT_CONVERSION_ERROR_COUNT = DataMetricCounter.builder()
            .withName("mkt_conversion_error_count")
            .withHelp("The number of errors encountered during market conversion phases.")
            .withLabelNames("phase")
            .build()
            .register();
    }
}
