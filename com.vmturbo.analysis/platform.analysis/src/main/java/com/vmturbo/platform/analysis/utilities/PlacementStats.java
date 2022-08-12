package com.vmturbo.platform.analysis.utilities;

import java.io.Serializable;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.pricefunction.QuoteFunctionFactory;

/**
 * Keeps statistics on placements (see {@link com.vmturbo.platform.analysis.ede.Placement}) performed
 * during analysis.
 *
 * Analysis may perform multiple rounds of placement. {@link PlacementStats} may be cleared
 * between each phase of analysis.
 *
 * The increment calls on this stats object are thread-safe relative to each other and themselves
 * (that is, it is ok for two threads to call {@link #incrementCliqueMinimizationCount} at the same time,
 * but the increment methods are NOT thread-safe relative to the {@link #clear()} and log methods.
 */
public class PlacementStats implements Serializable {

    private static final long serialVersionUID = 4307272830280335360L;

    /**
     * The number of times {@link com.vmturbo.platform.analysis.ede.CliqueMinimizer#accept(long)} was called
     * during a phase of analysis.
     */
    private final AtomicLong cliqueMinimizationCount = new AtomicLong();

    /**
     * The number of times {@link com.vmturbo.platform.analysis.ede.QuoteSummer#accept(Entry)} was called
     * during a phase of analysis.
     */
    private final AtomicLong quoteSummerCount = new AtomicLong();

    /**
     * The number of times {@link com.vmturbo.platform.analysis.ede.QuoteSummer#accept(Entry)} was called
     * during a phase of analysis.
     */
    private final AtomicLong quoteMinimizationCount = new AtomicLong();

    /**
     * The number of times a quote was collected via {@link QuoteFunctionFactory#sumOfCommodityQuoteFunction()}
     */
    private final AtomicLong sumOfCommodityQuoteCount = new AtomicLong();

    /**
     * The number of times a quote was collected via {@link QuoteFunctionFactory#sumOfCommodityQuoteFunction()}
     */
    private final AtomicLong budgetDepletionRiskBasedQuoteCount = new AtomicLong();

    /**
     * Create new {@link PlacementStats} for tracking statistics about placement.
     */
    public PlacementStats() {
        clear();
    }

    /**
     * Clear the {@link PlacementStats}. This resets all counters back to zero.
     */
    public void clear() {
        cliqueMinimizationCount.set(0);
        quoteSummerCount.set(0);
        quoteMinimizationCount.set(0);
        sumOfCommodityQuoteCount.set(0);
        budgetDepletionRiskBasedQuoteCount.set(0);
    }

    public String logMessage() {
        return String.format("[PlacementStats: (cliqueMin:%d, quoteSum:%d, quoteMin:%d; " +
                "socQuotes:%d, budgetDepletionQuotes:%d)]",
            cliqueMinimizationCount.get(),
            quoteSummerCount.get(),
            quoteMinimizationCount.get(),
            sumOfCommodityQuoteCount.get(),
            budgetDepletionRiskBasedQuoteCount.get());
    }

    /**
     * Increment the number of times that {@link com.vmturbo.platform.analysis.ede.CliqueMinimizer#accept(long)}
     * was called.
     *
     * This call is thread-safe relative to all the other increment calls.
     *
     * @return The new number of times after performing the increment.
     */
    public long incrementCliqueMinimizationCount() {
        return cliqueMinimizationCount.incrementAndGet();
    }

    /**
     * Increment the number of times that {@link com.vmturbo.platform.analysis.ede.QuoteSummer#accept(Entry)}
     * was called.
     *
     * This call is thread-safe relative to all the other increment calls.
     *
     * @return The new number of times after performing the increment.
     */
    public long incrementQuoteSummerCount() {
        return quoteSummerCount.incrementAndGet();
    }

    /**
     * Increment the number of times that {@link com.vmturbo.platform.analysis.ede.QuoteMinimizer#accept(Trader)}
     * was called.
     *
     * This call is thread-safe relative to all the other increment calls.
     *
     * @return The new number of times after performing the increment.
     */
    public long incrementQuoteMinimizerCount() {
        return quoteMinimizationCount.incrementAndGet();
    }

    /**
     * Increment the number of times that a quote was collected via
     * {@link QuoteFunctionFactory#sumOfCommodityQuoteFunction()}
     *
     * This call is thread-safe relative to all the other increment calls.
     *
     * @return The new number of times after performing the increment.
     */
    public long incrementSumOfCommodityQuoteCount() {
        return sumOfCommodityQuoteCount.incrementAndGet();
    }

    /**
     * Increment the number of times that a quote was collected via
     * {@link QuoteFunctionFactory#budgetDepletionRiskBasedQuoteFunction()} ()}
     *
     * This call is thread-safe relative to all the other increment calls.
     *
     * @return The new number of times after performing the increment.
     */
    public long incrementBudgetDepletionQuoteCount() {
        return budgetDepletionRiskBasedQuoteCount.incrementAndGet();
    }
}
