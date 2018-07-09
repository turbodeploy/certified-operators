package com.vmturbo.platform.analysis.utilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Tests for {@link PlacementStats}.
 */
public class PlacementStatsTest {
    final PlacementStats placementStats = new PlacementStats();

    @Test
    public void testLogMessage() {
        assertEquals("[PlacementStats: (cliqueMin:0, quoteSum:0, quoteMin:0; socQuotes:0, budgetDepletionQuotes:0)]",
            placementStats.logMessage());
    }

    @Test
    public void testIncrementCliqueMinimizationCount() {
        placementStats.incrementCliqueMinimizationCount();
        assertTrue(placementStats.logMessage().contains("cliqueMin:1"));
    }

    @Test
    public void testIncrementQuoteSummerCount() {
        for (int i = 0; i < 123; i++) {
            placementStats.incrementQuoteSummerCount();
        }

        assertTrue(placementStats.logMessage().contains("quoteSum:123"));
    }

    @Test
    public void testIncrementQuoteMinimizerCount() {
        placementStats.incrementQuoteMinimizerCount();

        assertTrue(placementStats.logMessage().contains("quoteMin:1"));

    }

    @Test
    public void testIncrementSumOfCommodityQuoteCount() {
        placementStats.incrementSumOfCommodityQuoteCount();

        assertTrue(placementStats.logMessage().contains("socQuotes:1"));
    }

    @Test
    public void testIncrementBudgetDepletionQuoteCount() {
        placementStats.incrementBudgetDepletionQuoteCount();

        assertTrue(placementStats.logMessage().contains("budgetDepletionQuotes:1"));
    }

    @Test
    public void testClear() {
        placementStats.incrementCliqueMinimizationCount();
        assertTrue(placementStats.logMessage().contains("cliqueMin:1"));

        placementStats.clear();
        assertTrue(placementStats.logMessage().contains("cliqueMin:0"));

        placementStats.incrementCliqueMinimizationCount();
        assertTrue(placementStats.logMessage().contains("cliqueMin:1"));
    }
}