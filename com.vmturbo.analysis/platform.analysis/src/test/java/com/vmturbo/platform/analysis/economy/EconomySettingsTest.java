package com.vmturbo.platform.analysis.economy;

import static org.junit.Assert.*;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

/**
 * A test case for the {@link EconomySettings} class.
 */
@RunWith(JUnitParamsRunner.class)
public class EconomySettingsTest {
    // Fields
    private EconomySettings fixture_;

    // Methods

    @Before
    public void setUp() {
        fixture_ = new EconomySettings();
    }

    @Test
    public final void testEconomySettings() {
        @NonNull EconomySettings settings = new EconomySettings();

        assertEquals(EconomySettings.DEFAULT_MIN_SELLERS_FOR_PARALLELISM, settings.getMinSellersForParallelism());
        assertEquals(EconomySettings.DEFAULT_QUOTE_FACTOR, settings.getQuoteFactor(), 0f);

        // Sanity check: make sure initial values are valid.
        settings.setMinSellersForParallelism(settings.getMinSellersForParallelism());
        settings.setQuoteFactor(settings.getQuoteFactor());
    }

    @Test
    @Parameters({"0","1","10","100","1000","2147483647"})
    @TestCaseName("Test #{index}: (set|get)MinSellersForParallelism({0})")
    public final void testSetGetMinSellersForParallelism_NormalInput(int minSellers) {
        fixture_.setMinSellersForParallelism(minSellers);
        assertEquals(minSellers, fixture_.getMinSellersForParallelism());
    }

    @Test(expected = IllegalArgumentException.class)
    @Parameters({"-1","-2","-100","-2147483648"})
    @TestCaseName("Test #{index}: setMinSellersForParallelism({0})")
    public final void testSetGetMinSellersForParallelism_InvalidInput(int minSellers) {
        fixture_.setMinSellersForParallelism(minSellers);
    }

    @Test
    @Parameters({"0","0.1","2","100","10e15"})
    @TestCaseName("Test #{index}: (set|get)QuoteFactor({0})")
    public final void testSetGetQuoteFactor_NormalInput(double quoteFactor) {
        fixture_.setQuoteFactor(quoteFactor);
        assertEquals(quoteFactor, fixture_.getQuoteFactor(), 0f);
    }

    @Test(expected = IllegalArgumentException.class)
    @Parameters({"-0.1","-2","-100","-1e15"})
    @TestCaseName("Test #{index}: setQuoteFactor({0})")
    public final void testSetGetQuoteFactor_InvalidInput(double quoteFactor) {
        fixture_.setQuoteFactor(quoteFactor);
    }

    @Test
    @Parameters({"0","1","10","100","1000","2147483647"})
    @TestCaseName("Test #{index}: clear() with minSellersForParallelism == {0}")
    public final void testClear(int minSellers) {
        fixture_.setMinSellersForParallelism(minSellers);
        fixture_.clear();
        assertEquals(EconomySettings.DEFAULT_MIN_SELLERS_FOR_PARALLELISM, fixture_.getMinSellersForParallelism());
        assertEquals(EconomySettings.DEFAULT_QUOTE_FACTOR, fixture_.getQuoteFactor(), 0f);
        // TODO: compare with newly constructed object when we implement equals.
    }

} // end EconomySettingsTest class
