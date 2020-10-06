package com.vmturbo.platform.analysis.ede;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map.Entry;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

/**
 * A test case for the {@link CliqueMinimizer} class.
 */
@RunWith(JUnitParamsRunner.class)
public class CliqueMinimizerTest {
    // Fields
    private static final @NonNull Basket EMPTY = new Basket();
    private static final @NonNull Basket HOST = new Basket(new CommoditySpecification(0),
                                                           new CommoditySpecification(1));
    // Methods

    @Test
    @Parameters
    @TestCaseName("Test #{index}: CliqueMinimizer({0},{1})")
    public final void testCliqueMinimizer_And_Getters(@NonNull Economy economy,
            @NonNull Collection<@NonNull Entry<@NonNull ShoppingList,@NonNull Market>> entries) {
        CliqueMinimizer minimizer = new CliqueMinimizer(economy, entries, null, false);

        assertSame(economy, minimizer.getEconomy());
        assertSame(entries, minimizer.getEntries());
        assertTrue(Double.isInfinite(minimizer.getBestTotalQuote()));
        assertTrue(minimizer.getBestTotalQuote() > 0);
        assertNull(minimizer.getBestSellers());
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestCliqueMinimizer_And_Getters() {
        Economy e1 = new Economy();
        Economy e2 = new Economy();
        Trader t1 = e1.addTrader(0, TraderState.ACTIVE, EMPTY, EMPTY);
        Trader t2 = e2.addTrader(0, TraderState.ACTIVE, EMPTY, EMPTY, HOST);

        return new Object[][] {
            {new Economy(),Arrays.asList()},
            {e1, e1.getMarketsAsBuyer(t1).entrySet()},
            {e2, e2.getMarketsAsBuyer(t2).entrySet()},
        };
    }

    @Test
    @Ignore
    public final void testAccept() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    @Ignore
    public final void testCombine() {
        fail("Not yet implemented"); // TODO
    }

} // end CliqueMinimizerTest class
