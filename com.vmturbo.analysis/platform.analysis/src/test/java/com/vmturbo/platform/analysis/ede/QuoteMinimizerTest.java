package com.vmturbo.platform.analysis.ede;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.TraderState;

/**
 * A test case for the {@link QuoteMinimizer} class.
 */
@RunWith(JUnitParamsRunner.class)
public class QuoteMinimizerTest {
    // Fields
    private static final @NonNull Basket EMPTY = new Basket();

    // Methods

    @Test
    @Parameters
    @TestCaseName("Test #{index}: QuoteMinimizer({0},{1})")
    public final void testQuoteMinimizer_And_Getters(@NonNull Economy economy, @NonNull ShoppingList shoppingList) {
        QuoteMinimizer minimizer = new QuoteMinimizer(economy, shoppingList, null, 0);

        assertSame(economy, minimizer.getEconomy());
        assertSame(shoppingList, minimizer.getShoppingList());
        assertTrue(Double.isInfinite(minimizer.getTotalBestQuote()));
        assertTrue(minimizer.getTotalBestQuote() > 0);
        assertNull(minimizer.getBestSeller());
        assertTrue(Double.isInfinite(minimizer.getCurrentQuote().getQuoteValue()));
        assertTrue(minimizer.getCurrentQuote().getQuoteValue() > 0);
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestQuoteMinimizer_And_Getters() {
        Economy e1 = new Economy();
        Economy e2 = new Economy();

        return new Object[][] {
            {e1,e1.addBasketBought(e1.addTrader(0, TraderState.ACTIVE, EMPTY), EMPTY)},
            {e2,e2.addBasketBought(e2.addTrader(0, TraderState.ACTIVE, EMPTY), EMPTY)},
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

} // end QuoteMinimizerTest class
