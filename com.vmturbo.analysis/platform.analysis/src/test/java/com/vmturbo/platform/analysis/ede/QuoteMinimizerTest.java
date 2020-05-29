package com.vmturbo.platform.analysis.ede;

import java.util.HashSet;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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

    /*
     * Test that the QuoteMinimizer does not return the current provider as the best seller when the provider returns
     * infinite price even when there are no other providers.
     */
    @Test
    public final void testQuoteMinimizerWhenCurrentSellerReturnsInfinity() {
        Economy e = new Economy();
        Trader consumer = e.addTrader(1, TraderState.ACTIVE, new Basket(), new Basket(new CommoditySpecification(1)));;
        ShoppingList sl = e.getMarketsAsBuyer(consumer).keySet().stream().findFirst().get();
        sl.setQuantity(0, 5);

        Trader provider = e.addTrader(2, TraderState.ACTIVE, new Basket(new CommoditySpecification(1)),
                new HashSet<>());
        provider.getCommoditiesSold().get(0).setQuantity(5).setCapacity(2);
        sl.move(provider);
        e.populateMarketsWithSellersAndMergeConsumerCoverage();

        QuoteMinimizer minimizer = new QuoteMinimizer(e, sl, null, 0);
        minimizer.accept(provider);

        assertNull(minimizer.getBestSeller());
        assertEquals(Double.POSITIVE_INFINITY, minimizer.getTotalBestQuote(), 0);
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
