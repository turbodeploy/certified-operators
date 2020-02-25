package com.vmturbo.platform.analysis.ede;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Map;
import java.util.stream.IntStream;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Before;
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
import com.vmturbo.platform.analysis.testUtilities.TestUtils;
import com.vmturbo.platform.analysis.utilities.QuoteCache;

/**
 * A test case for the {@link QuoteSummer} class.
 */
@RunWith(JUnitParamsRunner.class)
public class QuoteSummerTest {
    // Fields
    private Economy economy;
    private Trader provider1;
    private Trader provider2;
    private Map.Entry<ShoppingList, Market> entry;
    private ShoppingList sl;
    private QuoteCache qc;


    @Before
    public void setUp() {
        Basket basketSold = new Basket(new CommoditySpecification(0, 1000));
        Basket basketBought = new Basket(IntStream.range(0, 10)
            .mapToObj(CommoditySpecification::new).toArray(CommoditySpecification[]::new));

        economy = new Economy();

        // Create provider 1
        provider1 = economy.addTrader(0, TraderState.ACTIVE, basketSold);
        // For provider 1, we set canSimulateAction to true
        provider1.getSettings().setCanSimulateAction(true);

        // Create provider 2
        provider2 = economy.addTrader(0, TraderState.ACTIVE, basketSold);
        // For provider 2, we set canSimulateAction to false
        provider2.getSettings().setCanSimulateAction(false);

        // Including a basket bought will create a market.
        Trader buyer = economy.addTrader(0, TraderState.ACTIVE, basketSold, basketBought);

        // Get the entry for the shopping list and the market. It's guaranteed there is one.
        entry = economy.getMarketsAsBuyer(buyer).entrySet().iterator().next();
        sl = entry.getKey();

        qc = new QuoteCache(0, 0, 0);
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: QuoteSummer({0},{1})")
    public final void testQuoteSummer_And_Getters(@NonNull Economy economy, long clique) {
        QuoteSummer summer = new QuoteSummer(economy, clique, qc);

        assertSame(economy, summer.getEconomy());
        assertSame(clique, summer.getClique());
        assertEquals(0.0, summer.getTotalQuote(), TestUtils.FLOATING_POINT_DELTA);
        assertTrue(summer.getBestSellers().isEmpty());
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestQuoteSummer_And_Getters() {
        return new Object[][]{
            {new Economy(), -3},
            {new Economy(), 0},
            {new Economy(), 42},
        };
    }

    @Test
    public void testSimulate() {
        QuoteMinimizer minimizer = new QuoteMinimizer(economy, sl, qc, 0);

        // Test for provider 1 which has canSimulate true
        final long clique_ = 5;
        QuoteSummer quoteSummer1 = new QuoteSummer(economy, clique_, qc);
        quoteSummer1.simulate(minimizer, entry, provider1);
        assertEquals(1, quoteSummer1.getSimulatedActions().size());

        // Test for provider 2 which has canSimulate false
        QuoteSummer quoteSummer2 = new QuoteSummer(economy, clique_, qc);
        quoteSummer2.simulate(minimizer, entry, provider2);
        assertEquals(0, quoteSummer2.getSimulatedActions().size());
    }

    @Test
    @Ignore
    public final void testCombine() {
        fail("Not yet implemented"); // TODO
    }

} // end QuoteSummerTest class
