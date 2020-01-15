package com.vmturbo.platform.analysis.ede;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.AbstractMap.SimpleEntry;
import java.util.Map;
import java.util.stream.IntStream;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderSettings;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;
import com.vmturbo.platform.analysis.utilities.QuoteCache;

/**
 * A test case for the {@link QuoteSummer} class.
 */
@RunWith(JUnitParamsRunner.class)
public class QuoteSummerTest {

    private final long clique_ = 5;
    Trader provider1 = Mockito.mock(Trader.class);
    Trader provider2 = Mockito.mock(Trader.class);
    private static Map.Entry<ShoppingList, Market> entry;
    ShoppingList sl = Mockito.mock(ShoppingList.class);
    private static QuoteCache qc = new QuoteCache(0, 0, 0);


    @Before
    public void setUp() {
        CommoditySpecification[] commodities = IntStream.range(0, 10).mapToObj(CommoditySpecification::new)
                .toArray(CommoditySpecification[]::new);
        Basket basket = new Basket(commodities);
        // Create an instance of the market
        Market market = new Market(basket);

        // Create provider 1
        Basket basketProvider1 = new Basket(new CommoditySpecification(0, 1000));
        Mockito.when(provider1.getEconomyIndex()).thenReturn(0);
        Mockito.when(provider1.getType()).thenReturn(0);
        Mockito.when(provider1.getState()).thenReturn(TraderState.ACTIVE);
        Mockito.when(provider1.getBasketSold()).thenReturn(basketProvider1);
        // For provider 1, we set canSimulateAction to true
        TraderSettings mockedtraderSettings = Mockito.mock(TraderSettings.class);
        Mockito.when(mockedtraderSettings.isCanSimulateAction()).thenReturn(true);
        Mockito.when(provider1.getSettings()).thenReturn(mockedtraderSettings);

        // Create provider 2
        Basket basketProvider2 = new Basket(new CommoditySpecification(0, 1000));
        Mockito.when(provider2.getEconomyIndex()).thenReturn(0);
        Mockito.when(provider2.getType()).thenReturn(0);
        Mockito.when(provider2.getState()).thenReturn(TraderState.ACTIVE);
        Mockito.when(provider2.getBasketSold()).thenReturn(basketProvider2);
        // For provider 2, we set canSimulateAction to false
        TraderSettings mockedtraderSettings2 = Mockito.mock(TraderSettings.class);
        Mockito.when(mockedtraderSettings2.isCanSimulateAction()).thenReturn(false);
        Mockito.when(provider2.getSettings()).thenReturn(mockedtraderSettings2);

        Trader buyer = Mockito.mock(Trader.class);
        Basket basketBuyer = new Basket(new CommoditySpecification(0, 1000));
        Mockito.when(buyer.getEconomyIndex()).thenReturn(0);
        Mockito.when(buyer.getType()).thenReturn(0);
        Mockito.when(buyer.getState()).thenReturn(TraderState.ACTIVE);
        Mockito.when(buyer.getBasketSold()).thenReturn(basketBuyer);

        Mockito.when(sl.getBuyer()).thenReturn(buyer);

        // Create an entry for a shopping list and a market
        entry = new SimpleEntry<>(sl, market);
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
        return new Object[][] {
            {new Economy(),-3},
            {new Economy(),0},
            {new Economy(),42},
        };
    }

    @Test
    public void testSimulate() {
        Economy e1 = new Economy();
        QuoteMinimizer minimizer = new QuoteMinimizer(e1, sl, qc, 0);

        // Test for provider 1 which has canSimulate true
        QuoteSummer mockedQuoteSummer = new QuoteSummer(e1, clique_, qc);
        mockedQuoteSummer.simulate(minimizer, entry, provider1);
        assertEquals(mockedQuoteSummer.getSimulatedActions().size(), 1);

        // Test for provider 2 which has canSimulate false
        QuoteSummer mockedQuoteSummer2 = new QuoteSummer(e1, clique_, qc);
        mockedQuoteSummer2.simulate(minimizer, entry, provider2);
        assertEquals(mockedQuoteSummer2.getSimulatedActions().size(), 0);
    }

    @Test
    @Ignore
    public final void testCombine() {
        fail("Not yet implemented"); // TODO
    }

} // end QuoteSummerTest class
