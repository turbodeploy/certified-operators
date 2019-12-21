package com.vmturbo.platform.analysis.utilities;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

import org.junit.Test;
import org.junit.runner.RunWith;

import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.utilities.Quote.CommodityQuote;

/**
 * A test case for the {@link QuoteCacheUtils} class.
 */
@RunWith(JUnitParamsRunner.class)
public class QuoteCacheUtilsTest {
    // Fields
    private static final Basket EMPTY = new Basket();

    /**
     * Dummy test for the {@link QuoteCacheUtils} implicitly-generated constructor as it's included
     * in the coverage report. The methods of this class are intended to be static and the
     * constructor is not intended to be used.
     */
    @Test
    public void testConstructor() {
        new QuoteCacheUtils();
    }

    /**
     * Tests that invalidating certain cache rows removes quote associations for these entire rows
     * without affecting other rows and that null values are handled properly.
     */
    @Test
    @Parameters
    @TestCaseName("Test #{index}: invalidate({0},{1}); cache == {2}")
    public void testInvalidate(QuoteCache cache, Move move, Double[][] expectedCacheState) {
        QuoteCacheUtils.invalidate(cache, move);

        for (int rowIndex = 0; rowIndex < expectedCacheState.length; ++rowIndex) {
            for (int colIndex = 0; colIndex < expectedCacheState[rowIndex].length; ++colIndex) {
                assertEquals(expectedCacheState[rowIndex][colIndex],
                    cache.get(rowIndex, colIndex) == null
                        ? null : cache.get(rowIndex, colIndex).getQuoteValue());
            }
        }
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static List<Object[]> parametersForTestInvalidate() {
        List<Object[]> testCases = new ArrayList<>();

        // null cache, null -> null move
        Economy e = new Economy();
        ShoppingList sl = e.addBasketBought(e.addTrader(0, TraderState.ACTIVE, EMPTY), EMPTY);
        testCases.add(new Object[]{null, new Move(e, sl, null, null), new Double[][]{}});

        // null cache, non-null -> null move
        e = new Economy();
        sl = e.addBasketBought(e.addTrader(0, TraderState.ACTIVE, EMPTY), EMPTY);
        Trader source = e.addTrader(1, TraderState.ACTIVE, EMPTY);
        sl.move(source);
        testCases.add(new Object[]{null, new Move(e, sl, source, null), new Double[][]{}});

        // null cache, null -> non-null move
        e = new Economy();
        sl = e.addBasketBought(e.addTrader(0, TraderState.ACTIVE, EMPTY), EMPTY);
        Trader destination = e.addTrader(1, TraderState.ACTIVE, EMPTY);
        testCases.add(new Object[]{null, new Move(e, sl, null, destination), new Double[][]{}});

        // null cache, non-null -> non-null move
        e = new Economy();
        sl = e.addBasketBought(e.addTrader(0, TraderState.ACTIVE, EMPTY), EMPTY);
        source = e.addTrader(1, TraderState.ACTIVE, EMPTY);
        destination = e.addTrader(1, TraderState.ACTIVE, EMPTY);
        sl.move(source);
        testCases.add(new Object[]{null, new Move(e, sl, source, destination), new Double[][]{}});

        // non-null cache, null -> null move
        e = new Economy();
        Trader buyer = e.addTrader(0, TraderState.ACTIVE, EMPTY);
        sl = e.addBasketBought(buyer, EMPTY);
        e.addBasketBought(buyer, EMPTY);
        e.addTrader(1, TraderState.ACTIVE, EMPTY);
        e.addTrader(1, TraderState.ACTIVE, EMPTY);
        e.addTrader(1, TraderState.ACTIVE, EMPTY);

        QuoteCache cache = new QuoteCache(e.getTraders().size(), 3, 2);
        cache.put(1, 0, new CommodityQuote(null, 0.2));
        cache.put(2, 1, new CommodityQuote(null, 0.1));
        cache.put(1, 1, new CommodityQuote(null, 4.2));

        testCases.add(new Object[]{
            cache,
            new Move(e, sl, null, null),
            new Double[][]{
                {null, null},
                {0.2, 4.2},
                {null, 0.1},
                {null, null},
            }
        });

        // non-null cache, non-null -> null move
        e = new Economy();
        buyer = e.addTrader(0, TraderState.ACTIVE, EMPTY);
        sl = e.addBasketBought(buyer, EMPTY);
        e.addBasketBought(buyer, EMPTY);
        source = e.addTrader(1, TraderState.ACTIVE, EMPTY);
        e.addTrader(1, TraderState.ACTIVE, EMPTY);
        e.addTrader(1, TraderState.ACTIVE, EMPTY);

        cache = new QuoteCache(e.getTraders().size(), 3, 2);
        cache.put(1, 0, new CommodityQuote(null, 0.2));
        cache.put(2, 1, new CommodityQuote(null, 0.1));
        cache.put(1, 1, new CommodityQuote(null, 4.2));

        testCases.add(new Object[]{
            cache,
            new Move(e, sl, source, null),
            new Double[][]{
                {null, null},
                {null, null},
                {null, 0.1},
                {null, null},
            }
        });

        // non-null cache, null -> non-null move
        e = new Economy();
        buyer = e.addTrader(0, TraderState.ACTIVE, EMPTY);
        sl = e.addBasketBought(buyer, EMPTY);
        e.addBasketBought(buyer, EMPTY);
        e.addTrader(1, TraderState.ACTIVE, EMPTY);
        destination = e.addTrader(1, TraderState.ACTIVE, EMPTY);
        e.addTrader(1, TraderState.ACTIVE, EMPTY);

        cache = new QuoteCache(e.getTraders().size(), 3, 2);
        cache.put(1, 0, new CommodityQuote(null, 0.2));
        cache.put(2, 1, new CommodityQuote(null, 0.1));
        cache.put(1, 1, new CommodityQuote(null, 4.2));

        testCases.add(new Object[]{
            cache,
            new Move(e, sl, null, destination),
            new Double[][]{
                {null, null},
                {0.2, 4.2},
                {null, null},
                {null, null},
            }
        });

        // non-null cache, non-null -> non-null move
        e = new Economy();
        buyer = e.addTrader(0, TraderState.ACTIVE, EMPTY);
        sl = e.addBasketBought(buyer, EMPTY);
        e.addBasketBought(buyer, EMPTY);
        source = e.addTrader(1, TraderState.ACTIVE, EMPTY);
        destination = e.addTrader(1, TraderState.ACTIVE, EMPTY);
        e.addTrader(1, TraderState.ACTIVE, EMPTY);

        cache = new QuoteCache(e.getTraders().size(), 3, 2);
        cache.put(1, 0, new CommodityQuote(null, 0.2));
        cache.put(2, 1, new CommodityQuote(null, 0.1));
        cache.put(1, 1, new CommodityQuote(null, 4.2));

        testCases.add(new Object[]{
            cache,
            new Move(e, sl, source, destination),
            new Double[][]{
                {null, null},
                {null, null},
                {null, null},
                {null, null},
            }
        });

        return testCases;
    }
} // end QuoteCacheUtilsTest
