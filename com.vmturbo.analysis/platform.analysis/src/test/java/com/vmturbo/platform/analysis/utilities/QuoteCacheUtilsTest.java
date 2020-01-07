package com.vmturbo.platform.analysis.utilities;

import static org.junit.Assert.assertEquals;

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
import com.vmturbo.platform.analysis.utilities.Quote.MutableQuote;

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
     *
     * @param supplyCache whether the 1st argument to
     *     {@link QuoteCacheUtils#invalidate(QuoteCache, Move)} should be a pre-populated cache or
     *     {@code null}. Currently the size and initial contents of this pre-populated cache aren't
     *     parameterizable.
     * @param moveSource Which seller to use as the source of the move that will be used as the 2nd
     *     argument to {@link QuoteCacheUtils#invalidate(QuoteCache, Move)}. The economy used for
     *     the test isn't parameterizable and only contains 3 sellers. Use 0, 1 or 2 to select one
     *     of those sellers or -1 for a {@code null} source.
     * @param moveDestination Which seller to use as the destination of the move that will be used
     *     as the 2nd argument to {@link QuoteCacheUtils#invalidate(QuoteCache, Move)}. The economy
     *     used for the test isn't parameterizable and only contains 3 sellers. Use 0, 1 or 2 to
     *     select one of those sellers or -1 for a {@code null} destination.
     * @param expectedCacheState A rectangular array of arrays representing the state of the cache
     *     after the call to {@link QuoteCacheUtils#invalidate(QuoteCache, Move)}. {@code null}
     *     values mean that the corresponding entry of the cache is empty, while non-{@code null}
     *     values mean that the corresponding entry of the cache stores a {@link MutableQuote} with
     *     that value. Since the size of the cache used in the test isn't currently parameterizable
     *     the size of this array is always 4x2, or empty when <b>supplyCache</b> is false.
     */
    @Test
    @Parameters
    @TestCaseName("Test #{index}: invalidate({0},{1}->{2}); cache == {3}")
    public void testInvalidate(boolean supplyCache, int moveSource, int moveDestination,
                               Double[][] expectedCacheState) {
        // Generate classes from test input
        Economy e = new Economy();

        Trader buyer = e.addTrader(0, TraderState.ACTIVE, EMPTY);
        Trader[] sellers = {
            null, // sentinel element to simplify translation from -1 to a null trader.
            e.addTrader(1, TraderState.ACTIVE, EMPTY),
            e.addTrader(1, TraderState.ACTIVE, EMPTY),
            e.addTrader(1, TraderState.ACTIVE, EMPTY),
        };

        ShoppingList sl = e.addBasketBought(buyer, EMPTY);
        e.addBasketBought(buyer, EMPTY);

        QuoteCache cache = null;
        if (supplyCache) {
            cache = new QuoteCache(e.getTraders().size(), 3, 2);
            cache.put(1, 0, new CommodityQuote(null, 0.2));
            cache.put(2, 1, new CommodityQuote(null, 0.1));
            cache.put(1, 1, new CommodityQuote(null, 4.2));
        }

        Move move = new Move(e, sl, sellers[moveSource + 1], sellers[moveDestination + 1]);

        // Perform the test
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
    private static Object[] parametersForTestInvalidate() {
        return new Object[][]{
            {
                false, -1, -1, // null cache, null -> null move
                new Double[][]{}
            },
            {
                false, 0, -1, // null cache, non-null -> null move
                new Double[][]{}
            },
            {
                false, -1, 0, // null cache, null -> non-null move
                new Double[][]{}
            },
            {
                false, 0, 1, // null cache, non-null -> non-null move
                new Double[][]{}
            },
            {
                true, -1, -1, // non-null cache, null -> null move
                new Double[][]{
                    {null, null},
                    {0.2, 4.2},
                    {null, 0.1},
                    {null, null},
                }
            },
            {
                true, 0, -1, // non-null cache, non-null -> null move
                new Double[][]{
                    {null, null},
                    {null, null},
                    {null, 0.1},
                    {null, null},
                }
            },
            {
                true, -1, 1, // non-null cache, null -> non-null move
                new Double[][]{
                    {null, null},
                    {0.2, 4.2},
                    {null, null},
                    {null, null},
                }
            },
            {
                true, 0, 1, // non-null cache, non-null -> non-null move
                new Double[][]{
                    {null, null},
                    {null, null},
                    {null, null},
                    {null, null},
                }
            },
        };
    }
} // end QuoteCacheUtilsTest
