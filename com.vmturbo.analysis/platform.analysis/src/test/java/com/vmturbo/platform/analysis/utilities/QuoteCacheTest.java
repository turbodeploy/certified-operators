package com.vmturbo.platform.analysis.utilities;

import static com.google.common.base.Preconditions.checkArgument;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.Random;
import java.util.stream.IntStream;

import com.google.common.primitives.Ints;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

import org.junit.Test;
import org.junit.runner.RunWith;

import com.vmturbo.platform.analysis.utilities.Quote.CommodityQuote;
import com.vmturbo.platform.analysis.utilities.Quote.MutableQuote;

/**
 * A test case for the {@link QuoteCache} class.
 */
@RunWith(JUnitParamsRunner.class)
public class QuoteCacheTest {

    /**
     * Tests that the class constructor correctly detects erroneous input.
     */
    @Test(expected = RuntimeException.class)
    @Parameters({
        // one invalid argument
        "-1,0,1", "-10,5,3",
        "10,-1,1", "5,-10,5",
        "3,2,-1", "10,7,-4",
        // two invalid arguments
        "-1,-1,1", "-10,-11,3",
        "-1,0,-1", "-7,4,-3",
        "9,-1,-1", "12,-42,-31",
        // three invalid arguments
        "-1,-1,-1", "-10,-5,-7",
        "-2,-1,-1", "-9,-5,-7",
    })
    @TestCaseName("Test #{index}: new QuoteCache({0},{1},{2})")
    public void testConstructor_negative(int nTradersInEconomy, int nPotentialSellers,
                                         int nBuyerShoppingLists) {
        new QuoteCache(nTradersInEconomy, nPotentialSellers, nBuyerShoppingLists);
    } // end testConstructor_negative

    /**
     * Tests that constructor creates an empty cache of expected size.
     */
    @Test
    @Parameters({"0,0,0", "0,0,1", "0,0,7",
                 "0,1,0", "0,1,1", "0,1,3",
                 "1,0,0", "1,0,1", "1,0,11",
                 "1,1,0", "1,1,1", "1,1,2",
                 "2,0,0", "2,0,1", "2,0,5",
                 "2,1,0", "2,1,1", "2,1,3",
                 "2,2,0", "2,2,1", "2,2,4",
                 "2,3,0", "2,3,1", "2,3,4",
                 "10,5,3", "10,15,7",
                 "7,6,2",
    })
    @TestCaseName("Test #{index}: new QuoteCache({0},{1},{2}).get(...)")
    public void testGet_emptyCache_positive(int nTradersInEconomy, int nPotentialSellers,
                                            int nShoppingLists) {
        // construction should succeed
        QuoteCache cache = new QuoteCache(nTradersInEconomy, nPotentialSellers, nShoppingLists);

        // check all valid indices.
        for (int traderIndex = 0; traderIndex < nTradersInEconomy; traderIndex++) {
            for (int slIndex = 0; slIndex < nShoppingLists; slIndex++) {
                assertNull(cache.get(traderIndex, slIndex));
            }
        }
    } // end testGet_emptyCache_positive

    /**
     * Tests that out-of-bounds requests to get a cached quote fail.
     */
    @Test(expected = RuntimeException.class)
    @Parameters({
        "0,0,0,0,0", "0,0,0,-1,0", "0,0,0,0,-1", "0,0,0,-1,-1",
        "1,1,1,2,0", "1,1,1,-1,1", "1,1,1,0,2", "1,1,1,0,-1",
        "1,1,1,2,2", "1,1,1,-1,2", "1,1,1,2,-1", "1,1,1,-1,-1",
        "10,7,2,10,0", "10,7,2,-1,1", "10,7,2,0,2", "10,7,2,0,-1",
        "10,7,2,10,2", "10,7,2,-1,2", "10,7,2,10,-1", "10,7,2,-1,-1",
    })
    @TestCaseName("Test #{index}: new QuoteCache({0},{1},{2}).get({3},{4})")
    public void testGet_emptyCache_negative(int nTradersInEconomy, int nPotentialSellers,
                        int nBuyerShoppingLists, int traderEconomyIndex, int shoppingListIndex) {
        new QuoteCache(nTradersInEconomy, nPotentialSellers, nBuyerShoppingLists)
            .get(traderEconomyIndex, shoppingListIndex);
    } // end testGet_emptyCache_negative

    /**
     * Tests that a value can be saved to and retrieved from a position in the cache and that saving
     * to a position doesn't effect other positions.
     *
     * <p>If more or less than 3 put operations need to be tested in the future this better be
     * refactored to get an array of tuples as and argument.</p>
     */
    @Test
    @Parameters({
        // 1x1 cache
        "1,1,1, 0,0,0, 0,0,1, 0,0,10",
        // same as 1x1 cache but with nPotentialSellers > nTradersInEconomy
        "1,2,1, 0,0,0, 0,0,1, 0,0,10",
        // 2x1 cache
        "2,2,1, 0,0,0, 0,0,1, 0,0,10", "2,2,1, 0,0,0, 0,0,1, 1,0,10",
        "2,2,1, 0,0,0, 1,0,1, 0,0,10", "2,2,1, 0,0,0, 1,0,1, 1,0,10",
        "2,2,1, 1,0,0, 0,0,1, 0,0,10", "2,2,1, 1,0,0, 0,0,1, 1,0,10",
        "2,2,1, 1,0,0, 1,0,1, 0,0,10", "2,2,1, 1,0,0, 1,0,1, 1,0,10",
        // same as 2x1 cache but with nPotentialSellers > nTradersInEconomy
        "2,3,1, 0,0,0, 0,0,1, 0,0,10", "2,4,1, 0,0,0, 0,0,1, 1,0,10",
        "2,3,1, 0,0,0, 1,0,1, 0,0,10", "2,4,1, 0,0,0, 1,0,1, 1,0,10",
        "2,3,1, 1,0,0, 0,0,1, 0,0,10", "2,4,1, 1,0,0, 0,0,1, 1,0,10",
        "2,3,1, 1,0,0, 1,0,1, 0,0,10", "2,4,1, 1,0,0, 1,0,1, 1,0,10",
        // 1x2 cache
        "1,1,2, 0,0,0, 0,0,1, 0,0,10", "1,1,2, 0,0,0, 0,0,1, 0,1,10",
        "1,1,2, 0,0,0, 0,1,1, 0,0,10", "1,1,2, 0,0,0, 0,1,1, 0,1,10",
        "1,1,2, 0,1,0, 0,0,1, 0,0,10", "1,1,2, 0,1,0, 0,0,1, 0,1,10",
        "1,1,2, 0,1,0, 0,1,1, 0,0,10", "1,1,2, 0,1,0, 0,1,1, 0,1,10",
        // same as 1x2 cache but with nPotentialSellers > nTradersInEconomy
        "1,10,2, 0,0,0, 0,0,1, 0,0,10", "1,11,2, 0,0,0, 0,0,1, 0,1,10",
        "1,10,2, 0,0,0, 0,1,1, 0,0,10", "1,11,2, 0,0,0, 0,1,1, 0,1,10",
        "1,10,2, 0,1,0, 0,0,1, 0,0,10", "1,11,2, 0,1,0, 0,0,1, 0,1,10",
        "1,10,2, 0,1,0, 0,1,1, 0,0,10", "1,11,2, 0,1,0, 0,1,1, 0,1,10",
        // 5x3 cache, 1st point steady, 2nd covering (<,=,>)x(<,=,>) combinations, 3rd point pseudo-
        // random either discrete from other two (left) or matching one or more of the two (right).
        "9,5,3, 2,1,0.1, 0,0,0.2, 1,0,0.3", "9,5,3, 2,1,0.1, 0,0,0.2, 2,1,1.3",
        "9,5,3, 2,1,0.1, 2,0,0.2, 0,1,0.4", "9,5,3, 2,1,0.1, 2,0,0.2, 2,0,1.4",
        "9,5,3, 2,1,0.1, 4,0,0.2, 8,2,0.5", "9,5,3, 2,1,0.1, 4,0,0.2, 2,1,1.5",
        "9,5,3, 2,1,0.1, 0,1,0.2, 7,0,0.6", "9,5,3, 2,1,0.1, 0,1,0.2, 2,1,1.6",
        "9,5,3, 2,1,0.1, 2,1,0.2, 0,1,0.7", "9,5,3, 2,1,0.1, 2,1,0.2, 2,1,1.7",
        "9,5,3, 2,1,0.1, 4,1,0.2, 6,0,0.8", "9,5,3, 2,1,0.1, 4,1,0.2, 4,1,1.8",
        "9,5,3, 2,1,0.1, 0,2,0.2, 5,2,0.9", "9,5,3, 2,1,0.1, 0,2,0.2, 2,1,1.9",
        "9,5,3, 2,1,0.1, 2,2,0.2, 4,1,1.0", "9,5,3, 2,1,0.1, 2,2,0.2, 2,2,2.0",
        "9,5,3, 2,1,0.1, 4,2,0.2, 3,0,1.1", "9,5,3, 2,1,0.1, 4,2,0.2, 2,1,2.1",
        // 3x5 cache, 1st point steady, 2nd covering (<,=,>)x(<,=,>) combinations, 3rd point pseudo-
        // random either discrete from other two (left) or matching one or more of the two (right).
        "7,3,5, 3,3,3.1, 1,1,3.2, 0,0,3.3", "7,3,5, 3,3,3.1, 1,1,3.2, 3,3,5.7",
        "7,3,5, 3,3,3.1, 1,3,3.2, 1,1,3.3", "7,3,5, 3,3,3.1, 1,3,3.2, 1,3,5.7",
        "7,3,5, 3,3,3.1, 1,4,3.2, 2,2,3.3", "7,3,5, 3,3,3.1, 1,4,3.2, 3,3,5.7",
        "7,3,5, 3,3,3.1, 3,1,3.2, 3,3,3.3", "7,3,5, 3,3,3.1, 3,1,3.2, 3,1,5.7",
        "7,3,5, 3,3,3.1, 3,3,3.2, 4,4,3.3", "7,3,5, 3,3,3.1, 3,3,3.2, 3,3,5.7",
        "7,3,5, 3,3,3.1, 3,4,3.2, 5,0,3.3", "7,3,5, 3,3,3.1, 3,4,3.2, 3,4,5.7",
        "7,3,5, 3,3,3.1, 6,1,3.2, 6,1,3.3", "7,3,5, 3,3,3.1, 6,1,3.2, 6,1,5.7",
        "7,3,5, 3,3,3.1, 6,3,3.2, 0,2,3.3", "7,3,5, 3,3,3.1, 6,3,3.2, 3,3,5.7",
        "7,3,5, 3,3,3.1, 6,4,3.2, 1,0,3.3", "7,3,5, 3,3,3.1, 6,4,3.2, 6,4,5.7",
    })
    @TestCaseName("Test #{index}: new QuoteCache({0},{1},{2})" +
            ".put({3},{4},{5}).put({6},{7},{8}).put({9},{10},{11})")
    public void testPutGet_emptyCache_sequence_positive(
            int nTradersInEconomy, int nPotentialSellers, int nShoppingLists,
            int traderIndex1, int shoppingListIndex1, double quoteValue1,
            int traderIndex2, int shoppingListIndex2, double quoteValue2,
            int traderIndex3, int shoppingListIndex3, double quoteValue3) {
        // Put the arguments into arrays to enable processing using loops
        int[] traderIndices = {traderIndex1, traderIndex2, traderIndex3};
        int[] shoppingListIndices = {shoppingListIndex1, shoppingListIndex2, shoppingListIndex3};
        double[] quoteValues = {quoteValue1, quoteValue2, quoteValue3};
        // Also keep results into an array.
        MutableQuote[] quoteObjects = new MutableQuote[traderIndices.length];

        // Initialize the cache
        QuoteCache cache = new QuoteCache(nTradersInEconomy, nPotentialSellers, nShoppingLists);

        // For each entry to be added to the cache, but also before the first entry is added.
        for (int curPos = -1; curPos < traderIndices.length; curPos++) {
            if (curPos >= 0) { // if it's time to add the first entry
                quoteObjects[curPos] = new CommodityQuote(null, quoteValues[curPos]);
                assertSame(cache, cache.put(traderIndices[curPos],
                                            shoppingListIndices[curPos], quoteObjects[curPos]));
            }

            // For each entry that was added to the cache before the current one, but also for the
            // current one.
            before: for (int pastPos = 0; pastPos <= curPos; pastPos++) {
                // Check to see if there is a newer entry that was added at the same position
                for (int futPos = pastPos + 1; futPos <= curPos; futPos++) {
                    if (traderIndices[futPos] == traderIndices[pastPos]
                            && shoppingListIndices[futPos] == shoppingListIndices[pastPos]) {
                        continue before;
                    }
                }
                // If the entry isn't supposed to be overridden by a newer one, assert it has
                // preserved its value.
                assertSame(quoteObjects[pastPos],
                            cache.get(traderIndices[pastPos], shoppingListIndices[pastPos]));
                assertEquals(quoteValues[pastPos], quoteObjects[pastPos].getQuoteValue(), 0.0);
            }

            // For each entry that will be added in the future
            after: for (int futPos = curPos + 1; futPos < traderIndices.length; futPos++) {
                // Check whether it happens to share a position with an existing entry.
                for (int pastPos = 0; pastPos <= curPos; pastPos++) {
                    if (traderIndices[futPos] == traderIndices[pastPos]
                            && shoppingListIndices[futPos] == shoppingListIndices[pastPos]) {
                        continue after;
                    }
                }
                // If the position isn't supposed to be already populated, assert that it's not
                // populated.
                assertNull(cache.get(traderIndices[futPos], shoppingListIndices[futPos]));
            }
        }
    } // end testPutGet_emptyCache_sequence_positive

    /**
     * Tests that out-of-bounds requests to put a quote to the cache fail without changing the cache
     * state.
     */
    @Test
    @Parameters({
        "0,0,0,0,0", "0,0,0,-1,0", "0,0,0,0,-1", "0,0,0,-1,-1",
        "1,1,1,2,0", "1,1,1,-1,1", "1,1,1,0,2", "1,1,1,0,-1",
        "1,1,1,2,2", "1,1,1,-1,2", "1,1,1,2,-1", "1,1,1,-1,-1",
        "10,7,2,10,0", "10,7,2,-1,1", "10,7,2,0,2", "10,7,2,0,-1",
        "10,7,2,10,2", "10,7,2,-1,2", "10,7,2,10,-1", "10,7,2,-1,-1",
    })
    @TestCaseName("Test #{index}: new QuoteCache({0},{1},{2}).put({3},{4})")
    public void testPut_emptyCache_negative(int nTradersInEconomy, int nPotentialSellers,
                                 int nShoppingLists, int traderEconomyIndex, int shoppingListIndex) {
        QuoteCache cache = new QuoteCache(nTradersInEconomy, nPotentialSellers, nShoppingLists);

        try {
            cache.put(traderEconomyIndex, shoppingListIndex, new CommodityQuote(null, 42));
            fail("Expected IllegalArgumentException | ArrayIndexOutOfBoundsException!");
        } catch (IllegalArgumentException | ArrayIndexOutOfBoundsException e) {
            // check all valid indices.
            for (int traderIndex = 0; traderIndex < nTradersInEconomy; traderIndex++) {
                for (int slIndex = 0; slIndex < nShoppingLists; slIndex++) {
                    assertNull(cache.get(traderIndex, slIndex));
                }
            }
        }
    } // end testPut_emptyCache_negative

    /**
     * Tests that attempting to put more values to the cache than initially declared fails without
     * changing the cache state.
     */
    @Test
    @Parameters({
        // 0x1 cache
        "7,0,1, 0,0, 0,0, 0,0", "7,0,2, 1,1, 1,1, 1,1",
        // 1x1 cache
        "5,1,1, 0,0, 1,0, 4,0", "5,1,1, 0,0, 0,0, 1,0", "5,1,1, 0,0, 0,0, 3,0",
        "5,1,1, 4,0, 3,0, 2,0", "5,1,1, 4,0, 4,0, 2,0", "5,1,1, 3,0, 3,0, 2,0",
        // 1x2 cache
        "5,1,2, 0,0, 1,1, 1,0", "5,1,2, 0,0, 0,1, 1,0", "5,1,2, 0,0, 0,1, 3,0",
        "5,1,2, 4,1, 3,0, 2,0", "5,1,2, 4,0, 4,1, 2,0", "5,1,2, 3,1, 3,0, 2,1",
        // 2x1 cache
        "12,2,1, 8,0, 7,0, 5,0", "12,2,1, 0,0, 1,0, 2,0", "12,2,1, 4,0, 3,0, 2,0",
        // 2x2 cache, 8 combinations for shopping list index. Trader index oscillating, increasing,
        // or decreasing
        "12,2,2, 8,0, 7,0, 5,0", "12,2,2, 0,0, 1,0, 2,1", "12,2,2, 4,0, 3,1, 2,0",
        "12,2,2, 9,0, 7,1, 6,1", "12,2,2, 1,1, 2,0, 5,0", "12,2,2, 11,1, 6,0, 1,1",
        "12,2,2, 5,1, 9,1, 6,0", "12,2,2, 0,1, 6,1, 11,1",
    })
    @TestCaseName("Test #{index}: new QuoteCache({0},{1},{2})" +
                    ".put({3},{4},...).put({5},{6},...).put({7},{8},...)")
    public void testPut_emptyCache_sequence_negative(
            int nTradersInEconomy, int nPotentialSellers, int nShoppingLists,
            int traderIndex1, int shoppingListIndex1,
            int traderIndex2, int shoppingListIndex2,
            int traderIndex3, int shoppingListIndex3) {
        QuoteCache cache = new QuoteCache(nTradersInEconomy, nPotentialSellers, nShoppingLists);

        try {
            MutableQuote quote1 = new CommodityQuote(null, 0.42);
            assertSame(cache, cache.put(traderIndex1, shoppingListIndex1, quote1));
            try {
                MutableQuote quote2 = new CommodityQuote(null, 4.2);
                assertSame(cache, cache.put(traderIndex2, shoppingListIndex2, quote2));
                try {
                    MutableQuote quote3 = new CommodityQuote(null, 42);
                    cache.put(traderIndex3, shoppingListIndex3, quote3);
                    fail("At least one of the put operations should have failed with an " +
                        "IllegalStateException!");
                } catch (IllegalStateException e) {
                    if (traderIndex1 != traderIndex2 || shoppingListIndex1 != shoppingListIndex2) {
                        assertSame(quote1, cache.get(traderIndex1, shoppingListIndex1));
                        assertEquals(0.42, quote1.getQuoteValue(), 0);
                    }
                    assertSame(quote2, cache.get(traderIndex2, shoppingListIndex2));
                    assertEquals(4.2, quote2.getQuoteValue(), 0);
                    assertNull(cache.get(traderIndex3, shoppingListIndex3));
                }
            } catch (IllegalStateException e) {
                assertSame(quote1, cache.get(traderIndex1, shoppingListIndex1));
                assertEquals(0.42, quote1.getQuoteValue(), 0);
                assertNull(cache.get(traderIndex2, shoppingListIndex2));
                assertNull(cache.get(traderIndex3, shoppingListIndex3));
            }
        } catch (IllegalStateException e) {
            assertNull(cache.get(traderIndex1, shoppingListIndex1));
            assertNull(cache.get(traderIndex2, shoppingListIndex2));
            assertNull(cache.get(traderIndex3, shoppingListIndex3));
        }
    } // end testPut_emptyCache_sequence_negative

    /**
     * Tests that invalidating certain cache rows removes quote associations for these entire rows
     * without affecting other rows.
     */
    @Test
    @Parameters
    @TestCaseName("Test #{index}: new QuoteCache({0},{1},{2}).put({3}).invalidate({4}) == {5}")
    public void testInvalidate_positive(
            int nTradersInEconomy, int nPotentialSellers, int nShoppingLists,
            Number[][] putArgTriplets, int[] invalidateArgs, Double[][] expectedCacheState) {
        QuoteCache cache = new QuoteCache(nTradersInEconomy, nPotentialSellers, nShoppingLists);

        for (Number[] putArgTriplet : putArgTriplets) {
            checkArgument(putArgTriplet.length == 3);
            assertSame(cache, cache.put((int)putArgTriplet[0], (int)putArgTriplet[1],
                                        new CommodityQuote(null, (double)putArgTriplet[2])));
        }

        for (int invalidateArg : invalidateArgs) {
            assertSame(cache, cache.invalidate(invalidateArg));
        }

        checkArgument(expectedCacheState.length == nTradersInEconomy);
        for (int rowIndex = 0; rowIndex < expectedCacheState.length; ++rowIndex) {
            checkArgument(expectedCacheState[rowIndex].length == nShoppingLists);
            for (int colIndex = 0; colIndex < expectedCacheState[rowIndex].length; ++colIndex) {
                assertEquals(expectedCacheState[rowIndex][colIndex],
                    cache.get(rowIndex, colIndex) == null
                        ? null : cache.get(rowIndex, colIndex).getQuoteValue());
            }
        }
    } // end testInvalidate_positive

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestInvalidate_positive() {
        return new Object[][]{
            // 1 trader in economy. Up to 1 shopping list
            {
                1, 0, 0, // constructor call
                new Number[][]{}, // put calls
                new int[]{0}, // invalidate calls
                new Double[][]{ // expected result state
                    {}
                }
            },
            {
                1, 0, 1, // constructor call
                new Number[][]{}, // put calls
                new int[]{0}, // invalidate calls
                new Double[][]{ // expected result state
                    {null}
                }
            },
            {
                1, 1, 0, // constructor call
                new Number[][]{}, // put calls
                new int[]{0}, // invalidate calls
                new Double[][]{ // expected result state
                    {}
                }
            },
            {
                1, 1, 1, // constructor call
                new Number[][]{}, // put calls
                new int[]{0}, // invalidate calls
                new Double[][]{ // expected result state
                    {null}
                }
            },
            {
                1, 1, 1, // constructor call
                new Number[][]{ // put calls
                    {0, 0, 0.0}
                },
                new int[]{0}, // invalidate calls
                new Double[][]{ // expected result state
                    {null}
                }
            },
            // 2 traders in economy. Up to 2 shopping lists
            {
                2, 0, 0, // constructor call
                new Number[][]{}, // put calls
                new int[]{0}, // invalidate calls
                new Double[][]{ // expected result state
                    {},
                    {}
                }
            },
            {
                2, 0, 1, // constructor call
                new Number[][]{}, // put calls
                new int[]{0}, // invalidate calls
                new Double[][]{ // expected result state
                    {null},
                    {null}
                }
            },
            {
                2, 0, 2, // constructor call
                new Number[][]{}, // put calls
                new int[]{0}, // invalidate calls
                new Double[][]{ // expected result state
                    {null, null},
                    {null, null}
                }
            },
            {
                2, 1, 0, // constructor call
                new Number[][]{}, // put calls
                new int[]{0}, // invalidate calls
                new Double[][]{ // expected result state
                    {},
                    {}
                }
            },
            {
                2, 1, 1, // constructor call
                new Number[][]{}, // put calls
                new int[]{0}, // invalidate calls
                new Double[][]{ // expected result state
                    {null},
                    {null}
                }
            },
            {
                2, 1, 1, // constructor call
                new Number[][]{ // put calls
                    {0, 0, 0.1}
                },
                new int[]{0}, // invalidate calls
                new Double[][]{ // expected result state
                    {null},
                    {null}
                }
            },
            {
                2, 1, 2, // constructor call
                new Number[][]{ // put calls
                    {0, 1, 0.1}
                },
                new int[]{0}, // invalidate calls
                new Double[][]{ // expected result state
                    {null, null},
                    {null, null}
                }
            },
            {
                2, 1, 2, // constructor call
                new Number[][]{ // put calls
                    {0, 0, 0.2},
                    {0, 1, 0.3}
                },
                new int[]{0}, // invalidate calls
                new Double[][]{ // expected result state
                    {null, null},
                    {null, null}
                }
            },
            {
                2, 2, 0, // constructor call
                new Number[][]{}, // put calls
                new int[]{0}, // invalidate calls
                new Double[][]{ // expected result state
                    {},
                    {}
                }
            },
            {
                2, 2, 1, // constructor call
                new Number[][]{}, // put calls
                new int[]{1}, // invalidate calls
                new Double[][]{ // expected result state
                    {null},
                    {null}
                }
            },
            {
                2, 2, 1, // constructor call
                new Number[][]{ // put calls
                    {0, 0, 0.4}
                },
                new int[]{0}, // invalidate calls
                new Double[][]{ // expected result state
                    {null},
                    {null}
                }
            },
            {
                2, 2, 1, // constructor call
                new Number[][]{ // put calls
                    {0, 0, 0.4}
                },
                new int[]{1}, // invalidate calls
                new Double[][]{ // expected result state
                    {0.4},
                    {null}
                }
            },
            {
                2, 2, 1, // constructor call
                new Number[][]{ // put calls
                    {1, 0, 0.4}
                },
                new int[]{1}, // invalidate calls
                new Double[][]{ // expected result state
                    {null},
                    {null}
                }
            },
            {
                2, 2, 1, // constructor call
                new Number[][]{ // put calls
                    {1, 0, 0.4}
                },
                new int[]{0}, // invalidate calls
                new Double[][]{ // expected result state
                    {null},
                    {0.4}
                }
            },
            {
                2, 2, 1, // constructor call
                new Number[][]{ // put calls
                    {1, 0, 0.4},
                    {0, 0, 0.5}
                },
                new int[]{0}, // invalidate calls
                new Double[][]{ // expected result state
                    {null},
                    {0.4}
                }
            },
            {
                2, 2, 1, // constructor call
                new Number[][]{ // put calls
                    {1, 0, 0.4},
                    {0, 0, 0.5}
                },
                new int[]{1}, // invalidate calls
                new Double[][]{ // expected result state
                    {0.5},
                    {null}
                }
            },
            {
                2, 2, 1, // constructor call
                new Number[][]{ // put calls
                    {1, 0, 0.4},
                    {0, 0, 0.5}
                },
                new int[]{0, 1}, // invalidate calls
                new Double[][]{ // expected result state
                    {null},
                    {null}
                }
            },
            {
                2, 2, 2, // constructor call
                new Number[][]{ // put calls
                    {1, 0, 0.6},
                    {1, 1, 0.7},
                    {0, 1, 0.8},
                    {0, 0, 0.9}
                },
                new int[]{0}, // invalidate calls
                new Double[][]{ // expected result state
                    {null, null},
                    {0.6, 0.7}
                }
            },
            {
                2, 2, 2, // constructor call
                new Number[][]{ // put calls
                    {1, 0, 0.6},
                    {1, 1, 0.7},
                    {0, 1, 0.8},
                    {0, 0, 0.9}
                },
                new int[]{1}, // invalidate calls
                new Double[][]{ // expected result state
                    {0.9, 0.8},
                    {null, null},
                }
            },
            {
                2, 2, 2, // constructor call
                new Number[][]{ // put calls
                    {1, 0, 0.6},
                    {1, 1, 0.7},
                    {0, 1, 0.8},
                    {0, 0, 0.9}
                },
                new int[]{0, 1}, // invalidate calls
                new Double[][]{ // expected result state
                    {null, null},
                    {null, null},
                }
            },
            // larger economies
            {
                5, 5, 2, // constructor call
                new Number[][]{ // put calls
                    {1, 0, 0.2}
                },
                new int[]{ // invalidate calls
                    1, 4
                },
                new Double[][]{ // expected result state
                    {null, null},
                    {null, null},
                    {null, null},
                    {null, null},
                    {null, null}
                }
            },
            {
                5, 5, 2, // constructor call
                new Number[][]{ // put calls
                    {1, 0, 0.2},
                    {2, 1, 0.1},
                    {1, 1, 4.2}
                },
                new int[]{ // invalidate calls
                    1, 3
                },
                new Double[][]{ // expected result state
                    {null, null},
                    {null, null},
                    {null, 0.1},
                    {null, null},
                    {null, null}
                }
            },
            {
                10, 5, 2, // constructor call
                new Number[][]{ // put calls
                    {0, 0, 0.1},
                    {0, 1, 0.2},
                    {1, 0, 0.3},
                    {2, 1, 0.4},
                    {3, 0, 0.5},
                    {3, 1, 0.6},
                    {4, 1, 0.7},
                },
                new int[]{ // invalidate calls
                    1, 3, 4, 9
                },
                new Double[][]{ // expected result state
                    {0.1, 0.2},
                    {null, null},
                    {null, 0.4},
                    {null, null},
                    {null, null},
                    {null, null},
                    {null, null},
                    {null, null},
                    {null, null},
                    {null, null}
                }
            },
            {
                10, 5, 2, // constructor call
                new Number[][]{ // put calls
                    {3, 0, 0.1},
                    {3, 1, 0.2},
                    {5, 0, 0.3},
                    {6, 1, 0.4},
                    {8, 0, 0.5},
                    {8, 1, 0.6},
                    {9, 1, 0.7},
                },
                new int[]{ // invalidate calls
                    1, 3, 4, 9
                },
                new Double[][]{ // expected result state
                    {null, null},
                    {null, null},
                    {null, null},
                    {null, null},
                    {null, null},
                    {0.3, null},
                    {null, 0.4},
                    {null, null},
                    {0.5, 0.6},
                    {null, null}
                }
            },
        };
    } // end parametersForTestInvalidate_positive

    /**
     * Tests that out-of-bounds requests to invalidate a cache row fail without changing the cache
     * state.
     */
    @Test
    @Parameters({
        "0,0,0,0", "0,0,0,-1",
        "0,0,1,5", "0,0,1,-42",
        "1,0,0,2", "1,0,0,-3",
        "1,1,0,1", "1,1,0,-2",
        "1,1,1,1", "1,1,1,-1", "1,1,1,2", "1,1,1,-2",
        "10,7,2,10", "10,7,2,-1", "10,7,2,42", "10,7,2,-42",
    })
    @TestCaseName("Test #{index}: new QuoteCache({0},{1},{2}).put(...).invalidate({3})")
    public void testInvalidate_randomCache_negative(int nTradersInEconomy, int nPotentialSellers,
                                                int nShoppingLists, int traderIndexToInvalidate) {
        QuoteCache cache = new QuoteCache(nTradersInEconomy, nPotentialSellers, nShoppingLists);
        Random generator = new Random(0); // constant seed ensures generated sequence is repeatable.

        // pseudo-randomly select nPotentialSellers unique indices from the range
        // [0, nTradersInEconomy). (Positions of the array after nPotentialSellers won't be used)
        int[] traderIndices = IntStream.range(0, nTradersInEconomy).toArray();
        Collections.shuffle(Ints.asList(traderIndices));

        try {
            // populate the cache with some random quotes
            for (int row = 0; row < nPotentialSellers; row++) {
                for (int slIndex = 0; slIndex < nShoppingLists; slIndex++) {
                    assertSame(cache, cache.put(traderIndices[row], slIndex,
                                                new CommodityQuote(null, generator.nextDouble())));
                }
            }
            // attempt to invalidate an out-of-bounds row
            cache.invalidate(traderIndexToInvalidate);
            fail("Expected IllegalArgumentException | ArrayIndexOutOfBoundsException!");
        } catch (IllegalArgumentException | ArrayIndexOutOfBoundsException e) {
            // check cache contents remain unchanged.
            generator.setSeed(0); // repeat the same sequence used when populating the cache
            for (int row = 0; row < nPotentialSellers; row++) {
                for (int slIndex = 0; slIndex < nShoppingLists; slIndex++) {
                    assertEquals(generator.nextDouble(),
                        cache.get(traderIndices[row], slIndex).getQuoteValue(), 0);
                }
            }
        }
    } // end testInvalidate_randomCache_negative
} // end QuoteCacheTest
