package com.vmturbo.platform.analysis.utilities;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import java.util.Arrays;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Deterministic;
import org.checkerframework.dataflow.qual.Pure;
import org.checkerframework.dataflow.qual.SideEffectFree;

import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.UnmodifiableEconomy;
import com.vmturbo.platform.analysis.ede.EdeCommon;
import com.vmturbo.platform.analysis.utilities.Quote.MutableQuote;

/**
 * A caching mechanism for {@link MutableQuote} objects.
 *
 * <p>The intention is to cache the results of {@link EdeCommon#quote(UnmodifiableEconomy,
 * ShoppingList, Trader, double, boolean)} during the execution of the SNM-enabled variant of the
 * Placement algorithm to avoid the (costly) recalculation of them.</p>
 *
 * <p>This class operates under the assumption that the {@link Economy} argument will be the same
 * each time (which is plausible given that the cache will be used during a single placement
 * analysis) and that the <b>bestQuoteSoFar</b> and <b>forTraderIncomeStmt</b> arguments will be
 * fixed to {@link Double#POSITIVE_INFINITY} and {@code false} respectively. Thus, the <b>seller</b>
 * and <b>shoppingList</b> arguments are the only ones used to index the cache.</p>
 *
 * <p>Manual invalidation will be needed when the customers of a seller change as that effects the
 * quantities sold and ultimately the quote and is not captured by the index arguments below.</p>
 *
 * <p>This version of the cache is optimized for minimal quote store/retrieval overhead at the
 * expense of slightly more work for invalidating quotes of traders whose list of customers has
 * changed.</p>
 *
 * <p>Currently there is no need to include utility methods to get the cache size after
 * construction, but such methods can be added if the need arises.</p>
 */
public final class QuoteCache {
    // Fields
    private final int[] rowAssignments; // A mapping from economy indices to rows in the cache table
    private int nextRowAssignment; // Index of the first row in cache table that hasn't been
                                  // assigned to a trader in rowAssignments array.
    private final MutableQuote[] cache; // Contains the cached quote for each (seller,shopping list)
        // pair. It's utilized as a 2-dimensional table with one row per seller and one column per
        // shopping list in row-major order. There is an additional 'sentinel' row at the beginning.
    private final int nShoppingLists; // The number of columns in the cache table. Equivalently the
        // number of shopping lists for the buyer SNM Analysis is currently attempting to place.

    // Methods

    /**
     * Constructs a new, empty quote cache of the given size.
     *
     * <p>The cache will consume a constant O(<b>nTradersInEconomy</b> +
     * min(<b>nPotentialSellers</b>, <b>nTradersInEconomy</b>) * <b>nBuyerShoppingLists</b>) amount
     * of memory throughout its lifetime.</p>
     *
     * <p>This is always an (<b>nTradersInEconomy</b> + min(<b>nPotentialSellers</b>,
     * <b>nTradersInEconomy</b>) * <b>nBuyerShoppingLists</b>) operation as allocating memory in
     * Java requires time at least linear to the amount of memory allocated.</p>
     *
     * @param nTradersInEconomy The number of traders in the {@link Economy} this cache will help
     *                          analyze. Must be non-negative.
     * @param nPotentialSellers The maximum number of unique sellers this cache can hold. Must be
     *                          non-negative. Note that, although permitted, values greater than
     *                          <b>nTradersInEconomy</b> won't increase the cache size as there
     *                          can't be more than <b>nTradersInEconomy</b> unique sellers anyway.
     * @param nBuyerShoppingLists The maximum number of shopping lists this cache can hold. Must be
     *                            non-negative.
     */
    @SideEffectFree
    public QuoteCache(int nTradersInEconomy, int nPotentialSellers, int nBuyerShoppingLists) {
        checkArgument(0 <= nPotentialSellers,
            "nPotentialSellers must be non-negative but was %s.", nPotentialSellers);
        checkArgument(0 <= nBuyerShoppingLists,
            "nBuyerShoppingLists must be non-negative but was %s.", nBuyerShoppingLists);

        rowAssignments = new int[nTradersInEconomy];

        cache = new MutableQuote[
            (Math.min(nPotentialSellers, nTradersInEconomy) + 1) * nBuyerShoppingLists];
        nShoppingLists = nBuyerShoppingLists;
        nextRowAssignment = 1; // 1st trader gets row 1: row 0 is the sentinel row.
    } // end QuoteCache constructor

    /**
     * Returns the cached quote for a given combination of trader and shopping list index if exists.
     *
     * <p>This is always an O(1) operation.</p>
     *
     * @param traderEconomyIndex The economy index of the trader for which to return the quote.
     *                           Must be in the range [0, nTradersInEconomy).
     * @param shoppingListIndex The index of the shopping list for which to return the quote. This
     *      index is defined in terms of the iteration order of the map returned by
     *      {@link Economy#getMarketsAsBuyer(Trader)}. i.e. the first key has index 0, the second
     *      index 1 and so on. Must be in the range [0, nShoppingLists).
     * @return The cached quote for this combination of indices, if there is one, or {@code null} if
     *         these isn't.
     *
     * @see #put(int, int, MutableQuote)
     */
    @Pure
    public @Nullable MutableQuote get(int traderEconomyIndex, int shoppingListIndex) {
        checkArgument(0 <= shoppingListIndex && shoppingListIndex < nShoppingLists,
            "shoppingListIndex must be in the range [0, %s) but was %s.",
            nShoppingLists, shoppingListIndex);

        return cache[rowAssignments[traderEconomyIndex] * nShoppingLists + shoppingListIndex];
    } // end method get

    /**
     * Puts a new or updated quote value for a given combination of trader and shopping list index
     * into {@code this} cache.
     *
     * <p>It is an error to attempt to cache quotes for more unique sellers than was declared when
     * constructing {@code this} cache.</p>
     *
     * <p>This method offers the strong exception safety guarantee. i.e. it will either succeed or
     * leave {@code this} cache in its original state.</p>
     *
     * <p>This is always an O(1) operation.</p>
     *
     * @param traderEconomyIndex The economy index of the trader for which to cache the quote.
     *                           Must be in the range [0, nTradersInEconomy).
     * @param shoppingListIndex The index of the shopping list for which to cache the quote.
     *                          Must be in the range [0, nShoppingLists). See {@link #get(int, int)}
     *                          about how this is defined.
     * @param quote The quote to associate with the above indices.
     * @return {@code this}
     *
     * @see #get(int, int)
     */
    @Deterministic
    public @NonNull QuoteCache put(int traderEconomyIndex, int shoppingListIndex,
                                   @NonNull MutableQuote quote) {
        checkArgument(0 <= shoppingListIndex && shoppingListIndex < nShoppingLists,
            "shoppingListIndex must be in the range [0, %s) but was %s.",
            nShoppingLists, shoppingListIndex);

        if (rowAssignments[traderEconomyIndex] == 0) { // 0 means "no assignment"
            checkState(nextRowAssignment * nShoppingLists < cache.length, "This cache cannot hold "
                + "quotes for any more distinct sellers. Up to %s are allowed!", nextRowAssignment);
            rowAssignments[traderEconomyIndex] = nextRowAssignment++;
        }
        cache[rowAssignments[traderEconomyIndex] * nShoppingLists + shoppingListIndex] = quote;

        return this;
    } // end method put

    /**
     * Invalidates all cached entries for the given {@link Trader}.
     *
     * <p>After this call, and until new quotes are cached, calls to {@link #get(int, int)}
     * involving the given trader will return {@code null} regardless of the shopping list index
     * given. Invalidating a cache row that was previously in use by some seller doesn't free up the
     * row for use by other sellers -- ownership remains to the initial seller.</p>
     *
     * <p>Attempting to invalidate a cache row for a seller for which no quotes have ever been
     * cached, is permitted and has no effect.</p>
     *
     * <p>This is an O(<b>nShoppingLists</b>) operation in the worst case.</p>
     *
     * @param traderEconomyIndex Seller for which to invalidate the quotes.
     *                           Must be in the range [0, nTradersInEconomy).
     * @return {@code this}
     */
    @Deterministic
    public @NonNull QuoteCache invalidate(int traderEconomyIndex) {
        final int firstIndexInRow = rowAssignments[traderEconomyIndex] * nShoppingLists;

        Arrays.fill(cache, firstIndexInRow, firstIndexInRow + nShoppingLists, null);

        return this;
    } // end method invalidate

} // end QuoteCache class
