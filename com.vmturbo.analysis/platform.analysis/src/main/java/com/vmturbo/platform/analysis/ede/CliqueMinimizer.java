package com.vmturbo.platform.analysis.ede;

import static com.vmturbo.platform.analysis.utilities.QuoteCacheUtils.invalidate;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.UnmodifiableEconomy;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.Context;
import com.vmturbo.platform.analysis.utilities.M2Utils;
import com.vmturbo.platform.analysis.utilities.QuoteCache;
import com.vmturbo.platform.analysis.utilities.QuoteTracker;

/**
 * A mutable collector class used to find the best k-partite clique to position a number of
 * {@link ShoppingList}s and the best total quote offered in this clique.
 *
 * <p>
 *  This is intended to be used with {@link Stream#collect(Supplier, BiConsumer, BiConsumer)}.
 * </p>
 *
 * A {@link CliqueMinimizer} is used during placement analysis.
 * See below for an example. Suppose for simplicity all sellers in the example belong to all cliques.
 *
 * <b>Buyer</b>
 *   |--> ShoppingList1 (movable=true)   / Market 1234 <Clique A,B,F,G> [Sellers T1/B, T2/B, T3/B, T4/F, T5/G]
 *   |--> ShoppingList2 (movable=true)   / Market 1235 <Clique B,F,G>   [Sellers T10/B, T11/F, T12/F, T13/G, T14/G]
 *   |--> ShoppingList3 (movable=false)  / Market 2870 <Clique B,F,G>   [Sellers T20/B/F/G]
 *   ---> ShoppingList4 (movable=true)   / Market 0832 <Clique A,B,G>   [Sellers T30/B, T31/F, T32/F, T33/G]
 *
 * <b>CLIQUE_MINIMIZER</b>(ShoppingList1,2,4) <--- Note that we do not minimize over 3 because it is movable=false!
 *   Find best clique in <B,F,G> (skip A because it is not in the intersection over the
 *                                cliques in the trader's shopping lists.)
 *     <b>QUOTE_SUMMER</b>(clique B) - Sum quotes for ShoppingLists 1,2,4 <-- sum = (33.3+33.3+33.3) = 99.9
 *         Add together minimum quotes for:
 *             <b>QUOTE_MINIMIZER</b>(ShoppingList1) <-- min = 33.3
 *                 T1 <-- quote = infinity
 *                 T2 <-- quote = 33.3
 *             <b>QUOTE_MINIMIZER</b>(ShoppingList2) <-- min = 33.3
 *                 T10 <-- quote = 33.3
 *             <b>QUOTE_MINIMIZER</b>(ShoppingList4) <-- min = 33.3
 *                 T30 <-- quote = 33.3
 *     QUOTE_SUMMER(clique F) - Sum quotes for ShoppingLists 1,2,4 <-- sum = (infinity+33.3+972.4) = infinity
 *         Add together minimum quotes for:
 *             <b>QUOTE_MINIMIZER</b>(ShoppingList1) <-- min = infinity
 *                 T4 <-- quote = infinity
 *             <b>QUOTE_MINIMIZER</b>(ShoppingList2) <-- min = 33.3
 *                 T11 <-- quote = infinity
 *                 T12 <-- quote = 33.3
 *             <b>QUOTE_MINIMIZER</b>(ShoppingList4) <-- min = 972.4
 *                 T31 <-- quote = 972.4
 *                 T32 <-- quote = 1204.3
 *     QUOTE_SUMMER(clique G) - Sum quotes for ShoppingLists 1,2,4 <-- sum = (91.4+100.0+23.1) = 214.5
 *         Add together minimum quotes for:
 *             <b>QUOTE_MINIMIZER</b>(ShoppingList1) <-- min = 91.4
 *                 T5 <-- quote = 91.4
 *             <b>QUOTE_MINIMIZER</b>(ShoppingList2) <-- min = 100.0
 *                 T13 <-- quote = 200.0
 *                 T14 <-- quote = 100.0
 *             <b>QUOTE_MINIMIZER</b>(ShoppingList4) <-- min = 23.1
 *                 T33 <-- quote = 23.1
 *  Best clique is B with a total quote of 99.9.
 */
final class CliqueMinimizer {

    private static final Logger logger = LogManager.getLogger();

    // Auxiliary Fields
    private final @NonNull Economy economy_; // should contain all the shopping lists
    // and cliques passed to #accept.
    // The collection of (shopping list, market) pairs for which to find the best k-partite clique
    private final @NonNull @ReadOnly Collection<@NonNull Entry<@NonNull ShoppingList, @NonNull Market>> entries_;

    // Accumulator Fields
    private @MonotonicNonNull List<@NonNull Trader> bestSellers_ = null; // will hold the best-so-far
    // a map which contains shopping lists and the each shopping list's best quote
    // associated context. It is used to help generating actions.
    private Map<ShoppingList, Optional<Context>> shoppingListContextMap = new HashMap<>();
    // valid placement. i.e. the one with minimum total quote
    private double bestTotalQuote_ = Double.POSITIVE_INFINITY; // will host the best-so-far total
    // quote. i.e. the minimum one.
    // Could also have included an accumulator for the best k-partite clique, but it wasn't strictly
    // needed.

    private @NonNull Map<ShoppingList, QuoteTracker> infiniteQuoteTrackers = Collections.emptyMap();

    private @Nullable QuoteCache cache_;
    // Constructors

    /**
     * Constructs an empty CliqueMinimizer that can be used as an identity element for the reduction.
     *
     * @param economy See {@link #getEconomy()}.
     * @param entries See {@link #getEntries()}.
     * @param cache A cache where to store quotes to avoid recomputing them in case where the same
     *              sellers are members of multiple cliques.
     */
    CliqueMinimizer(@NonNull Economy economy, @NonNull @ReadOnly
            Collection<@NonNull Entry<@NonNull ShoppingList, @NonNull Market>> entries,
            @Nullable QuoteCache cache) {
        economy_ = economy;
        entries_ = entries;
        cache_ = cache;
    }

    // Getters

    /**
     * Returns the {@link Economy} that contains all the the {@link ShoppingList}s and
     * {@link Market}s used in the reduction.
     */
    @Pure
    public @NonNull UnmodifiableEconomy getEconomy(@ReadOnly CliqueMinimizer this) {
        return economy_;
    }

    /**
     * Returns the collection of (shopping list, market) pairs for which the best k-partite clique
     * will be found.
     */
    @Pure
    public @NonNull @ReadOnly Collection<@NonNull Entry<@NonNull ShoppingList, @NonNull Market>>
    getEntries(@ReadOnly CliqueMinimizer this) {
        return entries_;
    }

    /**
     * Returns the minimum total of minimum quotes between the ones offered in the k-partite cliques
     * seen by {@code this} minimizer, or {@link Double#POSITIVE_INFINITY} if no clique have been
     * seen or none has offered a finite total quote so far.
     */
    @Pure
    public double getBestTotalQuote(@ReadOnly CliqueMinimizer this) {
        return bestTotalQuote_;
    }

    /**
     * Returns a map which contains shopping lists and the each shopping list's best quote
     * associated context.
     * Note: the map is necessary for keep track of context data which will be needed when generating
     * actions. It is populated and passed by QuoteSummer.
     *
     * @return the shopping list to context mapping.
     */
    public Map<ShoppingList, Optional<Context>> getShoppingListContextMap() {
        return shoppingListContextMap;
    }

    /**
     * Get the {@link QuoteTracker}s for {@link ShoppingList}s that received infinite quotes
     * during minimization.
     *
     * @return The {@link QuoteTracker}s for {@link ShoppingList}s that received infinite quotes.
     */
    @Pure
    @NonNull
    public Map<ShoppingList, QuoteTracker> getInfiniteQuoteTrackers() {
        return infiniteQuoteTrackers;
    }

    /**
     * Get the combined (summed) rank of all {@link QuoteTracker}s being tracked by this minimizer.
     *
     * @param quoteTrackers The {@link QuoteTracker}s whose ranks should be summed.
     * @return The sum of the ranks for all quote trackers being tracked by this minimizer.
     */
    public int getInfiniteQuoteCombinedRank(@NonNull final Map<ShoppingList, QuoteTracker> quoteTrackers) {
        return quoteTrackers.values().stream()
            .mapToInt(QuoteTracker::getQuoteRank)
            .sum();
    }

    /**
     * Returns an unmodifiable list of sellers that represents the best valid placement.
     *
     * <p>
     *  'Best' means the one offering the minimum total of minimum quotes between the k-partite
     *  cliques examined by {@code this} minimizer.
     * </p>
     *
     * <p>
     *  Will be {@code null} iff no sellers have been seen or no seller offered a finite quote.
     * </p>
     *
     * <p>
     *  In other words it will return {@code null} iff {@link #getBestTotalQuote()} returns
     *  {@link Double#POSITIVE_INFINITY}.
     * </p>
     *
     * <p>
     *  Also {@code getBestSellers()}.{@link Collection#size() size()} ==
     *  {@link #getEntries()}.{@link Collection#size() size()}
     * </p>
     */
    @Pure
    public @MonotonicNonNull List<@NonNull Trader> getBestSellers(@ReadOnly CliqueMinimizer this) {
        return bestSellers_;
    }

    // Reduction Methods

    /**
     * Updates the internal state based on the quotes offered in a given k-partite clique.
     *
     * <p>
     *  This will update the values returned by {@link #getBestTotalQuote()} and
     *  {@link #getBestSellers()}.
     * </p>
     *
     * @param clique The k-partite clique in which to ask for quotes and update internal state.
     */
    public void accept(long clique) {
        final @NonNull QuoteSummer quoteSummer = entries_.stream()
            .collect(() -> new QuoteSummer(economy_, clique, cache_, entries_.size()), QuoteSummer::accept, QuoteSummer::combine);

        // keep the minimum between total quotes
        if (quoteSummer.getTotalQuote() < bestTotalQuote_) {
            logMessagesForAccept(clique, quoteSummer);
            bestTotalQuote_ = quoteSummer.getTotalQuote();
            bestSellers_ = quoteSummer.getBestSellers();
            Map<ShoppingList, Optional<Context>> map = quoteSummer.getShoppingListContextMap();
            if (!map.isEmpty()) {
                shoppingListContextMap = map;
            }
            infiniteQuoteTrackers = Collections.emptyMap();
        }

        if (Double.isInfinite(bestTotalQuote_)) {
            combineQuoteTrackers(quoteSummer.getUnplacedShoppingListQuoteTrackers());
        }

        economy_.getPlacementStats().incrementCliqueMinimizationCount();
        Lists.reverse(quoteSummer.getSimulatedActions()).forEach(move -> {
            move.rollback();
            invalidate(cache_, move);
        });
    }

    /**
     * Updates the internal state based on another {@link CliqueMinimizer} object.
     *
     * <p>
     *  This will update the values returned by {@link #getBestTotalQuote()} and
     *  {@link #getBestSellers()}.
     * </p>
     *
     * @param other The minimizer that should be used to update the internal state.
     */
    public void combine(@NonNull @ReadOnly CliqueMinimizer other) {
        if (other.bestTotalQuote_ < bestTotalQuote_) {
            bestTotalQuote_ = other.bestTotalQuote_;
            bestSellers_ = other.bestSellers_;
            shoppingListContextMap = other.getShoppingListContextMap();
            infiniteQuoteTrackers = Collections.emptyMap();
        } else if (Double.isInfinite(other.bestTotalQuote_) && Double.isInfinite(bestTotalQuote_)) {
            combineQuoteTrackers(other.getInfiniteQuoteTrackers());
        }
    }

    /**
     * Combine quote trackers. The rules for combination are as follows:
     * Comparisons are made according to the combined rank of all quotes in the quoteTrackers map.
     *
     * 1. If either map of quote trackers is empty, set the quote trackers to be an empty map.
     * 2. If the other rank is less than this rank, take their quote trackers.
     * 3. If this rank is less than the other rank, retain the current quote trackers.
     * 4. If ranks are equal:
     *    a. Prefer the quote trackers with fewer unplaced shopping lists.
     *    b. If they each have the same number of shopping lists and those shopping lists are the same,
     *       combine the quote trackers.
     *    c. If they each have the same number of shopping lists but those shopping lists are different,
     *       it doesn't matter which quote trackers we take, so retain the current map.
     *
     * @param quoteTrackers The quote trackers to combine.
     */
    private void combineQuoteTrackers(@NonNull final Map<ShoppingList, QuoteTracker> quoteTrackers) {
        if (infiniteQuoteTrackers.isEmpty()) {
            // If our map is empty, just take the other one.
            infiniteQuoteTrackers = quoteTrackers;
        } else {
            int thisRank = getInfiniteQuoteCombinedRank(infiniteQuoteTrackers);
            int otherRank = getInfiniteQuoteCombinedRank(quoteTrackers);

            if (otherRank < thisRank) {
                // If the other has smaller rank, prefer that one.
                infiniteQuoteTrackers = quoteTrackers;
            } else if (otherRank == thisRank) {
                if (quoteTrackers.keySet().size() < infiniteQuoteTrackers.keySet().size()) {
                    // If the other has fewer unplaced shopping lists, prefer that one.
                    infiniteQuoteTrackers = quoteTrackers;
                } else if (quoteTrackers.keySet().size() > infiniteQuoteTrackers.keySet().size()) {
                    // The other quote tracker has more unplaced shopping lists. Prefer ours. This can happen
                    // when, for example, we have 1 unplaced shopping list of rank 2 and the other has
                    // 2 unplaced shopping lists of rank 1.
                } else if (quoteTrackers.keySet().equals(infiniteQuoteTrackers.keySet())) {
                    // If ranks are equal and the shopping lists are the same, combine.
                    quoteTrackers.forEach(((sl, otherQuoteTracker) ->
                        infiniteQuoteTrackers.get(sl).combine(otherQuoteTracker)));
                } else {
                    // The other quote tracker has equal rank and same number of shopping lists, but
                    // those shopping lists are different. It doesn't really matter which one we pick
                    // at this point, so continue to use the one we already have.
                    // TODO: Maybe pick one based on some criteria to increase reliability you get the
                    // same results across analysis runs.
                }
            }
        }
    }

    /**
     * Logs messages if the logger's trace mode is enabled or any of the quoteSummer's best
     * sellers have their debug enabled.
     *
     * @param clique The k-partite clique in which to ask for quotes and update internal state.
     * @param quoteSummer The QuoteSummer used to sum quotes
     */
    private void logMessagesForAccept(long clique, QuoteSummer quoteSummer) {
        if (logger.isTraceEnabled() || quoteSummer.getBestSellers().stream()
            .filter(trader -> trader != null)
            .anyMatch(Trader::isDebugEnabled)) {
            long topologyId = M2Utils.getTopologyId(economy_);
            logger.debug("topology id = {}, clique = {}, oldBestQuote = {}, newBestQuote = {}",
                topologyId, clique, bestTotalQuote_, quoteSummer.getTotalQuote());
            String oldSellers = "";
            if (bestSellers_ != null) {
                oldSellers = bestSellers_.stream().map(Trader::getDebugInfoNeverUseInCode)
                    .collect(Collectors.joining(", "));
            }
            String newSellers = "";
            if (quoteSummer.getBestSellers() != null) {
                newSellers = quoteSummer.getBestSellers().stream()
                    .map(Trader::getDebugInfoNeverUseInCode)
                    .collect(Collectors.joining(", "));
            }
            logger.debug("topology id = {}, oldBestSellers = {}", topologyId, oldSellers);
            logger.debug("topology id = {}, newBestSellers = {}", topologyId, newSellers);
        }
    }
} // end CliqueMinimizer class
