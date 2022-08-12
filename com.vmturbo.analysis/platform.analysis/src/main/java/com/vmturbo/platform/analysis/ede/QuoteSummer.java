package com.vmturbo.platform.analysis.ede;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.UnmodifiableEconomy;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.Context;
import com.vmturbo.platform.analysis.utilities.Quote.CommodityContext;
import com.vmturbo.platform.analysis.utilities.QuoteCache;
import com.vmturbo.platform.analysis.utilities.QuoteCacheUtils;
import com.vmturbo.platform.analysis.utilities.QuoteTracker;

/**
 * A mutable collector class used to compute the total quote obtained for a number of
 * {@link ShoppingList}s assuming the best supplier for each one.
 *
 * <p>
 *  This is intended to be used with {@link Stream#collect(Supplier, BiConsumer, BiConsumer)}.
 * </p>
 *
 * Often used in conjunction with {@link CliqueMinimizer}. For example usage
 * {@see CliqueMinimizer}.
 */
final class QuoteSummer {
    // Auxiliary Fields
    private final @NonNull Economy economy_; // should contain all the shopping lists
                                                        // and markets that are passed to #accept.
    private final long clique_; // the k-partite clique for which to compute the total quote.

    // Accumulator Fields
    private final @NonNull List<@Nullable Trader> bestSellers_ = new ArrayList<>(); // will contain
                                // one seller for each (shopping list, market) pair passed to #accept.

    // List of simulatedActions constituent Move actions of a CompoundMove
    private final @NonNull List<@NonNull Move> simulatedMoveActions_ = new ArrayList<>();

    private double totalQuote_ = 0.0; // will accumulate the sum of all best quotes.

    // Cached data
    // Cached unmodifiable view of the bestSellers_ list.
    private final @NonNull List<@Nullable Trader> unmodifiableBestSellers_ = Collections.unmodifiableList(bestSellers_);

    static final Logger logger = LogManager.getLogger(QuoteSummer.class);

    private final Map<ShoppingList, QuoteTracker> unplacedShoppingListQuoteTrackers = new HashMap<>();
    // a map which contains shopping lists and the each shopping list's best quote
    // associated context. It is used to help generating actions.
    // Note: The QuoteMinimizer contains CommodityCloudQuote object which stores the context,
    // we could populate a map for Quote by shopping list, but only "context" is needed
    // outside of CliqueMinimizer, hence we can directly store the context for efficiency.
    private final Map<ShoppingList, Optional<Context>> shoppingListContextMap = new HashMap<>();

    private final Map<ShoppingList, List<CommodityContext>> shoppingListCommodityContextMap = new HashMap<>();

    private final QuoteCache cache_;
    private int shoppingListIndex_ = 0;
    private int numOfSLs_ = 0;
    private boolean performOptimization_ = false;
    private double bestTotalQuote_;

    // Constructors

    /**
     * Constructs an empty QuoteSummer that can be used as an identity element for the reduction.
     *
     * @param economy See {@link #getEconomy()}.
     * @param quality See {@link #getClique()}.
     * @param cache See {@link QuoteCache}
     * @param numOfSLs number of shopping lists for this trader
     * @param performOptimization If true, simulation is skipped for the first and last shopping lists.
     *                            This assumes knowledge of the fact that the first (PM) and
     *                            last (storage) shopping list don't need simulation.
     *                            If false, simulation is performed for all shopping lists.
     * @param bestTotalQuote When the totalQuote for all the shopping lists calculated so far becomes
     *                       greater than bestTotalQuote, then we skip calculating the quote for the
     *                       rest of the shopping lists.
     */
    QuoteSummer(@NonNull Economy economy, long quality, QuoteCache cache, int numOfSLs,
                boolean performOptimization, double bestTotalQuote) {
        economy_ = economy;
        clique_ = quality;
        cache_ = cache;
        numOfSLs_ = numOfSLs;
        performOptimization_ = performOptimization;
        bestTotalQuote_ = economy.getSettings().isBranchAndBoundEnabled() ? bestTotalQuote : Double.POSITIVE_INFINITY;
    }

    // Getters

    /**
     * Returns the {@link Economy} that should contain all the {@link ShoppingList}s and
     * {@link Market}s of the reduction.
     *
     * <p>
     *  Passing a (shopping list, market) pair to {@link #accept(Entry)} that is not in that
     *  economy is an error.
     * </p>
     */
    @Pure
    public @NonNull UnmodifiableEconomy getEconomy(@ReadOnly QuoteSummer this) {
        return economy_;
    }

    /**
     * Returns the k-partite clique for which the best quotes will be queried and summed.
     */
    @Pure
    public long getClique(@ReadOnly QuoteSummer this) {
        return clique_;
    }

    /**
     * Returns the sum of the best quotes corresponding to (shopping list, market) pairs seen by
     * {@code this} summer and the summer's clique, or 0 if no such pairs have been seen.
     */
    @Pure
    public double getTotalQuote(@ReadOnly QuoteSummer this) {
        return totalQuote_;
    }

    /**
     * Returns an unmodifiable list of the sellers that offered the minimum quote per
     * (shopping list, market) pair seen by {@code this} summer.
     *
     * <p>
     *  If the result of {@link #getTotalQuote()} is finite, then the contents of this list are not
     *  {@code null}.
     * </p>
     */
    @Pure
    public @NonNull List<@Nullable Trader> getBestSellers(@ReadOnly QuoteSummer this) {
        return unmodifiableBestSellers_;
    }

    /**
     * Returns a map which contains shopping lists and the each shopping list's best quote
     * associated context.
     * Note: the map is necessary for keep track of context data which will be needed when generating
     * actions. The QuoteMinimizer has a reference to Quote which stores the context, but in this
     * QuoteSummer class, Quote object doesnt exist.
     *
     * @return the shopping list to context mapping.
     */
    public @NonNull Map<ShoppingList, Optional<Context>> getShoppingListContextMap() {
        return shoppingListContextMap;
    }

    public @NonNull Map<ShoppingList, List<CommodityContext>> getShoppingListCommodityContextMap() {
        return shoppingListCommodityContextMap;
    }

    /**
     * Returns an list of the Move actions to the sellers that offered the minimum quote per
     * (shopping list, market) pair seen by {@code this} summer.
     *
     */
    @Pure
    public @NonNull List<@NonNull Move> getSimulatedActions(@ReadOnly QuoteSummer this) {
        return simulatedMoveActions_;
    }

    public Map<ShoppingList, QuoteTracker> getUnplacedShoppingListQuoteTrackers() {
        return unplacedShoppingListQuoteTrackers;
    }

    // Reduction Methods

    /**
     * Updates the internal state based on the best quote offered by a set of traders.
     *
     * <p>
     *  This will update the values returned by {@link #getTotalQuote()} and {@link #getBestSellers()}.
     * </p>
     *
     * @param entry A ({@link ShoppingList}, {@link Market}) pair. The best quote offered by any
     *              active seller in the market that is a member of {@code this} summer's clique,
     *              for this shopping list, will be added in the sum.
     */
    public void accept(@NonNull @ReadOnly Entry<@NonNull ShoppingList, @NonNull Market> entry) {
        if (economy_.getSettings().isBranchAndBoundEnabled() && totalQuote_ >= bestTotalQuote_) {
            return;
        }
        // consider only active sellers while performing SNM
        @NonNull LinkedHashSet<@NonNull Trader> sellers = entry.getValue().getCliques().get(clique_).stream()
                .filter(seller -> seller.getState().isActive()
                        && (seller.getSettings().canAcceptNewCustomers() ||
                        seller == entry.getKey().getSupplier())).collect(Collectors.toCollection(LinkedHashSet::new));
        // InitiateQuoteMinimizer takes in sellers as a set. To maintain order this set is created as a
        // LinkedHashSet whenever possible. This helps with consistent actions.
        QuoteMinimizer minimizer = Placement.initiateQuoteMinimizer(economy_, sellers,
                                            entry.getKey(), cache_, shoppingListIndex_, bestTotalQuote_);
        Optional<Context> context = minimizer.getBestQuote().getContext();
        if (context.isPresent()) {
            shoppingListContextMap.put(entry.getKey(), minimizer.getBestQuote().getContext());
        }
        List<CommodityContext> commodityContexts = minimizer.getBestQuote().getCommodityContexts();
        if (!commodityContexts.isEmpty()) {
            shoppingListCommodityContextMap.put(entry.getKey(), commodityContexts);
        }
        totalQuote_ += minimizer.getTotalBestQuote();
        bestSellers_.add(minimizer.getBestSeller());
        economy_.getPlacementStats().incrementQuoteSummerCount();
        Trader bestSeller = minimizer.getBestSeller();
        if (!isBestSellerPresentAndDifferentFromCurrentSupplier(bestSeller, entry)) {
            unplacedShoppingListQuoteTrackers.put(entry.getKey(), minimizer.getQuoteTracker());
        }
        // We simulate the effect of placing the shopping list on the bestSeller, so that the
        // next shopping lists of this trader get quotes which reflect the fact that this SL was
        // placed on bestSeller. But we don't need to do this for the first and last shopping lists
        // because for a VM, the first SL is the PM shopping list while the last SL is the
        // last storage shopping list, and there are no further PM/Storage shopping lists which
        // will ask for quote.
        if (!performOptimization_ || (shoppingListIndex_ > 0 && shoppingListIndex_ < numOfSLs_ - 1)) {
            simulate(minimizer, entry, bestSeller);
        }
        shoppingListIndex_++;
    }

    /**
     * Updates the internal state based on another {@link QuoteSummer} object.
     *
     * <p>
     *  This will update the values returned by {@link #getTotalQuote()} and {@link #getBestSellers()}.
     * </p>
     *
     * @param other The summer that should be used to update the internal state.
     */
    public void combine(@NonNull @ReadOnly QuoteSummer other) {
        totalQuote_ += other.getTotalQuote();
        bestSellers_.addAll(other.getBestSellers());
        other.getShoppingListContextMap().entrySet().forEach(e -> {
            shoppingListContextMap.put(e.getKey(), e.getValue());
        });

        shoppingListCommodityContextMap.putAll(other.getShoppingListCommodityContextMap());

        other.getUnplacedShoppingListQuoteTrackers().forEach((sl, otherQuoteTracker) -> {
            final QuoteTracker thisQuoteTracker = unplacedShoppingListQuoteTrackers.get(sl);
            if (thisQuoteTracker != null) {
                thisQuoteTracker.combine(otherQuoteTracker);
            } else {
                unplacedShoppingListQuoteTrackers.put(sl, otherQuoteTracker);
            }
        });
    }

    /**
     * Method to simulate a MOVE if possible
     *
     * @param minimizer  The current minimizer
     * @param entry  Entry object corresponding to shopping list and market
     * @param bestSeller The best provider
     */
    public void simulate(QuoteMinimizer minimizer, @NonNull @ReadOnly Entry<@NonNull ShoppingList, @NonNull Market> entry, Trader bestSeller) {
        if (isBestSellerPresentAndDifferentFromCurrentSupplier(bestSeller, entry)) {
            if (bestSeller.getSettings().isCanSimulateAction()) {
                // when we have a best seller for a shoppingList, we simulate the move to this seller
                // not doing so can lead to ping-pongs as observed in OM-34056
                Move move = new Move(economy_, entry.getKey(), minimizer.getBestSeller()).take();
                QuoteCacheUtils.invalidate(cache_, move);
                simulatedMoveActions_.add(move);
            }
        }
    }

    /**
     * Is bestSeller present and different from current supplier?
     * @param bestSeller the bestSeller for the SL in entry
     * @param entry the entry of SL and its market
     * @return true if bestSeller is present and different from current supplier.
     */
    private boolean isBestSellerPresentAndDifferentFromCurrentSupplier(Trader bestSeller,
                                                                      @NonNull Entry<@NonNull ShoppingList, @NonNull Market> entry) {
        return bestSeller != null && (entry.getKey().getSupplier() != bestSeller);
    }

} // end QuoteSummer class
