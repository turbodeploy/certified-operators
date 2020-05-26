package com.vmturbo.platform.analysis.ede;

import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.UnmodifiableEconomy;
import com.vmturbo.platform.analysis.utilities.M2Utils;
import com.vmturbo.platform.analysis.utilities.Quote;
import com.vmturbo.platform.analysis.utilities.Quote.InitialInfiniteQuote;
import com.vmturbo.platform.analysis.utilities.Quote.MutableQuote;
import com.vmturbo.platform.analysis.utilities.QuoteCache;
import com.vmturbo.platform.analysis.utilities.QuoteTracker;

/**
 * A mutable collector class used to find the best quote and corresponding seller.
 *
 * <p>
 *  This is intended to be used with {@link Stream#collect(Supplier, BiConsumer, BiConsumer)}.
 * </p>
 */
public final class QuoteMinimizer {

    private static final Logger logger = LogManager.getLogger();

    public static long cacheHits = 0;

    private final QuoteCache cache_;
    private final int shoppingListIndex_;

    // Auxiliary Fields
    private final @NonNull UnmodifiableEconomy economy_; // should contain all the seller arguments to #accept.
    private final @NonNull ShoppingList shoppingList_; // the shopping list for which to get a quote.

    // Accumulator Fields
    private @MonotonicNonNull Trader bestSeller_ = null; // will hold the best-so-far seller i.e.
                                                        // the one giving the minimum quote.
    private Quote bestQuote_ = new InitialInfiniteQuote(); // will hold the best-so-far quote. i.e. the minimum one.
    private Quote currentQuote_ = new InitialInfiniteQuote(); // may hold the quote from the current
                                                            // supplier if it's in the list of sellers.

    /**
     * Used to track infinite quotes. Quotes tracked by the quote tracker can be used to generate
     * explanations for traders that cannot be placed on any trader during the placement process.
     *
     * A trader will be unplaced if all possible sellers provide it an infinite quote.
     */
    private QuoteTracker quoteTracker;

    // Constructors

    /**
     * Constructs an empty QuoteMinimizer that can be used as an identity element for the reduction.
     *
     * @param economy See {@link #getEconomy()}.
     * @param shoppingList See {@link #getShoppingList()}
     * @param cache see {@link QuoteCache}
     * @param shoppingListIndex The index of <b>shoppingList</b> in the iteration order of the map
     *                          returned by {@link Economy#getMarketsAsBuyer(Trader)}. i.e. the
     *                          first key has index 0, the second index 1 and so on.
     */
    public QuoteMinimizer(@NonNull UnmodifiableEconomy economy, @NonNull ShoppingList shoppingList,
                          @Nullable QuoteCache cache, final int shoppingListIndex) {
        economy_ = economy;
        shoppingList_ = shoppingList;
        quoteTracker = new QuoteTracker(shoppingList);
        cache_ = cache;
        shoppingListIndex_ = shoppingListIndex;
    }

    /**
     * Constructs an empty QuoteMinimizer that can be used as an identity element for the reduction.
     *
     * @param economy See {@link #getEconomy()}.
     * @param shoppingList See {@link #getShoppingList()}
     */
    public QuoteMinimizer(@NonNull UnmodifiableEconomy economy, @NonNull ShoppingList shoppingList) {
        economy_ = economy;
        shoppingList_ = shoppingList;
        quoteTracker = new QuoteTracker(shoppingList);
        cache_ = null;
        shoppingListIndex_ = 0;
    }

    // Getters

    /**
     * Returns the {@link Economy} that should contain all the sellers of the reduction.
     *
     * <p>
     *  Passing a seller to {@link #accept(Trader)} that is not in that economy is an error.
     * </p>
     */
    @Pure
    public @NonNull UnmodifiableEconomy getEconomy(@ReadOnly QuoteMinimizer this) {
        return economy_;
    }

    /**
     * Returns the {@link ShoppingList} for which {@code this} minimizer will get and compare the
     * quotes.
     */
    @Pure
    public @NonNull ShoppingList getShoppingList(@ReadOnly QuoteMinimizer this) {
        return shoppingList_;
    }

    /**
     * Returns the minimum quote between the ones offered by sellers seen by {@code this} minimizer,
     * getQuoteValue() will return {@link Double#POSITIVE_INFINITY} if no sellers have been seen.
     */
    @Pure
    public Quote getBestQuote(@ReadOnly QuoteMinimizer this) {
        return bestQuote_;
    }

    /**
     * Returns the minimum sum of the bestQuote and the moveCost
     */
    @Pure
    public double getTotalBestQuote(@ReadOnly QuoteMinimizer this) {
        return bestQuote_.getQuoteValue() + bestQuote_.getMoveCost();
    }

    /**
     * Returns the seller that offered the minimum quote between the ones seen by {@code this}
     * minimizer, or {@code null} if no sellers have been seen or no seller offered a finite quote.
     *
     * <p>
     *  In other words it will return {@code null} iff {@link #getBestQuote()} returns
     *  {@link Double#POSITIVE_INFINITY}.
     * </p>
     */
    @Pure
    public @MonotonicNonNull Trader getBestSeller(@ReadOnly QuoteMinimizer this) {
        return bestSeller_;
    }

    /**
     * Returns the quote offered by the current supplier if it was seen by this minimizer, or
     * getQuoteValue() will return {@link Double#POSITIVE_INFINITY} if the current supplier hasn't been seen.
     */
    @Pure
    public Quote getCurrentQuote(@ReadOnly QuoteMinimizer this) {
        return currentQuote_;
    }

    @Pure
    public QuoteTracker getQuoteTracker(@ReadOnly QuoteMinimizer this) {
        return quoteTracker;
    }

    // Reduction Methods

    /**
     * Updates the internal state based on the quote offered by a given seller.
     *
     * <p>
     *  This will update the values returned by {@link #getBestSeller()}, {@link #getBestQuote()}
     *  and {@link #getCurrentQuote()} to reflect the new minima.
     * </p>
     *
     * Any VMs that receive an infinite quote are added to the internal {@link QuoteTracker}.
     *
     * @param seller The seller from which to get a quote and update internal state.
     */
    public void accept(@NonNull Trader seller) {
        MutableQuote quote;
        if (cache_ != null) {
            quote = cache_.get(seller.getEconomyIndex(), shoppingListIndex_);
            if(quote == null) {
                // need to recompute complete quote without any stopping criteria
                // since this cached quote could turn out to be the best in a different clique
                quote = EdeCommon.quote(economy_, shoppingList_, seller, Double.POSITIVE_INFINITY, false);
                cache_.put(seller.getEconomyIndex(), shoppingListIndex_, quote);
            } else {
                cacheHits++;
            }
        } else {
            quote = EdeCommon.quote(economy_, shoppingList_, seller,
                    bestQuote_.getQuoteValue(), false);
        }

        if (seller == shoppingList_.getSupplier()) {
            currentQuote_ = quote;
            // if the bestQuote so far is the same as the one from the currentSupplier, update the bestSeller.
            if (Double.isFinite(quote.getQuoteValue())
                    && quote.getQuoteValue() == this.getTotalBestQuote()) {
                logMessagesForAccept(seller, quote.getQuoteValues());
                bestQuote_ = quote;
                bestSeller_ = seller;
            }
            if (logger.isTraceEnabled()) {
                logger.trace("topology id = {}, shoppingList = {}, currentQuote = {}, currentSeller = {}"
                            , M2Utils.getTopologyId(economy_), shoppingList_,
                            currentQuote_.getQuoteValue(), seller.getDebugInfoNeverUseInCode());
            }
        } else {
            quote.setMoveCost(shoppingList_.getMoveCost());
        }

        // keep the minimum between quotes
        if ((quote.getQuoteValue() + quote.getMoveCost()) < this.getTotalBestQuote()) {
            logMessagesForAccept(seller, quote.getQuoteValues());
            bestQuote_ = quote;
            bestSeller_ = seller;
        }
        quoteTracker.trackQuote(quote);
        economy_.getPlacementStats().incrementQuoteMinimizerCount();
    }

    /**
     * Updates the internal state based on another {@link QuoteMinimizer} object.
     *
     * <p>
     *  This will update the values returned by {@link #getBestSeller()}, {@link #getBestQuote()}
     *  and {@link #getCurrentQuote()} to reflect the new minima.
     * </p>
     *
     * @param other The minimizer that should be used to update the internal state.
     */
    public void combine(@NonNull @ReadOnly QuoteMinimizer other) {
        if (other.getTotalBestQuote() < this.getTotalBestQuote()) {
            bestQuote_ = other.bestQuote_;
            bestSeller_ = other.bestSeller_;
        }

        // Test if the other minimizer has seen the current supplier.
        if (other.currentQuote_.getQuoteValue() != Double.POSITIVE_INFINITY) {
            currentQuote_ = other.currentQuote_;
        }

        quoteTracker.combine(other.quoteTracker);
    }

    /**
     * Logs messages if the logger's trace is enabled or the seller/buyer of shopping list
     * have their debug enabled.
     *
     * @param seller The trader who is selling all the commodities in the shoppingList
     * @param quote The quote given by the seller
     */
    private void logMessagesForAccept(Trader seller, double[] quote) {
        if (logger.isTraceEnabled() || seller.isDebugEnabled()
                        || shoppingList_.getBuyer().isDebugEnabled()) {
            logger.debug("topology id = {}, shoppingList = {}, oldBestQuote = {}, oldBestSeller = {}, "
                    + "newBestQuote = {}, newBestSeller = {}, currentQuote = {},"
                , M2Utils.getTopologyId(economy_), shoppingList_,
                bestQuote_.getQuoteValue(), bestSeller_, quote[0], seller, currentQuote_.getQuoteValue());
        }
    }
} // end QuoteMinimizer class
