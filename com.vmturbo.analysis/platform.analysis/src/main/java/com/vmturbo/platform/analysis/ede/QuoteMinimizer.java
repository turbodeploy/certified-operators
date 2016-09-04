package com.vmturbo.platform.analysis.ede;

import java.util.stream.Stream;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.UnmodifiableEconomy;

/**
 * A mutable collector class used to find the best quote and corresponding seller.
 *
 * <p>
 *  This is intended to be used with {@link Stream#collect(Supplier, BiConsumer, BiConsumer)}.
 * </p>
 */
final class QuoteMinimizer {
    // Auxiliary Fields
    private final @NonNull UnmodifiableEconomy economy_; // should contain all the seller arguments to #accept.
    private final @NonNull ShoppingList shoppingList_; // the shopping list for which to get a quote.

    // Accumulator Fields
    private @MonotonicNonNull Trader bestSeller_ = null; // will hold the best-so-far seller i.e.
                                                        // the one giving the minimum quote.
    private double bestQuote_ = Double.POSITIVE_INFINITY; // will hold the best-so-far quote. i.e. the minimum one.
    private double currentQuote_ = Double.POSITIVE_INFINITY; // may hold the quote from the current
                                                            // supplier if it's in the list of sellers.

    // Constructors

    /**
     * Constructs an empty QuoteMinimizer that can be used as an identity element for the reduction.
     *
     * @param economy See {@link #getEconomy()}.
     * @param shoppingList See {@link #getShoppingList()}
     */
    public QuoteMinimizer(@NonNull UnmodifiableEconomy economy, @NonNull ShoppingList shoppingList) {
        economy_ = economy;
        shoppingList_ = shoppingList;
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
     * or {@link Double#POSITIVE_INFINITY} if no sellers have been seen.
     */
    @Pure
    public double getBestQuote(@ReadOnly QuoteMinimizer this) {
        return bestQuote_;
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
     * {@link Double#POSITIVE_INFINITY} if the current supplier hasn't been seen.
     */
    @Pure
    public double getCurrentQuote(@ReadOnly QuoteMinimizer this) {
        return currentQuote_;
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
     * @param seller The seller from which to get a quote and update internal state.
     */
    public void accept(@NonNull Trader seller) {
        final double[] quote = EdeCommon.quote(economy_, shoppingList_, seller, bestQuote_, false);

        if (seller == shoppingList_.getSupplier()) {
            currentQuote_ = quote[0];
        }

        // keep the minimum between quotes
        if (quote[0] < bestQuote_) {
            bestQuote_ = quote[0];
            bestSeller_ = seller;
        }
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
        if (other.bestQuote_ < bestQuote_) {
            bestQuote_ = other.bestQuote_;
            bestSeller_ = other.bestSeller_;
        }

        // Test if the other minimizer has seen the current supplier.
        if (other.currentQuote_ != Double.POSITIVE_INFINITY) {
            currentQuote_ = other.currentQuote_;
        }
    }

} // end QuoteMinimizer class
