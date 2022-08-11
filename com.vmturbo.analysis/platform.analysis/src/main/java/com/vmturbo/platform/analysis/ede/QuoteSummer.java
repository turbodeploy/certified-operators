package com.vmturbo.platform.analysis.ede;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.UnmodifiableEconomy;

/**
 * A mutable collector class used to compute the total quote obtained for a number of
 * {@link ShoppingList}s assuming the best supplier for each one.
 *
 * <p>
 *  This is intended to be used with {@link Stream#collect(Supplier, BiConsumer, BiConsumer)}.
 * </p>
 */
final class QuoteSummer {
    // Auxiliary Fields
    private final @NonNull UnmodifiableEconomy economy_; // should contain all the shopping lists
                                                        // and markets that are passed to #accept.
    private final int clique_; // the k-partite clique for which to compute the total quote.

    // Accumulator Fields
    private final @NonNull List<@Nullable Trader> bestSellers_ = new ArrayList<>(); // will contain
                                // one seller for each (shopping list, market) pair passed to #accept.
    private double totalQuote_ = 0.0; // will accumulate the sum of all best quotes.

    // Cached data
    // Cached unmodifiable view of the bestSellers_ list.
    private final @NonNull List<@Nullable Trader> unmodifiableBestSellers_ = Collections.unmodifiableList(bestSellers_);

    // Constructors

    /**
     * Constructs an empty QuoteSummer that can be used as an identity element for the reduction.
     *
     * @param economy See {@link #getEconomy()}.
     * @param quality See {@link #getClique()}.
     */
    public QuoteSummer(@NonNull UnmodifiableEconomy economy, int quality) {
        economy_ = economy;
        clique_ = quality;
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
    public int getClique(@ReadOnly QuoteSummer this) {
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
        @NonNull List<@NonNull Trader> sellers = entry.getValue().getCliques().get(clique_);
        @NonNull Stream<@NonNull Trader> stream = sellers.size() < economy_.getSettings().getMinSellersForParallelism()
            ? sellers.stream() : sellers.parallelStream();
        @NonNull QuoteMinimizer minimizer = stream.collect(()->new QuoteMinimizer(economy_,entry.getKey()),
                                                           QuoteMinimizer::accept, QuoteMinimizer::combine);

        totalQuote_ += minimizer.getBestQuote();
        bestSellers_.add(minimizer.getBestSeller());
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
    }

} // end QuoteSummer class
