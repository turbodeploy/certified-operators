package com.vmturbo.platform.analysis.ede;

import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Pure;

import com.google.common.collect.Lists;
import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.UnmodifiableEconomy;
import com.vmturbo.platform.analysis.utilities.M2Utils;

/**
 * A mutable collector class used to find the best k-partite clique to position a number of
 * {@link ShoppingList}s and the best total quote offered in this clique.
 *
 * <p>
 *  This is intended to be used with {@link Stream#collect(Trader, BiConsumer, BiConsumer)}.
 * </p>
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
                                            // valid placement. i.e. the one with minimum total quote
    private double bestTotalQuote_ = Double.POSITIVE_INFINITY; // will host the best-so-far total
                                                              // quote. i.e. the minimum one.
    // Could also have included an accumulator for the best k-partite clique, but it wasn't strictly
    // needed.

    // Constructors

    /**
     * Constructs an empty CliqueMinimizer that can be used as an identity element for the reduction.
     *
     * @param economy See {@link #getEconomy()}.
     * @param entries See {@link #getEntries()}.
     */
    public CliqueMinimizer(@NonNull Economy economy, @NonNull @ReadOnly Collection
                           <@NonNull Entry<@NonNull ShoppingList, @NonNull Market>> entries) {
        economy_ = economy;
        entries_ = entries;
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
     *  Also {@link #getBestSellers()}.{@link Collection#size() size()} ==
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
            .collect(()->new QuoteSummer(economy_,clique), QuoteSummer::accept, QuoteSummer::combine);

        // keep the minimum between total quotes
        if (quoteSummer.getTotalQuote() < bestTotalQuote_) {
            logMessagesForAccept(clique, quoteSummer);
            bestTotalQuote_ = quoteSummer.getTotalQuote();
            bestSellers_ = quoteSummer.getBestSellers();
        }
        Lists.reverse(quoteSummer.getSimulatedActions()).forEach(Action::rollback);
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
