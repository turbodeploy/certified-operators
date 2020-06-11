package com.vmturbo.platform.analysis.utilities;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.google.common.base.Preconditions;

import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.utilities.Quote.CommodityQuote;

/**
 * A quote tracker tracks when shopping for a trader's shopping lists across various map-reduce operations
 * in {@link com.vmturbo.platform.analysis.ede.CliqueMinimizer},
 * {@link com.vmturbo.platform.analysis.ede.QuoteSummer}, and
 * {@link com.vmturbo.platform.analysis.ede.QuoteMinimizer}.
 *
 * A {@link QuoteTracker} accumulates infinite quotes as we shop for a {@link ShoppingList} during placement.
 * By scanning the infinite {@link Quote}s in the {@link QuoteTracker}, one can come up with a reasonable
 * explanation for why the associated {@link ShoppingList} could not be placed because {@link Quote}s
 * include information about why they received an infinite quote if they did so.
 *
 * A quote tracker has a "rank" which allows it to be compared to other quote trackers. All quotes in a quote
 * tracker should be of the same rank, allowing the rank of the quote tracker to match the rank of the quotes it
 * contains. {@link Quote}s and {@link QuoteTracker}s of a lower rank are more interesting than those of a
 * higher rank and should be preferred as sources of an explanation.
 */
public class QuoteTracker {
    private final ShoppingList shoppingList;
    private List<Quote> infiniteQuotesToExplain;
    private int quoteRank;

    /**
     * Create a new {@link QuoteTracker} to track {@link Quote}s for a {@link ShoppingList} when
     * we run placement for that {@link ShoppingList}.
     *
     * The {@link QuoteTracker} initially has an invalid rank indicating it is not tracking any quotes.
     *
     * @param shoppingList The shopping list whose quotes should be tracked.
     */
    public QuoteTracker(@Nonnull final ShoppingList shoppingList) {
        clear();
        this.shoppingList = Objects.requireNonNull(shoppingList);
    }

    /**
     * Check if this {@link QuoteTracker} is tracking any quotes that should be explained.
     *
     * A {@link QuoteTracker} will not have any quotes to explain if the {@link ShoppingList} whose
     * {@link Quote}s it is tracking was successfully placed or if we have not run placement
     * for that {@link ShoppingList} yet.
     *
     * @return Whether this {@link QuoteTracker} is tracking any quotes that should be explained.
     */
    public boolean hasQuotesToExplain() {
        return infiniteQuotesToExplain != null;
    }

    /**
     * Get the rank of the {@link QuoteTracker}, which matches the rank of all {@link Quote}s
     * being tracked internally.
     *
     * If the {@link QuoteTracker} has not been asked to track any {@link Quote}s, its rank will be
     * {@link Quote#INVALID_RANK}. If it has been asked to track ANY non-infinite {@link Quote}s,
     * its rank will be 0 (the lowest rank) which indicates the {@link ShoppingList} can be
     * successfully placed.
     *
     * A {@link QuoteTracker} with a lower rank has more explanatory power than one with a higher rank
     * and {@link Quote}s with a lower rank should be preferred to those with higher ranks.
     *
     * @return The rank of the quotes being tracked by the {@link QuoteTracker}.
     *         {@link Quote#INVALID_RANK} if the {@link QuoteTracker} has not been asked to track any quotes.
     *         Returns 0 if the {@link ShoppingList} can be successfully placed.
     */
    public int getQuoteRank() {
        return quoteRank;
    }

    /**
     * Return whether this {@link QuoteTracker} is tracking a {@link Quote} that was successfully placed.
     *
     * @return whether this {@link QuoteTracker} is tracking a {@link Quote} that was successfully placed.
     */
    public boolean successfullyPlaced() {
        return quoteRank == 0;
    }

    /**
     * Whether this {@link QuoteTracker} has valid state. A {@link QuoteTracker} is initialized to invalid
     * state. Its state becomes valid when it has tracked one or more quotes.
     *
     * @return Whether the state of this {@link QuoteTracker} is valid.
     */
    public boolean isValid() {
        return quoteRank <= Quote.MAX_RANK;
    }

    /**
     * Get the {@link ShoppingList} whose quotes are being tracked by this {@link QuoteTracker}.
     *
     * @return the {@link ShoppingList} whose quotes are being tracked by this {@link QuoteTracker}.
     */
    @Nonnull
    public ShoppingList getShoppingList() {
        return shoppingList;
    }

    /**
     * Get a list of the infinite {@link Quote}s tracked by this {@link QuoteTracker}.
     *
     * If no infinite {@link Quote}s are being tracked, returns an empty {@link List}.
     *
     * An infinite {@link Quote} is defined as a {@link Quote} whose {@link Quote#getQuoteValue()}
     * returns {@link Double#POSITIVE_INFINITY}.
     *
     * @return a list of the infinite {@link Quote}s tracked by this {@link QuoteTracker}.
     */
    @Nonnull
    public List<Quote> getInfiniteQuotesToExplain() {
        return infiniteQuotesToExplain == null ? Collections.emptyList() : infiniteQuotesToExplain;
    }

    /**
     * Track a {@link Quote} using this {@link QuoteTracker}. If the {@link QuoteTracker} is currently
     * invalid, it will become valid after tracking the quote.
     *
     * If the {@link Quote} is finite, we will note that the {@link QuoteTracker} is now tracking a
     * {@link ShoppingList} that can be successfully placed.
     *
     * If the value on the {@link Quote} is infinite, we will determine what to do based on its rank.
     *
     * If the rank on the {@link Quote} is lower than this {@link QuoteTracker}s, we will drop all
     * infinite {@link Quote}s currently being tracked and start tracking {@link Quote}s of the
     * new rank.
     *
     * If the rank on the {@link Quote} is higher than this {@link QuoteTracker}s, it is not of
     * interest because we are already tracking lower rank {@link Quote}s and it will not be tracked.
     *
     * If the rank on the {@link Quote} is the same as this {@link QuoteTracker}s, it is of equal
     * interest to the {@link Quote}s currently being tracked and we will add it to the list of
     * infinite {@link Quote}s we are tracking.
     *
     * @param quote The {@link Quote} to track.
     */
    public void trackQuote(@Nonnull final Quote quote) {
        if (quote.isFinite()) {
            markPlaced();
        } else {
            if (quote.getRank() == quoteRank && hasQuotesToExplain()) {
                // This quote has exactly as many insufficient commodities as the least we have seen.
                // Add it to the list of quotes we may want to explain later.
                infiniteQuotesToExplain.add(quote);
            } else if (quote.getRank() < quoteRank) {
                // This quote has fewer insufficient commodities than some others than any other we have
                // seen. Those other quotes are no longer worth explaining, but this one is.
                if (infiniteQuotesToExplain == null) {
                    infiniteQuotesToExplain = new ArrayList<>();
                }

                infiniteQuotesToExplain.clear();
                infiniteQuotesToExplain.add(quote);
                quoteRank = quote.getRank();
            } else {
                // This quote has more insufficient commodities than some others so it
                // is not worth explaining.
            }
        }
    }

    /**
     * Combine the quotes of this {@link QuoteTracker} with those of another. Mutates the
     * original {@link QuoteTracker} to contain the combined information. Quotes are
     * combined according to the following rules which are evaluated in order:
     *
     * If this quote tracker is successfully placed, remain so no matter the contents of the other.
     * If the other quote tracker is successfully placed, this quote tracker should become so.
     * If this quote tracker is not valid (is not tracking anything yet), take the state of the other.
     * If the other quote tracker is not valid, retain current state.
     *
     * If the none of the above conditions are met, both quote trackers are valid and unplaced
     * and we must use ranks to decide what to do with the quote trackers.
     * If this and the other tracker are of equal rank, add the quotes from the other tracker to this one's.
     * If the other tracker has a lower rank, drop this one's and adopt the other's, also taking its rank.
     * If the other tracker has a higher rank, retain this one's.
     *
     * @param other The other quote tracker whose state should be combined with the state of this
     *              quote tracker. Note that the shoppingList of the other quote tracker MUST match
     *              the shopping list on this quote tracker. Combining quote trackers for different
     *              shopping lists makes no sense because they are tracking different things.
     */
    public void combine(@Nonnull final QuoteTracker other) {
        Preconditions.checkArgument(shoppingList.equals(other.shoppingList),
            "Attempt to combine quote trackers with different shopping lists: {} vs {}",
            shoppingList, other.shoppingList);

        /*
         * First check placement status and validity of the trackers being combined.
         */
        if (successfullyPlaced()) {
            // Keep what is here because placement was successful.
            return;
        } else if (other.successfullyPlaced()) {
            markPlaced(); // Take the state from the other which has no quotes to explain.
            return;
        } else if (!isValid()) {
            // This quote tracker is invalid. Copy state from the other quote tracker.
            quoteRank = other.quoteRank;
            infiniteQuotesToExplain = other.infiniteQuotesToExplain;
            return;
        } else if (!other.isValid()) {
            // The other tracker is invalid. It has no state to merge here.
            return;
        }

        /*
         * Both quote trackers are tracking unplaced shopping lists. Use their ranks to determine
         * how to combine their state.
         */
        if (quoteRank == other.quoteRank) {
            // Ranks are equal. Quotes in both are equally important, so keep both our quotes
            // and the one from the infinite quotes.
            infiniteQuotesToExplain.addAll(other.infiniteQuotesToExplain);
        } else if (other.quoteRank < quoteRank) {
            // The other tracker has a lower rank. It's quotes take priority over the ones here.
            infiniteQuotesToExplain = other.infiniteQuotesToExplain;
        } else {
            // This tracker has quotes with lower insufficient commodity counts than the other.
            // Keep what is here, and drop the ones from the other.
        }
    }

    /**
     * Generate an explanation string for ALL infinite {@link Quote}s being tracked
     * (as opposed to only the "interesting" sellers).
     *
     * @return An explanation string for ALL infinite {@link Quote}s.
     */
    public String explainAllInfiniteQuotes() {
        // Only use shopping list IDs because an outer print statement will likely print the
        // buying trader ID.
        if (!hasQuotesToExplain()) {
            return shoppingListId() + ": Successfully placed";
        }
        return shoppingListId() + ": " + infiniteQuotesToExplain.stream()
            .map(quote -> quote.getExplanation(shoppingList))
            .collect(Collectors.joining(", ", "[", "]"));
    }

    /**
     * Generate an explanation string for sellers of interest. A seller of interest is the seller
     * that comes closest to meeting the requested quantities of a given shopping list.
     *
     * @return A string explaining the most interest sellers for a shopping list that could not be placed.
     */
    public String explainInterestingSellers() {
        if (!hasQuotesToExplain()) {
            return shoppingListId() + ": Successfully placed";
        }

        return shoppingListId() + ": " + new InfiniteQuotesOfInterest(this).explanation();
    }

    /**
     * Mark this {@link QuoteTracker} as tracking a {@link ShoppingList} that was successfully placed.
     */
    private void markPlaced() {
        quoteRank = 0;
        infiniteQuotesToExplain = null;
    }

    /**
     * Clear the quote tracker to return it to its original state.
     * Drops all infinite quotes and sets the rank back to {@link Quote#INVALID_RANK}.
     */
    private void clear() {
        quoteRank = Quote.INVALID_RANK;
        infiniteQuotesToExplain = null;
    }

    /**
     * Get a String for the ID of the {@link ShoppingList} being tracked.
     *
     * @return a String for the ID of the {@link ShoppingList} being tracked.
     */
    private String shoppingListId() {
        return "SL_" + shoppingList.getShoppingListId();
    }

    /**
     * Returns the mapping for commodity specification to IndividualCommodityQuote. The
     * IndividualCommodityQuote contains the closest seller and the max quantity available.
     *
     * @return commodity specification to IndividualCommodityQuote mapping.
     */
    public Map<CommoditySpecification, IndividualCommodityQuote> getIndividualCommodityQuotes() {
        InfiniteQuotesOfInterest quoteOfInterest = new InfiniteQuotesOfInterest(this);
        return quoteOfInterest.individualCommodityQuotes;
    }


    /**
     * Returns the list of non commodity quotes.
     *
     * @return a list of non commodity quotes.
     */
    public List<Quote> getNonCommodityQuotes() {
        InfiniteQuotesOfInterest quoteOfInterest = new InfiniteQuotesOfInterest(this);
        return quoteOfInterest.nonCommodityQuotes;
    }


    /**
     * {@link InfiniteQuotesOfInterest}, given a collection of {@link Quote}s for a {@link ShoppingList},
     * determines which of those {@link Quote}s is most relevant and can be used to explain those
     * {@link Quote}s.
     *
     * Will attempt to explain ALL non-{@link CommodityQuote}s.
     * For {@link CommodityQuote}s, will find the most relevant {@link Quote} for
     * each {@link CommoditySpecification} found to be insufficient in any of the quotes.
     *
     * For example, if the shopping list wants to buy 1000 units of Commodity "Foo",
     * and it got two quotes, one with availableQuantity=500, and one with availableQuantity=600,
     * the one with availableQuantity=600 is more relevant because it is closer to the
     * requested amount, so only explain that one but not the 500 one because it is not interesting
     * in light of the 600 quote.
     */
    private static class InfiniteQuotesOfInterest {
        private final List<Quote> nonCommodityQuotes;
        private final ShoppingList shoppingList;
        private final Map<CommoditySpecification, IndividualCommodityQuote> individualCommodityQuotes;

        public InfiniteQuotesOfInterest(@Nonnull final QuoteTracker quoteTracker) {
            nonCommodityQuotes = new ArrayList<>();
            shoppingList = quoteTracker.getShoppingList();
            individualCommodityQuotes = new HashMap<>();

            quoteTracker.getInfiniteQuotesToExplain().stream()
                .forEach(quote -> {
                if (quote instanceof Quote.CommodityQuote) {
                    // For each individual commodity, the quote of interest is the one with the most available
                    // quantity.
                    final CommodityQuote cq = (CommodityQuote) quote;
                    cq.getInsufficientCommodities().forEach(insufficientCommodity -> {
                        final CommoditySpecification commodity = insufficientCommodity.commodity;
                        final IndividualCommodityQuote interestingQuote = individualCommodityQuotes.get(commodity);
                        if (interestingQuote == null) {
                            individualCommodityQuotes.put(commodity, new IndividualCommodityQuote(quote,
                                insufficientCommodity.availableQuantity));
                        }
                        else if (interestingQuote.availableQuantity < insufficientCommodity.availableQuantity) {
                            // Replace the seller of interest for this commodity
                            individualCommodityQuotes.put(commodity, new IndividualCommodityQuote(
                                quote, insufficientCommodity.availableQuantity));
                        }
                    });
                } else {
                    // Explain all non-commodity quotes.
                    nonCommodityQuotes.add(quote);
                }
            });
        }

        /**
         * Generate an explanation string for all quotes of interest.
         * The order of the quotes in the explanation is not guaranteed.
         *
         * @return A String explaining the quotes of interest.
         */
        public String explanation() {
            return Stream.concat(nonCommodityQuotes.stream(),
                individualCommodityQuotes.values().stream()
                    .map(commodityQuote -> commodityQuote.quote))
                .map(quote -> quote.getExplanation(shoppingList))
                .sorted()
                .collect(Collectors.joining(", and ", "[", "]"));
        }
    }

    /**
     * A small helper class that pairs a quote with an available quantity for an individual commodity.
     * Used by {@link InfiniteQuotesOfInterest}.
     */
    @Immutable
    public static class IndividualCommodityQuote {
        public final Quote quote;
        public final double availableQuantity;

        public IndividualCommodityQuote(@Nullable final Quote quote,
                                        final double availableQuantity) {
            this.quote = Objects.requireNonNull(quote);
            this.availableQuantity = availableQuantity;
        }
    }
}
