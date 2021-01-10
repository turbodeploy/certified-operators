package com.vmturbo.platform.analysis.utilities;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.ReconfigureConsumer;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.utilities.Quote.CommodityQuote;

/**
 * Contain the results of a round of placement.
 *
 * Contains both the {@link Action}s that result, as well as {@link QuoteTracker}s for {@link Trader}s
 * that could not be placed which can be used to generate explanations for why the {@link Trader}s
 * could not be placed.
 */
public class PlacementResults {
    /**
     * Actions that are the result of a round of placement.
     */
    private final List<Action> actions;

    /**
     * A map of traders that do not have a proper quote in placement to {@link QuoteTracker}s that
     * keep track of infinity quotes.
     *
     */
    private final Map<Trader, Collection<QuoteTracker>> infinityQuoteTraders;

    private final Map<Trader, List<InfiniteQuoteExplanation>> traderExplanations;

    /**
     * Create new, initially empty, placement results.
     */
    public PlacementResults() {
        this.actions = new ArrayList<>();
        this.infinityQuoteTraders = new HashMap<>();
        this.traderExplanations = new HashMap<>();
    }

    /**
     * Create new placement results with specific actions and unplaced traders.
     *
     * @param actions The actions that resulted from a round of placement.
     * @param infinityQuoteTraders {@link QuoteTracker}s for the traders that do not have a proper quote.
     * @param traderExplanations A map for trader with infinity quote and its explanations.
     */
    public PlacementResults(@Nonnull final List<Action> actions,
                            @Nonnull final Map<Trader, Collection<QuoteTracker>> infinityQuoteTraders,
                            @Nonnull final Map<Trader, List<InfiniteQuoteExplanation>> traderExplanations) {
        this.actions = Objects.requireNonNull(actions);
        this.infinityQuoteTraders = Objects.requireNonNull(infinityQuoteTraders);
        this.traderExplanations = Objects.requireNonNull(traderExplanations);
    }

    /**
     * Get the {@link Action}s that resulted from the round of placement.  This includes the top
     * level actions as well as any subsequent actions present in those actions. WARNING: when
     * using this call, all top level and subsequent actions are returned, but the subsequent
     * actions list still contains actions.  Users of this method should ignore the subsequent
     * actions list.
     *
     * @return The {@link Action}s that resulted from the round of placement.
     */
    public List<Action> getActions() {
        return actions.stream()
            .flatMap(action -> Stream.concat(Stream.of(action),
                action.getSubsequentActions().stream()))
            .collect(Collectors.toList());
    }

    /**
     * Get the {@link QuoteTracker}s for traders that does not have a proper quote. These
     * {@link QuoteTracker}s can be used to explain why the traders could not be placed on
     * a per-{@link ShoppingList} basis.
     *
     * @return The infinity quote traders for a round of placement together with associated
     * {@link QuoteTracker}s.
     */
    public Map<Trader, Collection<QuoteTracker>> getInfinityQuoteTraders() {
        return infinityQuoteTraders;
    }

    /**
     * Add an action to the {@link PlacementResults}.
     *
     * @param action The {@link Action} to add.
     */
    public void addAction(@Nonnull final Action action) {
        actions.add(Objects.requireNonNull(action));
    }

    /**
     * Add a {@link Collection} of actions to the {@link PlacementResults}.
     *
     * @param actions The {@link Action}s to add.
     */
    public void addActions(@Nonnull final Collection<Action> actions) {
        this.actions.addAll(actions);
    }

    /**
     * Add traders with no proper quotes and their associated {@link QuoteTracker}s to the
     * {@link PlacementResults}.
     *
     * @param buyingTrader The trader that does not have a proper quote.
     * @param quoteTrackers The {@link QuoteTracker}s, one for each of the {@link ShoppingList}s on
     *                      the trader that could not be placed.
     */
    public void addInfinityQuoteTraders(@Nonnull final Trader buyingTrader,
                                        @Nonnull final List<QuoteTracker> quoteTrackers) {
        if (quoteTrackers.isEmpty()) {
            return;
        }
        // Get existing quote trackers of the given buyer from infinityQuoteTraders,
        // keep those that associated with shopping lists different from those of the new trackers.
        Set<ShoppingList> newTrackerSls = quoteTrackers.stream()
                .map(t -> t.getShoppingList())
                .collect(Collectors.toSet());
        Collection<QuoteTracker> existingTrackers = this.infinityQuoteTraders.get(buyingTrader);
        if (existingTrackers != null) {
            Set<QuoteTracker> trackersToKeep = existingTrackers.stream()
                    .filter(q -> !newTrackerSls.contains(q.getShoppingList()))
                    .collect(Collectors.toSet());
            quoteTrackers.addAll(trackersToKeep);
        }
        this.infinityQuoteTraders.put(buyingTrader, quoteTrackers);
    }

    /**
     * Combine this group of {@link PlacementResults} with another.
     *
     * @param other The other group of {@link PlacementResults}.
     */
    public void combine(@Nonnull final PlacementResults other) {
        this.actions.addAll(other.actions);
        this.infinityQuoteTraders.putAll(other.infinityQuoteTraders);
    }

    /**
     * Get a reference to an empty {@link PlacementResults} object.
     *
     * @return An empty {@link PlacementResults} object.
     */
    public static PlacementResults empty() {
        return new PlacementResults();
    }

    /**
     * Generate placement results for a single action, with no unplaced traders.
     * Helpful when a method must return placement results for only a single action.
     *
     * @param action The action in the {@link PlacementResults}.
     * @return placement results for a single action, with no unplaced traders.
     */
    public static PlacementResults forSingleAction(@Nonnull final Action action) {
        return new PlacementResults(Collections.singletonList(
            Objects.requireNonNull(action)), Collections.emptyMap(), Collections.emptyMap());
    }

    /**
     * When there is a reconfigure action, it does not have {@link QuoteTracker} stored in the
     * {@link PlacementResults}. This method will try to create quoteTrackers for such traders so
     * that explanations can be generated for them.
     *
     * @param actions A list of actions.
     */

    public void createQuoteTrackerForReconfigures(@Nonnull final List<Action> actions) {
        Map<Trader, List<QuoteTracker>> quoteTrackersByTrader = new HashMap<>();
        Map<Trader, List<ReconfigureConsumer>> reconfiguresByTrader = actions.stream()
                .filter(a -> a instanceof ReconfigureConsumer)
                .map(a -> (ReconfigureConsumer)a)
                .collect(Collectors.groupingBy(Action::getActionTarget));
        for (Map.Entry<Trader, List<ReconfigureConsumer>> entry : reconfiguresByTrader.entrySet()) {
            List<QuoteTracker> quoteTrackers = new ArrayList<>();
            for (ReconfigureConsumer reconf : entry.getValue()) {
                final QuoteTracker quoteTracker = new QuoteTracker(reconf.getTarget());
                final CommodityQuote quote = new CommodityQuote(null);
                // UnavailableCommodities contains a set of commodities that satisfied by any seller.
                // Add all unavailable commodities to the same CommodityQuote of a given shopping list.
                reconf.getUnavailableCommodities().forEach(unsoldCommodity -> {
                    quote.addCostToQuote(Double.POSITIVE_INFINITY, 0, unsoldCommodity);
                    // this will adds the quote to the quoteTracker's infiniteQuotesToExplain
                    quoteTracker.trackQuote(quote);
                });
                quoteTrackers.add(quoteTracker);
            }
            if (!quoteTrackers.isEmpty()) {
                quoteTrackersByTrader.put(entry.getKey(), quoteTrackers);
            }
        }
        quoteTrackersByTrader.entrySet().forEach(e -> addInfinityQuoteTraders(e.getKey(), e.getValue()));
        return;
    }

    /**
     * Populate {@link InfiniteQuoteExplanation} for each trader. A trader can have multiple
     * {@link InfiniteQuoteExplanation}s, each corresponding with a shopping list that gets infinity
     * as best quote.
     *
     * @return A map of traders and their {@link InfiniteQuoteExplanation}.
     */
    public Map<Trader, List<InfiniteQuoteExplanation>> populateExplanationForInfinityQuoteTraders() {
        Map<Trader, List<InfiniteQuoteExplanation>> traderToExplanationMap = new HashMap<>();
        for (Map.Entry<Trader, Collection<QuoteTracker>> entry
                : this.getInfinityQuoteTraders().entrySet()) {
            List<InfiniteQuoteExplanation> explanationList = new ArrayList<>();
            for (QuoteTracker tracker : entry.getValue()) {
                InfiniteQuotesOfInterest interestingQuotes = new InfiniteQuotesOfInterest(tracker);
                // First try to find if we can explain by commodities.
                // If there are a number of commodities failed, try to use the seller that provides
                // most commodities with close quantity.
                List<Quote> closestSellerQuote = interestingQuotes.getIndividualCommodityQuotes()
                        .values().stream()
                        .filter(individual -> individual != null)
                        .map(q -> q.quote)
                        .filter(quote -> quote.getSeller() != null)
                        .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
                        .entrySet().stream()
                        .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toList());
                if (!closestSellerQuote.isEmpty()) {
                    Optional<InfiniteQuoteExplanation> exp = closestSellerQuote.get(0)
                            .getExplanation(tracker.getShoppingList());
                    if (exp.isPresent()) {
                        explanationList.add(exp.get());
                    }
                } else if (!interestingQuotes.getIndividualCommodityQuotes().isEmpty()) {
                    // There is no seller that can provide those commodities, which means the
                    // infinity quote comes from reconfigure actions.
                    List<Quote> reconfigureCommodityQuotes = interestingQuotes
                            .getIndividualCommodityQuotes()
                            .values().stream()
                            .filter(individual -> individual != null)
                            .map(q -> q.quote)
                            .filter(quote -> quote.getSeller() == null)
                            .collect(Collectors.toList());
                    if (!reconfigureCommodityQuotes.isEmpty()) {
                        Optional<InfiniteQuoteExplanation> exp = reconfigureCommodityQuotes.get(0)
                                .getExplanation(tracker.getShoppingList());
                        if (exp.isPresent()) {
                            explanationList.add(exp.get());
                        }
                    }
                }

                // The size of explanationList is still 0, which means infinity comes from non
                // commodity quotes, use the first non commodity quote as the explanation.
                if (explanationList.isEmpty()) {
                    Optional<InfiniteQuoteExplanation> nonCommExplanation = interestingQuotes
                            .getNonCommodityQuotes().stream()
                            .map(q -> q.getExplanation(tracker.getShoppingList()))
                            .filter(q -> q.isPresent())
                            .map(q -> q.get())
                            .findFirst();
                    if (nonCommExplanation.isPresent()) {
                        explanationList.add(nonCommExplanation.get());
                    }
                }
            }
            if (!explanationList.isEmpty()) {
                traderToExplanationMap.put(entry.getKey(), explanationList);
            }
        }
        traderToExplanationMap.entrySet().forEach(e -> {
            this.addExplanations(e.getKey(), e.getValue());
        });
        return traderToExplanationMap;

    }

    /**
     * Returns the map for trader with infinity quote and its explanations.
     *
     * @return A map for trader with infinity quote and its explanations.
     */
    public Map<Trader, List<InfiniteQuoteExplanation>> getExplanations() {
        return traderExplanations;
    }

    /**
     * Fill in contents for trader and the {@link InfiniteQuoteExplanation} of each shopping list
     * that gets infinity as best quote.
     *
     * @param trader the trader which gets infnity quote as best quote.
     * @param explanations a list of {@link InfiniteQuoteExplanation}.
     */
    public void addExplanations(Trader trader, List<InfiniteQuoteExplanation> explanations) {
        List<InfiniteQuoteExplanation> expList = traderExplanations.get(trader);
        if (expList == null || expList.isEmpty()) {
            traderExplanations.put(trader, explanations);
        } else {
            expList.addAll(explanations);
        }

    }
}
