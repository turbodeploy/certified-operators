package com.vmturbo.platform.analysis.utilities;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.InvertedIndex.ActiveSellerLookup;
import com.vmturbo.platform.analysis.economy.Market;
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
     * A map of traders that could not be placed to {@link QuoteTracker}s that explain why the trader
     * could not be placed.
     *
     * A trader that has multiple shopping lists that cannot be placed has one {@link QuoteTracker} for
     * each {@link ShoppingList} that could not be placed.
     */
    private final Map<Trader, Collection<QuoteTracker>> unplacedTraders;

    /**
     * Create new, initially empty, placement results.
     */
    public PlacementResults() {
        this.actions = new ArrayList<>();
        this.unplacedTraders = new HashMap<>();
    }

    /**
     * Create new placement results with specific actions and unplaced traders.
     *
     * @param actions The actions that resulted from a round of placement.
     * @param unplacedTraders {@link QuoteTracker}s for the traders that could not be placed.
     */
    public PlacementResults(@Nonnull final List<Action> actions,
                            @Nonnull final Map<Trader, Collection<QuoteTracker>> unplacedTraders) {
        this.actions = Objects.requireNonNull(actions);
        this.unplacedTraders = Objects.requireNonNull(unplacedTraders);
    }

    /**
     * Get the {@link Action}s that resulted from the round of placement.
     *
     * @return The {@link Action}s that resulted from the round of placement.
     */
    public List<Action> getActions() {
        return actions;
    }

    /**
     * Get the {@link QuoteTracker}s for traders that could not be placed. These {@link QuoteTracker}s
     * can be used to explain why the traders could not be placed on a per-{@link ShoppingList} basis.
     *
     * @return The unplaced traders for a round of placement together with associated {@link QuoteTracker}s.
     */
    public Map<Trader, Collection<QuoteTracker>> getUnplacedTraders() {
        return unplacedTraders;
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
     * Add unplaced traders and their associated {@link QuoteTracker}s to the {@link PlacementResults}.
     *
     * @param buyingTrader The trader that could not be placed.
     * @param quoteTrackers The {@link QuoteTracker}s, one for each of the {@link ShoppingList}s on
     *                      the trader that could not be placed.
     */
    public void addUnplacedTraders(@Nonnull final Trader buyingTrader,
                                   @Nonnull final Collection<QuoteTracker> quoteTrackers) {
        this.unplacedTraders.put(buyingTrader, quoteTrackers);
    }

    /**
     * Combine this group of {@link PlacementResults} with another.
     *
     * @param other The other group of {@link PlacementResults}.
     */
    public void combine(@Nonnull final PlacementResults other) {
        this.actions.addAll(other.actions);
        this.unplacedTraders.putAll(other.unplacedTraders);
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
            Objects.requireNonNull(action)), Collections.emptyMap());
    }

    /**
     * Add unplaced trader results for markets in the economy that have no suppliers.
     * {@link ShoppingList}s in the economy that are attempting to buy in {@link Market}s that
     * have no sellers will be unplaced, but we will not have {@link QuoteTracker}s for them
     * because we never even attempt placement on {@link Market}s with no sellers.
     *
     * This is an attempt to fill in {@link QuoteTracker}s for these {@link Trader}s whose
     * {@link ShoppingList}s were unplaced and we did not shop for.
     *
     * @param trader The trader whose {@link ShoppingList}s may be unplaced.
     * @param shoppingLists The {@link ShoppingList}s that were unplaced due to being in a market
     *                      with no sellers.
     * @param economy The {@link Economy} being analyzed.
     * @param activeSellerLookup A {@link ActiveSellerLookup} that can be used to find active sellers
     *                           for a given commodity.
     */
    public void addResultsForMarketsWithNoSuppliers(@Nonnull final Trader trader,
                                                    @Nonnull final Collection<ShoppingList> shoppingLists,
                                                    @Nonnull final Economy economy,
                                                    @Nonnull final ActiveSellerLookup activeSellerLookup) {
        final List<QuoteTracker> trackers = shoppingLists.stream()
            .map(sl -> quoteTrackerForMarketWithNoSupplier(sl, economy.getMarket(sl), activeSellerLookup))
            .collect(Collectors.toList());

        addUnplacedTraders(trader, trackers);
    }

    /**
     * Add {@link QuoteTracker}s for traders that cannot be placed because they are in markets with
     * no suppliers.
     *
     * @param shoppingList The {@link ShoppingList} that could not be placed.
     * @param market The {@link Market} the {@link ShoppingList} is shopping in.
     * @param activeSellerLookup A {@link ActiveSellerLookup} that can be used to find active sellers
     *                           for a given commodity.
     * @return A {@link QuoteTracker} that can be used to explain why the {@link ShoppingList}
     *         could not be placed.
     */
    private QuoteTracker quoteTrackerForMarketWithNoSupplier(@Nonnull final ShoppingList shoppingList,
                                                             @Nonnull final Market market,
                                                             @Nonnull final ActiveSellerLookup activeSellerLookup) {
        // Find the commodity or commodities in the market basket that no seller is selling.
        final List<CommoditySpecification> unsoldCommodities = new ArrayList<>();
        for (CommoditySpecification commSpec : market.getBasket()) {
            if (!activeSellerLookup.hasActiveSellers(commSpec)) {
                unsoldCommodities.add(commSpec);
            }
        }

        // TODO: This only handles the case when a market has no sellers because no one is selling some
        // individual commodity X. If the market has no sellers due to a specific combination of goods
        // no one is selling (ie a shopper wants to buy X and Y, some trader is selling X but not Y,
        // and another trader is selling Y but not X, that will NOT be captured here. Hopefully the
        // more common case is that no trader is selling the commodity.
        final QuoteTracker quoteTracker = new QuoteTracker(shoppingList);
        unsoldCommodities.forEach(unsoldCommodity -> {
            final CommodityQuote quote = new CommodityQuote(null);
            quote.addCostToQuote(Double.POSITIVE_INFINITY, 0, unsoldCommodity);
            quoteTracker.trackQuote(quote);
        });

        return quoteTracker;
    }
}
