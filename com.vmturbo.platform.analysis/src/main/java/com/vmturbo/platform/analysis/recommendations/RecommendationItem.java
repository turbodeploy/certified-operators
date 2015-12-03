package com.vmturbo.platform.analysis.recommendations;

import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.Trader;

/**
 * A set of fields describing the recommendations produced by the Economic Decisions Engine.
 *
 * <p>
 * For now it contains two strings, description and reason, and the economy trader references
 * for the buyer, the current supplier and the new supplier. This is only temporary and will change
 * in the near future.
 * </p>
 */
public class RecommendationItem {

    // Fields

    // buyer: the {@link Trader trader} for which the recommendation is intended
    private final Trader buyer_;
    // current supplier: The {@link Trader seller} currently hosting the {@link Trader buyer}
    private final Trader currentSupplier_;
    // new supplier: The {@link Trader seller} where the {@link Trader buyer} should move to
    private final Trader newSupplier_;
    // the market where the recommendation originated from. Used for 'reason' generation.
    private final Market market_;


    // Constructors

    /**
     *
     */
    public RecommendationItem(Trader buyer, Trader currentSupplier, Trader newSupplier, Market market) {
        buyer_ = buyer;
        currentSupplier_ = currentSupplier;
        newSupplier_ = newSupplier;
        market_  = market;
    }

    // Methods

    /**
     * Returns the buyer for a {@link RecommendationItem recommendation item}.
     */
    public Trader getBuyer() {
        return buyer_;
    }

    /**
     * Returns the current supplier for a {@link RecommendationItem recommendation item}.
     */
    public Trader getCurrentSupplier() {
        return currentSupplier_;
    }

    /**
     * Returns the new supplier for a {@link RecommendationItem recommendation item}.
     */
    public Trader getNewSupplier() {
        return newSupplier_;
    }

    /**
     * Returns the market where the {@link RecommendationItem recommendation item} originated from.
     */
    public Market getMarket() {
        return market_;
    }
}
