package com.vmturbo.platform.analysis.recommendations;

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
    // description of the recommendation
    private String description_ = null;
    // reason for the recommendation
    private String reason_ = null;

    // Constructors

    /**
     *
     * @param description - String describing the recommendation
     * @param reason - String describing the reason for the recommendation
     */
    public RecommendationItem(String description, String reason, Trader buyer,
                    Trader currentSupplier, Trader newSupplier) {
        description_ = description;
        reason_ = reason;
        buyer_ = buyer;
        currentSupplier_ = currentSupplier;
        newSupplier_ = newSupplier;
    }

    // Methods

    /**
     * Returns the description for a {@link RecommendationItem recommendation item}.
     */
    public String getDescription() {
        return description_;
    }

    /**
     * Returns the reason for a {@link RecommendationItem recommendation item}.
     */
    public String getReason() {
        return reason_;
    }

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
}
