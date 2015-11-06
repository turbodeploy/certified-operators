package com.vmturbo.platform.analysis.recommendations;

/**
 * A set of fields describing the recommendation.
 *
 * <p>
 * For now it contains two strings, description and reason, and the economy trader indexes 
 * for the buyer, the current supplier and the new supplier.
 * </p>
 */
public class RecommendationItem {

	// Fields

    // buyer
    int buyer_;
    // current supplier
    int currentSupplier_;
    // new supplier
    int newSupplier_;
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
    public RecommendationItem(String description, String reason, int buyerIndex,
                    int currentSupplierIndex, int newSupplierIndex) {
        description_ = description;
        reason_ = reason;
        buyer_ = buyerIndex;
        currentSupplier_ = currentSupplierIndex;
        newSupplier_ = newSupplierIndex;
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
     * Returns the buyer index for a {@link RecommendationItem recommendation item}.
     */
    public int getBuyer() {
        return buyer_;
    }

    /**
     * Returns the current supplier index for a {@link RecommendationItem recommendation item}.
     */
    public int getCurrentSupplier() {
        return currentSupplier_;
    }

    /**
     * Returns the new supplier index for a {@link RecommendationItem recommendation item}.
     */
    public int getNewSupplier() {
        return newSupplier_;
    }
}
