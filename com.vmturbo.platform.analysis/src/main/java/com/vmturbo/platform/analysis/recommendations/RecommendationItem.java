package com.vmturbo.platform.analysis.recommendations;

/**
 * A set of fields describing the recommendation.
 *
 * <p>
 * For now it only contains two strings, description and reason. In the future it will
 * contain information that will allow a module that is between the market and mediation to
 * translate the entities involved to human readable representations, and information to create
 * the recommendation explanation.
 * </p>
 */
public class RecommendationItem {

	// Fields

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
    public RecommendationItem(String description, String reason) {
        description_ = description;
        reason_ = reason;
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
}
