package com.vmturbo.platform.analysis.economy;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.dataflow.qual.Deterministic;
import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.actions.Move;

/**
 * The settings associated with and parameterizing the behavior of a single {@link Economy}.
 */
public final class EconomySettings implements Serializable {
    // Fields

    /**
     * The value returned by {@link #getMinSellersForParallelism()} when called on a newly
     * constructed instance.
     */
    // '256' was selected as the default value after finding the time-as-a-function-of-size curves
    // for parallel and sequential execution and finding their intersection.
    public static final int DEFAULT_MIN_SELLERS_FOR_PARALLELISM = 256;

    // the maximum number of placements to be 10, when reaching this limit, we force stop
    // the placements. 10 was chosen from experience of running M2 at various customers.
    // This value can be overridden.
    public static final int DEFAULT_MAX_PLACEMENT_ITERATIONS = 10;

    /**
     * The value returned by {@link #getUseQuoteCacheDuringSNM()} when called on a newly
     * constructed instance.
     */
    // The default is false because we have found more topologies that are negatively impacted by
    // enabling the quote cache than topologies that are positively impacted.
    public static final boolean DEFAULT_USE_QUOTE_CACHE_DURING_SNM = false;

    /**
     * The value returned by {@link #getQuoteFactor()} when called on a newly constructed instance.
     */
    // 0.75 corresponding to a 25% improvement seemed reasonable but there is no particular reason
    // not to use another value.
    public static final double DEFAULT_QUOTE_FACTOR = 0.75;

    private int minSellersForParallelism_ = DEFAULT_MIN_SELLERS_FOR_PARALLELISM;
    private double quoteFactor_ = DEFAULT_QUOTE_FACTOR;

    private double rightSizeLower_;
    private double rightSizeUpper_;
    private boolean useExpenseMetricForTermination_;
    private float expenseMetricFactor_;
    private float defaultRateOfResize = 1.0f;
    private boolean isEstimatesEnabled_ = true;
    private boolean isResizeDependentCommodities_ = true;
    private int maxPlacementIterations_ = DEFAULT_MAX_PLACEMENT_ITERATIONS;
    private boolean useQuoteCacheDuringSNM_ = DEFAULT_USE_QUOTE_CACHE_DURING_SNM;
    private boolean sortShoppingLists_ = false;
    private float discountedComputeCostFactor = -1f;

    /**
     * Override the default rate of resize on a per-trader-type basis.
     * Key: Trader type
     * Value: Rate of resize for that type of trader
     */
    private final Map<Integer, Float> overrideRateOfResize = new HashMap<>();

    // Constructors
    /**
     * Constructs a new default-initialized EconomySettings instance.
     */
    public EconomySettings() {/* empty body */}

    // Getters

    /**
     * Returns minimum number of sellers in a market required for parallel computation of minimum
     * quote to kick-in.
     *
     * <p>
     *  This is used because the overhead of running the computation in multiple threads can exceed
     *  the gains when the number of sellers is low.
     * </p>
     */
    @Pure
    public int getMinSellersForParallelism(@ReadOnly EconomySettings this) {
        return minSellersForParallelism_;
    }

    /**
     * Returns the <b>quote factor</b>.
     *
     * <p>
     *  The quote factor is a number (normally between 0 and 1), so that {@link Move} actions are
     *  only generated if best-quote < quote-factor * current-quote. That means that if we only want
     *  {@link Move}s that result in at least 1% improvement we should use a quote-factor of 0.99.
     * </p>
     */
    @Pure
    public double getQuoteFactor(@ReadOnly EconomySettings this) {
        return quoteFactor_;
    }

    // Setters

    /**
     * Sets the value of the <b>min sellers for parallelism</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param minSellersForParallelism the new value for the field. Must be non-negative.
     * @return {@code this}
     *
     * @see #getMinSellersForParallelism()
     */
    @Deterministic
    public EconomySettings setMinSellersForParallelism(int minSellersForParallelism) {
        checkArgument(minSellersForParallelism >= 0, "minSellersForParallelism = " + minSellersForParallelism);
        minSellersForParallelism_ = minSellersForParallelism;
        return this;
    }

    /**
     * Sets the value of the <b>quote factor</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param quoteFactor the new value for the field. Must be non-negative.
     * @return {@code this}
     *
     * @see #getQuoteFactor()
     */
    @Deterministic
    public EconomySettings setQuoteFactor(double quoteFactor) {
        checkArgument(quoteFactor >= 0, "quoteFactor = " + quoteFactor);
        quoteFactor_ = quoteFactor;
        return this;
    }

    @Pure
    public double getRightSizeLower(@ReadOnly EconomySettings this) {
        return rightSizeLower_;
    }

    @Pure
    public double getRightSizeUpper(@ReadOnly EconomySettings this) {
        return rightSizeUpper_;
    }

    @Pure
    public boolean isUseExpenseMetricForTermination(@ReadOnly EconomySettings this) {
        return useExpenseMetricForTermination_;
    }

    @Pure
    public float getExpenseMetricFactor(@ReadOnly EconomySettings this) {
        return expenseMetricFactor_;
    }

    @Pure
    public float getRateOfResize(@ReadOnly EconomySettings this, final int traderType) {
        return overrideRateOfResize.getOrDefault(traderType, defaultRateOfResize);
    }

    @Pure
    public float getDefaultRateOfResize(@ReadOnly EconomySettings this) {
        return defaultRateOfResize;
    }

    @Deterministic
    public EconomySettings setRightSizeLower(double rightSizeLower) {
        checkArgument(rightSizeLower >= 0, "rightSizeLower = " + rightSizeLower);
        rightSizeLower_ = rightSizeLower;
        return this;
    }

    @Pure
    public boolean isEstimatesEnabled(@ReadOnly EconomySettings this) {
        return isEstimatesEnabled_;
    }

    @Deterministic
    public EconomySettings setEstimatesEnabled(boolean isEstimatesEnabled) {
        isEstimatesEnabled_ = isEstimatesEnabled;
        return this;
    }

    @Pure
    public boolean getSortShoppingLists(@ReadOnly EconomySettings this) {
        return sortShoppingLists_;
    }

    @Deterministic
    public EconomySettings setSortShoppingLists(boolean sortShoppingLists) {
        sortShoppingLists_ = sortShoppingLists;
        return this;
    }

    @Deterministic
    public EconomySettings setRightSizeUpper(double rightSizeUpper) {
        checkArgument(rightSizeUpper >= 0, "rightSizeUpper = " + rightSizeUpper);
        rightSizeUpper_ = rightSizeUpper;
        return this;
    }

    @Deterministic
    public EconomySettings setUseExpenseMetricForTermination(
                              boolean useExpenseMetricForTermination) {
        useExpenseMetricForTermination_ = useExpenseMetricForTermination;
        return this;
    }

    @Deterministic
    public EconomySettings setExpenseMetricFactor(float expenseMetricFactor) {
        expenseMetricFactor_ = expenseMetricFactor;
        return this;
    }

    @Deterministic
    public EconomySettings setDefaultRateOfResize(float rateOfRightSize) {
        checkArgument(rateOfRightSize > 0, "rateOfRightSize = " + rateOfRightSize);
        defaultRateOfResize = rateOfRightSize;
        return this;
    }

    @Deterministic
    public EconomySettings setRateOfResizeForTraderType(int traderType, float rateOfRightSize) {
        checkArgument(rateOfRightSize > 0, "rateOfRightSize = " + rateOfRightSize);
        overrideRateOfResize.put(traderType, rateOfRightSize);
        return this;
    }

    @Pure
    public boolean isResizeDependentCommodities(@ReadOnly EconomySettings this) {
        return isResizeDependentCommodities_;
    }

    @Deterministic
    public EconomySettings setResizeDependentCommodities(boolean isResizeDependentCommodities) {
        isResizeDependentCommodities_ = isResizeDependentCommodities;
        return this;
    }

    /**
     * Get the maximum number of rounds of placements that are allowed before stopping
     * the placement phase.
     *
     * @return the maximum number of rounds of placements that are allowed before stopping
     *         the placement phase.
     */
    @Pure
    public int getMaxPlacementIterations(@ReadOnly EconomySettings this) {
        return maxPlacementIterations_;
    }

    /**
     * Set the maximum number of rounds of placements that are allowed before stopping
     * the placement phase.
     *
     * @return {@code this}
     */
    @Deterministic
    public EconomySettings setMaxPlacementIterations(final int maxPlacementIterations) {
        if (maxPlacementIterations > 0) {
            maxPlacementIterations_ = maxPlacementIterations;
        }
        return this;
    }

    /**
     * Returns whether quotes should be cached for reuse during SNM-enabled placement analysis.
     *
     * <p>Setting to true can improve performance in some cases. Usually those cases involve a high
     * number of biclique overlaps and volumes per VM.</p>
     *
     * @see #setUseQuoteCacheDuringSNM(boolean)
     */
    @Pure
    public boolean getUseQuoteCacheDuringSNM(@ReadOnly EconomySettings this) {
        return useQuoteCacheDuringSNM_;
    }

    /**
     * Sets the value of the <b>use quote cache during SNM</b> field.
     *
     * <p>Has no observable side-effects except setting the above field.</p>
     *
     * @param useQuoteCacheDuringSNM the new value for the field.
     * @return {@code this}
     *
     * @see #getUseQuoteCacheDuringSNM()
     */
    @Deterministic
    public EconomySettings setUseQuoteCacheDuringSNM(boolean useQuoteCacheDuringSNM) {
        useQuoteCacheDuringSNM_ = useQuoteCacheDuringSNM;
        return this;
    }

    public float getDiscountedComputeCostFactor() {
        return discountedComputeCostFactor;
    }

    public void setDiscountedComputeCostFactor(float discountedComputeCostFactor) {
        checkArgument(discountedComputeCostFactor >= 0,
            "discountedComputeCostFactor = " + discountedComputeCostFactor);
        this.discountedComputeCostFactor = discountedComputeCostFactor;
    }

    public boolean hasDiscountedComputeCostFactor() {
       return discountedComputeCostFactor != -1;
    }
    // Methods

    /**
     * Resets {@code this} {@link EconomySettings} instance to the state it was in just after
     * construction.
     *
     * <p>It has no other observable side-effects.</p>
     */
    public void clear() {
        minSellersForParallelism_ = DEFAULT_MIN_SELLERS_FOR_PARALLELISM;
        quoteFactor_ = DEFAULT_QUOTE_FACTOR;
        maxPlacementIterations_ = DEFAULT_MAX_PLACEMENT_ITERATIONS;
        useQuoteCacheDuringSNM_ = DEFAULT_USE_QUOTE_CACHE_DURING_SNM;
    }
} // end EconomySettings class
