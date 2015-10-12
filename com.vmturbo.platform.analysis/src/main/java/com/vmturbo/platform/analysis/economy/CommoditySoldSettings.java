package com.vmturbo.platform.analysis.economy;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Pure;

/**
 * The settings associated with and controlling the behavior of a single {@link CommoditySold}.
 */
public interface CommoditySoldSettings {
    /**
     * Returns whether {@code this} commodity is resizable.
     */
    @Pure
    boolean isResizable();

    /**
     * Returns the upper bound for {@code this} commodity's capacity, in case of a resize.
     */
    @Pure
    double getCapacityUpperBound();

    /**
     * Returns the lower bound for {@code this} commodity's capacity, in case of a resize.
     */
    @Pure
    double getCapacityLowerBound();

    /**
     * Returns the minimum allowed change of {@code this} commodity's capacity, is case of a resize.
     *
     * <p>
     *  More specifically, the new capacity, in case of a resize, should be an integer multiple of
     *  the capacity increment.
     * </p>
     */
    @Pure
    double getCapacityIncrement();

    /**
     * Returns the utilization upper bound for {@code this} commodity.
     *
     * <p>
     *  The utilization for this commodity won't be allowed to exceed this value.
     *  It may be used in price calculations.
     * </p>
     */
    @Pure
    double getUtilizationUpperBound();

    // need to add the price function here once we know its type...

    /**
     * Sets the value of the <b>resizable</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param resizable the new value for the field.
     * @return {@code this}
     */
    @NonNull CommoditySoldSettings setResizable(boolean resizable);

    /**
     * Sets the value of the <b>capacity upper bound</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param capacityUpperBound the new value for the field. Must be >= getCapacityLowerBound().
     * @return {@code this}
     */
    @NonNull CommoditySoldSettings setCapacityUpperBound(double capacityUpperBound);

    /**
     * Sets the value of the <b>capacity lower bound</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param capacityLowerBound the new value for the field. Must be non-negative and <= getCapacityUpperBound().
     * @return {@code this}
     */
    @NonNull CommoditySoldSettings setCapacityLowerBound(double capacityLowerBound);

    /**
     * Sets the value of the <b>capacity increment</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param capacityIncrement the new value for the field. Must be non-negative
     * @return {@code this}
     */
    @NonNull CommoditySoldSettings setCapacityIncrement(double capacityIncrement);

    /**
     * Sets the value of the <b>utilization upper bound</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param utilizationUpperBound the new value for the field. Must be in the interval [0,1].
     * @return {@code this}
     */
    @NonNull CommoditySoldSettings setUtilizationUpperBound(double utilizationUpperBound);

} // end CommoditySoldSettings interface
