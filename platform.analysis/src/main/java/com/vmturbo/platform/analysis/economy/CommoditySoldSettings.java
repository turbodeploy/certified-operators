package com.vmturbo.platform.analysis.economy;

import org.checkerframework.checker.javari.qual.PolyRead;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.pricefunction.PriceFunction;

/**
 * The settings associated with and controlling the behavior of a single {@link CommoditySold}.
 */
public interface CommoditySoldSettings {
    /**
     * Returns whether {@code this} commodity is resizable.
     *
     * <p>
     *  When 'resize' recommendations are generated, only 'resizable' commodities are considered.
     *  A 'resize' recommendation essentially recommends changing the commodity's capacity.
     * </p>
     */
    @Pure
    boolean isResizable(@ReadOnly CommoditySoldSettings this);

    /**
     * Returns the upper bound for {@code this} commodity's capacity, in case of a resize.
     *
     * <p>
     *  No recommendation will recommend a value for capacity higher than this bound.
     * </p>
     */
    @Pure
    double getCapacityUpperBound(@ReadOnly CommoditySoldSettings this);

    /**
     * Returns the lower bound for {@code this} commodity's capacity, in case of a resize.
     *
     * <p>
     *  No recommendation will recommend a value for capacity lower than this bound.
     * </p>
     */
    @Pure
    double getCapacityLowerBound(@ReadOnly CommoditySoldSettings this);

    /**
     * Returns the minimum allowed change of {@code this} commodity's capacity, is case of a resize.
     *
     * <p>
     *  More specifically, the new capacity, in case of a resize, should be an integer multiple of
     *  the capacity increment.
     * </p>
     */
    @Pure
    double getCapacityIncrement(@ReadOnly CommoditySoldSettings this);

    /**
     * Returns the utilization upper bound for {@code this} commodity.
     *
     * <p>
     *  Our system should never produce a recommendation that causes the utilization of a commodity
     *  sold to exceed this limit, but external factors like manual moves using other tools or just
     *  changes in the load might. It may be used in price calculations.
     * </p>
     */
    @Pure
    double getUtilizationUpperBound(@ReadOnly CommoditySoldSettings this);

    /**
     * Returns the price function for {@code this} commodity.
     *
     * <p>
     *  A price function P(Q) accepts a quantity of this commodity and returns the price the seller
     *  of this commodity will charge a potential buyer of this quantity.
     * </p>
     *
     * <p>
     *  Our system has a complex notion of 'quantity' that's composed of quantity, peak quantity and
     *  other factors and a higher level notion of 'price' that's the combination of different prices
     *  returned by this function. In that sense, this is the low level price function.
     * </p>
     */
    // TODO: update the documentation when we settle on the semantics...
    @Pure
    @NonNull @PolyRead PriceFunction getPriceFunction(@PolyRead CommoditySoldSettings this);

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

    /**
     * Sets the value of the <b>price function</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param priceFunction the new value for the field.
     * @return {@code this}
     *
     * @see #getPriceFunction()
     */
    @NonNull CommoditySoldSettings setPriceFunction(@NonNull PriceFunction priceFunction);

} // end CommoditySoldSettings interface
