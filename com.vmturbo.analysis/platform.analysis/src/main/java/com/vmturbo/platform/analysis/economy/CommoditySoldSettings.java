package com.vmturbo.platform.analysis.economy;

import org.checkerframework.checker.javari.qual.PolyRead;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.pricefunction.PriceFunction;
import com.vmturbo.platform.analysis.utilities.FunctionalOperator;

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
     * Returns whether {@code this} commodity is resold by a provider.
     *
     * <p> A resold commodity is designed to model situations in which the true source of supply for that resold commodity
     *  is further down the supply chain rather than directly at the trader providing the commodity. In these cases, the
     *  true supplier is responsible for setting the price of the resold commodity. For a real-life economic analogy,
     *  consider the case of buying a BigMac at McDonald's - the individual restaurant does not get to set the price.
     *  Instead, the price may be set by the franchise based on national or regional prices of its ingredients along
     *  with other factors.
     *
     * When computing the expense or desired capacity of a reseller's resold commodity, we perform a recursive traversal
     * down the supply chain based on the raw materials map to find the true source of supply (that is, the commodities
     * not marked as resold) and use those values. For example, in the supply chain:
     *
     * Foo
     *  |
     * Bar (reseller)
     *  |
     * Baz (reseller)
     *  |
     * Quux
     *
     * When trying to calculate Foo's expenses or desired capacity, we will traverse all the way to Quux because the
     * commodities Foo is buying from Bar and the commodities Bar is buying from Baz are resold.</p>
     */
    @Pure
    boolean isResold(@ReadOnly CommoditySoldSettings this);

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
     * Gets the value of utilizationCheckForCongestion for {@code this} commodity.
     *
     * @return true if utilization should be considered to check if this commodity
     * is the reason for congestion. false otherwise.
     */
    @Pure
    boolean getUtilizationCheckForCongestion(@ReadOnly CommoditySoldSettings this);

    /**
     * Returns the value of the <b>cloneWithNewType</b> field.
     *
     */
    boolean isCloneWithNewType();

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
     * Sets the value of the <b>resold</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param resold the new value for the field.
     * @return {@code this}
     */
    @NonNull CommoditySoldSettings setResold(boolean resold);

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
     * Sets the value of the <b>utilization upper bound</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param utilizationUpperBound the new value for the field. Must be in the interval [0,1].
     * @return {@code this}
     */
    @NonNull CommoditySoldSettings setOrigUtilizationUpperBound(double utilizationUpperBound);

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
    double getOrigUtilizationUpperBound(@ReadOnly CommoditySoldSettings this);

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

    /**
     * Sets the value of the <b>updating function</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param updatingFunction the new value for the field.
     * @return {@code this}
     *
     * @see #getUpdatingFunction()
     */
    @NonNull CommoditySoldSettings setUpdatingFunction(@NonNull FunctionalOperator updatingFunction);

    /**
     * Returns the updating function for {@code this} commodity.
     *
     * <p>
     *  A updating function simulates a move and returns a potential used value of the commoditySold
     * </p>
     *
     */
    @Pure
    @NonNull @PolyRead FunctionalOperator getUpdatingFunction(@PolyRead CommoditySoldSettings this);

    /**
     * Sets the value of the <b>cloneWithNewKey</b> field.
     *
     * @param cloneWithNewKey the new value for the field.
     * @return {@code this}
     */
    @NonNull CommoditySoldSettings setCloneWithNewKey(boolean cloneWithNewKey);

    /**
     * Sets the value of the <b>utilizationCheckForCongestion</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param utilizationCheckForCongestion the new value for the field.
     * @return {@code this}
     *
     * @see #getUtilizationCheckForCongestion()
     */
    @Pure
    @NonNull CommoditySoldSettings setUtilizationCheckForCongestion(boolean utilizationCheckForCongestion);

} // end CommoditySoldSettings interface
