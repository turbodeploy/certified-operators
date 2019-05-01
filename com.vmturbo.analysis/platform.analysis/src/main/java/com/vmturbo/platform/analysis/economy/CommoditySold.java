package com.vmturbo.platform.analysis.economy;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.Serializable;

import org.checkerframework.checker.javari.qual.PolyRead;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Deterministic;
import org.checkerframework.dataflow.qual.Pure;

/**
 * A commodity sold by a {@link Trader}.
 *
 * <p>
 *  A CommoditySold instance should be sold by exactly one Trader instance.
 * </p>
 */
public abstract class CommoditySold implements Serializable {
    // Fields
    private double quantity_ = 0.0;
    private double peakQuantity_ = 0.0;
    private double maxQuantity_ = 0.0;
    private double capacity_ = Double.MAX_VALUE;
    private double startQuantity_ = 0.0;
    private double startPeakQuantity_ = 0.0;
    // The negative one value for historicalQuantity_ represents that it has not been populated
    private double historicalQuantity_ = -1D;
    private boolean thin_ = false;
    // numConsumers is the sum of the traders sent to the market that are consuming a resource
    // these can be active or inactive traders
    private int numConsumers_ = 0;

    // Methods

    /**
     * The {@link CommoditySoldSettings settings} controlling {@code this} commodity's behavior.
     */
    @Pure
    public abstract @NonNull @PolyRead CommoditySoldSettings getSettings(@PolyRead CommoditySold this);

    /**
     * Returns the <b>utilization</b> of {@code this} commodity.
     *
     * @return {@link #getQuantity()}/{@link #getCapacity()}.
     */
    @Pure
    public double getUtilization(@ReadOnly CommoditySold this) {
        return getQuantity()/getCapacity();
    }

    /**
     * Returns the <b>peak utilization</b> of {@code this} commodity.
     *
     * @return {@link #getPeakQuantity()}/{@link #getCapacity()}.
     */
    @Pure
    public double getPeakUtilization(@ReadOnly CommoditySold this) {
        return getPeakQuantity()/getCapacity();
    }

    /**
     * Returns the <b>quantity</b> of {@code this} commodity.
     *
     * <p>
     *  This is the aggregate quantity of {@code this} commodity the respective trader is selling to
     *  all its customers, but it should not be assumed that it equals the total amount, all the
     *  customers are buying! Various factors including sharing and overheads may lead to values
     *  greater or less than the total. The quantity is usually retrieved by Mediation from the
     *  Hypervisor.
     * </p>
     */
    @Pure
    public double getQuantity(@ReadOnly CommoditySold this) {
        return quantity_;
    }

    /**
     * Returns the <b>peak quantity</b> of {@code this} commodity.
     *
     * <p>
     *  The peak quantity is the running max of quantity in some rolling time window, but the width
     *  of this window is not known or customizable form inside the Economy and it is assumed that
     *  the Mediation can get more accurate values for peak quantity from the Hypervisor than could
     *  be calculated using the quantity samples alone.
     * </p>
     */
    @Pure
    public double getPeakQuantity(@ReadOnly CommoditySold this) {
        return peakQuantity_;
    }

    /**
     *
     * @return The maximum quantity.
     */
    @Pure
    public double getMaxQuantity(@ReadOnly CommoditySold this) {
        return maxQuantity_;
    }

    /**
     * Returns the <b>capacity</b> of {@code this} commodity.
     */
    @Pure
    public double getCapacity(@ReadOnly CommoditySold this) {
        return capacity_;
    }

    /**
     * Returns the <b>numConsumers</b> of {@code this} commodity.
     */
    @Pure
    public int getNumConsumers(@ReadOnly CommoditySold this) {
        return numConsumers_;
    }

    /**
    *
    * @return The start quantity.
    */
   @Pure
   public double getStartQuantity(@ReadOnly CommoditySold this) {
       return startQuantity_;
   }

    /**
    *
    * @return The start peak quantity.
    */
   @Pure
   public double getStartPeakQuantity(@ReadOnly CommoditySold this) {
       return startPeakQuantity_;
   }

    /**
     * Returns the <b>effective capacity</b> of {@code this} commodity.
     *
     * @return {@link #getSettings()}.{@link CommoditySoldSettings#getUtilizationUpperBound()
     *          getUtilizationUpperBound()}*{@link #getCapacity()}
     *
     * @see #getCapacity()
     */
    @Pure
    public abstract double getEffectiveCapacity(@ReadOnly CommoditySold this);

    /**
     * Returns whether {@code this} commodity is <b>thin</b> provisioned.
     *
     * <p>
     *  This is related to <a href="https://en.wikipedia.org/wiki/Thin_provisioning">thin provisioning</a>.
     * </p>
     */
    @Pure
    public boolean isThin(@ReadOnly CommoditySold this) {
        return thin_;
    }

    /**
     * Sets the value of the <b>quantity</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param quantity the new value for the field. Must be non-negative and less than or equal to
     *          capacity. May exceed effective capacity if utilization upper bound is set low but
     *          generated recommendations are not taken to reduce utilization.
     * @return {@code this}
     *
     * @see #getQuantity()
     */
    @Deterministic
    public @NonNull CommoditySold setQuantity(double quantity) {
        // quantity can be over capacity
        checkArgument(0 <= quantity, "quantity = %s", quantity);
        quantity_ = quantity;
        return this;
    }

    /**
     * Sets the value of the <b>peak quantity</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param peakQuantity the new value for the field. Must be non-negative and less than or equal to
     *          capacity. May exceed effective capacity if utilization upper bound is set low but
     *          generated recommendations are not taken to reduce utilization.
     * @return {@code this}
     *
     * @see #getPeakQuantity()
     */
    @Deterministic
    public @NonNull CommoditySold setPeakQuantity(double peakQuantity) {
        // peakQuantity can be over capacity
        checkArgument(0 <= peakQuantity, "peakQuantity = %s", peakQuantity);
        peakQuantity_ = peakQuantity;
        return this;
    }

    /**
     * Sets the value of maximum quantity.
     *
     * @param maxQuantity Maximum quantity value.
     * @return {@code this}
     */
    @Deterministic
    public @NonNull CommoditySold setMaxQuantity(double maxQuantity) {
        // quantity can be over capacity
        checkArgument(0 <= maxQuantity, "maxQuantity = %s", maxQuantity);
        maxQuantity_ = maxQuantity;
        return this;
    }

    /**
     * Sets the value of the <b>capacity</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param capacity the new value for the field. Must be non-negative.
     * @return {@code this}
     *
     * @see #getCapacity()
     */
    @Deterministic
    public @NonNull CommoditySold setCapacity(double capacity) {
        checkArgument(0 <= capacity, "capacity = %s", capacity);
        capacity_ = capacity;
        return this;
    }

    /**
     * Sets the value of start quantity.
     *
     * @param startQuantity Start quantity value.
     * @return {@code this}
     */
    @Deterministic
    public @NonNull CommoditySold setStartQuantity(double startQuantity) {
        // startQuantity can be over capacity
        checkArgument(0 <= startQuantity, "startQuantity = %s", startQuantity);
        startQuantity_ = startQuantity;
        return this;
    }

    /**
     * Sets the value of start peak quantity.
     *
     * @param startPeakQuantity Start peak quantity value.
     * @return {@code this}
     */
    @Deterministic
    public @NonNull CommoditySold setStartPeakQuantity(double startPeakQuantity) {
        // startPeakQuantity can be over capacity
        checkArgument(0 <= startPeakQuantity, "startPeakQuantity = %s", startPeakQuantity);
        startPeakQuantity_ = startPeakQuantity;
        return this;
    }

    /**
     * Sets the value for the historical quantity for the commodity.
     *
     * The value returned for historical quantity can be negative if it has not been set. However,
     * this method will throw an exception if you try sending a negative value to this method.
     *
     * @param historicalQuantity the value for the historical quantity.
     * @return {@code this}
     */
    @Deterministic
    public @NonNull CommoditySold setHistoricalQuantity(double historicalQuantity) {
        checkArgument(0 <= historicalQuantity, "historicalQuantity = %s", historicalQuantity);
        historicalQuantity_ = historicalQuantity;
        return this;
    }

    /**
     * Sets the value of the <b>thin</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param thin the new value for the field.
     * @return {@code this}
     *
     * @see #isThin()
     */
    @Deterministic
    public @NonNull CommoditySold setThin(boolean thin) {
        thin_ = thin;
        return this;
    }

    /**
     * Sets the number of consumers of this commodity
     *
     * @param numConsumers is the number of consumers of commodity
     * @return {@code this}
     */
    @Deterministic
    public @NonNull CommoditySold setNumConsumers(int numConsumers) {
        checkArgument(0 <= numConsumers, "numConsumers = %s", numConsumers);
        numConsumers_ = numConsumers;
        return this;
    }

    /**
     * Returns true when a non-negative value has been assigned to historical quantity.
     *
     * @return true when historical commodity has been set.
     */
    public boolean isHistoricalQuantitySet() {
        return historicalQuantity_ >= 0;
    }

    /**
     * Returns the <b>historical quantity</b> or <b>current quantity</b> of {@code this} commodity.
     *
     * <p>
     *  This is the aggregate quantity of {@code this} commodity the respective trader is selling to
     *  all its customers, but it should not be assumed that it equals the total amount, all the
     *  customers are buying! Various factors including sharing and overheads may lead to values
     *  greater or less than the total. The quantity is usually retrieved by Mediation from the
     *  Hypervisor.
     * </p>
     *
     * @return  the historical quantity if present otherwise it will return current quantity.
     */
    @Pure
    public double getHistoricalOrElseCurrentQuantity(@ReadOnly CommoditySold this) {
        return isHistoricalQuantitySet() ? historicalQuantity_ : quantity_;
    }

    /**
     * Returns the <b>historical utilization</b> or <b>current quantity</b> of {@code this} commodity.
     *
     * @return return the historical utilization if present otherwise it will return current
     * utilization.
     */
    @Pure
    public double getHistoricalOrElseCurrentUtilization(@ReadOnly CommoditySold this) {
        return getHistoricalOrElseCurrentQuantity() / getCapacity();
    }

} // end CommoditySold interface
