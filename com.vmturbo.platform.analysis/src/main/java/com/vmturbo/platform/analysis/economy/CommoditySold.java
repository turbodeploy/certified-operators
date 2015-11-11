package com.vmturbo.platform.analysis.economy;

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
public interface CommoditySold {
    /**
     * The {@link CommoditySoldSettings settings} controlling {@code this} commodity's behavior.
     */
    @Pure
    @NonNull @PolyRead CommoditySoldSettings getSettings(@PolyRead CommoditySold this);

    /**
     * Returns the <b>utilization</b> of {@code this} commodity.
     *
     * @return {@link #getQuantity()}/{@link #getCapacity()}.
     */
    @Pure
    double getUtilization(@ReadOnly CommoditySold this);

    /**
     * Returns the <b>peak utilization</b> of {@code this} commodity.
     *
     * @return {@link #getPeakQuantity()}/{@link #getCapacity()}.
     */
    @Pure
    double getPeakUtilization(@ReadOnly CommoditySold this);

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
    double getQuantity(@ReadOnly CommoditySold this);

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
    double getPeakQuantity(@ReadOnly CommoditySold this);

    /**
     * Returns the <b>capacity</b> of {@code this} commodity.
     */
    @Pure
    double getCapacity(@ReadOnly CommoditySold this);

    /**
     * Returns the <b>effective capacity</b> of {@code this} commodity.
     *
     * @return {@link #getSettings()}.{@link CommoditySoldSettings#getUtilizationUpperBound()
     *          getUtilizationUpperBound()}*{@link #getCapacity()}
     *
     * @see #getCapacity()
     */
    @Pure
    double getEffectiveCapacity(@ReadOnly CommoditySold this);

    /**
     * Returns whether {@code this} commodity is <b>thin</b> provisioned.
     *
     * <p>
     *  This is related to <a href="https://en.wikipedia.org/wiki/Thin_provisioning">thin provisioning</a>.
     * </p>
     */
    @Pure
    boolean isThin(@ReadOnly CommoditySold this);

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
    @NonNull CommoditySold setQuantity(double quantity);

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
    @NonNull CommoditySold setPeakQuantity(double peakQuantity);

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
    @NonNull CommoditySold setCapacity(double capacity);

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
    @NonNull CommoditySold setThin(boolean thin);

} // end CommoditySold interface
