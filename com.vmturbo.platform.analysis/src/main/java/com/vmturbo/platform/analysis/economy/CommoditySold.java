package com.vmturbo.platform.analysis.economy;

import java.util.List;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Pure;

/**
 * A commodity sold by a single {@link Trader}.
 */
public interface CommoditySold {
    /**
     * Returns an unmodifiable list of traders buying {@code this} commodity.
     */
    @Pure
    @NonNull @ReadOnly List<@NonNull @ReadOnly Trader> getBuyers(@ReadOnly CommoditySold this);

    /**
     * Returns the (unique) trader selling {@code this} commodity.
     */
    @Pure
    @NonNull @ReadOnly Trader getSeller(@ReadOnly CommoditySold this);

    /**
     * The {@link CommoditySoldSettings settings} controlling {@code this} commodity's behavior.
     */
    @Pure
    @NonNull CommoditySoldSettings getSettings();

    /**
     * Returns the <b>capacity</b> for {@code this} commodity.
     */
    @Pure
    double getCapacity();

    /**
     * Returns whether {@code this} commodity is <b>thin</b> provisioned.
     *
     * <p>
     *  This is related to <a href="https://en.wikipedia.org/wiki/Thin_provisioning">thin provisioning</a>.
     * </p>
     */
    @Pure
    boolean isThin();

    /**
     * Sets the value of the <b>capacity</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param capacity the new value for the field. Must be non-negative.
     * @return {@code this}
     */
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
     */
    @NonNull CommoditySold setThin(boolean thin);

} // end CommoditySold interface
