package com.vmturbo.platform.analysis.economy;

import java.util.List;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Pure;

/**
 * An unmodifiable interface to a commodity sold by a single {@link Trader}.
 */
public interface UnmodifiableCommoditySold {
    /**
     * Returns an unmodifiable list of traders buying {@code this} commodity.
     */
    @Pure
    @NonNull @ReadOnly List<@NonNull @ReadOnly Trader> getBuyers(@ReadOnly UnmodifiableCommoditySold this);

    /**
     * Returns the (unique) trader selling {@code this} commodity.
     */
    @Pure
    @NonNull @ReadOnly Trader getSeller(@ReadOnly UnmodifiableCommoditySold this);

    /**
     * The {@link CommoditySoldSettings settings} controlling {@code this} commodity's behavior.
     */
    @Pure
    @NonNull CommoditySoldSettings getSettings();

    /**
     * Returns the <b>type</b> of {@code this} commodity.
     */
    @Pure
    @NonNull CommodityType getType();

    /**
     * Returns the <b>quality</b> of {@code this} commodity.
     *
     * <p>
     *  A buyer interested in a commodity of a given quality, will not accept a commodity of a
     *  lesser quality. e.g. A buyer may request a CPU of at least 8 cores.
     * </p>
     */
    // double getQuality(); // this will probably replace the concept of key soon.

    /**
     * Returns the <b>key</b> of {@code this} commodity.
     *
     * <p>
     *  A buyer interested in a commodity with a given key, will not accept a commodity with any
     *  other key. e.g. This is used for access commodities.
     * </p>
     */
    @Pure
    long getKey();

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

} // end UnmodifiableCommoditySold interface
