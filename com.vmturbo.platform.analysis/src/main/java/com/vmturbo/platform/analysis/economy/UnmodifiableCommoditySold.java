package com.vmturbo.platform.analysis.economy;

import java.util.List;

import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * An unmodifiable interface to a commodity sold by a single {@link Trader}.
 */
public interface UnmodifiableCommoditySold {
    /**
     * Returns an unmodifiable list of {@link Trader traders} buying {@code this} commodity.
     */
    @NonNull List<Trader> getConsumers();

    /**
     * The {@link CommoditySoldSettings settings} controlling {@code this} commodity's behavior.
     */
    @NonNull CommoditySoldSettings getSettings();

    /**
     * Returns the <b>type</b> of {@code this} commodity.
     */
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
    long getKey();

    /**
     * Returns the <b>capacity</b> for {@code this} commodity.
     */
    double getCapacity();

    /**
     * Returns whether {@code this} commodity is <b>thin</b> provisioned.
     *
     * <p>
     *  This is related to <a href="https://en.wikipedia.org/wiki/Thin_provisioning">thin provisioning</a>.
     * </p>
     */
    boolean isThin();

} // end UnmodifiableCommoditySold interface
