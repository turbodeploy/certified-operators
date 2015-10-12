package com.vmturbo.platform.analysis.economy;

import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * A commodity sold by a single {@link Trader}.
 */
public interface CommoditySold extends UnmodifiableCommoditySold {
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
