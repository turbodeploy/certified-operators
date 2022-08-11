package com.vmturbo.platform.analysis.economy;

import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * The settings associated with and controlling the behavior of a single {@link Trader}.
 */
public interface TraderSettings {
    /**
     * Whether the associated {@link Trader} should be considered for suspension.
     */
    boolean isSuspendable();

    /**
     * Whether the associated {@link Trader} should be considered for cloning.
     *
     * <p>
     *  Cloning an existing trader is one way our <em>provision algorithms</em> may introduce new
     *  traders to the economy.
     * </p>
     */
    boolean isCloneable();

    /**
     * Whether the associated {@link Trader} should consume the clone of provider.
     * @return
     */
    boolean isGuaranteedBuyer();

    /**
     * Whether the associated {@link Trader} can accept a {@link ShoppingList} move to itself.
     */
    boolean canAcceptNewCustomers();

    /**
     * Returns the maximum desired utilization of {@code this} trader's commodities.
     *
     * <p>
     *  The algorithms will attempt to drive the system in a state where all commodities of
     *  {@code this} trader have a utilization <= to this value <em>if possible</em>.
     * </p>
     */
    double getMaxDesiredUtil();

    /**
     * Returns the minimum desired utilization of {@code this} trader's commodities.
     *
     * <p>
     *  The algorithms will attempt to drive the system in a state where all commodities of
     *  {@code this} trader have a utilization >= to this value <em>if possible</em>.
     * </p>
     */
    double getMinDesiredUtil();

    /**
     * Sets the value of the <b>suspendable</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param suspendable the new value for the field.
     * @return {@code this}
     */
    @NonNull TraderSettings setSuspendable(boolean suspendable);

    /**
     * Sets the value of the <b>cloneable</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param cloneable the new value for the field.
     * @return {@code this}
     */
    @NonNull TraderSettings setCloneable(boolean cloneable);

    /**
     * Sets the value of the <b>max desired utilization</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param maxDesiredUtilization the new value for the field. Must be >= minDesiredUtilization.
     * @return {@code this}
     */
    @NonNull TraderSettings setMaxDesiredUtil(double maxDesiredUtilization);

    /**
     * Sets the value of the <b>min desired utilization</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param minDesiredUtilization the new value for the field. Must be in [0,maxDesiredUtilization].
     * @return {@code this}
     */
    @NonNull TraderSettings setMinDesiredUtil(double minDesiredUtilization);

    /**
     * Sets the value of the <b>guranteedBuyer</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param guranteedBuyer the new value for the field.
     * @return {@code this}
     */
    @NonNull TraderSettings setGuaranteedBuyer(boolean guaranteedBuyer);

    /**
     * Sets the value of the <b>canAcceptNewCustomer</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param canAcceptNewCustomer the new value for the field.
     * @return {@code this}
     */
    @NonNull
    TraderSettings setCanAcceptNewCustomers(boolean canAcceptNewCustomers);
} // end TraderSettings interface
