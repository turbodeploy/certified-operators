package com.vmturbo.platform.analysis.economy;

import java.util.Optional;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.pricefunction.QuoteFunction;
import com.vmturbo.platform.analysis.utilities.CostFunction;

/**
 * The settings associated with and controlling the behavior of a single {@link Trader}.
 */
public interface TraderSettings {
    /**
     * Whether the associated {@link Trader} is reconfigurable.
     *
     * @return if it is reconfigurable.
     */
    boolean isReconfigurable();

    /**
     * Whether the associated {@link Trader} is controllable.
     */
    boolean isControllable();

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
     * Returns whether the associated {@link Trader} should consume the clone of provider.
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
     * Returns rate of resize of an entity.
     */
    float getRateOfResize();

    /**
     * Whether the associated {@link Trader} can take resize down action.
     */
    boolean isEligibleForResizeDown();

    /**
     * Whether the associated {@link Trader} should shop together or not.
     */
    boolean isShopTogether();

    /**
     * Whether the associated provider must clone is this {@link Trader} clones.
     */
    boolean isProviderMustClone();

    /**
     * Whether the associated {@link Trader} is a daemon.
     */
    boolean isDaemon();

    /**
     * Whether the associated {@link Trader} can simulate an action
     */
    boolean isCanSimulateAction();

    /**
     * Whether the associated {@link Trader} resizes commodity capacities through Supplier cloning
     * or suspending.
     */
    boolean isResizeThroughSupplier();

    /**
     * Returns the associated {@link CostFunction} if there is any.
     */
    CostFunction getCostFunction();

    /**
     * Returns the associated {@link QuoteFunction}.
     */
    QuoteFunction getQuoteFunction();

    /**
     * Returns the associated context with trader. Allow Optional context for cloud migration case,
     * where we need to ignore current context and find the cheapest quote from new CSP tiers.
     *
     * @return The context associated with the trader. If no context, will return empty Optional.
     */
    // TODO: Have this method return a Context class which is generic for both on prem and cloud.
    @NonNull
    Optional<Context> getContext();

    /**
     * Sets the value of the <b>reconfigurable</b> field.
     * Has no observable side-effects except setting the above field.
     *
     * @param reconfigurable the new value for the field.
     * @return {@code this}
     */
    @NonNull TraderSettings setReconfigurable(boolean reconfigurable);

    /**
     * Sets the value of the <b>controllable</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param controllable the new value for the field.
     * @return {@code this}
     */
    @NonNull TraderSettings setControllable(boolean controllable);

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
     * Sets the value of the <b>rateOfResize</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param rateOfResize rate of resizse
     * @return {@code this}
     */
    @NonNull TraderSettings setRateOfResize(float rateOfResize);

    /**
     * Returns the quote factor of {@code this} trader.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     */
    double getQuoteFactor();

    /**
     * Returns the quote move cost factor of {@code this} trader.
     *
     * <p>
     *  Gets Move Cost Factor value.
     * </p>
     */
    double getMoveCostFactor();



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
     * Sets the value of the <b>guaranteedBuyer</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param guaranteedBuyer the new value for the field.
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
     * @param canAcceptNewCustomers the new value for the field.
     * @return {@code this}
     */
    @NonNull
    TraderSettings setCanAcceptNewCustomers(boolean canAcceptNewCustomers);

    /**
     * Sets the value of the <b>canSimulateAction</b> field
     *
     * @param canSimulate new value for this field
     * @return {@code this}
     */
    TraderSettings setCanSimulateAction(boolean canSimulate);


    /**
     * Sets the value of the <b>isEligibleForResizeDown</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param isEligibleForResizeDown the new value for the field.
     * @return {@code this}
     */
    @NonNull TraderSettings setIsEligibleForResizeDown(boolean isEligibleForResizeDown);

    /**
     * Sets the value of the <b>isShopTogether</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param isShopTogether the new value for the field.
     * @return {@code this}
     */
    @NonNull
    TraderSettings setIsShopTogether(boolean isShopTogether);

    /**
     * Sets the value of the <b>providerMustClone</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param providerMustClone the new value for the field.
     * @return {@code this}
     */
    @NonNull TraderSettings setProviderMustClone(boolean providerMustClone);

    /**
     * Sets the value of the <b>isDaemon</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param daemon the new value for the field.
     * @return {@code this}
     */
    @NonNull TraderSettings setDaemon(boolean daemon);

    /**
     * Sets the value of the <b>resizeThroughSupplier</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param resizeThroughSupplier the new value for the field.
     * @return {@code this}
     */
    @NonNull TraderSettings setResizeThroughSupplier(boolean resizeThroughSupplier);

    /**
     * Sets the value of the <b>quote factor</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param quoteFactor the new value for the field. Must be 0 < quoteFactor <= 1.
     * @return {@code this}
     */
    @NonNull TraderSettings setQuoteFactor(double quoteFactor);

    /**
     * Sets the value of the <b>move cost factor</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param moveCostFactor the new value for the field. Must be 0 <= moveCostFactor.
     * @return {@code this}
     */
    @NonNull TraderSettings setMoveCostFactor(double moveCostFactor);


    /**
     * Sets the {@link CostFunction} for a trader.
     */
    void setCostFunction(CostFunction costFunction);

    /**
     * Sets the {@link QuoteFunction} for a trader.
     */
    void setQuoteFunction(QuoteFunction quoteFunction);

    /**
     * Sets the {@link Context} with a trader.
     *
     * @param context The context to be set on the trader.
     */
    void setContext(Context context);

} // end TraderSettings interface
