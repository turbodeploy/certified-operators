package com.vmturbo.platform.analysis.economy;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nullable;

import org.checkerframework.checker.javari.qual.PolyRead;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Deterministic;
import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.pricefunction.QuoteFunction;
import com.vmturbo.platform.analysis.pricefunction.QuoteFunctionFactory;
import com.vmturbo.platform.analysis.utilities.CostFunction;

final class TraderWithSettings extends Trader implements TraderSettings {
    // Internal fields
    private final @NonNull Map<@NonNull ShoppingList,@NonNull Market> marketsAsBuyer_ = new LinkedHashMap<>();
    private final @NonNull List<Market> marketsAsSeller_ = new ArrayList<>();

    // Fields for TraderSettings
    private boolean canSimulate_ = true;
    private boolean controllable_ = true;
    private boolean suspendable_ = false;
    private boolean cloneable_ = false;
    private boolean guaranteedBuyer_ = false;
    private boolean isProviderMustClone_ = false;
    private boolean canAcceptNewCustomers_ = false;
    private boolean isEligibleForResizeDown_ = true;
    private boolean isShopTogether_ = false;
    private double maxDesiredUtilization_ = 1.0;
    private double minDesiredUtilization_ = 0.0;
    private double quoteFactor_ = 0.75f;
    private double moveCostFactor_ = 0.005f;
    private float rateOfResize_ = 1f;
    @Nullable private CostFunction costFunction_ = null;
    // default quote function is sum of commodity
    private QuoteFunction quoteFunction_ = QuoteFunctionFactory.sumOfCommodityQuoteFunction();
    private Context context_;
    private boolean isDaemon_ = false;

    // Whether the trader resizes commodity capacities through its Supplier cloning or suspending.
    private boolean isResizeThroughSupplier_ = false;

    // Constructors

    /**
     * Constructs a new TraderWithSettings instance with the specified attributes.
     *
     * @param economyIndex see {@link #setEconomyIndex(int)}.
     * @param type see {@link #getType()}.
     * @param state see {@link #setState(TraderState)}.
     * @param basketSold see {@link #getBasketSold()}.
     */
    public TraderWithSettings(int economyIndex, int type, @NonNull TraderState state, @NonNull Basket basketSold) {
        super(economyIndex,type,state,basketSold);
    }

    // Internal methods

    /**
     * Returns a modifiable {@link Map} with the mapping from shopping lists of {@code this}
     * buyer to the markets he participates in with these shopping lists.
     *
     * <p>
     *  A trader does not know how to modify this map, so it just returns it for the economy to
     *  modify.
     * </p>
     */
    @Pure
    @NonNull @PolyRead Map<@NonNull ShoppingList, @NonNull Market> getMarketsAsBuyer(@PolyRead TraderWithSettings this) {
        return marketsAsBuyer_;
    }

    /**
     * Returns a modifiable List with the markets {@code this} seller participates in.
     *
     * <p>
     *  A trader does not know how to modify this list, so it just returns it for the economy to
     *  modify.
     * </p>
     */
    @Pure
    @NonNull @PolyRead List<@NonNull @PolyRead Market> getMarketsAsSeller(@PolyRead TraderWithSettings this) {
        return marketsAsSeller_;
    }

    // Methods for Trader

    @Override
    @Pure
    public @NonNull TraderSettings getSettings(@ReadOnly TraderWithSettings this) {
        return this;
    }

    @Override
    @Deterministic
    public @NonNull TraderState changeState(@NonNull TraderState newState) {
        return Market.changeTraderState(this, newState);
    }

    // Methods for TraderSettings

    @Override
    @Pure
    public boolean isControllable(@ReadOnly TraderWithSettings this) {
        return controllable_;
    }

    @Override
    @Pure
    public boolean isSuspendable(@ReadOnly TraderWithSettings this) {
        return suspendable_;
    }

    @Override
    @Pure
    public boolean isCloneable(@ReadOnly TraderWithSettings this) {
        return cloneable_;
    }

    @Override
    @Pure
    public boolean isGuaranteedBuyer(@ReadOnly TraderWithSettings this) {
        return guaranteedBuyer_;
    }

    @Override
    @Pure
    public boolean isProviderMustClone(@ReadOnly TraderWithSettings this) {
        return isProviderMustClone_;
    }

    @Override
    @Pure
    public boolean isDaemon(@ReadOnly TraderWithSettings this) {
        return isDaemon_;
    }

    @Override
    @Pure
    public boolean isResizeThroughSupplier(@ReadOnly TraderWithSettings this) {
        return isResizeThroughSupplier_;
    }

    public boolean isCanSimulateAction() {
        return canSimulate_;
    }

    @Override
    @Pure
    public boolean canAcceptNewCustomers() {
        return canAcceptNewCustomers_;
    }

    @Override
    @Pure
    public double getMaxDesiredUtil(@ReadOnly TraderWithSettings this) {
        return maxDesiredUtilization_;
    }

    @Override
    @Pure
    public double getMinDesiredUtil(@ReadOnly TraderWithSettings this) {
        return minDesiredUtilization_;
    }

    @Override
    public boolean isEligibleForResizeDown() {
        return isEligibleForResizeDown_;
    }

    @Override
    public boolean isShopTogether() {
        return isShopTogether_;
    }

    @Override
    @Pure
    public double getQuoteFactor(@ReadOnly TraderWithSettings this) {
        return quoteFactor_;
    }

    @Override
    @Pure
    public double getMoveCostFactor(@ReadOnly TraderWithSettings this) {
        return moveCostFactor_;
    }

    @Override
    @Pure
    public float getRateOfResize(@ReadOnly TraderWithSettings this) {
        return rateOfResize_;
    }

    @Override
    @Deterministic
    public @NonNull TraderWithSettings setControllable(boolean controllable) {
        controllable_ = controllable;
        return this;
    }

    @Override
    @Deterministic
    public @NonNull TraderWithSettings setSuspendable(boolean suspendable) {
        suspendable_ = suspendable;
        return this;
    }

    @Override
    @Deterministic
    public @NonNull TraderWithSettings setCloneable(boolean cloneable) {
        cloneable_ = cloneable;
        return this;
    }

    @Override
    @Deterministic
    public @NonNull TraderWithSettings setRateOfResize(float rateOfResize) {
        rateOfResize_ = rateOfResize;
        return this;
    }

    @Override
    @Deterministic
    public @NonNull TraderWithSettings setMaxDesiredUtil(double maxDesiredUtilization) {
        checkArgument(maxDesiredUtilization <= 1.0, "maxDesiredUtilization = " + maxDesiredUtilization);
        checkArgument(minDesiredUtilization_ <= maxDesiredUtilization,
            "minDesiredUtilization_ = " + minDesiredUtilization_ + " maxDesiredUtilization = " + maxDesiredUtilization);
        maxDesiredUtilization_ = maxDesiredUtilization;
        return this;
    }

    @Override
    @Deterministic
    public @NonNull TraderWithSettings setMinDesiredUtil(double minDesiredUtilization) {
        checkArgument(0.0 <= minDesiredUtilization, "minDesiredUtilization = " + minDesiredUtilization);
        checkArgument(minDesiredUtilization <= maxDesiredUtilization_,
            "minDesiredUtilization = " + minDesiredUtilization + " maxDesiredUtilization_ = " + maxDesiredUtilization_);
        minDesiredUtilization_ = minDesiredUtilization;
        return this;
    }

    @Override
    @Deterministic
    public @NonNull TraderSettings setGuaranteedBuyer(boolean guaranteedBuyer) {
        guaranteedBuyer_ = guaranteedBuyer;
        return this;
    }

    @Override
    @Deterministic
    public @NonNull TraderSettings setProviderMustClone(boolean isProviderMustClone) {
        isProviderMustClone_ = isProviderMustClone;
        return this;
    }

    @Override
    @Deterministic
    public @NonNull TraderSettings setDaemon(boolean isDaemon) {
        isDaemon_ = isDaemon;
        return this;
    }

    @Override
    @Deterministic
    public @NonNull TraderSettings setResizeThroughSupplier(boolean isResizeThroughSupplier) {
        isResizeThroughSupplier_ = isResizeThroughSupplier;
        return this;
    }

    @Override
    @Deterministic
    public @NonNull TraderSettings setCanAcceptNewCustomers(boolean canAcceptNewCustomers) {
        canAcceptNewCustomers_ = canAcceptNewCustomers;
        return this;
    }

    @Override
    public TraderSettings setCanSimulateAction(final boolean canSimulate) {
        canSimulate_ = canSimulate;
        return this;
    }

    @Override
    public @NonNull TraderSettings setIsEligibleForResizeDown(boolean isEligibleForResizeDown) {
        isEligibleForResizeDown_ = isEligibleForResizeDown;
        return this;
    }

    @Override
    public @NonNull TraderSettings setIsShopTogether(boolean isShopTogether) {
        isShopTogether_ = isShopTogether;
        return this;
    }

    @Override
    @Deterministic
    public @NonNull TraderWithSettings setQuoteFactor(double quoteFactor) {
        checkArgument(quoteFactor > 0.0, "quoteFactor = " + quoteFactor);
        checkArgument(quoteFactor <= 1.0, "quoteFactor = " + quoteFactor);
        quoteFactor_ = quoteFactor;
        return this;
    }

    @Override
    @Deterministic
    public @NonNull TraderWithSettings setMoveCostFactor(double moveCostFactor) {
        checkArgument(moveCostFactor >= 0.0);
        moveCostFactor_ = moveCostFactor;
        return this;
    }

    @Override
    public CostFunction getCostFunction() {
        return costFunction_;
    }

    @Override
    public void setCostFunction(CostFunction costFunction) {
        costFunction_ = costFunction;

    }

    @Override
    public QuoteFunction getQuoteFunction() {
        return quoteFunction_;
    }

    @Override
    public void setQuoteFunction(QuoteFunction quoteFunction) {
        quoteFunction_ = quoteFunction;

    }

    @Override
    @NonNull
    public Optional<Context> getContext() {
        return Optional.ofNullable(context_);
    }

    @Override
    public void setContext(Context context) {
        this.context_ = context;
    }

    @Override
    public void clearShoppingAndMarketData() {
        super.clearShoppingAndMarketData();
        marketsAsBuyer_.clear();
        marketsAsSeller_.clear();
    }
} // end TraderWithSettings class
