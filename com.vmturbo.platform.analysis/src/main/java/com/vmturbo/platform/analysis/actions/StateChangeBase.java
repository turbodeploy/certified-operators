package com.vmturbo.platform.analysis.actions;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.Trader;

public abstract class StateChangeBase implements Action {
    // Fields
    private final @NonNull Trader target_;
    private final @NonNull Market sourceMarket_;

    // Constructors

    /**
     * Constructs a new StateChangeBase action with the specified target and source market.
     *
     * @param target The trader that will be activated/deactivated as a result of taking
     *               {@code this} action.
     * @param sourceMarket The market where the action originated from.
     */
    public StateChangeBase(@NonNull Trader target, @NonNull Market sourceMarket) {
        target_ = target;
        sourceMarket_ = sourceMarket;
    }

    // Methods

    /**
     * Returns the target of this action. i.e. the one that will be activated or deactivated.
     */
    @Pure
    public @NonNull Trader getTarget(@ReadOnly StateChangeBase this) {
        return target_;
    }

    /**
     * Returns the source market of this action. i.e. the market the algorithms were examining when
     * the action was generated.
     */
    @Pure
    public @NonNull Market getSourceMarket(@ReadOnly StateChangeBase this) {
        return sourceMarket_;
    }

} // end StateChangeBase
