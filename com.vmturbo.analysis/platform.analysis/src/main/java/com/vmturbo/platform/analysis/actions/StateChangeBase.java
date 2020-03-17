package com.vmturbo.platform.analysis.actions;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.Trader;

/**
 * A number of factored-out getters and fields needed by both {@link Activate} and {@link Deactivate}.
 */
class StateChangeBase extends ActionImpl {
    // Fields
    private final @NonNull Trader target_;
    private final @NonNull Market sourceMarket_; // only needed for debugDescription/debugReason.

    // Constructors

    /**
     * Constructs a new StateChangeBase object. It's not intended to be used independently, but
     * rather as the base object of {@link Activate} and {@link Deactivate}.
     *
     * @param economy The economy of {@code this} activation or deactivation.
     * @param target The target of {@code this} activation or deactivation.
     * @param sourceMarket The source market of {@code this} activation or deactivation.
     */
    StateChangeBase(@NonNull Economy economy, @NonNull Trader target,
                    @NonNull Market sourceMarket) {
        super(economy);
        target_ = target;
        sourceMarket_ = sourceMarket;
    }

    // Methods

    /**
     * Returns the target of {@code this} activation or deactivation. i.e. the one that will be
     * activated or deactivated.
     */
    @Pure
    public @NonNull Trader getTarget(@ReadOnly StateChangeBase this) {
        return target_;
    }

    /**
     * Returns the source market of {@code this} activation or deactivation. i.e. the market the
     * algorithms were examining when the action was generated.
     */
    @Pure
    public @NonNull Market getSourceMarket(@ReadOnly StateChangeBase this) {
        return sourceMarket_;
    }

    @Override
    public @NonNull Trader getActionTarget() {
        return getTarget();
    }
} // end StateChangeBase
