package com.vmturbo.platform.analysis.actions;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;

/**
 * A number of factored-out getters and fields needed by both {@link Activate} and {@link Deactivate}.
 */
abstract class StateChangeBase extends ActionImpl {
    // Fields
    private final @NonNull Trader target_;
    private final @NonNull Basket triggeringBasket_; // For use in action descriptions. Possibly
                                                    // outside analysis library.

    // Constructors

    /**
     * Constructs a new StateChangeBase object. It's not intended to be used independently, but
     * rather as the base object of {@link Activate} and {@link Deactivate}.
     *
     * @param economy The economy of {@code this} activation or deactivation.
     * @param target The target of {@code this} activation or deactivation.
     * @param triggeringBasket The basket of commodities that triggered {@code this} activation or
     *                         deactivation. i.e. There was high or low demand respectively for this
     *                         basket.
     */
    StateChangeBase(@NonNull Economy economy, @NonNull Trader target,
                    @NonNull Basket triggeringBasket) {
        super(economy);
        target_ = target;
        triggeringBasket_ = triggeringBasket;
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
     * Returns the triggering basket of {@code this} activation or deactivation. i.e. the basket of
     * the market the algorithms were examining when {@code this} action was generated.
     */
    @Pure
    public @NonNull Basket getTriggeringBasket(@ReadOnly StateChangeBase this) {
        return triggeringBasket_;
    }

    @Override
    public @NonNull Trader getActionTarget() {
        return getTarget();
    }
} // end StateChangeBase
