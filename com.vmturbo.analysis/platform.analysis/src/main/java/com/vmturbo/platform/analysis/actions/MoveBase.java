package com.vmturbo.platform.analysis.actions;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;

/**
 * A number of factored-out getters and fields needed by both {@link Move} and {@link Reconfigure}.
 */
class MoveBase extends ActionImpl {
    // Fields
    private final @NonNull ShoppingList target_;
    private final @Nullable Trader source_;

    // Constructors

    /**
     * Constructs a new MoveBase object. It's not intended to be used independently, but rather as
     * the base object of {@link Move} and {@link Reconfigure}.
     *
     * @param economy The economy of {@code this} move or reconfiguration
     * @param target The target of {@code this} move or reconfiguration.
     * @param source The source of {@code this} move or reconfiguration. i.e. the supplier, target
     *               is supposed to be buying from just before the action is taken.
     */
    public MoveBase(@NonNull Economy economy, @NonNull ShoppingList target, @Nullable Trader source) {
        super(economy);
        target_ = target;
        source_ = source;
    }

    // Methods



    /**
     * Returns the target of {@code this} move or reconfiguration. i.e. the shopping list that
     * will be moved or reconfigured.
     */
    @Pure
    public @NonNull ShoppingList getTarget(@ReadOnly MoveBase this) {
        return target_;
    }

    /**
     * Returns the source of {@code this} move or reconfiguration. i.e. the trader, target is
     * currently placed on.
     */
    @Pure
    public @Nullable Trader getSource(@ReadOnly MoveBase this) {
        return source_;
    }

} // end TargetedAction class
