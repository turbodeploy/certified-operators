package com.vmturbo.platform.analysis.actions;

import java.util.function.Function;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.economy.BuyerParticipation;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;

public abstract class MoveBase implements Action {
    // Fields
    private final @NonNull Economy economy_;
    private final @NonNull BuyerParticipation target_;
    private final @Nullable Trader source_;

    // Constructors

    public MoveBase(@NonNull Economy economy, @NonNull BuyerParticipation target) {
        economy_ = economy;
        target_ = target;
        source_ = target.getSupplier();
    }

    // Methods

    /**
     * Returns the economy of {@code this} move. i.e. the economy containing the target, source and
     * destination.
     */
    @Pure
    public @NonNull Economy getEconomy(@ReadOnly MoveBase this) {
        return economy_;
    }

    /**
     * Returns the target of {@code this} move. i.e. the buyer participation that will move.
     */
    @Pure
    public @NonNull BuyerParticipation getTarget(@ReadOnly MoveBase this) {
        return target_;
    }

    /**
     * Returns the source of {@code this} move. i.e. the trader, target will move from.
     */
    @Pure
    public @Nullable Trader getSource(@ReadOnly MoveBase this) {
        return source_;
    }

    /**
     * Appends a human-readable string identifying a trader to a string builder in the form
     * "name [oid] (#index)".
     *
     * @param builder The {@link StringBuilder} to which the string should be appended.
     * @param economy The {@link Economy} containing the trader to be appended.
     * @param trader The {@link Trader} for which to append the identifying string.
     * @param oid A function from {@link Trader} to trader OID.
     * @param name A function from {@link Trader} to human-readable trader name.
     */
    protected static void appendTrader(@NonNull StringBuilder builder, @NonNull Economy economy,
            @NonNull Trader trader, @NonNull Function<@NonNull Trader, @NonNull String> oid,
                                    @NonNull Function<@NonNull Trader, @NonNull String> name) {
        builder.append(name.apply(trader)).append(" [").append(oid.apply(trader)).append("] (#")
               .append(economy.getIndex(trader)).append(")");
    }

} // end TargetedAction class
