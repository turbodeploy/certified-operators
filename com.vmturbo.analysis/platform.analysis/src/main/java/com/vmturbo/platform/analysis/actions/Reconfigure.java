package com.vmturbo.platform.analysis.actions;

import java.util.function.Function;
import java.util.function.IntFunction;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.economy.ShoppingList;

import com.google.common.hash.Hashing;

import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;

import static com.vmturbo.platform.analysis.actions.Utility.appendTrader;

/**
 * An action to reconfigure a {@link ShoppingList}.
 */
public class Reconfigure extends MoveBase implements Action { // inheritance for code reuse
    // Fields

    // Constructors

    /**
     * Constructs a new reconfigure action with the specified target and economy.
     *
     * @param economy The economy containing target.
     * @param target The shopping list of the trader that needs reconfiguration.
     */
    public Reconfigure(@NonNull Economy economy, @NonNull ShoppingList target) {
        super(economy,target,target.getSupplier());
    }


    // Methods

    @Override
    public @NonNull String serialize(@NonNull Function<@NonNull Trader, @NonNull String> oid) {
        return new StringBuilder()
            // TODO: is it enough to send the buyer or is the basket needed as well?
            // TODO: How do we serialize null sources?
            .append("<action type=\"reconfigure\" target=\"").append(oid.apply(getTarget().getBuyer()))
            .append("\" source=\"").append(oid.apply(getSource()))
            .append("\" />")
            .toString();
    }

    @Override
    public @NonNull Reconfigure take() {
        super.take();
        // Nothing can be done automatically
        return this;
    }

    @Override
    public @NonNull Reconfigure rollback() {
        super.rollback();
        // Nothing to roll back!
        return this;
    }

    @Override
    public @NonNull String debugDescription(@NonNull Function<@NonNull Trader, @NonNull String> uuid,
                                            @NonNull Function<@NonNull Trader, @NonNull String> name,
                                            @NonNull IntFunction<@NonNull String> commodityType,
                                            @NonNull IntFunction<@NonNull String> traderType) {
        final @NonNull StringBuilder sb = new StringBuilder();

        sb.append("Change constraints of ");
        appendTrader(sb, getTarget().getBuyer(), uuid, name);
        sb.append(" or ");

        if (getSource() != null) {
            sb.append(traderType.apply(getSource().getType()));
            sb.append("s");
        } else {
            sb.append("[need supply chain to fill in correct trader type]");
        }

        // TODO: add provision alternative.
        return sb.toString();
    }

    @Override
    public @NonNull String debugReason(@NonNull Function<@NonNull Trader, @NonNull String> uuid,
                                       @NonNull Function<@NonNull Trader, @NonNull String> name,
                                       @NonNull IntFunction<@NonNull String> commodityType,
                                       @NonNull IntFunction<@NonNull String> traderType) {
        final @NonNull StringBuilder sb = new StringBuilder();

        if (getSource() == null) { // No current provider
            sb.append("Unable to start ");
            appendTrader(sb, getTarget().getBuyer(), uuid, name);
            sb.append(" because no [need supply chain to fill in correct trader type] can ");
            sb.append("satisfy the following combination of constraints: ");
            sb.append(getTarget().getBasket()); // TODO: add more cases and substitute commodity types.
        } else {
            appendTrader(sb, getTarget().getBuyer(), uuid, name);
            sb.append(" is currently placed on a ").append(traderType.apply(getSource().getType()));
            sb.append("that does not satisfy the ");
            sb.append(getTarget().getBasket()); // TODO: substitute commodity types.
            sb.append("constraint(s)");
            // TODO: add text in case of a violated utilization upper bound.
            sb.append(" and there is currently no ").append(traderType.apply(getSource().getType()));
            sb.append(" that can satisfy them.");
        }

        return sb.toString();
    }

    @Override
    public @NonNull Trader getActionTarget() {
        return getTarget().getBuyer();
    }

    /**
     * Tests whether two Reconfigure actions are equal field by field.
     */
    @Override
    @Pure
    public boolean equals(@ReadOnly Reconfigure this,@ReadOnly Object other) {
        if (other == null || !(other instanceof Reconfigure)) {
            return false;
        }
        Reconfigure otherReconfigure = (Reconfigure)other;
        return otherReconfigure.getEconomy() == getEconomy()
                        && otherReconfigure.getTarget() == getTarget();
    }

    /**
     * Use the hashCode of each field to generate a hash code, consistent with {@link #equals(Object)}.
     */
    @Override
    @Pure
    public int hashCode() {
        return Hashing.md5().newHasher().putInt(getEconomy().hashCode())
                        .putInt(getTarget().hashCode()).hash()
                        .asInt();
    }

} // end Reconfigure class
