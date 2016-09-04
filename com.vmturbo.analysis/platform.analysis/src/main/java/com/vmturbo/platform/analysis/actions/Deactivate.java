package com.vmturbo.platform.analysis.actions;

import static com.google.common.base.Preconditions.checkArgument;
import static com.vmturbo.platform.analysis.actions.Utility.appendTrader;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.IntFunction;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Pure;

import com.google.common.hash.Hashing;

import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;

/**
 * An action to deactivate an active {@link Trader trader}.
 */
public class Deactivate extends StateChangeBase { // inheritance for code reuse

    // Constructors

    private final @NonNull List<@NonNull ShoppingList> shoppingListForGuaranteedBuyers = new ArrayList<>();
    private final @NonNull Economy economy_;

    /**
     * Constructs a new Deactivate action with the specified target.
     *
     * @param target The trader that will be deactivated as a result of taking {@code this} action.
     * @param sourceMarket The market that benefits from deactivating target.
     */
    public Deactivate(@NonNull Economy economy, @NonNull Trader target, @NonNull Market sourceMarket) {
        super(target,sourceMarket);
        shoppingListForGuaranteedBuyers.addAll(GuaranteedBuyerHelper.findShoppingListForGuaranteedBuyer(
                                                economy, target));
        economy_ = economy;

    }

    // Methods

    /**
     * Returns the economy in which the new seller will be added.
     */
    @Pure
    public @NonNull Economy getEconomy(@ReadOnly Deactivate this) {
        return economy_;
    }

    @Override
    public @NonNull String serialize(@NonNull Function<@NonNull Trader, @NonNull String> oid) {
        return new StringBuilder()
                        // TODO (Vaptistis): do I also need to send the market or basket?
                        .append("<action type=\"deactivate\" target=\"")
                        .append(oid.apply(getTarget())).append("\" />").toString();
    }

    /**
     * Takes a deactivate action to change the state of a trader from active to inactive.
     */
    @Override
    public @NonNull Deactivate take() {
        super.take();
        checkArgument(getTarget().getState().isActive());
        TraderState oldState = getTarget().getState();
        getTarget().changeState(TraderState.INACTIVE);
        // when deactivate an activate trader, remove the shoppingList associated with its guaranteed buyers
        if (oldState == TraderState.ACTIVE) {
            GuaranteedBuyerHelper.removeShoppingListForGuaranteedBuyers(getEconomy(),
                            shoppingListForGuaranteedBuyers, getTarget());
        }
        return this;
    }

    /**
     * Rolls back a deactivate action to change the state of a trader from active to inactive.
     */
    @Override
    public @NonNull Deactivate rollback() {
        super.rollback();
        checkArgument(!getTarget().getState().isActive());
        TraderState oldState = getTarget().getState();
        getTarget().changeState(TraderState.ACTIVE);
        // when roll back a deactivate action, adds back shoppingList for the target and its guaranteed buyers
        if (oldState == TraderState.INACTIVE) {
            GuaranteedBuyerHelper.addShoppingListForGuaranteedBuyers(getEconomy(), shoppingListForGuaranteedBuyers,
                            getTarget(), shoppingListForGuaranteedBuyers.size() != 0 ? shoppingListForGuaranteedBuyers
                                            .get(0).getBasket() : null);
        }
        return this;
    }

    // TODO: update description and reason when we create the corresponding matrix.
    @Override
    public @NonNull String debugDescription(@NonNull Function<@NonNull Trader, @NonNull String> uuid,
                    @NonNull Function<@NonNull Trader, @NonNull String> name,
                    @NonNull IntFunction<@NonNull String> commodityType,
                    @NonNull IntFunction<@NonNull String> traderType) {
        final @NonNull StringBuilder sb = new StringBuilder();

        sb.append("Deactivate ");
        appendTrader(sb, getTarget(), uuid, name);
        sb.append(".");

        return sb.toString();
    }

    @Override
    public @NonNull String debugReason(@NonNull Function<@NonNull Trader, @NonNull String> uuid,
                    @NonNull Function<@NonNull Trader, @NonNull String> name,
                    @NonNull IntFunction<@NonNull String> commodityType,
                    @NonNull IntFunction<@NonNull String> traderType) {
        return new StringBuilder()
                        .append("Because of insufficient demand for ")
                        .append(getSourceMarket().getBasket()).append(".").toString(); // TODO: print basket in human-readable form.
    }

    /**
     * Tests whether two Deactivate actions are equal field by field.
     */
    @Override
    @Pure
    public boolean equals(@ReadOnly Deactivate this,@ReadOnly Object other) {
        if (other == null || !(other instanceof Deactivate)) {
            return false;
        }
        Deactivate otherDeactivate = (Deactivate)other;
        return otherDeactivate.getEconomy() == getEconomy()
                        && otherDeactivate.getTarget() == getTarget()
                        && otherDeactivate.getSourceMarket() == getSourceMarket();
    }

    /**
     * Use the hashCode of each field to generate a hash code, consistent with {@link #equals(Object)}.
     */
    @Override
    @Pure
    public int hashCode() {
        return Hashing.md5().newHasher().putInt(getEconomy().hashCode())
                        .putInt(getTarget().hashCode()).putInt(getSourceMarket().hashCode()).hash()
                        .asInt();
    }
} // end Deactivate class
