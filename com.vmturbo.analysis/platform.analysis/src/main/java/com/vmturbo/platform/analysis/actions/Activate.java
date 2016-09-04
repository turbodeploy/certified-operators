package com.vmturbo.platform.analysis.actions;

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

import static com.google.common.base.Preconditions.checkArgument;
import static com.vmturbo.platform.analysis.actions.Utility.appendTrader;

/**
 * An action to activate a deactivated {@link Trader trader}.
 */
public class Activate extends StateChangeBase { // inheritance for code reuse

    private final @NonNull Trader modelSeller_;
    private final @NonNull Economy economy_;
    // Constructors

    /**
     * Constructs a new Activate action with the specified target.
     *
     * @param target The trader that will be activated as a result of taking {@code this} action.
     * @param sourceMarket The market that benefits from activating target.
     * @param modelSeller the trader which will be used  the shopping
     */
    public Activate(@NonNull Economy economy, @NonNull Trader target, @NonNull Market sourceMarket, @NonNull Trader modelSeller) {
        super(target,sourceMarket);
        modelSeller_ = modelSeller;
        economy_ = economy;
    }

    // Methods

    /**
    * Returns the model seller that should be used to serve as the template for creation of guaranteed buyers, if any
    */
   @Pure
   public @NonNull Trader getModelSeller(@ReadOnly Activate this) {
       return modelSeller_;
   }

    /**
    * Returns the economy in which the trader will be added.
    */
   @Pure
   public @NonNull Economy getEconomy(@ReadOnly Activate this) {
       return economy_;
   }

    @Override
    public @NonNull String serialize(@NonNull Function<@NonNull Trader, @NonNull String> oid) {
        return new StringBuilder()
            // TODO (Vaptistis): do I also need to send the market or basket?
            .append("<action type=\"activate\" target=\"").append(oid.apply(getTarget()))
            .append("\" />").toString();
    }

    /**
     * Takes an activate action to change the state of a trader from inactive to active.
     */
    @Override
    public @NonNull Activate take() {
        super.take();
        checkArgument(!getTarget().getState().isActive());
        getTarget().changeState(TraderState.ACTIVE);
        // when activate an inactive trader, update its relation with guaranteed buyers if any
        List<ShoppingList> shoppingLists = GuaranteedBuyerHelper.findShoppingListForGuaranteedBuyer(
                                            getEconomy(), getModelSeller());
        GuaranteedBuyerHelper.addShoppingListForGuaranteedBuyers(getEconomy(), shoppingLists, getTarget(),
                        shoppingLists.size() != 0 ? shoppingLists.get(0).getBasket() : null);
        return this;
    }

    /**
     * Rolls back an activate action to change the state of a trader from active back to inactive.
     */
    @Override
    public @NonNull Activate rollback() {
        super.rollback();
        checkArgument(getTarget().getState().isActive());
        getTarget().changeState(TraderState.INACTIVE);
        // when roll back an activate action, remove the shoppingList for the target and its guaranteed buyers
        GuaranteedBuyerHelper.removeShoppingListForGuaranteedBuyers(getEconomy(),
                        GuaranteedBuyerHelper.findShoppingListForGuaranteedBuyer(getEconomy(),
                                        getTarget()), getTarget());
        return this;
    }

    // TODO: update description and reason when we create the corresponding matrix.
    @Override
    public @NonNull String debugDescription(@NonNull Function<@NonNull Trader, @NonNull String> uuid,
                                            @NonNull Function<@NonNull Trader, @NonNull String> name,
                                            @NonNull IntFunction<@NonNull String> commodityType,
                                            @NonNull IntFunction<@NonNull String> traderType) {
        final @NonNull StringBuilder sb = new StringBuilder();

        sb.append("Activate ");
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
            .append("To satisfy increased demand for ").append(getSourceMarket().getBasket())
            .append(".").toString(); // TODO: print basket in human-readable form.
    }

    /**
     * Tests whether two Activate actions are equal field by field.
     */
    @Override
    @Pure
    public boolean equals(@ReadOnly Activate this,@ReadOnly Object other) {
        if (other == null || !(other instanceof Activate)) {
            return false;
        }
        Activate otherActivate = (Activate)other;
        return otherActivate.getEconomy() == getEconomy()
                        && otherActivate.getTarget() == getTarget()
                        && otherActivate.getSourceMarket() == getSourceMarket()
                        && otherActivate.getModelSeller() == getModelSeller();
    }

    /**
     * Use the hashCode of each field to generate a hash code, consistent with {@link #equals(Object)}.
     */
    @Override
    @Pure
    public int hashCode() {
        return Hashing.md5().newHasher().putInt(getEconomy().hashCode())
                        .putInt(getTarget().hashCode()).putInt(getSourceMarket().hashCode())
                        .putInt(getModelSeller().hashCode()).hash().asInt();
    }
} // end Activate class
