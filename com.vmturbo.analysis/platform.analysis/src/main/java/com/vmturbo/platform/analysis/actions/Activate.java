package com.vmturbo.platform.analysis.actions;

import static com.vmturbo.platform.analysis.actions.Utility.appendTrader;

import java.util.function.Function;
import java.util.function.IntFunction;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.hash.Hashing;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;

/**
 * An action to activate a deactivated {@link Trader trader}.
 */
public class Activate extends StateChangeBase { // inheritance for code reuse

    private final @NonNull Trader modelSeller_;
    private @Nullable CommoditySpecification reasonCommodity;
    // Constructors

    /**
     * Constructs a new Activate action with the specified target.
     *
     * @param target The trader that will be activated as a result of taking {@code this} action.
     * @param triggeringBasket The basket of the market that benefits from activating target.
     * @param modelSeller The trader whose high profits led to activation.
     * @param commCausingActivation commodity that led to activation
     */
    public Activate(@NonNull Economy economy, @NonNull Trader target,
                    @NonNull Basket triggeringBasket, @NonNull Trader modelSeller,
                    @Nullable CommoditySpecification commCausingActivation) {
        super(economy, target, triggeringBasket);
        modelSeller_ = modelSeller;
        reasonCommodity = commCausingActivation;
    }

    // Methods

    /**
    * Returns the model seller that should be used to serve as the template for creation of guaranteed buyers, if any
    */
   @Pure
   public @NonNull Trader getModelSeller(@ReadOnly Activate this) {
       return modelSeller_;
   }

   @Override
   public CommoditySpecification getReason() {
       return reasonCommodity;
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
        Preconditions.checkState(!getTarget().getState().isActive(),
            "Trying to activate %s which is already Active", getTarget());
        getTarget().changeState(TraderState.ACTIVE);
        return this;
    }

    /**
     * Rolls back an activate action to change the state of a trader from active back to inactive.
     */
    @Override
    public @NonNull Activate rollback() {
        super.rollback();
        Preconditions.checkState(getTarget().getState().isActive(),
            "Trying to deactivate %s which is already Inactive", getTarget());
        getTarget().changeState(TraderState.INACTIVE);
        return this;
    }

    @Override
    public @NonNull Activate port(@NonNull final Economy destinationEconomy,
            @NonNull final Function<@NonNull Trader, @NonNull Trader> destinationTrader,
            @NonNull final Function<@NonNull ShoppingList, @NonNull ShoppingList>
                                                                        destinationShoppingList) {
        return new Activate(destinationEconomy, destinationTrader.apply(getTarget()),
            getTriggeringBasket(), destinationTrader.apply(getModelSeller()), getReason());
    }

    /**
     * Returns whether {@code this} action respects constraints and can be taken.
     *
     * <p>Currently an activate is considered valid iff the model seller is cloneable and inactive.
     * </p>
     */
    // TODO: Do we want to check if some other trader is cloneable or if this one is activate-able?
    @Override
    public boolean isValid() {
        return getModelSeller().getSettings().isCloneable() && !getTarget().getState().isActive();
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
            .append("To satisfy increased demand for ").append(getTriggeringBasket())
            .append(".").toString(); // TODO: print basket in human-readable form.
    }

    /**
     * Tests whether two Activate actions are equal field by field.
     */
    @Override
    @Pure
    public boolean equals(@ReadOnly Activate this,@ReadOnly Object other) {
        if (!(other instanceof Activate)) {
            return false;
        }
        Activate otherActivate = (Activate)other;
        return otherActivate.getEconomy() == getEconomy()
                        && otherActivate.getTarget() == getTarget()
                        && otherActivate.getTriggeringBasket().equals(getTriggeringBasket())
                        && otherActivate.getModelSeller() == getModelSeller();
    }

    /**
     * Use the hashCode of each field to generate a hash code, consistent with {@link #equals(Object)}.
     */
    @Override
    @Pure
    public int hashCode() {
        return Hashing.md5().newHasher().putInt(getEconomy().hashCode())
                        .putInt(getTarget().hashCode()).putInt(getTriggeringBasket().hashCode())
                        .putInt(getModelSeller().hashCode()).hash().asInt();
    }

    @Override
    public ActionType getType() {
        return ActionType.ACTIVATE;
    }
} // end Activate class
