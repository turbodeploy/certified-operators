package com.vmturbo.platform.analysis.actions;

import java.util.function.Function;
import java.util.function.IntFunction;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

import com.google.common.hash.Hashing;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;

import static com.vmturbo.platform.analysis.actions.Utility.appendTrader;

/**
 * An action to provision a new {@link Trader seller} using another {@link Trader buyer} as the
 * template.
 */
public class ProvisionByDemand implements Action {
    // Fields
    private final @NonNull Economy economy_;
    private final @NonNull ShoppingList modelBuyer_; // TODO: also add source market? Desired state?
    private final @NonNull Trader modelSeller_;
    private @Nullable Trader provisionedSeller_;

    // Constructors

    /**
     * Constructs a new ProvisionByDemand action with the specified attributes.
     *
     * @param economy The economy in which the seller will be provisioned.
     * @param modelBuyer The shopping list that should be satisfied by the new seller.
     */
    public ProvisionByDemand(@NonNull Economy economy, @NonNull ShoppingList modelBuyer,
                    @NonNull Trader modelSeller) {
        economy_ = economy;
        modelBuyer_ = modelBuyer;
        modelSeller_ = modelSeller;
    }

    // Methods

    /**
     * Returns the economy in which the new seller will be added.
     */
    @Pure
    public @NonNull Economy getEconomy(@ReadOnly ProvisionByDemand this) {
        return economy_;
    }

    /**
     * Returns the model buyer that should be satisfied by the new seller.
     */
    @Pure
    public @NonNull ShoppingList getModelBuyer(@ReadOnly ProvisionByDemand this) {
        return modelBuyer_;
    }

    /**
     * Returns the seller that was added as a result of taking {@code this} action.
     *
     * <p>
     *  It will be {@code null} before the action is taken and/or after it is rolled back.
     * </p>
     */
    @Pure
    public @Nullable Trader getProvisionedSeller(@ReadOnly ProvisionByDemand this) {
        return provisionedSeller_;
    }

    /**
     * Returns the seller that is used as a model in taking {@code this} action.
     */
    @Pure
    public @Nullable Trader getModelSeller(@ReadOnly ProvisionByDemand this) {
        return modelSeller_;
    }

    @Override
    public @NonNull String serialize(@NonNull Function<@NonNull Trader, @NonNull String> oid) {
        return new StringBuilder().append("<action type=\"provisionByDemand\" modelBuyer=\"")
            .append(oid.apply(getModelBuyer().getBuyer())).append("\" />").toString();
        // TODO: should I send the shopping list and basket bought instead?
    }

    @Override
    public @NonNull Action take() {
        @NonNull Basket basketSold = getModelBuyer().getBasket();
        provisionedSeller_ = getEconomy().addTrader(modelSeller_.getType(),
            TraderState.ACTIVE, basketSold /*, what should it buy? */);

        for (int i = 0 ; i < basketSold.size() ; ++i) {
            getProvisionedSeller().getCommoditiesSold().get(i).setCapacity(
                Math.max(getModelBuyer().getQuantity(i), getModelBuyer().getPeakQuantity(i)));
            // TODO (Vaptistis): use desired state once we have EconomySettings
        }

        return this;
    }

    @Override
    public @NonNull Action rollback() {
        getEconomy().removeTrader(provisionedSeller_);
        provisionedSeller_ = null;

        return this;
    }

    @Override
    public @NonNull String debugDescription(@NonNull Function<@NonNull Trader, @NonNull String> uuid,
                                            @NonNull Function<@NonNull Trader, @NonNull String> name,
                                            @NonNull IntFunction<@NonNull String> commodityType,
                                            @NonNull IntFunction<@NonNull String> traderType) {
        return new StringBuilder()
            .append("Provision a new ").append(traderType.apply(getModelBuyer().getBuyer().getType()/* Substitute correct type */))
            .append(" with the following characteristics: ").toString(); // TODO: print characteristics
    }

    @Override
    public @NonNull String debugReason(@NonNull Function<@NonNull Trader, @NonNull String> uuid,
                                       @NonNull Function<@NonNull Trader, @NonNull String> name,
                                       @NonNull IntFunction<@NonNull String> commodityType,
                                       @NonNull IntFunction<@NonNull String> traderType) {
        final @NonNull StringBuilder sb = new StringBuilder();
        sb.append("No ").append(traderType.apply(getModelBuyer().getBuyer().getType()/* Substitute correct type */))
          .append(" has enough capacity for ");
        appendTrader(sb, getModelBuyer().getBuyer(), uuid, name);
        sb.append(".");
        // TODO: update when we create the recommendation matrix for provisioning and possibly
        // create additional classes for provisioning actions.

        return sb.toString();
    }

    @Override
    public @NonNull Trader getActionTarget() {
        return getModelBuyer().getBuyer();
    }

    /**
     * Tests whether two ProvisionByDemand actions are equal field by field.
     */
    @Override
    @Pure
    public boolean equals(@ReadOnly ProvisionByDemand this,@ReadOnly Object other) {
        if (other == null || !(other instanceof ProvisionByDemand)) {
            return false;
        }
        ProvisionByDemand otherProvisionByDemand = (ProvisionByDemand)other;
        return otherProvisionByDemand.getEconomy().equals(getEconomy())
                        && otherProvisionByDemand.getModelBuyer().equals(getModelBuyer())
                        && otherProvisionByDemand.getModelSeller().equals(getModelSeller());
    }

    /**
     * Use the hashCode of each field to generate a hash code, consistent with {@link #equals(Object)}.
     */
    @Override
    @Pure
    public int hashCode() {
        return Hashing.md5().newHasher().putInt(getEconomy().hashCode())
                        .putInt(getModelBuyer().hashCode()).putInt(getModelSeller().hashCode())
                        .hash().asInt();
    }
} // end ProvisionByDemand class
