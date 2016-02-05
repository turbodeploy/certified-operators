package com.vmturbo.platform.analysis.actions;

import java.util.function.Function;
import java.util.function.IntFunction;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;

/**
 * An action to provision a new {@link Trader seller} using another {@link Trader buyer} as the
 * template.
 */
public class ProvisionByDemand implements Action {
    // Fields
    private final @NonNull Economy economy_;
    private final @NonNull Trader modelBuyer_; // TODO: also add source market? Desired state?
    private @Nullable Trader provisionedSeller_;

    // Constructors

    /**
     * Constructs a new ProvisionByDemand action with the specified attributes.
     *
     * @param economy The economy in which the seller will be provisioned.
     * @param modelBuyer The buyer that should be satisfied by the new seller.
     */
    public ProvisionByDemand(@NonNull Economy economy, @NonNull Trader modelBuyer) {
        economy_ = economy;
        modelBuyer_ = modelBuyer;
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
    public @NonNull Trader getModelBuyer(@ReadOnly ProvisionByDemand this) {
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

    @Override
    public @NonNull String serialize(@NonNull Function<@NonNull Trader, @NonNull String> oid) {
        return new StringBuilder().append("<action type=\"provisionByDemand\" modelBuyer=\"")
            .append(oid.apply(getModelBuyer())).append("\" />").toString();
    }

    @Override
    public void take() {
        provisionedSeller_ = getEconomy().addTrader(0 /* what type should it have? */,
            TraderState.ACTIVE, null /* basket to sell */ /*, what should it buy? */);
        // TODO: update commodities
    }

    @Override
    public void rollback() {
        getEconomy().removeTrader(provisionedSeller_);
        provisionedSeller_ = null;
    }

    @Override
    public @NonNull String debugDescription(@NonNull Function<@NonNull Trader, @NonNull String> oid,
                                            @NonNull Function<@NonNull Trader, @NonNull String> name,
                                            @NonNull IntFunction<@NonNull String> commodityType,
                                            @NonNull IntFunction<@NonNull String> traderType) {
        return new StringBuilder()
            .append("Provision a new ").append(traderType.apply(getModelBuyer().getType()/* Substitute correct type */))
            .append(" with the following characteristics: ").toString(); // TODO: print characteristics
    }

    @Override
    public @NonNull String debugReason(@NonNull Function<@NonNull Trader, @NonNull String> oid,
                                       @NonNull Function<@NonNull Trader, @NonNull String> name,
                                       @NonNull IntFunction<@NonNull String> commodityType,
                                       @NonNull IntFunction<@NonNull String> traderType) {
        return new StringBuilder()
            .append("No ").append(traderType.apply(getModelBuyer().getType()/* Substitute correct type */))
            .append(" has enough capacity for [buyer].").toString();
        // TODO: update when we create the recommendation matrix for provisioning and possibly
        // create additional classes for provisioning actions.
    }
} // end ProvisionByDemand class
