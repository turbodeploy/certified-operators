package com.vmturbo.platform.analysis.actions;

import java.util.Map;
import java.util.function.Function;

import com.google.common.collect.Lists;
import com.google.common.hash.Hashing;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderWithSettings;

/**
 * Action to Reconfigure a Trader by removing reconfigurable commodities.
 */
public class ReconfigureProviderRemoval extends ReconfigureProvider {

    /**
     * Generates ReconfigureProviderRemoval.
     *
     * @param economy - Economy.
     * @param provider - Provider to Reconfigure.
     * @param commodities - Commodities to remove.
     */
    public ReconfigureProviderRemoval(@NonNull Economy economy, @NonNull TraderWithSettings provider,
        @NonNull Map<CommoditySpecification, CommoditySold> commodities) {
        super(economy, provider, commodities);
    }

    @Override
    public @NonNull Action port(@NonNull Economy destinationEconomy,
            @NonNull Function<@NonNull Trader, @NonNull Trader> destinationTrader,
            @NonNull Function<@NonNull ShoppingList, @NonNull ShoppingList> destinationShoppingList) {
        return this;
    }

    @Override
    public boolean isValid() {
        return true;
    }

    @Override
    public @NonNull ReconfigureProviderRemoval take() {
        super.take();
        removeCommodities();
        return this;
    }

    @Override
    public @NonNull ReconfigureProviderRemoval rollback() {
        super.rollback();
        addCommodities();
        return this;
    }

    @Override
    public ActionType getType() {
        return ActionType.RECONFIGURE_PROVIDER_REMOVAL;
    }

    /**
     * Tests whether two ReconfigureCommodityRemoval actions are equal field by field.
     */
    @Override
    @Pure
    public boolean equals(@ReadOnly Object other) {
        if (!(other instanceof ReconfigureProviderRemoval)) {
            return false;
        }
        ReconfigureProviderRemoval otherAction = (ReconfigureProviderRemoval)other;
        return otherAction.getEconomy() == getEconomy()
            && otherAction.getActionTarget() == provider_
            && otherAction.getReconfiguredCommodities().keySet().equals(commodities_.keySet());
    }

    /**
     * Use the hashCode of each field to generate a hash code.
     */
    @Override
    @Pure
    public int hashCode() {
        return Hashing.md5().newHasher()
                        .putInt(getEconomy().hashCode())
                        .putInt(provider_.hashCode())
                        .putInt(commodities_.keySet().hashCode())
                        .hash()
                        .asInt();
    }

    @Override
    @Pure
    public @NonNull @ReadOnly Object getCombineKey() {
        return Lists.newArrayList(ReconfigureProviderRemoval.class, provider_,
            commodities_.keySet());
    }
}