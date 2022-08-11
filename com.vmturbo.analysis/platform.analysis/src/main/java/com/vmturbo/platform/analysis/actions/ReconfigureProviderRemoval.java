package com.vmturbo.platform.analysis.actions;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.IntUnaryOperator;

import com.google.common.collect.Lists;
import com.google.common.hash.Hashing;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.commons.analysis.NumericIDAllocator;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderWithSettings;
import com.vmturbo.platform.analysis.utilities.exceptions.ActionCantReplayException;

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
    public @NonNull ReconfigureProviderRemoval port(@NonNull Economy destinationEconomy,
            @NonNull Function<@NonNull Trader, @NonNull Trader> destinationTrader,
            @NonNull Function<@NonNull ShoppingList, @NonNull ShoppingList> destinationShoppingList) {
        return new ReconfigureProviderRemoval(destinationEconomy,
                (TraderWithSettings)destinationTrader.apply(getActionTarget()),
                getReconfiguredCommodities());
    }

    @Override
    public boolean isValid() {
        return getActionTarget().getSettings().isReconfigurable();
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

    /**
     * Extract commodity ids that appear in the action,
     * which includes commodities_.
     *
     * @return commodity ids that appear in the action
     */
    @Override
    public @NonNull Set<Integer> extractCommodityIds() {
        final IntOpenHashSet commodityIds = new IntOpenHashSet();
        for (CommoditySpecification commSpec : commodities_.keySet()) {
            commodityIds.add(commSpec.getType());
        }
        return commodityIds;
    }

    /**
     * Create the same type of action with new commodity ids.
     * Here we create a new reconfigurable commodity.
     *
     * @param commodityIdMapping a mapping from old commodity id to new commodity id.
     *                           If the returned value is {@link NumericIDAllocator#nonAllocableId},
     *                           it means no mapping is available for the input and we thrown an
     *                           {@link ActionCantReplayException}
     * @return a new action
     * @throws ActionCantReplayException ActionCantReplayException
     */
    @Override
    public @NonNull ReconfigureProviderRemoval createActionWithNewCommodityId(
            final IntUnaryOperator commodityIdMapping) throws ActionCantReplayException {
        for (int id : extractCommodityIds()) {
            if (commodityIdMapping.applyAsInt(id) == NumericIDAllocator.nonAllocableId) {
                throw new ActionCantReplayException(id);
            }
        }

        Map<CommoditySpecification, CommoditySold> newReasons = new HashMap<>();
        if (!commodities_.isEmpty()) {
            commodities_.forEach((commSpec, commSold) -> {
                newReasons.put(commSpec.createCommSpecWithNewCommodityId(commodityIdMapping
                    .applyAsInt(commSpec.getType())), commSold);
            });
        }

        return new ReconfigureProviderRemoval(getEconomy(), (TraderWithSettings)getActionTarget(), newReasons);
    }
}