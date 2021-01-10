package com.vmturbo.platform.analysis.actions;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderWithSettings;

/**
 * Action to Reconfigure a Provider.
 */
public abstract class ReconfigureProvider extends ReconfigureBase {

    /**
     * Price weight for price function of software license commodities that are currently the
     * Reconfigurable commodities.
     */
    protected static final int PRICE_WEIGHT_SCALE = 10;

    protected @NonNull TraderWithSettings provider_;
    protected @NonNull Map<CommoditySpecification, CommoditySold> commodities_;

    /**
     * ReconfigureProvider.
     *
     * @param economy - Economy.
     * @param provider - Provider to Reconfigure.
     * @param commodities - Commodities to be updated for provider.
     */
    public ReconfigureProvider(@NonNull Economy economy, @NonNull TraderWithSettings provider,
        @NonNull Map<CommoditySpecification, CommoditySold> commodities) {
        super(economy);
        provider_ = provider;
        commodities_ = commodities;
    }

    @Override
    public @NonNull Trader getActionTarget() {
        return provider_;
    }

    @Override
    @Pure
    public @Nullable @ReadOnly Action combine(@NonNull Action action) {
        ReconfigureProvider otherAction = (ReconfigureProvider)action;
        return new ReconfigureProviderRemoval(getEconomy(), provider_,
            Stream.of(commodities_, otherAction.getReconfiguredCommodities())
                .flatMap(map -> map.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (comm1, comm2)
                    -> comm1)));
    }

    public Map<CommoditySpecification, CommoditySold> getReconfiguredCommodities() {
        return commodities_;
    }

    protected void addCommodities() {
        Map<CommoditySpecification, CommoditySold> addedMap = new HashMap<>();
        commodities_.forEach((spec, comm) -> {
            CommoditySpecification newCommSpec = new CommoditySpecification(spec.getType(),
                spec.getBaseType()).setDebugInfoNeverUseInCode(spec.getDebugInfoNeverUseInCode());
            CommoditySold newCommoditySold = provider_.addCommoditySold(newCommSpec);
            newCommoditySold.setCapacity(comm.getCapacity()).setQuantity(comm.getQuantity());
            ProvisionBase.copyCommoditySoldSettingsForClone(newCommoditySold, comm);
            addedMap.put(newCommSpec, newCommoditySold);
        });
        commodities_ = addedMap;

        for (Market market : getEconomy().getMarkets()) {
            if (!market.isSellerPresent(provider_)
                && market.getBasket().isSatisfiedBy(provider_.getBasketSold())) {
                market.addSeller(provider_);
            }
        }

        provider_.getCommoditiesSold()
            .forEach(commSold -> commSold.getSettings().setPriceFunction(commSold.getSettings()
                .getPriceFunction().updatePriceFunctionWithWeight(provider_
                    .getReconfigureableCount(getEconomy()) * PRICE_WEIGHT_SCALE + 1)));
    }

    protected void removeCommodities() {
        commodities_.forEach((spec, comm) -> {
            provider_.removeCommoditySold(spec);
        });

        Market[] marketsToCheck = new Market[provider_.getMarketsAsSeller().size()];
        marketsToCheck = provider_.getMarketsAsSeller().toArray(marketsToCheck);
        for (Market market : marketsToCheck) {
            if (market.isSellerPresent(provider_)
                    && !market.getBasket().isSatisfiedBy(provider_.getBasketSold())) {
                market.removeSeller(provider_);
            }
        }

        provider_.getCommoditiesSold()
            .forEach(commSold -> commSold.getSettings().setPriceFunction(commSold.getSettings()
                .getPriceFunction().updatePriceFunctionWithWeight(provider_
                    .getReconfigureableCount(getEconomy()) * PRICE_WEIGHT_SCALE + 1)));
    }
}