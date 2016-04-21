package com.vmturbo.platform.analysis.translators;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.BuyerParticipation;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySoldSettings;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderSettings;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.economy.UnmodifiableEconomy;
import com.vmturbo.platform.analysis.pricefunction.PriceFunction;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.CommodityBoughtTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.CommoditySoldSettingsTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.CommoditySoldTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.CommoditySpecificationTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.ShoppingListTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderSettingsTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderStateTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.analysis.protobuf.PriceFunctionDTOs.PriceFunctionTO;
import com.vmturbo.platform.analysis.protobuf.PriceFunctionDTOs.PriceFunctionTO.Constant;
import com.vmturbo.platform.analysis.protobuf.PriceFunctionDTOs.PriceFunctionTO.StandardWeighted;
import com.vmturbo.platform.analysis.protobuf.PriceFunctionDTOs.PriceFunctionTO.Step;

/**
 * A class containing methods to convert java classes used by analysis to protobuf messages.
 */
public final class AnalysisToProtobuf {
    // Methods
    public static @NonNull CommoditySpecificationTO commoditySpecificationTO(@NonNull CommoditySpecification input) {
        return CommoditySpecificationTO.newBuilder()
            .setType(input.getType())
            .setQualityLowerBound(input.getQualityLowerBound())
            .setQualityUpperBound(input.getQualityUpperBound()).build();
    }

    public static @NonNull CommodityBoughtTO commodityBoughtTO(double quantity, double peakQuantity,
                                                               @NonNull CommoditySpecification specification) {
        return CommodityBoughtTO.newBuilder()
            .setQuantity(quantity)
            .setPeakQuantity(peakQuantity)
            .setSpecification(commoditySpecificationTO(specification)).build();
    }

    public static @NonNull PriceFunctionTO priceFunctionTO(@NonNull PriceFunction input) {
        // Warning: converting price functions to TOs is not properly supported!
        PriceFunctionTO.Builder builder = PriceFunctionTO.newBuilder();

        if (input == PriceFunction.Cache.createStandardWeightedPriceFunction(1.0)) {
            builder.setStandardWeighted(StandardWeighted.newBuilder().setWeight(1.0));
        } else if (input == PriceFunction.Cache.createConstantPriceFunction(0.0)){
            builder.setConstant(Constant.newBuilder().setValue(0.0));
        } else if (input == PriceFunction.Cache.createConstantPriceFunction(27.0)) {
            builder.setConstant(Constant.newBuilder().setValue(27.0));
        } else if (input == PriceFunction.Cache.createStepPriceFunction(1.0, 0.0, 20000.0)) {
            builder.setStep(Step.newBuilder().setStepAt(1.0).setPriceBefore(0.0).setPriceAfter(20000.0));
        }

        return builder.build();
    }

    public static @NonNull CommoditySoldSettingsTO commoditySoldSettingsTO(@NonNull CommoditySoldSettings input) {
        return CommoditySoldSettingsTO.newBuilder()
            .setResizable(input.isResizable())
            .setCapacityLowerBound(input.getCapacityLowerBound())
            .setCapacityUpperBound(input.getCapacityUpperBound())
            .setCapacityIncrement(input.getCapacityIncrement())
            .setUtilizationUpperBound(input.getUtilizationUpperBound())
            .setPriceFunction(priceFunctionTO(input.getPriceFunction())).build();
    }

    public static @NonNull CommoditySoldTO commoditySoldTO(@NonNull CommoditySold commodity,
                                                           @NonNull CommoditySpecification specification) {
        return CommoditySoldTO.newBuilder()
            .setSpecification(commoditySpecificationTO(specification))
            .setQuantity(commodity.getQuantity())
            .setPeakQuantity(commodity.getPeakQuantity())
            .setCapacity(commodity.getCapacity())
            .setThin(commodity.isThin())
            .setSettings(commoditySoldSettingsTO(commodity.getSettings())).build();
    }

    public static @NonNull ShoppingListTO shoppingListTO(long oid, @NonNull UnmodifiableEconomy economy,
                                                         @NonNull BuyerParticipation participation) {
        ShoppingListTO.Builder builder = ShoppingListTO.newBuilder()
            .setOid(oid)
            .setMovable(participation.isMovable());
        if (participation.getSupplier() != null)
            builder.setSupplier(participation.getSupplier().getEconomyIndex()); // only because we
                                                                    // are sending existing economy!
        Basket basketBought = economy.getMarket(participation).getBasket();
        for (int i = 0; i < basketBought.size() ; ++i) {
            builder.addCommoditiesBought(commodityBoughtTO(participation.getQuantity(i),
                                                           participation.getPeakQuantity(i), basketBought.get(i)));
        }

        return builder.build();
    }

    public static @NonNull TraderSettingsTO traderSettingsTO(@NonNull TraderSettings input) {
        return TraderSettingsTO.newBuilder()
            .setClonable(input.isCloneable())
            .setSuspendable(input.isSuspendable())
            .setMinDesiredUtilization(input.getMinDesiredUtil())
            .setMaxDesiredUtilization(input.getMaxDesiredUtil()).build();
    }

    public static @NonNull TraderStateTO traderStateTO(@NonNull TraderState state) {
        switch (state) {
            case ACTIVE:
                return TraderStateTO.ACTIVE;
            case INACTIVE:
                return TraderStateTO.INACTIVE;
            default:
                throw new IllegalArgumentException("Unknown enumerator: " + state);
        }
    }

    public static @NonNull TraderTO traderTO(@NonNull UnmodifiableEconomy economy, @NonNull Trader trader) {
        TraderTO.Builder builder = TraderTO.newBuilder()
            .setOid(trader.getEconomyIndex()) // only because we are sending an existing economy!
            .setType(trader.getType())
            .setState(traderStateTO(trader.getState()))
            .setSettings(traderSettingsTO(trader.getSettings()));

        for (int i = 0 ; i < trader.getBasketSold().size() ; ++i) {
            builder.addCommoditiesSold(commoditySoldTO(trader.getCommoditiesSold().get(i), trader.getBasketSold().get(i)));
        }

        int i = 0; // Warning: the computation of shopping list oid is just a hack. Need to replace!
        for (@NonNull BuyerParticipation participation : economy.getMarketsAsBuyer(trader).keySet()) {
            builder.addShoppingLists(shoppingListTO((trader.getEconomyIndex() << 10) + i++, economy, participation));
        }

        return builder.build();
    }

} // end AnalysisToProtobuf class
