package com.vmturbo.platform.analysis.translators;

import java.util.ArrayList;
import java.util.List;
import java.util.function.DoubleBinaryOperator;
import java.util.function.ToLongFunction;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.Activate;
import com.vmturbo.platform.analysis.actions.Deactivate;
import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.actions.ProvisionByDemand;
import com.vmturbo.platform.analysis.actions.ProvisionBySupply;
import com.vmturbo.platform.analysis.actions.Reconfigure;
import com.vmturbo.platform.analysis.actions.Resize;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.BuyerParticipation;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySoldSettings;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderSettings;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.economy.UnmodifiableEconomy;
import com.vmturbo.platform.analysis.pricefunction.PriceFunction;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActionTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActivateTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.DeactivateTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.MoveTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ProvisionByDemandTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ProvisionBySupplyTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ReconfigureTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ResizeTO;
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
import com.vmturbo.platform.analysis.protobuf.QuantityUpdatingFunctionDTOs.QuantityUpdatingFunctionTO;
import com.vmturbo.platform.analysis.protobuf.QuantityUpdatingFunctionDTOs.QuantityUpdatingFunctionTO.Max;

/**
 * A class containing methods to convert java classes used by analysis to Protobuf messages.
 *
 * <p>
 *  This is intended to contain only static methods.
 * </p>
 */
public final class AnalysisToProtobuf {
    // Methods for converting PriceFunctionDTOs.

    /**
     * Converts a {@link PriceFunction} to a {@link PriceFunctionTO}.
     *
     * @param input The {@link PriceFunction} to convert.
     * @return The resulting {@link PriceFunctionTO}.
     */
    public static @NonNull PriceFunctionTO priceFunctionTO(@NonNull PriceFunction input) {
        // Warning: converting price functions to TOs is not properly supported!
        PriceFunctionTO.Builder builder = PriceFunctionTO.newBuilder();

        if (input == PriceFunction.Cache.createStandardWeightedPriceFunction(1.0)) {
            builder.setStandardWeighted(StandardWeighted.newBuilder().setWeight(1.0f));
        } else if (input == PriceFunction.Cache.createConstantPriceFunction(0.0)){
            builder.setConstant(Constant.newBuilder().setValue(0.0f));
        } else if (input == PriceFunction.Cache.createConstantPriceFunction(27.0f)) {
            builder.setConstant(Constant.newBuilder().setValue(27.0f));
        } else if (input == PriceFunction.Cache.createStepPriceFunction(1.0, 0.0, 20000.0)) {
            builder.setStep(Step.newBuilder().setStepAt(1.0f).setPriceBelow(0.0f).setPriceAbove(20000.0f));
        }

        return builder.build();
    }

    // Methods for converting QuantityUpdatingFunctionDTOs.

    /**
     * Converts a {@link DoubleBinaryOperator quantity updating function} to a
     * {@link QuantityUpdatingFunctionTO}.
     *
     * @param input The {@link DoubleBinaryOperator quantity updating function} to convert.
     * @return The resulting {@link QuantityUpdatingFunctionTO}.
     */
    public static @NonNull QuantityUpdatingFunctionTO quantityUpdatingFunctionTO(@NonNull DoubleBinaryOperator input) {
        // Warning: converting quantity updating functions to TOs is not properly supported!
        if (input == (DoubleBinaryOperator)Math::max) {
            return QuantityUpdatingFunctionTO.newBuilder().setMax(Max.newBuilder()).build();
        } else {
            throw new IllegalArgumentException("input = " + input);
        }
    }

    // Methods for converting EconomyDTOs.

    /**
     * Converts a {@link CommoditySpecification} to a {@link CommoditySpecificationTO}.
     *
     * @param input The {@link CommoditySpecification} to convert.
     * @return The resulting {@link CommoditySpecificationTO}
     */
    public static @NonNull CommoditySpecificationTO commoditySpecificationTO(@NonNull CommoditySpecification input) {
        return CommoditySpecificationTO.newBuilder()
            .setType(input.getType())
            .setQualityLowerBound(input.getQualityLowerBound())
            .setQualityUpperBound(input.getQualityUpperBound()).build();
    }

    /**
     * Packs a quantity, a peak quantity and a {@link CommoditySpecification} as a {@link CommodityBoughtTO}.
     *
     * @param quantity The quantity to pack.
     * @param peakQuantity The peak quantity to pack.
     * @param specification The {@link CommoditySpecification} to pack.
     * @return The resulting {@link CommodityBoughtTO}.
     */
    public static @NonNull CommodityBoughtTO commodityBoughtTO(double quantity, double peakQuantity,
                                                               @NonNull CommoditySpecification specification) {
        return CommodityBoughtTO.newBuilder()
            .setQuantity((float)quantity)
            .setPeakQuantity((float)peakQuantity)
            .setSpecification(commoditySpecificationTO(specification)).build();
    }

    /**
     * Converts a {@link CommoditySoldSettings} instance to a {@link CommoditySoldSettingsTO}.
     *
     * @param input The {@link CommoditySoldSettings} instance to convert.
     * @return The resulting {@link CommoditySoldSettingsTO}
     */
    public static @NonNull CommoditySoldSettingsTO commoditySoldSettingsTO(@NonNull CommoditySoldSettings input) {
        return CommoditySoldSettingsTO.newBuilder()
            .setResizable(input.isResizable())
            .setCapacityLowerBound((float)input.getCapacityLowerBound())
            .setCapacityUpperBound((float)input.getCapacityUpperBound())
            .setCapacityIncrement((float)input.getCapacityIncrement())
            .setUtilizationUpperBound((float)input.getUtilizationUpperBound())
            .setPriceFunction(priceFunctionTO(input.getPriceFunction())).build();
    }

    /**
     * Packs a {@link CommoditySold} and a {@link CommoditySpecification} as a {@link CommoditySoldTO}.
     *
     * @param commodity The {@link CommoditySold} to pack.
     * @param specification The {@link CommoditySpecification} to pack.
     * @return The resulting {@link CommoditySoldTO}.
     */
    public static @NonNull CommoditySoldTO commoditySoldTO(@NonNull CommoditySold commodity,
                                                           @NonNull CommoditySpecification specification) {
        return CommoditySoldTO.newBuilder()
            .setSpecification(commoditySpecificationTO(specification))
            .setQuantity((float)commodity.getQuantity())
            .setPeakQuantity((float)commodity.getPeakQuantity())
            .setCapacity((float)commodity.getCapacity())
            .setThin(commodity.isThin())
            .setSettings(commoditySoldSettingsTO(commodity.getSettings())).build();
    }

    /**
     * Converts a {@link BuyerParticipation} to a {@link ShoppingListTO} given some additional context.
     *
     * @param oid The OID used to refer to this {@link BuyerParticipation} across process boundaries.
     * @param economy The {@link Economy} containing <b>participation</b>.
     * @param participation The {@link BuyerParticipation} to convert.
     * @return The resulting {@link ShoppingListTO}.
     */
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

    /**
     * Converts a {@link TraderSettings} instance to a {@link TraderSettingsTO}.
     *
     * @param input The {@link TraderSettings} instance to convert.
     * @return The resulting {@link TraderSettingsTO}
     */
    public static @NonNull TraderSettingsTO traderSettingsTO(@NonNull TraderSettings input) {
        return TraderSettingsTO.newBuilder()
            .setClonable(input.isCloneable())
            .setSuspendable(input.isSuspendable())
            .setMinDesiredUtilization((float)input.getMinDesiredUtil())
            .setMaxDesiredUtilization((float)input.getMaxDesiredUtil()).build();
    }

    /**
     * Converts a {@link TraderState} instance to a {@link TraderStateTO}.
     *
     * @param input The {@link TraderState} instance to convert.
     * @return The resulting {@link TraderStateTO}
     */
    public static @NonNull TraderStateTO traderStateTO(@NonNull TraderState input) {
        switch (input) {
            case ACTIVE:
                return TraderStateTO.ACTIVE;
            case INACTIVE:
                return TraderStateTO.INACTIVE;
            default:
                throw new IllegalArgumentException("Unknown enumerator: " + input);
        }
    }

    /**
     * Converts a {@link Trader} to a {@link TraderTO} given some additional context.
     *
     * @param economy The {@link Economy} containing <b>trader</b>.
     * @param trader The {@link Trader} to convert.
     * @return The resulting {@link TraderTO}.
     */
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

    // Methods for converting ActionDTOs.

    /**
     * Converts a {@link Basket} to a list of {@link CommoditySpecificationTO}s.
     *
     * @param input The {@link Basket} to convert.
     * @return The resulting list of {@link CommoditySpecificationTO}s.
     */
    // TODO: consider creating a BasketTO instead of using list.
    public static @NonNull List<CommoditySpecificationTO> specificationTOs(@NonNull Basket input) {
        List<CommoditySpecificationTO> output = new ArrayList<>();

        for (@NonNull @ReadOnly CommoditySpecification specification : input) {
            output.add(commoditySpecificationTO(specification));
        }

        return output;
    }

    /**
     * Converts an {@link Action} to an {@link ActionTO} given some additional context.
     *
     * @param input The {@link Action} to convert.
     * @param traderOid A function mapping {@link Trader}s to their OIDs.
     * @param participationOid A function mapping {@link BuyerParticipation}s to their OIDs.
     * @return The resulting {@link ActionTO}.
     */
    public static @NonNull ActionTO actionTO(@NonNull Action input, @NonNull ToLongFunction<@NonNull Trader> traderOid,
                                             @NonNull ToLongFunction<@NonNull BuyerParticipation> participationOid) {
        ActionTO.Builder builder = ActionTO.newBuilder();

        if (input instanceof Move) {
            Move move = (Move)input;
            MoveTO.Builder moveTO = MoveTO.newBuilder();

            moveTO.setShoppingListToMove(participationOid.applyAsLong(move.getTarget()));
            if (move.getSource() != null) {
                moveTO.setSource(traderOid.applyAsLong(move.getSource()));
            }
            if (move.getDestination() != null) {
                moveTO.setDestination(traderOid.applyAsLong(move.getDestination()));
            }
            builder.setMove(moveTO);
        } else if (input instanceof Reconfigure) {
            Reconfigure reconfigure = (Reconfigure)input;
            ReconfigureTO.Builder reconfigureTO = ReconfigureTO.newBuilder();

            reconfigureTO.setShoppingListToReconfigure(participationOid.applyAsLong(reconfigure.getTarget()));
            if (reconfigure.getSource() != null) {
                reconfigureTO.setSource(traderOid.applyAsLong(reconfigure.getSource()));
            }
            builder.setReconfigure(reconfigureTO);
        } else if (input instanceof Activate) {
            Activate activate = (Activate)input;
            builder.setActivate(ActivateTO.newBuilder()
                .setTraderToActivate(traderOid.applyAsLong(activate.getTarget()))
                .addAllTriggeringBasket(specificationTOs(activate.getSourceMarket().getBasket())));
        } else if (input instanceof Deactivate) {
            Deactivate deactivate = (Deactivate)input;
            builder.setDeactivate(DeactivateTO.newBuilder()
                .setTraderToDeactivate(traderOid.applyAsLong(deactivate.getTarget()))
                .addAllTriggeringBasket(specificationTOs(deactivate.getSourceMarket().getBasket())));
        } else if (input instanceof ProvisionByDemand) {
            builder.setProvisionByDemand(ProvisionByDemandTO.newBuilder()
                .setModelBuyer(participationOid.applyAsLong(((ProvisionByDemand)input).getModelBuyer())));
        } else if (input instanceof ProvisionBySupply) {
            builder.setProvisionBySupply(ProvisionBySupplyTO.newBuilder()
                .setModelSeller(traderOid.applyAsLong(((ProvisionBySupply)input).getModelSeller())));
        } else if (input instanceof Resize) {
            Resize resize = (Resize)input;
            builder.setResize(ResizeTO.newBuilder()
                .setSellingTrader(traderOid.applyAsLong(resize.getSellingTrader()))
                .setSpecification(commoditySpecificationTO(resize.getResizedCommodity()))
                .setOldCapacity((float)resize.getOldCapacity())
                .setNewCapacity((float)resize.getNewCapacity()));
        }

        return builder.build();
    }

} // end AnalysisToProtobuf class
