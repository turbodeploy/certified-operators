package com.vmturbo.platform.analysis.translators;

import java.util.ArrayList;
import java.util.List;
import java.util.function.DoubleBinaryOperator;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import com.google.common.collect.BiMap;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.ActionImpl;
import com.vmturbo.platform.analysis.actions.Activate;
import com.vmturbo.platform.analysis.actions.CompoundMove;
import com.vmturbo.platform.analysis.actions.Deactivate;
import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.actions.ProvisionByDemand;
import com.vmturbo.platform.analysis.actions.ProvisionBySupply;
import com.vmturbo.platform.analysis.actions.Reconfigure;
import com.vmturbo.platform.analysis.actions.Resize;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.ShoppingList;
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
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.CompoundMoveTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.DeactivateTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.MoveTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ProvisionByDemandTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ProvisionByDemandTO.CommodityNewCapacityEntry;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ProvisionBySupplyTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ReconfigureTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ResizeTO;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.AnalysisResults;
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
import com.vmturbo.platform.analysis.protobuf.UpdatingFunctionDTOs.UpdatingFunctionTO;
import com.vmturbo.platform.analysis.protobuf.UpdatingFunctionDTOs.UpdatingFunctionTO.Max;
import com.vmturbo.platform.analysis.protobuf.UpdatingFunctionDTOs.UpdatingFunctionTO.Min;
import com.vmturbo.platform.analysis.topology.Topology;

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

    // Methods for converting UpdatingFunctionDTOs.

    /**
     * Converts a {@link DoubleBinaryOperator quantity updating function} to a
     * {@link UpdatingFunctionTO}.
     *
     * @param input The {@link DoubleBinaryOperator quantity updating function} to convert.
     * @return The resulting {@link UpdatingFunctionTO}.
     */
    public static @NonNull UpdatingFunctionTO updatingFunctionTO(@NonNull DoubleBinaryOperator input) {
        // Warning: converting updating functions to TOs is not properly supported!
        if (input == (DoubleBinaryOperator)Math::max) {
            return UpdatingFunctionTO.newBuilder().setMax(Max.newBuilder()).build();
        }  else if (input == (DoubleBinaryOperator)Math::min) {
            return UpdatingFunctionTO.newBuilder().setMin(Min.newBuilder()).build();
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
            .setBaseType(input.getBaseType())
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
            .setHistoricalQuantity((float)commodity.getHistoricalQuantity())
            .setHistoricalPeakQuantity((float)commodity.getHistoricalPeakQuantity())
            .setMaxQuantity((float)commodity.getMaxQuantity())
            .setCapacity((float)commodity.getCapacity())
            .setThin(commodity.isThin())
            .setSettings(commoditySoldSettingsTO(commodity.getSettings())).build();
    }

    /**
     * Converts a {@link ShoppingList} to a {@link ShoppingListTO} given some additional context.
     *
     * @param oid The OID used to refer to this {@link ShoppingList} across process boundaries.
     * @param economy The {@link Economy} containing <b>shopping list</b>.
     * @param shoppingList The {@link ShoppingList} to convert.
     * @return The resulting {@link ShoppingListTO}.
     */
    public static @NonNull ShoppingListTO shoppingListTO(long oid, @NonNull UnmodifiableEconomy economy,
                                                         @NonNull ShoppingList shoppingList) {
        ShoppingListTO.Builder builder = ShoppingListTO.newBuilder()
            .setOid(oid)
            .setMovable(shoppingList.isMovable());
        if (shoppingList.getSupplier() != null)
            builder.setSupplier(shoppingList.getSupplier().getEconomyIndex()); // only because we
                                                                    // are sending existing economy!
        Basket basketBought = economy.getMarket(shoppingList).getBasket();
        for (int i = 0; i < basketBought.size() ; ++i) {
            builder.addCommoditiesBought(commodityBoughtTO(shoppingList.getQuantity(i),
                                                           shoppingList.getPeakQuantity(i), basketBought.get(i)));
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
                        .setClonable(input.isCloneable()).setSuspendable(input.isSuspendable())
                        .setMinDesiredUtilization((float)input.getMinDesiredUtil())
                        .setMaxDesiredUtilization((float)input.getMaxDesiredUtil())
                        .setGuaranteedBuyer(input.isGuaranteedBuyer())
                        .setCanAcceptNewCustomers(input.canAcceptNewCustomers()).build();
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
            .addAllCliques(trader.getCliques())
            .setSettings(traderSettingsTO(trader.getSettings()));

        for (int i = 0 ; i < trader.getBasketSold().size() ; ++i) {
            builder.addCommoditiesSold(commoditySoldTO(trader.getCommoditiesSold().get(i), trader.getBasketSold().get(i)));
        }

        int i = 0; // Warning: the computation of shopping list oid is just a hack. Need to replace!
        for (@NonNull ShoppingList shoppingList : economy.getMarketsAsBuyer(trader).keySet()) {
            builder.addShoppingLists(shoppingListTO((trader.getEconomyIndex() << 10) + i++, economy, shoppingList));
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
     * @param traderOid A map for {@link Trader}s to their OIDs.
     * @param shoppingListOid A map for {@link ShoppingList}s to their OIDs.
     * @param topology The topology associates with traders received from legacy market.
     * It keeps a traderOid map which will be used to populate the oid for traders.
     * @return The resulting {@link ActionTO}.
     */
    public static @NonNull ActionTO actionTO(@NonNull Action input,
                    @NonNull BiMap<@NonNull Trader, @NonNull Long> traderOid,
                    @NonNull BiMap<@NonNull ShoppingList, @NonNull Long> shoppingListOid,
                    Topology topology) {
        ActionTO.Builder builder = ActionTO.newBuilder();

        if (input instanceof Move) {
            Move move = (Move)input;
            MoveTO.Builder moveTO = MoveTO.newBuilder();

            moveTO.setShoppingListToMove(shoppingListOid.get(move.getTarget()));
            if (move.getSource() != null) {
                moveTO.setSource(traderOid.get(move.getSource()));
            }
            if (move.getDestination() != null) {
                moveTO.setDestination(traderOid.get(move.getDestination()));
            }
            builder.setMove(moveTO);
        } else if (input instanceof Reconfigure) {
            Reconfigure reconfigure = (Reconfigure)input;
            ReconfigureTO.Builder reconfigureTO = ReconfigureTO.newBuilder();

            reconfigureTO.setShoppingListToReconfigure(
                            shoppingListOid.get(reconfigure.getTarget()));
            if (reconfigure.getSource() != null) {
                reconfigureTO.setSource(traderOid.get(reconfigure.getSource()));
            }
            builder.setReconfigure(reconfigureTO);
        } else if (input instanceof Activate) {
            Activate activate = (Activate)input;
            builder.setActivate(ActivateTO.newBuilder()
                            .setTraderToActivate(traderOid.get(activate.getTarget()))
                            .setModelSeller(traderOid.get(activate.getModelSeller()))
                .addAllTriggeringBasket(specificationTOs(activate.getSourceMarket().getBasket())));
        } else if (input instanceof Deactivate) {
            Deactivate deactivate = (Deactivate)input;
            builder.setDeactivate(DeactivateTO.newBuilder()
                            .setTraderToDeactivate(traderOid.get(deactivate.getTarget()))
                            .addAllTriggeringBasket(specificationTOs(
                                            deactivate.getSourceMarket().getBasket())));
        } else if (input instanceof ProvisionByDemand) {
            ProvisionByDemand provDemand = (ProvisionByDemand)input;
            ProvisionByDemandTO.Builder provDemandTO = ProvisionByDemandTO.newBuilder()
                            .setModelBuyer(shoppingListOid.get(provDemand.getModelBuyer()))
                            .setModelSeller(traderOid.get(provDemand.getModelSeller()))
                            // the newly provisioned trader does not have OID, assign one for it and adds
                            // the oid into BiMap traderOids_
                            .setProvisionedSeller(topology.addProvisionedTrader(
                                            provDemand.getProvisionedSeller()));
            // send commodity to new capacity map to legacy market so that newly provisioned trader
            // gets the correct capacity
            provDemand.getCommodityNewCapacityMap().forEach((key, value) -> provDemandTO
                            .addCommodityNewCapacityEntry(CommodityNewCapacityEntry.newBuilder()
                                            .setCommodityBaseType(key)
                                            .setNewCapacity(value.floatValue()).build()));
            builder.setProvisionByDemand(provDemandTO);
        } else if (input instanceof ProvisionBySupply) {
            ProvisionBySupply provSupply = (ProvisionBySupply)input;
            // create shopping list OIDs for the provisioned shopping lists
            topology.getEconomy()
            	.getMarketsAsBuyer(provSupply.getProvisionedSeller())
            	.keySet()
            	.stream()
            	.forEach(topology::addProvisionedShoppingList);
            ProvisionBySupplyTO.Builder provSupplyTO = ProvisionBySupplyTO.newBuilder()
                            .setModelSeller(traderOid.get(provSupply.getModelSeller()))
                            // the newly provisioned trader does not have OID, assign one for it and adds
                            // the oid into BiMap traderOids_
                            .setProvisionedSeller(topology.addProvisionedTrader(
                                            provSupply.getProvisionedSeller()));
            builder.setProvisionBySupply(provSupplyTO);
        } else if (input instanceof Resize) {
            Resize resize = (Resize)input;
            builder.setResize(ResizeTO.newBuilder()
                            .setSellingTrader(traderOid.get(resize.getSellingTrader()))
                .setSpecification(commoditySpecificationTO(resize.getResizedCommoditySpec()))
                .setOldCapacity((float)resize.getOldCapacity())
                .setNewCapacity((float)resize.getNewCapacity()));
        } else if (input instanceof CompoundMove) {
            CompoundMove compoundMove = (CompoundMove)input;
            CompoundMoveTO.Builder compoundMoveTO = CompoundMoveTO.newBuilder();
            for (Move m : compoundMove.getConstituentMoves()) {
                compoundMoveTO.addMoves(AnalysisToProtobuf.actionTO(m, traderOid, shoppingListOid,
                                topology).getMove());
            }
            builder.setCompoundMove(compoundMoveTO);
        }
        builder.setImportance(((ActionImpl)input).getImportance());
        return builder.build();
    }

    // Methods for converting CommunicationDTOs.

    /**
     * Converts a list of {@link Action}s to an {@link AnalysisResults} message given some
     * additional context.
     *
     * @param actions The list of {@link Action}s to convert.
     * @param traderOid A function mapping {@link Trader}s to their OIDs.
     * @param shoppingListOid A function mapping {@link ShoppingList}s to their OIDs.
     * @param timeToAnalyze_ns The amount of time it took to analyze the topology and produce the
     *        list of actions in nanoseconds.
     * @param topology The topology associates with traders received from legacy market.
     * It keeps a traderOid map which will be used to populate the oid for traders.
     * @return The resulting {@link AnalysisResults} message.
     */
    public static @NonNull AnalysisResults analysisResults(@NonNull List<Action> actions,
                    @NonNull BiMap<@NonNull Trader, @NonNull Long> traderOid,
                    @NonNull BiMap<@NonNull ShoppingList, @NonNull Long> shoppingListOid,
                    long timeToAnalyze_ns, Topology topology) {
        AnalysisResults.Builder builder = AnalysisResults.newBuilder();

        for (Action action : actions) {
            builder.addActions(actionTO(action, traderOid, shoppingListOid, topology));
        }

        return builder.setTimeToAnalyzeNs(timeToAnalyze_ns).build();
    }

} // end AnalysisToProtobuf class
