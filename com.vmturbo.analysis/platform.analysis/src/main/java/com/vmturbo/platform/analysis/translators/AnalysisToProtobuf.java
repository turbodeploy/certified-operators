package com.vmturbo.platform.analysis.translators;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.DoubleBinaryOperator;

import org.apache.log4j.Logger;
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
import com.vmturbo.platform.analysis.ledger.PriceStatement;
import com.vmturbo.platform.analysis.ledger.PriceStatement.TraderPriceStatement;
import com.vmturbo.platform.analysis.pricefunction.PriceFunction;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActionTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActivateTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.Compliance;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.CompoundMoveTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.Congestion;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.DeactivateTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.Evacuation;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.InitialPlacement;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.MoveExplanation;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.MoveTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.Performance;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ProvisionByDemandTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ProvisionByDemandTO.CommodityMaxAmountAvailableEntry;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ProvisionByDemandTO.CommodityNewCapacityEntry;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ProvisionBySupplyTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ReconfigureTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ResizeTO;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.AnalysisResults;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.AnalysisResults.NewShoppingListToBuyerEntry;
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
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs.PriceIndexMessage;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs.PriceIndexMessagePayload;
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


    private static final Logger logger = Logger.getLogger(AnalysisToProtobuf.class);

    private static final double MAX_PRICE_INDEX = 20000;
    private static final double MAX_REASON_COMMODITY = 2;
    // TODO: we need to do some experiment to get a reasonable threshold
    private static final double QUOTE_DIFF_THRESHOLD = 1;

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
            Trader newSupplier = move.getDestination();
            if (newSupplier == null) {
                logger.error("The destination for the move action is null!");
                return null;
            }

            MoveTO.Builder moveTO = MoveTO.newBuilder();
            moveTO.setShoppingListToMove(shoppingListOid.get(move.getTarget()));
            moveTO.setDestination(traderOid.get(newSupplier));
            moveTO = explainMoveAction(move.getSource(), newSupplier, traderOid, move, moveTO);
            builder.setMove(moveTO);

        } else if (input instanceof Reconfigure) {
            Reconfigure reconfigure = (Reconfigure)input;
            ReconfigureTO.Builder reconfigureTO = ReconfigureTO.newBuilder();

            reconfigureTO.setShoppingListToReconfigure(
                            shoppingListOid.get(reconfigure.getTarget()));
            if (reconfigure.getSource() != null) {
                reconfigureTO.setSource(traderOid.get(reconfigure.getSource()));
            }
            for (CommoditySpecification c : reconfigure.getTarget().getBasket()) {
                if (!reconfigure.getSource().getBasketSold().contains(c)) {
                    reconfigureTO.addCommodityToReconfigure(c.getBaseType());
                }
            }
            builder.setReconfigure(reconfigureTO);
        } else if (input instanceof Activate) {
            Activate activate = (Activate)input;
            builder.setActivate(ActivateTO.newBuilder()
                            .setTraderToActivate(traderOid.get(activate.getTarget()))
                            .setModelSeller(traderOid.get(activate.getModelSeller()))
                            .setMostExpensiveCommodity(findMostExpensiveCommodity(activate
                                            .getModelSeller()).getBaseType())
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
                            // the newly provisioned trader does not have OID, assign one for it
                            // and add the oid into BiMap traderOids_
                            .setProvisionedSeller(topology.addProvisionedTrader(
                                            provDemand.getProvisionedSeller()));
            // create shopping list OIDs for the provisioned shopping lists
            topology.getEconomy()
                .getMarketsAsBuyer(provDemand.getProvisionedSeller())
                .keySet()
                .stream()
                .forEach(topology::addProvisionedShoppingList);
            // send commodity to new capacity map to legacy market so that newly provisioned trader
            // gets the correct capacity
            provDemand.getCommodityNewCapacityMap().forEach((key, value) -> provDemandTO
                            .addCommodityNewCapacityEntry(CommodityNewCapacityEntry.newBuilder()
                                            .setCommodityBaseType(key)
                                            .setNewCapacity(value.floatValue()).build()));
             // send the commodity whose requested quantity can not be satisfied, its requested
            // amount and the max amount could be provided by any seller in market
            Basket basketSold = provDemand.getProvisionedSeller().getBasketSold();
            List<Trader> sellers = provDemand.getEconomy().getMarket(basketSold).getActiveSellers();

            provDemand.getCommodityNewCapacityMap().forEach((key, value) -> {
                int index = basketSold.indexOfBaseType(key);
                provDemandTO.addCommodityMaxAmountAvailable(CommodityMaxAmountAvailableEntry
                                .newBuilder().setCommodityBaseType(key).setMaxAmountAvailable((float)
                                                sellers.stream().max((s1, s2) ->
                                                Double.compare(s1.getCommoditiesSold()
                                                .get(index).getEffectiveCapacity(),
                                                s2.getCommoditiesSold().get(index)
                                                .getEffectiveCapacity())).get().getCommoditiesSold()
                                                .get(index).getEffectiveCapacity())
                                .setRequestedAmount((float)provDemand.getModelBuyer()
                                                .getQuantities()[provDemand.getModelBuyer()
                                                                 .getBasket().indexOfBaseType(key)])
                                .build());
            });
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
                // the newly provisioned trader does not have OID, assign one for it and add
                // the oid into BiMap traderOids_
                .setProvisionedSeller(topology.addProvisionedTrader(
                                provSupply.getProvisionedSeller()))
                .setMostExpensiveCommodity(findMostExpensiveCommodity(provSupply
                                .getModelSeller()).getBaseType());
            builder.setProvisionBySupply(provSupplyTO);
        } else if (input instanceof Resize) {
            Resize resize = (Resize)input;
            builder.setResize(ResizeTO.newBuilder()
                            .setSellingTrader(traderOid.get(resize.getSellingTrader()))
                .setSpecification(commoditySpecificationTO(resize.getResizedCommoditySpec()))
                .setOldCapacity((float)resize.getOldCapacity())
                .setNewCapacity((float)resize.getNewCapacity())
                // first multiply upper util bound with old capacity to get the old effective
                // capacity, then get start util by using start quantity divided by old effective
                // capacity
                .setStartUtilization((float)(resize.getResizedCommodity().getStartQuantity() /
                            (resize.getResizedCommodity().getSettings()
                                            .getUtilizationUpperBound() *
                                            resize.getOldCapacity())))
                .setEndUtilization((float)(resize.getResizedCommodity().getStartQuantity() /
                            resize.getResizedCommodity().getEffectiveCapacity())));
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
     * @param priceStatement contains all the {@link traderPriceStatement}s
     * @return The resulting {@link AnalysisResults} message.
     */
    public static @NonNull AnalysisResults analysisResults(@NonNull List<Action> actions,
                    @NonNull BiMap<@NonNull Trader, @NonNull Long> traderOid,
                    @NonNull BiMap<@NonNull ShoppingList, @NonNull Long> shoppingListOid,
                    long timeToAnalyze_ns, Topology topology, PriceStatement startPriceStatement) {
        AnalysisResults.Builder builder = AnalysisResults.newBuilder();
        for (Action action : actions) {
            ActionTO actionTO = actionTO(action, traderOid, shoppingListOid, topology);
            if (actionTO != null) {
                builder.addActions(actionTO);
            }
        }
        // New shoppingList map has to be created after action conversion, because assigning
        // oid for new shopping list happens in conversion
        builder.addAllNewShoppingListToBuyerEntry(createNewShoppingListToBuyerMap(topology));
        final UnmodifiableEconomy economy = topology.getEconomy();
        // compulate the endPriceIndex
        PriceStatement endPriceStatement = new PriceStatement().computePriceIndex(economy);
        List<TraderPriceStatement> startTraderPriceStmts = startPriceStatement.getTraderPriceStatements();
        // populate AnalysisResults with priceIndex info
        PriceIndexMessage.Builder piBuilder = PriceIndexMessage.newBuilder();
        int traderIndex = 0;
        int startPriceStatementSize = startPriceStatement.getTraderPriceStatements().size();
        for (TraderPriceStatement endTraderPriceStmt : endPriceStatement.getTraderPriceStatements()) {
            PriceIndexMessagePayload.Builder payloadBuilder = PriceIndexMessagePayload.newBuilder();
            Trader trader = economy.getTraders().get(traderIndex);
            // for inactive traders we don't care much about price index, so setting it to 0
            // if the inactive trader is a clone created in M2, skip it
            if (!trader.getState().isActive() && traderOid.containsKey(trader)) {
                payloadBuilder.setOid(traderOid.get(trader));
                double startPriceIndex = (traderIndex < startPriceStatementSize) ? startTraderPriceStmts.get(traderIndex).getPriceIndex() : 0;
                payloadBuilder.setPriceindexCurrent(Double.isInfinite(startPriceIndex)
                                ? MAX_PRICE_INDEX : startPriceIndex);
                payloadBuilder.setPriceindexProjected(0);
                piBuilder.addPayload(payloadBuilder.build());
            } else if (trader.getState() == TraderState.ACTIVE) {
                payloadBuilder.setOid(traderOid.get(trader));
                double startPriceIndex = (traderIndex < startPriceStatementSize) ? startTraderPriceStmts.get(traderIndex).getPriceIndex() : 0;
                payloadBuilder.setPriceindexCurrent(Double.isInfinite(startPriceIndex)
                                ? MAX_PRICE_INDEX : startPriceIndex);
                payloadBuilder.setPriceindexProjected(Double.isInfinite(endTraderPriceStmt.getPriceIndex())
                                ? MAX_PRICE_INDEX : endTraderPriceStmt.getPriceIndex());
                piBuilder.addPayload(payloadBuilder.build());
            }
            traderIndex++;
        }

        return builder.setTimeToAnalyzeNs(timeToAnalyze_ns).setPriceIndexMsg(piBuilder.build())
                        .build();
    }


    /**
     * Finds the commodity with the highest price for a given trader.
     *
     * @param modelSeller the model seller used in provision or activation
     * @return the {@link CommoditySpecification} of the most expensive commodity
     */
    private static CommoditySpecification findMostExpensiveCommodity(Trader modelSeller) {
        double mostExpensivePrice = 0;
        CommoditySpecification mostExpensiveComm = null;
        // we find the most expensive commodity at the start state to explain the provisionBySupply
        for (CommoditySold c : modelSeller.getCommoditiesSold()) {
            double usedPrice = c.getSettings().getPriceFunction().unitPrice(c.getStartQuantity() /
                            c.getEffectiveCapacity());
            double peakPrice = c.getSettings().getPriceFunction().unitPrice(Math.max(0,
                            c.getStartPeakQuantity() - c.getStartQuantity())
                               / (c.getEffectiveCapacity() - c.getSettings().getUtilizationUpperBound()
                                               * c.getStartQuantity()));
            double greaterPrice = usedPrice > peakPrice ? usedPrice : peakPrice;
            if (mostExpensivePrice < greaterPrice) {
                mostExpensivePrice = greaterPrice;
                mostExpensiveComm = modelSeller.getBasketSold().get(modelSeller.getCommoditiesSold().indexOf(c));
            }
        }
        return mostExpensiveComm;
    }

    /**
     * Convert the newShoppingListToBuyerMap to DTO.
     * @param topology The topology keeps trader oid and shopping list oid information.
     * @return The result {@link NewShoppingListToBuyerMap} message
     */
    public static @NonNull List<NewShoppingListToBuyerEntry> createNewShoppingListToBuyerMap(Topology topology) {
        List<NewShoppingListToBuyerEntry> entryList = new ArrayList<NewShoppingListToBuyerEntry>();
        topology.getNewShoppingListToBuyerMap().entrySet().forEach(entry -> {
            entryList.add(NewShoppingListToBuyerEntry.newBuilder().setNewShoppingList(entry
                            .getKey()).setBuyer(entry.getValue()).build());
        });
        return entryList;
    }

    /**
     * Generates the explanation for a given move action. The type of explanation could be
     * Compliance: move is because the old supplier failed to provide certain commodities
     * Congestion: move is because certain commodities utilization are lower at new supplier
     * Evacuation: move is because the old supplier suspended
     * InitialPlacement: move is to start placing the consumer at a proper supplier
     * Performance: move is to improve the overall performance
     *
     * @param oldSupplier the original supplier for the consumer
     * @param newSupplier the destination supplier of the move
     * @param traderOid A map for {@link Trader}s to their OIDs
     * @param move the move action to explain
     * @param moveTO the DTO for the move action to explain
     * @return The resulting {@link MoveTO.Builder}.
     */
    private static MoveTO.Builder explainMoveAction(Trader oldSupplier, Trader newSupplier,
                    @NonNull BiMap<@NonNull Trader, @NonNull Long> traderOid, Move move,
                    MoveTO.Builder moveTO) {
        if (oldSupplier == null) {
            // when the source does not exist, move is actually initial placement.
            moveTO.setMoveExplanation(MoveExplanation.newBuilder().setInitialPlacement(
                            InitialPlacement.newBuilder().build()).build());
        } else {
            // when the source exists, the move can be either a result of compliance,
            // cheaper quote or suspension.
            moveTO.setSource(traderOid.get(oldSupplier));
            // when old supplier is inactive, move is a result of supplier suspension.
            // TODO: we need to understand if old host is in failover state, could it still has
            // some consumers? If so, is it valid to consider move as a result of suspension?
            if (oldSupplier.getState() == TraderState.INACTIVE) {
                moveTO.setMoveExplanation(MoveExplanation.newBuilder().setEvacuation(Evacuation
                                .newBuilder().setSuspendedTrader(traderOid.get(oldSupplier)).build())
                                .build());
            } else {
                // old supplier exists and its state is Active
                Map<Double, List<Integer>> quoteDiffPerComm = new TreeMap<Double, List<Integer>>();
                Basket basketBought = move.getTarget().getBasket();
                Set<Integer> complianceCommSet = new HashSet<Integer>();
                // iterate over all comm that the shopping list requests, calculate at the old
                // supplier and the new supplier  the priceUsed and peakPriceUsed
                for (int i = 0; i < basketBought.size(); i++) {
                    CommoditySpecification commBought = basketBought.get(i);
                    CommoditySold oldCommSold = oldSupplier.getCommoditySold(commBought);
                    // if old supplier does not have the shopping list's requested commodity,
                    // move is due to compliance
                    if (oldCommSold == null) {
                        complianceCommSet.add(commBought.getBaseType());
                    } else {
                        CommoditySold newCommSold = newSupplier.getCommoditySold(commBought);
                        // if the comm utilization is less than min desired util, we dont consider
                        // it as reason commodities even though the quote at destination may be
                        // smaller, because from a user's point of view, move away from such a low
                        // utilized supplier is not congestion
                        if (oldCommSold.getUtilization() < oldCommSold.getSettings()
                                        .getUtilizationUpperBound() * oldSupplier.getSettings()
                                        .getMinDesiredUtil()) {
                            continue;
                        }

                        double quantityBought = move.getTarget().getQuantity(i);
                        double peakQuantityBought = move.getTarget().getPeakQuantity(i);
                        // calculate the price at the old and new supplier and get the quote
                        // difference
                        double oldQuote = calculateQuote(oldCommSold, quantityBought, peakQuantityBought);
                        double newQuote = calculateQuote(newCommSold, quantityBought, peakQuantityBought);
                        double quoteDiff = newQuote - oldQuote;

                        // if the difference in quote is positive, or it is not bigger than
                        // threshold, skip it
                        if (quoteDiff >= 0 && (Math.abs(quoteDiff) < QUOTE_DIFF_THRESHOLD)) {
                            continue;
                        }
                        if (quoteDiffPerComm.containsKey(quoteDiff)) {
                            quoteDiffPerComm.get(quoteDiff).add(commBought.getBaseType());
                        } else {
                            List<Integer> list = new ArrayList<>();
                            list.add(commBought.getBaseType());
                            quoteDiffPerComm.put(newQuote - oldQuote, list);
                        }
                    }

                }
                if (!complianceCommSet.isEmpty()) {
                    complianceCommSet.forEach(i -> moveTO.setMoveExplanation(MoveExplanation
                                    .newBuilder().setCompliance(Compliance.newBuilder()
                                                    .addMissingCommodities(i).build())).build());
                } else {
                    // move could be due to cheaper quote, we limit the number of congested commodity to 2
                    // which means we pick up the 2 commodities with most different quote.
                    Iterator<Entry<Double, List<Integer>>> iterator = quoteDiffPerComm.entrySet().iterator();
                    Congestion.Builder congestion = Congestion.newBuilder();
                    int counter = 0;
                    while(iterator.hasNext() && counter < MAX_REASON_COMMODITY) {
                        // if newQuote-oldQuote is less than 0, quote is cheaper at the new supplier
                        // otherwise, do not consider it
                        @SuppressWarnings("rawtypes")
                        Map.Entry entry = iterator.next();
                        if ((Double)entry.getKey() <= 0) {
                            @SuppressWarnings("unchecked")
                            List<Integer> commList = (List<Integer>)entry.getValue();
                            for (Integer i : commList) {
                                if (counter < MAX_REASON_COMMODITY) {
                                    congestion.addCongestedCommodities(i);
                                    counter++;
                                }
                            }
                        }
                    }
                    // if all commodities utilization are greater at destination, move is to improve
                    // overall performance
                    if (counter == 0) {
                        moveTO.setMoveExplanation(MoveExplanation.newBuilder().setPerformance(
                                        Performance.newBuilder().build()).build());
                    } else {
                        moveTO.setMoveExplanation(MoveExplanation.newBuilder().setCongestion(
                                        congestion.build()).build());
                    }
                }
            }
        }
        return moveTO;
    }

    /**
     * Computes the quote for a given {@link CommoditySold}.
     *
     * @param commSold The {@link CommoditySold} to compute the quote
     * @param quantityBought The quantity bought of the commodity
     * @param peakQuantityBought The peak quantity bought of the commodity
     * @return quote for the given {@link CommoditySold}
     */
    private static double calculateQuote(CommoditySold commSold, double quantityBought,
                    double peakQuantityBought) {
        PriceFunction pf = commSold.getSettings().getPriceFunction();
        double startQuantity = commSold.getStartQuantity();
        double startPeakQuantity = commSold.getStartPeakQuantity();
        double effectiveCapacity = commSold.getEffectiveCapacity();
        double excessQuantity = peakQuantityBought - quantityBought;

        double usedPrice = pf.unitPrice(startQuantity / effectiveCapacity);
        double peakPrice = pf.unitPrice(Math.max(0, startPeakQuantity - startQuantity)/
                                        (effectiveCapacity - commSold.getSettings()
                                                        .getUtilizationUpperBound()*startQuantity));

        return ((((quantityBought == 0) ? 0 : quantityBought * usedPrice) +
                        excessQuantity > 0 ? excessQuantity * peakPrice : 0)) / effectiveCapacity;
    }
} // end AnalysisToProtobuf class
