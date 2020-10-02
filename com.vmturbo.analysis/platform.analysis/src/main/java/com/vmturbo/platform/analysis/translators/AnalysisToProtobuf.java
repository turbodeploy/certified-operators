package com.vmturbo.platform.analysis.translators;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BiMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;

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
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySoldSettings;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderSettings;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.economy.UnmodifiableEconomy;
import com.vmturbo.platform.analysis.ede.QuoteMinimizer;
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
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.MoveTO.CommodityContext;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.Performance;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ProvisionByDemandTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ProvisionByDemandTO.CommodityMaxAmountAvailableEntry;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ProvisionByDemandTO.CommodityNewCapacityEntry;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ProvisionBySupplyTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ReconfigureTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ResizeTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ResizeTriggerTraderTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommodityBoughtTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldSettingsTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySpecificationTO;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.AnalysisResults;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.AnalysisResults.NewShoppingListToBuyerEntry;
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
import com.vmturbo.platform.analysis.topology.Topology;
import com.vmturbo.platform.analysis.utilities.FunctionalOperator;

/**
 * A class containing methods to convert java classes used by analysis to Protobuf messages.
 *
 * <p>
 *  This is intended to contain only static methods.
 * </p>
 */
public final class AnalysisToProtobuf {


    private static final Logger logger = LogManager.getLogger(AnalysisToProtobuf.class);

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
            .setBaseType(input.getBaseType()).build();
    }

    /**
     * Packs a quantity, a peak quantity and a {@link CommoditySpecification} as a {@link CommodityBoughtTO}.
     *
     * @param quantity The quantity to pack.
     * @param peakQuantity The peak quantity to pack.
     * @param assignedCapacity The assigned capacity to pack.
     * @param specification The {@link CommoditySpecification} to pack.
     * @return The resulting {@link CommodityBoughtTO}.
     */
    public static @NonNull CommodityBoughtTO commodityBoughtTO(double quantity,
                                                               double peakQuantity,
                                                               double assignedCapacity,
                                                               @NonNull CommoditySpecification specification) {
        return CommodityBoughtTO.newBuilder()
            .setQuantity((float)quantity)
            .setPeakQuantity((float)peakQuantity)
            .setAssignedCapacityForBuyer((float)assignedCapacity)
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
     * provisioned traders
     * @return The resulting {@link ShoppingListTO}.
     */
    public static @NonNull ShoppingListTO shoppingListTO(long oid, @NonNull UnmodifiableEconomy economy,
                    @NonNull ShoppingList shoppingList) {
        ShoppingListTO.Builder builder = ShoppingListTO.newBuilder()
            .setOid(oid)
            .setMovable(shoppingList.isMovable());

        if (shoppingList.getContext().isPresent()) {
            builder.setContext(shoppingList.getContext().get());
        }
        // This mirrors the behavior in AnalysisToProtobuf::actionTO, in which we're resolving
        // a CBTP to a TP. The intent is to reconcile the sl's supplier with the move's destination.
        // The check for a clique mirrors the behavior for resolving move destination, in that it is
        // meant to prevent replacing storage providers
        Trader origSupplier = shoppingList.getSupplier();
        if (origSupplier != null) {

            Trader newSupplier = origSupplier;
            if (!newSupplier.getCliques().isEmpty()) {
                Trader supplier = replaceNewSupplier(shoppingList, economy, newSupplier);
                if (supplier != null && supplier != origSupplier && !supplier.getCliques().isEmpty()) {
                    newSupplier = supplier;
                    // Set the couponId to the CBTPs id
                    builder.setCouponId(origSupplier.getOid());
                }
            }
            builder.setSupplier(newSupplier.getOid());
        }
        Basket basketBought = shoppingList.getBasket();
        for (int i = 0; i < basketBought.size() ; ++i) {
            final int baseType = basketBought.get(i).getBaseType();
            builder.addCommoditiesBought(commodityBoughtTO(shoppingList.getQuantity(i),
                shoppingList.getPeakQuantity(i),
                Optional.ofNullable(shoppingList.getAssignedCapacity(baseType)).orElse(0D),
                basketBought.get(i)));
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
     * @param shoppingListOid The ShoppingList to oid mapping which includes both original and newly
     * provisioned ShoppingLists
     * @param preferentialTraders traders in economy's preferential shopping list.
     * @return The resulting {@link TraderTO}.
     */
    public static @NonNull TraderTO traderTO(@NonNull UnmodifiableEconomy economy, @NonNull Trader trader,
                                             @NonNull BiMap<@NonNull ShoppingList, @NonNull Long> shoppingListOid,
                                             @Nonnull Set<Trader> preferentialTraders) {
        TraderTO.Builder builder = TraderTO.newBuilder()
            .setOid(trader.getOid())
            .setType(trader.getType())
            .setState(traderStateTO(trader.getState()))
            .setDebugInfoNeverUseInCode(trader.getDebugInfoNeverUseInCode())
            .addAllCliques(trader.getCliques())
            .setSettings(traderSettingsTO(trader.getSettings()))
            .setNumOfProduces((int)trader.getUniqueCustomers().stream()
                                        .filter(t -> t.getState().isActive()
                                            && !preferentialTraders.contains(t))
                                        .count());

        if (trader.isClone()) {
            final Trader cloneOfTrader = ((Economy)economy).getCloneOfTrader(trader);
            if (cloneOfTrader != null) {
                builder.setCloneOf(cloneOfTrader.getOid());
            } else {
                logger.error("Trader {} is a clone but the cloneOfTrader is null",
                    trader.getDebugInfoNeverUseInCode());
            }
        }

        for (int i = 0 ; i < trader.getBasketSold().size() ; ++i) {
            builder.addCommoditiesSold(commoditySoldTO(trader.getCommoditiesSold().get(i), trader.getBasketSold().get(i)));
        }

        for (@NonNull ShoppingList shoppingList : economy.getMarketsAsBuyer(trader).keySet()) {
            builder.addShoppingLists(shoppingListTO(shoppingListOid.get(shoppingList), economy,
                            shoppingList));
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
     * @param shoppingListOid A map for {@link ShoppingList}s to their OIDs.
     * @param topology The topology associates with traders received from legacy market.
     * It keeps a traderOid map which will be used to populate the oid for traders.
     * @return The resulting {@link ActionTO}.
     */
    public static @NonNull ActionTO actionTO(@NonNull Action input,
                    @NonNull BiMap<@NonNull ShoppingList, @NonNull Long> shoppingListOid,
                    Topology topology) {
        ActionTO.Builder builder = ActionTO.newBuilder();
        builder.setIsNotExecutable(!input.isExecutable());

        if (input instanceof Move) {
            Move move = (Move)input;
            Trader newSupplier = move.getDestination();
            if (newSupplier == null) {
                logger.error("The destination for the move action is null!");
                return null;
            }

            MoveTO.Builder moveTO = MoveTO.newBuilder();
            moveTO.setShoppingListToMove(shoppingListOid.get(move.getTarget()));
            moveTO.setCouponId(newSupplier.getOid());
            if (move.getContext().isPresent()) {
                moveTO.setMoveContext(move.getContext().get());
            }
            final Optional<Double> moveTargetCost = move.getTarget().getCost();
            if(moveTargetCost.isPresent()){
                moveTO.setCouponDiscount(moveTargetCost.get());
            }
            if (move.getActionTarget() != null) {
                String scalingGroupId = move.getActionTarget().getScalingGroupId();
                if (!scalingGroupId.isEmpty()) {
                    moveTO.setScalingGroupId(scalingGroupId);
                }
            }
            final List<CommodityContext> commodityContexts = move.getResizeCommodityContexts();
            if (!commodityContexts.isEmpty()) {
                moveTO.addAllCommodityContext(commodityContexts);
            }
            // the provision by demand action may not have been handled
            if (!newSupplier.isOidSet()) {
                topology.addProvisionedTrader(newSupplier);
                logger.info("NPE newSupplier=" + newSupplier.getDebugInfoNeverUseInCode() +
                            " buyer=" + move.getActionTarget().getDebugInfoNeverUseInCode());
            }
            Trader origSupplier = newSupplier;
            try {
                @NonNull UnmodifiableEconomy economy = topology.getEconomy();
                // TODO: Remove this workaround for OM-32457 once OM-32793 is fixed
                if (!newSupplier.getCliques().isEmpty()) {
                    Trader supplier = replaceNewSupplier(move, economy, newSupplier);
                    if (supplier != null && !supplier.getCliques().isEmpty()) {
                        newSupplier = supplier;
                    }
                }
                moveTO.setDestination(newSupplier.getOid());
            } catch (Exception e) {
                logger.error("Exception when replacing supplier: original supplier="
                             + origSupplier.getDebugInfoNeverUseInCode() +
                             " replaced supplier=" + newSupplier.getDebugInfoNeverUseInCode() +
                             " buyer=" + move.getActionTarget().getDebugInfoNeverUseInCode() +
                             " oid of replaced supplier=" + newSupplier.getOid());
            }
            moveTO = explainMoveAction(move.getSource(), newSupplier, move, moveTO,
                                       topology.getEconomy());
            builder.setMove(moveTO);

        } else if (input instanceof Reconfigure) {
            Reconfigure reconfigure = (Reconfigure)input;
            ReconfigureTO.Builder reconfigureTO = ReconfigureTO.newBuilder();

            reconfigureTO.setShoppingListToReconfigure(
                            shoppingListOid.get(reconfigure.getTarget()));
            if (reconfigure.getSource() != null) {
                reconfigureTO.setSource(reconfigure.getSource().getOid());
            }
            reconfigure.getUnavailableCommodities().forEach(c -> reconfigureTO
                    .addCommodityToReconfigure(c.getType()));
            if (reconfigure.getActionTarget() != null) {
                String scalingGroupId = reconfigure.getActionTarget().getScalingGroupId();
                if (!scalingGroupId.isEmpty()) {
                    reconfigureTO.setScalingGroupId(scalingGroupId);
                }
            }
            builder.setReconfigure(reconfigureTO);
        } else if (input instanceof Activate) {
            Activate activate = (Activate)input;
            ActivateTO.Builder activateBuilder = ActivateTO.newBuilder()
                    .setTraderToActivate(activate.getTarget().getOid())
                    .setModelSeller(activate.getModelSeller().getOid())
                    .addAllTriggeringBasket(specificationTOs(activate.getTriggeringBasket()));
            if (activate.getReason() != null) {
                activateBuilder.setMostExpensiveCommodity(activate.getReason().getBaseType());
            }
            builder.setActivate(activateBuilder);
        } else if (input instanceof Deactivate) {
            Deactivate deactivate = (Deactivate)input;
            builder.setDeactivate(DeactivateTO.newBuilder()
                   .setTraderToDeactivate(deactivate.getTarget().getOid())
                   .addAllTriggeringBasket(specificationTOs(deactivate.getTriggeringBasket())));
        } else if (input instanceof ProvisionByDemand) {
            ProvisionByDemand provDemand = (ProvisionByDemand)input;
            ProvisionByDemandTO.Builder provDemandTO = ProvisionByDemandTO.newBuilder()
                            .setModelBuyer(shoppingListOid.get(provDemand.getModelBuyer()))
                            .setModelSeller(provDemand.getModelSeller().getOid())
                            // the newly provisioned trader does not have OID, assign one for it
                            // and into the traderOids.
                            .setProvisionedSeller(topology.addProvisionedTrader(
                                            provDemand.getProvisionedSeller()));
            // create shopping list OIDs for the provisioned shopping lists
            topology.getEconomy()
                .getMarketsAsBuyer(provDemand.getProvisionedSeller())
                .keySet()
                .stream()
                .forEach(topology::addProvisionedShoppingList);
            // if the provisionedSeller has a guaranteed buyer, there is a new sl from
            // guaranteed buyer to provisionedSeller needs to be added to
            // topology.shoppingListOids_
            provDemand.getProvisionedSeller().getCustomers().stream().filter(sl ->
                            sl.getBuyer().getSettings().isGuaranteedBuyer())
                            .forEach(topology::addProvisionedShoppingList);
            // send commodity to new capacity map to legacy market so that newly provisioned trader
            // gets the correct capacity
            provDemand.getCommodityNewCapacityMap().forEach((key, value) -> provDemandTO
                            .addCommodityNewCapacityEntry(CommodityNewCapacityEntry.newBuilder()
                                            .setCommodityBaseType(key)
                                            .setNewCapacity(value.floatValue()).build()));
            // find the sellers(excluding the newly provisioned one) that can sell to the model
            // buyer, we will use it later to compute the maximum amount that the model buyer
            // could get
            List<Trader> sellers = new ArrayList<>();
            provDemand.getEconomy().getMarket(provDemand.getModelBuyer()).getActiveSellers()
                .forEach(s -> {if (!s.isClone() && s.getSettings().isCloneable()) {sellers.add(s);}});
            provDemand.getEconomy().getMarket(provDemand.getModelBuyer()).getInactiveSellers()
                .forEach(s -> {if (!s.isClone() && s.getSettings().isCloneable()) {sellers.add(s);}});
            sellers.remove(provDemand.getProvisionedSeller());

            // send the commodity whose requested quantity can not be satisfied, its requested
            // amount and the max amount could be provided by any seller in market
            provDemand.getCommodityNewCapacityMap().forEach((key, value) -> {
                Basket basket = provDemand.getProvisionedSeller().getBasketSold();
                CommoditySpecification commSpec = basket.get(basket.indexOfBaseType(key));
                provDemandTO.addCommodityMaxAmountAvailable(CommodityMaxAmountAvailableEntry
                                .newBuilder().setCommodityBaseType(key).setMaxAmountAvailable((float)
                                                sellers.stream().max((s1, s2) ->
                                                Double.compare(s1.getCommoditySold(commSpec).getEffectiveCapacity(),
                                                s2.getCommoditySold(commSpec).getEffectiveCapacity()))
                                                .get().getCommoditySold(commSpec).getEffectiveCapacity())
                                .setRequestedAmount((float)provDemand.getModelBuyer()
                                                .getQuantities()[provDemand.getModelBuyer()
                                                                 .getBasket().indexOfBaseType(key)])
                                .build());
            });
            builder.setProvisionByDemand(provDemandTO);
        } else if (input instanceof ProvisionBySupply) {
            ProvisionBySupply provSupply = (ProvisionBySupply)input;
            ProvisionBySupplyTO.Builder provSupplyTO = ProvisionBySupplyTO.newBuilder()
                            .setModelSeller(provSupply.getModelSeller().getOid())
                            // the newly provisioned trader does not have OID, assign one for it and add
                            // the into traderOids_
                            .setProvisionedSeller(topology.addProvisionedTrader(
                                            provSupply.getProvisionedSeller()))
                            .setMostExpensiveCommodity(commoditySpecificationTO(
                                            provSupply.getReason() == null
                                                            ? findMostExpensiveCommodity(provSupply
                                                                            .getModelSeller(),
                                                                            topology.getEconomy())
                                                            : provSupply.getReason()));
            // create shopping list OIDs for the provisioned shopping lists
            topology.getEconomy()
                .getMarketsAsBuyer(provSupply.getProvisionedSeller())
                .keySet()
                .stream()
                .forEach(topology::addProvisionedShoppingList);
            // if the provisionedSeller has a guaranteed buyer, there is a new sl from guaranteed
            // buyer to provisionedSeller needs to be added to topology.shoppingListOids_
            provSupply.getProvisionedSeller().getCustomers().stream().filter(sl ->
                            sl.getBuyer().getSettings().isGuaranteedBuyer())
                            .forEach(topology::addProvisionedShoppingList);
            builder.setProvisionBySupply(provSupplyTO);
        } else if (input instanceof Resize) {
            Resize resize = (Resize)input;
            ResizeTO.Builder resizeBuilder = ResizeTO.newBuilder()
                .setSellingTrader(resize.getSellingTrader().getOid())
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
                            resize.getResizedCommodity().getEffectiveCapacity()));
            String scalingGroupId = resize.getSellingTrader().getScalingGroupId();
            if (!scalingGroupId.isEmpty()) {
                resizeBuilder.setScalingGroupId(scalingGroupId);
            }
            if (!resize.getResizeTriggerTraders().isEmpty()) {
                resizeBuilder.addAllResizeTriggerTrader(resize.getResizeTriggerTraders().entrySet()
                    .stream()
                    .filter(entry -> entry.getKey().isOidSet())
                    .map(entry -> {
                        ResizeTriggerTraderTO.Builder resizeTriggerTrader = ResizeTriggerTraderTO.newBuilder();
                        resizeTriggerTrader.setTrader(entry.getKey().getOid());
                        resizeTriggerTrader.addAllRelatedCommodities(entry.getValue());
                        return resizeTriggerTrader.build();
                    })
                    .collect(Collectors.toList()));
            }
            builder.setResize(resizeBuilder);
        } else if (input instanceof CompoundMove) {
            CompoundMove compoundMove = (CompoundMove)input;
            CompoundMoveTO.Builder compoundMoveTO = CompoundMoveTO.newBuilder();
            for (Move m : compoundMove.getConstituentMoves()) {
                compoundMoveTO.addMoves(AnalysisToProtobuf.actionTO(m, shoppingListOid,
                                topology).getMove());
            }
            builder.setCompoundMove(compoundMoveTO);
        }
        builder.setImportance(((ActionImpl)input).getImportance());
        return builder.build();
    }

    // Methods for converting CommunicationDTOs.

    /**
     * wrapper method that converts a move to a shopping list
     *
     */
    private static Trader replaceNewSupplier(Move move, UnmodifiableEconomy economy, Trader newSupplier) {
        ShoppingList buyer = move.getTarget();
        return replaceNewSupplier(buyer, economy, newSupplier);
    }

    /**
     * returns the template provider if the newSupplier is a cbtp.
     *
     */
    public static Trader replaceNewSupplier(ShoppingList buyer, UnmodifiableEconomy economy, Trader newSupplier) {
        final Set<Entry<ShoppingList, Market>> shoppingListsInMarket =
                economy.getMarketsAsBuyer(newSupplier).entrySet();
        if (shoppingListsInMarket.isEmpty()) {
            return null;
        }
        Market market = shoppingListsInMarket.iterator().next().getValue();
        if (market == null) {
            return null;
        }
        List<Trader> sellers = market.getActiveSellers();
        List<Trader> mutableSellers = new ArrayList<Trader>();
        mutableSellers.addAll(sellers);
        mutableSellers.retainAll(economy.getMarket(buyer).getActiveSellers());
        // Get cheapest quote, that will be provided by the matching template
        final QuoteMinimizer minimizer = mutableSellers.stream().collect(
                        () -> new QuoteMinimizer(economy, buyer), QuoteMinimizer::accept,
                        QuoteMinimizer::combine);
        return minimizer.getBestSeller();
    }

    /**
     * Converts a list of {@link Action}s to an {@link AnalysisResults} message given some
     * additional context.
     *
     * @param actions The list of {@link Action}s to convert.
     * @param shoppingListOid A function mapping {@link ShoppingList}s to their OIDs.
     * @param timeToAnalyze_ns The amount of time it took to analyze the topology and produce the
     *        list of actions in nanoseconds.
     * @param topology The topology associates with traders received from legacy market.
     * It keeps a traderOid map which will be used to populate the oid for traders.
     * @param startPriceStatement contains all the {@link PriceStatement}s for traders
     * @return The resulting {@link AnalysisResults} message.
     */
    public static @NonNull AnalysisResults analysisResults(@NonNull List<Action> actions,
                    @NonNull BiMap<@NonNull ShoppingList, @NonNull Long> shoppingListOid,
                    long timeToAnalyze_ns, Topology topology, PriceStatement startPriceStatement) {
        return analysisResults(actions, shoppingListOid, timeToAnalyze_ns, topology,
                        startPriceStatement, true);
    }
    /**
     * Converts a list of {@link Action}s to an {@link AnalysisResults} message given some
     * additional context.
     *
     * @param actions The list of {@link Action}s to convert.
     * @param shoppingListOid A function mapping {@link ShoppingList}s to their OIDs.
     * @param timeToAnalyze_ns The amount of time it took to analyze the topology and produce the
     *        list of actions in nanoseconds.
     * @param topology The topology associates with traders received from legacy market.
     * It keeps a traderOid map which will be used to populate the oid for traders.
     * @param startPriceStatement contains all the {@link PriceStatement}s for traders
     * @param sendBack whether to send back traderTO or not
     * @return The resulting {@link AnalysisResults} message.
     */
    public static @NonNull AnalysisResults analysisResults(@NonNull List<Action> actions,
                    @NonNull BiMap<@NonNull ShoppingList, @NonNull Long> shoppingListOid,
                    long timeToAnalyze_ns, Topology topology, PriceStatement startPriceStatement,
                    boolean sendBack) {
        AnalysisResults.Builder builder = AnalysisResults.newBuilder();
        builder.setTopologyId(topology.getTopologyId());
        for (Action action : actions) {
            ActionTO actionTO = actionTO(action, shoppingListOid, topology);
            if (actionTO != null) {
                builder.addActions(actionTO);
            }
        }
        // New shoppingList map has to be created after action conversion, because assigning
        // oid for new shopping list happens in conversion
        builder.addAllNewShoppingListToBuyerEntry(createNewShoppingListToBuyerMap(topology));
        final UnmodifiableEconomy economy = topology.getEconomy();
        // populate the endPriceIndex
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
            if (trader.getState().isActive()) {
                payloadBuilder.setOid(trader.getOid());
                double startPriceIndex = (traderIndex < startPriceStatementSize) ? startTraderPriceStmts
                                .get(traderIndex).getPriceIndex() : 0;
                payloadBuilder.setPriceindexCurrent(Double.isInfinite(startPriceIndex)
                                ? MAX_PRICE_INDEX : startPriceIndex);
                payloadBuilder.setPriceindexProjected(Double.isInfinite(endTraderPriceStmt.getPriceIndex())
                                ? MAX_PRICE_INDEX : endTraderPriceStmt.getPriceIndex());
                piBuilder.addPayload(payloadBuilder.build());
            } else {
                if (!trader.isClone()) {
                    payloadBuilder.setOid(trader.getOid());
                    double startPriceIndex = (traderIndex < startPriceStatementSize) ? startTraderPriceStmts
                                    .get(traderIndex).getPriceIndex() : 0;
                    payloadBuilder.setPriceindexCurrent(Double.isInfinite(startPriceIndex)
                                    ? MAX_PRICE_INDEX : startPriceIndex);
                    payloadBuilder.setPriceindexProjected(0);
                    piBuilder.addPayload(payloadBuilder.build());
                }
            }
            traderIndex++;
        }

        Set<Trader> preferentialTraders = economy.getPreferentialShoppingLists().stream()
            .map(sl -> sl.getBuyer())
            .collect(Collectors.toSet());
        List<TraderTO> traderTOList = new ArrayList<>();
        if (sendBack) {
            for (@NonNull @ReadOnly Trader trader : economy.getTraders()) {
                if (!trader.isClone() || trader.getState() != TraderState.INACTIVE) {
                    // in case of a cloned trader, if it is suspended(INACTIVE state), skip
                    // creating a traderTO for it
                    traderTOList.add(AnalysisToProtobuf.traderTO(economy, trader,
                        shoppingListOid, preferentialTraders));
                }
            }
        }

        return builder.setTimeToAnalyzeNs(timeToAnalyze_ns).setPriceIndexMsg(piBuilder.build())
                        .addAllProjectedTopoEntityTO(traderTOList).build();

    }


    /**
     * Finds the commodity with the highest price for a given trader.
     *
     * @param modelSeller the model seller used in provision or activation
     * @param economy that the commodities are traded in
     * @return the {@link CommoditySpecification} of the most expensive commodity
     */
    private static CommoditySpecification findMostExpensiveCommodity(Trader modelSeller,
                                                                     UnmodifiableEconomy economy) {
        double mostExpensivePrice = 0;
        CommoditySpecification mostExpensiveComm = null;
        // we find the most expensive commodity at the start state to explain the provisionBySupply
        // Since we are calculating price for commodity sold, shopping list is passed as null
        for (CommoditySold c : modelSeller.getCommoditiesSold()) {
            double usedPrice = c.getSettings().getPriceFunction().unitPrice(c.getStartQuantity() /
                            c.getEffectiveCapacity(), null, modelSeller, c, economy);
            double peakPrice = c.getSettings().getPriceFunction().unitPrice(Math.max(0,
                            c.getStartPeakQuantity() - c.getStartQuantity())
                               / (c.getEffectiveCapacity() - c.getSettings().getUtilizationUpperBound()
                                               * c.getStartQuantity()), null, modelSeller, c, economy);
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
     * @return The result {@link NewShoppingListToBuyerEntry} message
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
     * @param move the move action to explain
     * @param moveTO the DTO for the move action to explain
     * @param economy that the actions re generated in
     * @return The resulting {@link MoveTO.Builder}.
     */
    private static MoveTO.Builder explainMoveAction(Trader oldSupplier, Trader newSupplier, Move move,
                    MoveTO.Builder moveTO, UnmodifiableEconomy economy) {
        if (oldSupplier == null) {
            // when the source does not exist, move is actually initial placement.
            moveTO.setMoveExplanation(MoveExplanation.newBuilder().setInitialPlacement(
                            InitialPlacement.newBuilder().build()).build());
        } else {
            // when the source exists, the move can be either a result of compliance,
            // cheaper quote or suspension.
            moveTO.setSource(oldSupplier.getOid());
            // when old supplier is inactive, move is a result of supplier suspension.
            // TODO: we need to understand if old host is in failover state, could it still has
            // some consumers? If so, is it valid to consider move as a result of suspension?
            if (oldSupplier.getState() == TraderState.INACTIVE) {
                moveTO.setMoveExplanation(MoveExplanation.newBuilder().setEvacuation(Evacuation
                                .newBuilder().setSuspendedTrader(oldSupplier.getOid()).build())
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
                    if (move.getTarget().getModifiableUnquotedCommoditiesBaseTypeList()
                                                        .contains(commBought.getBaseType())) {
                        continue;
                    }
                    CommoditySold oldCommSold = oldSupplier.getCommoditySold(commBought);
                    // if old supplier does not have the shopping list's requested commodity,
                    // move is due to compliance
                    if (oldCommSold == null) {
                        complianceCommSet.add(commBought.getType());
                    } else {
                        CommoditySold newCommSold = newSupplier.getCommoditySold(commBought);
                        FunctionalOperator updatingFunction = newCommSold.getSettings().getUpdatingFunction();
                        // if the comm utilization is less than min desired util, we don't consider
                        // it as reason commodities even though the quote at destination may be
                        // smaller, because from a user's point of view, move away from such a low
                        // utilized supplier is not congestion
                        if (oldCommSold.getSettings().getUtilizationCheckForCongestion()
                                        && oldCommSold.getUtilization() < oldCommSold.getSettings()
                                        .getUtilizationUpperBound() * oldSupplier.getSettings()
                                        .getMinDesiredUtil()) {
                            continue;
                        }

                        double quantityBought = move.getTarget().getQuantity(i);
                        double peakQuantityBought = move.getTarget().getPeakQuantity(i);
                        // calculate the price at the old and new supplier and get the quote
                        // difference
                        double oldQuote = calculateQuote(oldCommSold, quantityBought, peakQuantityBought,
                                                         oldSupplier, economy, move.getTarget());
                        double newQuote = calculateQuote(newCommSold, quantityBought, peakQuantityBought,
                                                         newSupplier, economy, move.getTarget());
                        double quoteDiff = newQuote - oldQuote;

                        // if the difference in quote is positive, or it is not bigger than
                        // threshold, skip it
                        if (quoteDiff >= 0 || (Math.abs(quoteDiff) < QUOTE_DIFF_THRESHOLD)) {
                            continue;
                        }
                        if (quoteDiffPerComm.containsKey(quoteDiff)) {
                            quoteDiffPerComm.get(quoteDiff).add(commBought.getType());
                        } else {
                            List<Integer> list = new ArrayList<>();
                            list.add(commBought.getType());
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
     * @param seller is the seller that sells the commSold
     * @param economy that the commodities are traded in
     * @param sl The shopping list
     * @return quote for the given {@link CommoditySold}
     */
    @VisibleForTesting
    static double calculateQuote(CommoditySold commSold, double quantityBought,
                    double peakQuantityBought, Trader seller, UnmodifiableEconomy economy, ShoppingList sl) {
        PriceFunction pf = commSold.getSettings().getPriceFunction();
        double startQuantity = commSold.getStartQuantity();
        double startPeakQuantity = commSold.getStartPeakQuantity();
        // We use effectiveStartCapacity to calculate quote because the capacity may have been resized by this point.
        // We should use the start capacity. See OM-60680.
        double effectiveStartCapacity = commSold.getEffectiveStartCapacity();
        double excessQuantity = peakQuantityBought - quantityBought;

        double usedPrice = pf.unitPrice(startQuantity / effectiveStartCapacity, sl, seller, commSold, economy);
        double peakPrice = pf.unitPrice(Math.max(0, startPeakQuantity - startQuantity)/
                                        (effectiveStartCapacity - commSold.getSettings()
                                                        .getUtilizationUpperBound()*startQuantity)
                                                        , sl, seller, commSold, economy);
        double quoteUsed = quantityBought != 0 ? (quantityBought / effectiveStartCapacity) * usedPrice : 0;
        double quotePeak = excessQuantity > 0 ? (excessQuantity / effectiveStartCapacity) * peakPrice : 0;
        return quoteUsed + quotePeak;
    }
} // end AnalysisToProtobuf class
