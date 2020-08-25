package com.vmturbo.platform.analysis.translators;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongFunction;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import com.vmturbo.platform.analysis.economy.CommodityResizeSpecification;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySoldSettings;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Context;
import com.vmturbo.platform.analysis.economy.Context.BalanceAccount;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.RawMaterials;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderSettings;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.pricefunction.PriceFunction;
import com.vmturbo.platform.analysis.pricefunction.QuoteFunction;
import com.vmturbo.platform.analysis.pricefunction.QuoteFunctionFactory;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActionTO;
import com.vmturbo.platform.analysis.protobuf.BalanceAccountDTOs.BalanceAccountDTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommodityBoughtTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldSettingsTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySpecificationTO;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.EndDiscoveredTopology;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.EndDiscoveredTopology.CommodityResizeDependency;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.EndDiscoveredTopology.CommodityResizeDependencyEntry;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.EndDiscoveredTopology.ResizeDependencySkipEntry;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.CbtpCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.ComputeTierCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.CostTuple;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageResourceCost;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageTierPriceData;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.ShoppingListTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderSettingsTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderStateTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.analysis.protobuf.PriceFunctionDTOs.PriceFunctionTO;
import com.vmturbo.platform.analysis.protobuf.QuoteFunctionDTOs.QuoteFunctionDTO;
import com.vmturbo.platform.analysis.protobuf.UpdatingFunctionDTOs.UpdatingFunctionTO;
import com.vmturbo.platform.analysis.topology.Topology;
import com.vmturbo.platform.analysis.utilities.CostFunctionFactory;
import com.vmturbo.platform.analysis.utilities.DoubleTernaryOperator;
import com.vmturbo.platform.analysis.utilities.FunctionalOperatorUtil;

/**
 * A class containing methods to convert Protobuf messages to java classes used by analysis.
 *
 * <p>
 *  This is intended to contain only static methods.
 * </p>
 */
public final class ProtobufToAnalysis {
    // Methods for converting PriceFunctionDTOs.
    private static final Logger logger = LogManager.getLogger(ProtobufToAnalysis.class);
    /**
     * Converts a {@link PriceFunctionTO} to a {@link PriceFunction}.
     *
     * @param input The {@link PriceFunctionTO} to convert.
     * @return The resulting {@link PriceFunction}.
     */
    public static PriceFunction priceFunction(@NonNull PriceFunctionTO input) {
        switch (input.getPriceFunctionTypeCase()) {
            case STANDARD_WEIGHTED:
                return PriceFunction.Cache.createStandardWeightedPriceFunction(input.getStandardWeighted().getWeight());
            case CONSTANT:
                return PriceFunction.Cache.createConstantPriceFunction(input.getConstant().getValue());
            case STEP:
                return PriceFunction.Cache.createStepPriceFunction(input.getStep().getStepAt(),
                    input.getStep().getPriceBelow(), input.getStep().getPriceAbove());
            case STEP_FOR_CLOUD:
                return PriceFunction.Cache.createStepPriceFunctionForCloud();
            case FINITE_STANDARD_WEIGHTED:
                return PriceFunction.Cache.createFiniteStandardWeightedPriceFunction(
                                input.getStandardWeighted().getWeight());
            case EXTERNAL_PRICE_FUNCTION:
                return PriceFunction.Cache.createExternalPriceFunction();
            case SQUARED_RECIPROCAL_BOUGHT:
                return PriceFunction.Cache
                                .createSquaredReciprocalBoughtUtilizationPriceFunction(input
                                                .getSquaredReciprocalBought().getWeight());
            case IGNORE_UTILIZATION:
                return PriceFunction.Cache.createIgnoreUtilizationPriceFunction();
            case SCALED_CAPACITY_STANDARD_WEIGHTED:
                return PriceFunction.Cache.createScaledCapacityStandardWeightedPriceFunction(
                        input.getScaledCapacityStandardWeighted().getWeight(),
                                input.getScaledCapacityStandardWeighted().getScale());
            case PRICEFUNCTIONTYPE_NOT_SET:
            default:
                throw new IllegalArgumentException("input = " + input);
        }
    }

    // Methods for converting UpdatingFunctionDTOs.

    /**
     * Converts a {@link UpdatingFunctionTO} to a {@link DoubleTernaryOperator quantity
     * updating function}.
     *
     * @param input The {@link UpdatingFunctionTO} to convert.
     * @return The resulting {@link DoubleTernaryOperator quantity updating function}.
     */
    public static @NonNull DoubleTernaryOperator updatingFunction(@NonNull UpdatingFunctionTO input) {
        switch (input.getUpdatingFunctionTypeCase()) {
            case MAX:
                return (DoubleTernaryOperator & Serializable) (a, b, c) -> Math.max(a, b);
            case MIN:
                return (DoubleTernaryOperator & Serializable) (a, b, c) -> Math.min(a, b);
            case PROJECT_SECOND:
                return (DoubleTernaryOperator & Serializable) (a, b, c) -> b;
            case DELTA:
                return (DoubleTernaryOperator & Serializable) (a, b, c) -> a + b;
            case AVG_ADD:
                return (DoubleTernaryOperator & Serializable) (a, b, c) -> (a*c + b)/(c + 1);
            case UPDATINGFUNCTIONTYPE_NOT_SET:
            default:
                throw new IllegalArgumentException("input = " + input);
        }
    }

    // Methods for converting EconomyDTOs.

    /**
     * Converts a {@link CommoditySpecificationTO} to a {@link CommoditySpecification}.
     *
     * @param input The {@link CommoditySpecificationTO} to convert.
     * @return The resulting {@link CommoditySpecification}.
     */
    public static @NonNull CommoditySpecification commoditySpecification(@NonNull CommoditySpecificationTO input) {
        return new CommoditySpecification(input.getType(),input.getBaseType(), input.getCloneWithNewType())
                .setDebugInfoNeverUseInCode(input.getDebugInfoNeverUseInCode());
    }

    /**
     * Converts a list of {@link CommoditySpecificationTO}s to a {@link Basket}.
     *
     * @param input The list of {@link CommoditySpecificationTO}s to convert.
     * @return The resulting {@link Basket}.
     *
     * @see #basket(TraderTO)
     * @see #basket(ShoppingListTO)
     */
    public static @NonNull Basket basket(@NonNull List<CommoditySpecificationTO> input) {
        return new Basket(input.stream().map(ProtobufToAnalysis::commoditySpecification).collect(Collectors.toList()));
    }

    /**
     * Creates a {@link Basket} from a {@link ShoppingListTO} for use as a basket bought.
     *
     * @param input The {@link ShoppingListTO} from which to create the {@link Basket}.
     * @return The resulting {@link Basket}.
     *
     * @see #basket(List)
     * @see #basket(TraderTO)
     */
    public static @NonNull Basket basket(@NonNull ShoppingListTO input) {
        return new Basket(input.getCommoditiesBoughtList().stream()
           .map(cb -> commoditySpecification(cb.getSpecification())).collect(Collectors.toList()));
    }

    /**
     * Creates a {@link Basket} from a {@link TraderTO} for use as a basket sold.
     *
     * @param input The {@link TraderTO} from which to create the {@link Basket}.
     * @return The resulting {@link Basket}.
     *
     * @see #basket(List)
     * @see #basket(ShoppingListTO)
     */
    public static @NonNull Basket basket(@NonNull TraderTO input) {
        Basket basket = new Basket(input.getCommoditiesSoldList().stream()
           .map(cs -> commoditySpecification(cs.getSpecification())).collect(Collectors.toList()));
        return basket;
    }

    /**
     * Adds a new {@link ShoppingList} to a {@link Trader} that is already in a
     * {@link Topology} given a {@link ShoppingListTO}.
     *
     * @param topology The {@link Topology} which contains the {@link Trader}.
     * @param scalingGroupId scaling group ID of associated buyer. Empty string if no scaling group.
     * @param buyer The {@link Trader} to add the {@link ShoppingList} to.
     * @param input The {@link ShoppingListTO} describing the {@link ShoppingList}.
     * @return The resulting {@link ShoppingList}.
     */
    static @NonNull ShoppingList addShoppingList(@NonNull Topology topology,
                                                 @Nonnull String scalingGroupId,
                                                 @NonNull Trader buyer,
                                                 @NonNull ShoppingListTO input) {
        @NonNull Basket basketBought = basket(input);
        @NonNull ShoppingList shoppingList = input.hasSupplier()
            ? topology.addBasketBought(input.getOid(), buyer, basketBought, input.getSupplier())
            : topology.addBasketBought(input.getOid(), buyer, basketBought);

        shoppingList.setMovable(input.getMovable());
        shoppingList.setMoveCost(input.getStorageMoveCost());
        if (input.hasGroupFactor()) {
            shoppingList.setGroupFactor(input.getGroupFactor());
            Economy economy = (Economy)topology.getEconomy();
            economy.registerShoppingListWithScalingGroup(scalingGroupId, shoppingList);
        }
        shoppingList.getUnquotedCommoditiesBaseTypeList().addAll(input.getUnquotedCommoditiesBaseTypeListList());

        for (CommodityBoughtTO commodityBought : input.getCommoditiesBoughtList()) {
            int index = basketBought.indexOf(commoditySpecification(commodityBought.getSpecification()));
            shoppingList.setQuantity(index, commodityBought.getQuantity());
            shoppingList.setPeakQuantity(index, commodityBought.getPeakQuantity());
        }

        return shoppingList;
    }

    /**
     * Populates the fields of a {@link CommoditySoldSettings} instance from information in a
     * {@link CommoditySoldSettingsTO}.
     *
     * @param source The {@link CommoditySoldSettingsTO} from which to get the settings.
     * @param destination The {@link CommoditySoldSettings} instance to put the settings to.
     */
    public static void populateCommoditySoldSettings(@NonNull CommoditySoldSettingsTO source,
                                                     @NonNull CommoditySoldSettings destination,
                                                     @NonNull TraderSettingsTO entitySett) {
        destination.setResizable(source.getResizable());
        destination.setCapacityLowerBound(source.getCapacityLowerBound());
        destination.setCapacityUpperBound(source.getCapacityUpperBound());
        destination.setCapacityIncrement(source.getCapacityIncrement());
        destination.setUtilizationUpperBound(source.getUtilizationUpperBound());
        destination.setOrigUtilizationUpperBound(source.getUtilizationUpperBound());
        destination.setResold(source.getResold());
        destination.setPriceFunction(priceFunction(source.getPriceFunction()));
        CostDTO costDTO = (entitySett.getQuoteFunction().hasRiskBased() == true) ?
                        entitySett.getQuoteFunction().getRiskBased().getCloudCost() : null;
        destination.setUpdatingFunction(FunctionalOperatorUtil.createUpdatingFunction(
                                                  costDTO, source.getUpdateFunction()));
        destination.setUtilizationCheckForCongestion(source.getUtilizationCheckForCongestion());
    }

    /**
     * Populates the fields of a {@link CommoditySold} from information in a {@link CommoditySoldTO}.
     *
     * @param source The {@link CommoditySoldTO} from which to get the data.
     * @param destination The {@link CommoditySold} to put the data to.
     */
    public static void populateCommoditySold(@NonNull CommoditySoldTO source,
                                             @NonNull CommoditySold destination,
                                             @NonNull TraderTO entity) {
        destination.setQuantity(source.getQuantity());
        destination.setPeakQuantity(source.getPeakQuantity());
        destination.setMaxQuantity(source.getMaxQuantity());
        destination.setCapacity(source.getCapacity());
        destination.setStartQuantity(source.getQuantity());
        destination.setNumConsumers(source.getNumConsumers());
        destination.setStartPeakQuantity(source.getQuantity() >
            source.getPeakQuantity() ? source.getQuantity() :
                source.getPeakQuantity());
        destination.setStartCapacity(source.getCapacity());
        destination.setThin(source.getThin());

        // Only populate the right size quantity if it has been sent
        if (source.hasHistoricalQuantity()) {
            destination.setHistoricalQuantity(source.getHistoricalQuantity());
        }

        try {
            populateCommoditySoldSettings(source.getSettings(), destination.getSettings(),
                                          entity.getSettings());
        } catch (IllegalArgumentException e) {
            logger.error("source commoditySold or entity=" + entity.getDebugInfoNeverUseInCode()
                            + " has illegal settings."
                            + " sourceSettings=" + source.getSettings().toString()
                            + " entitySettings=" + entity.getSettings().toString());
        }
    }

    /**
     * Populates the fields of a {@link TraderSettings} instance from information in a
     * {@link TraderSettingsTO}.
     *
     * @param topology the economy associated topology
     * @param input The {@link TraderTO} from which to get the settings.
     * @param output The {@link TraderSettings} instance to put the settings to.
     */
    public static void populateTraderSettings(@NonNull Topology topology,
                                              @Nonnull TraderTO input,
                                              @NonNull Trader output) {
        TraderSettings destination = output.getSettings();
        @NonNull TraderSettingsTO source = input.getSettings();
        destination.setControllable(source.getControllable());
        destination.setCloneable(source.getClonable());
        destination.setSuspendable(source.getSuspendable());
        destination.setMinDesiredUtil(source.getMinDesiredUtilization());
        destination.setMaxDesiredUtil(source.getMaxDesiredUtilization());
        destination.setRateOfResize(source.getRateOfResize());
        destination.setGuaranteedBuyer(source.getGuaranteedBuyer());
        destination.setCanAcceptNewCustomers(source.getCanAcceptNewCustomers());
        destination.setIsEligibleForResizeDown(source.getIsEligibleForResizeDown());
        destination.setIsShopTogether(source.getIsShopTogether());
        destination.setProviderMustClone(source.getProviderMustClone());
        destination.setDaemon(source.getDaemon());
        destination.setResizeThroughSupplier(source.getResizeThroughSupplier());
        destination.setQuoteFunction(populateQuoteFunction(destination, source.getQuoteFunction()));
        destination.setQuoteFactor(source.getQuoteFactor());
        destination.setMoveCostFactor(source.getMoveCostFactor());
        destination.setCanSimulateAction(source.getCanSimulateAction());
        if (source.getQuoteFunction().hasRiskBased() && source.getQuoteFunction().getRiskBased().hasCloudCost()) {
            CostDTO costDTO = source.getQuoteFunction().getRiskBased().getCloudCost();
            destination.setCostFunction(CostFunctionFactory.createCostFunction(costDTO));
            // source has costDTO suggests that it is a cloud tier, we are populating a list
            // of contexts for it by extracting the region and ba information from costDTO.
            // NOTE: the cloud tiers do not have context in the TraderDTO. Only cloud workloads have
            // context in the TraderDTO.
            if (!source.hasCurrentContext()) {
                topology.getEconomy().getTraderWithContextMap().put(output, constructContext(costDTO));
            }
        }
        // the traderDTO has context and business account suggests that it is a cloud workload
        if (source.hasCurrentContext() && source.getCurrentContext().hasBalanceAccount()) {
            populateCloudSpent(topology, input, destination);
        }

    }

    /**
     * Create a list of contexts based on data provided by a {@link CostDTO}.
     *
     * @param costDTO costDTO
     * @return a list of context, each context represents a region/zone and business account
     * combination.
     */
    private static List<Context> constructContext(@Nonnull CostDTO costDTO) {
        switch (costDTO.getCostTypeCase()) {
            case STORAGE_TIER_COST:
                 return populateAllContextStorageTier(costDTO.getStorageTierCost()
                         .getStorageResourceCostList());
            case COMPUTE_TIER_COST:
                return populateAllContextComputeTier(costDTO.getComputeTierCost());
            // DB is not a shop together provider so no need to populate context
            case DATABASE_TIER_COST:
                return new ArrayList<>();
            case CBTP_RESOURCE_BUNDLE:
                return populateAllContextCbtp(costDTO.getCbtpResourceBundle());
            default:
                throw new IllegalArgumentException("input = " + costDTO);
        }

    }

    /**
     * Populate contexts based on a {@link CbtpCostDTO}.
     *
     * @param cbtpResourceBundle a CbtpCostDTO.
     * @return a list of contexts.
     */
    private static List<Context> populateAllContextCbtp(@Nonnull CbtpCostDTO cbtpResourceBundle) {
        final Map<Long, Set<Long>> regionListByAccount =
                        extractContextFromCostTuple(cbtpResourceBundle.getCostTupleListList());
        // ParentId is only expected to be one if present, so pass that in.
        Long parentId = cbtpResourceBundle.getScopeIdsList().isEmpty() ? null
                : cbtpResourceBundle.getScopeIds(0);
        return createContextList(regionListByAccount, parentId);
    }

    /**
     * Populate contexts based on a {@link ComputeTierCostDTO}.
     *
     * @param computeTierCost a ComputeTierCostDTO.
     * @return a list of contexts.
     */
    private static List<Context> populateAllContextComputeTier(
            @NonNull ComputeTierCostDTO computeTierCost) {
        final Map<Long, Set<Long>> regionListByAccount =
                extractContextFromCostTuple(computeTierCost.getCostTupleListList());
        return createContextList(regionListByAccount, null);
    }

    /**
     * Populate contexts based on a list of {@link StorageResourceCost}.
     *
     * @param storageResourceCostList a list of StorageResourceCost.
     * @return a list of contexts.
     */
    private static List<Context> populateAllContextStorageTier(
            @Nonnull List<StorageResourceCost> storageResourceCostList) {
        final Map<Long, Set<Long>> regionOrZoneSetByAccount = new HashMap<>();
        for (StorageResourceCost cost : storageResourceCostList) {
            for (StorageTierPriceData priceData : cost.getStorageTierPriceDataList()) {
                extractContextFromCostTuple(priceData.getCostTupleListList()).entrySet().forEach(e -> {
                    Set<Long> regionSet = regionOrZoneSetByAccount.get(e.getKey());
                    if (regionSet == null) {
                        regionSet = new HashSet<>();
                        regionOrZoneSetByAccount.put(e.getKey(), regionSet);
                    }
                    regionSet.addAll(e.getValue());
                });

            }
        }
        return createContextList(regionOrZoneSetByAccount, null);
    }

    /**
     * Create a context list based on a map of business account id to region/zone set.
     *
     * @param regionOrZoneSetByAccount a map of business account id to a region/zone set.
     * @param parentId Scope/BillingFamily id set in case of CBTP contexts only.
     * @return a list of contexts.
     */
    private static List<Context> createContextList(Map<Long, Set<Long>> regionOrZoneSetByAccount,
            @Nullable Long parentId) {
        List<Context> contextList = new ArrayList<>();
        regionOrZoneSetByAccount.entrySet().forEach(e -> {
            e.getValue().stream().forEach(regionOrZoneId -> {
                long accountId = e.getKey();
                // each context will represent a region X ba combination or a zone X ba combination
                contextList.add(new Context(regionOrZoneId, regionOrZoneId,
                        parentId == null
                                ? new BalanceAccount(accountId)
                                : new BalanceAccount(accountId, parentId)));
            });
        });
        return contextList;
    }

    /**
     * Extract business account id and region/zone ids from cost tuples.
     *
     * @param costTupleListList a list of {@link CostTuple}.
     * @return a map of business account id to region/zone set.
     */
    private static Map<Long, Set<Long>> extractContextFromCostTuple(List<CostTuple> costTupleListList) {
        Map<Long, Set<Long>> regionListByAccount = new HashMap<>();
        for (CostTuple tuple : costTupleListList) {
            long baId = tuple.getBusinessAccountId();
            Set<Long> zoneOrRegionList = regionListByAccount.get(baId);
            if (zoneOrRegionList == null) {
                zoneOrRegionList = new HashSet<>();
                regionListByAccount.put(baId, zoneOrRegionList);
            }
            zoneOrRegionList.add(tuple.getZoneId() != 0 ? tuple.getZoneId() : tuple.getRegionId());
        }
        return regionListByAccount;
    }

    /**
     * Populates the {@link QuoteFunction} for each trader.
     *
     * @param quoteFunctionDTO The {@link QuoteFunctionDTO}
     * @return QuoteFunction
     */
    public static QuoteFunction populateQuoteFunction(TraderSettings traderSettings,
                    QuoteFunctionDTO quoteFunctionDTO) {
        switch (quoteFunctionDTO.getQuoteFunctionTypeCase()) {
            case SUM_OF_COMMODITY:
                return QuoteFunctionFactory.sumOfCommodityQuoteFunction();
            case RISK_BASED:
                return QuoteFunctionFactory.budgetDepletionRiskBasedQuoteFunction();
            default:
                throw new IllegalArgumentException("input = " + quoteFunctionDTO);
        }
    }
    /**
     * Converts a {@link TraderStateTO} to a {@link TraderState} instance.
     *
     * @param input The {@link TraderStateTO} to convert.
     * @return The resulting {@link TraderState} instance.
     */
    public static TraderState traderState(@NonNull TraderStateTO input) {
        switch (input) {
            case ACTIVE:
            case IDLE:
                return TraderState.ACTIVE;
            case INACTIVE:
                return TraderState.INACTIVE;
            default:
                throw new IllegalArgumentException("input = " + input);
        }
    }

    /**
     * Adds a new {@link Trader} to a {@link Topology} given a {@link TraderTO}.
     *
     * @param topology The {@link Topology} to add the {@link Trader} to.
     * @param input The {@link TraderTO} describing the {@link Trader}.
     * @return The resulting {@link Trader}.
     */
    public static @NonNull Trader addTrader(@NonNull Topology topology, @NonNull TraderTO input) {
        @NonNull Basket basketSold = basket(input);
        @NonNull Trader output = topology.addTrader(input.getOid(), input.getType(), traderState(input.getState()),
                                                    basketSold, input.getCliquesList());
        output.setDebugInfoNeverUseInCode(input.getDebugInfoNeverUseInCode());
        final String scalingGroupId = input.getScalingGroupId();
        output.setScalingGroupId(scalingGroupId);
        ((Economy)topology.getEconomy()).populatePeerMembersForScalingGroup(output, scalingGroupId);
        populateTraderSettings(topology, input, output);

        output.setDebugEnabled(input.getDebugEnabled());
        for (CommoditySoldTO commoditySold : input.getCommoditiesSoldList()) {
            populateCommoditySold(commoditySold,
                                  output.getCommoditySold(
                                                 commoditySpecification(commoditySold.getSpecification())),
                                  input);
        }

        if (input.getState() == TraderStateTO.IDLE || input.getPreferentialPlacement()) {
            for (ShoppingListTO sl : input.getShoppingListsList()) {
                if (!sl.getCommoditiesBoughtList().isEmpty()) {
                    topology.addPreferentialSl(addShoppingList(topology, scalingGroupId, output, sl));
                }
            }
        } else {
            for (ShoppingListTO sl : input.getShoppingListsList()) {
                if (!sl.getCommoditiesBoughtList().isEmpty()) {
                    addShoppingList(topology, scalingGroupId, output, sl);
                }
            }
        }
        // adds the shop together trader to a list in economy
        if (input.getSettings().getIsShopTogether()) {
            topology.addShopTogetherTraders(output);
        }
        // adds the placement entity trader to a list in economy
        if (input.getIsPlacementEntity()) {
            output.setPlacementEntity(true);
            topology.getEconomy().getPlacementEntities().add(output);
        }
        output.setTemplateProvider(input.getTemplateProvider());

        return output;
    }

    // Methods for converting ActionDTOs.

    /**
     * Converts an {@link ActionTO} to an {@link Action} given some additional context.
     *
     * @param input The {@link ActionTO} to convert.
     * @param economy The {@link Economy} containing the acted on objects.
     * @param shoppingList A function mapping OIDs to {@link ShoppingList}s in <b>economy</b>.
     * @param trader A function mapping OIDs to {@link Trader}s in <b>economy</b>.
     * @return The resulting {@link Action}.
     */
    public static @NonNull Action action(@NonNull ActionTO input, @NonNull Economy economy,
            LongFunction<ShoppingList> shoppingList, LongFunction<Trader> trader) {
        switch (input.getActionTypeCase()) {
            case MOVE:
                return new Move(economy, shoppingList.apply(input.getMove().getShoppingListToMove()),
                    input.getMove().hasSource() ? trader.apply(input.getMove().getSource()) : null,
                    input.getMove().hasDestination() ? trader.apply(input.getMove().getDestination()) : null);
            case RECONFIGURE:
                return new Reconfigure(economy, shoppingList.apply(input.getReconfigure().getShoppingListToReconfigure()));
            case ACTIVATE:
                Trader traderToActivate = trader.apply(input.getActivate().getTraderToActivate());
                return new Activate(economy, traderToActivate,
                                basket(input.getActivate().getTriggeringBasketList()),
                                trader.apply(input.getActivate().getModelSeller()),
                                traderToActivate.getBasketSold().get(
                                                input.getActivate().getMostExpensiveCommodity()));
            case DEACTIVATE:
                return new Deactivate(economy, trader.apply(input.getDeactivate().getTraderToDeactivate()),
                                      basket(input.getDeactivate().getTriggeringBasketList()));
            case PROVISION_BY_DEMAND:
                return new ProvisionByDemand(economy,
                                shoppingList.apply(input.getProvisionByDemand().getModelBuyer()),
                                trader.apply(input.getProvisionByDemand().getModelSeller()));
            case PROVISION_BY_SUPPLY:
                Trader modelSeller = trader.apply(input.getProvisionBySupply().getModelSeller());
                return new ProvisionBySupply(economy, modelSeller, modelSeller.getBasketSold()
                                .get(input.getProvisionBySupply().getMostExpensiveCommodity().getBaseType()));
            case RESIZE:
                return new Resize(economy,trader.apply(input.getResize().getSellingTrader()),
                    commoditySpecification(input.getResize().getSpecification()),
                    input.getResize().getOldCapacity(),input.getResize().getNewCapacity());
            case ACTIONTYPE_NOT_SET:
            default:
                throw new IllegalArgumentException("input = " + input);
        }
    }

    // Methods for converting CommunicationDTOs.

    /**
     * Populates the commodity resize dependency map of a {@link Topology} from information in an
     * {@link EndDiscoveredTopology} message.
     *
     * @param source The {@link EndDiscoveredTopology} message from which to get the map entries.
     * @param destination destination The {@link Topology} to put the entries to.
     */
    public static void populateCommodityResizeDependencyMap(@NonNull EndDiscoveredTopology source,
                                                 @NonNull Topology destination) {
        Map<Integer, List<CommodityResizeSpecification>> resizeDependencyMap =
                        destination.getModifiableCommodityResizeDependencyMap();
        for (CommodityResizeDependencyEntry entry : source.getResizeDependencyList()) {
            int commodityType = entry.getCommodityType();
            List<CommodityResizeDependency> dependentCommodities =
                                                entry.getCommodityResizeDependencyList();
            List<CommodityResizeSpecification> resizeSpecs =
                                                new ArrayList<>(dependentCommodities.size());
            for (CommodityResizeDependency dependentCommodity : dependentCommodities) {
                int dependentCommodityType = dependentCommodity.getDependentCommodityType();
                UpdatingFunctionTO incrementFunctionTO = dependentCommodity.getIncrementFunction();
                DoubleTernaryOperator incrementOperator = updatingFunction(incrementFunctionTO);
                UpdatingFunctionTO decrementFunctionTO = dependentCommodity.getDecrementFunction();
                DoubleTernaryOperator decrementOperator = updatingFunction(decrementFunctionTO);
                resizeSpecs.add(new CommodityResizeSpecification(dependentCommodityType,
                                                     incrementOperator, decrementOperator));
            }
            resizeDependencyMap.put(commodityType, resizeSpecs);

        }
    }


    /**
     * Populates the commodity history based resize dependency skip map of a {@link Topology}
     * from information in an {@link EndDiscoveredTopology} message.
     *
     * @param source The {@link EndDiscoveredTopology} message from which to get the map entries.
     * @param destination destination The {@link Topology} to put the entries to.
     */
    public static void populateHistoryBasedResizeDependencyMap(@NonNull EndDiscoveredTopology source,
                                                 @NonNull Topology destination) {
        Map<Integer, List<Integer>> resizeDependencyMap =
                        destination.getModifiableHistoryBasedResizeSkipDependency();

        for (ResizeDependencySkipEntry entry : source.getSkipListForHistoryBasedResizeList()) {
            resizeDependencyMap.put(entry.getCommodityType(), entry.getDependentCommoditiesList());
        }
    }

    /**
     * Populates the raw commodity map of a {@link Topology} from information in an
     * {@link EndDiscoveredTopology} message.
     *
     * @param source The {@link EndDiscoveredTopology} message from which to get the map entries.
     * @param destination destination The {@link Topology} to put the entries to.
     */
    public static void populateRawCommodityMap(@NonNull EndDiscoveredTopology source,
                                                            @NonNull Topology destination) {
        Map<Integer, RawMaterials> rawCommodityMap = destination.getModifiableRawCommodityMap();
        source.getRawMaterialEntryList().stream().forEach(entry ->
            rawCommodityMap.put(entry.getProcessedCommodityType(), new RawMaterials(entry.getRawCommoditiesList()))
        );
    }

    /**
     * Populates the produces commodity map of a {@link Topology} from information in an
     * {@link EndDiscoveredTopology} message.
     *
     * @param source The {@link EndDiscoveredTopology} message from which to get the map entries.
     * @param destination destination The {@link Topology} to put the entries to.
     */
    public static void populateCommodityProducesDependancyMap(@NonNull EndDiscoveredTopology source,
                                               @NonNull Topology destination) {
        source.getProducesEntryList().forEach(entry ->
                destination.addToModifiableCommodityProducesDependencyMap(entry.getBaseCommodityType(),
                                                                          entry.getCoDependantCommoditiesTypeList()));
    }

    public static void commToAdjustOverhead (@NonNull EndDiscoveredTopology source,
                                              @NonNull Topology destination) {
        for (CommoditySpecificationTO csTO : source.getCommToAllowOverheadInCloneList()) {
            destination.addCommsToAdjustOverhead(commoditySpecification(csTO));
        }

    }

    /**
     * Construct balance account map for a trader and save it in economy.
     *
     * @param topology the topology holding the economy
     * @param input the TraderTO holding the settings
     * @param destination the TraderSettings to be created based on source
     */
    private static void populateCloudSpent(@Nonnull Topology topology,
                                           @Nonnull TraderTO input,
                                           @Nonnull TraderSettings destination) {
        final TraderSettingsTO source = input.getSettings();
        final EconomyDTOs.Context sourceContext = source.getCurrentContext();
        final BalanceAccountDTO balanceAccountDTO = sourceContext.getBalanceAccount();
        BalanceAccount balanceAccount = topology.getEconomy().getBalanceAccountMap()
                        .get(balanceAccountDTO.getId());
        if (balanceAccount == null) {
            final Long parentId = balanceAccountDTO.hasParentId()
                    ? balanceAccountDTO.getParentId()
                    : null;
            balanceAccount = new BalanceAccount(
                    balanceAccountDTO.getSpent(),
                    balanceAccountDTO.getBudget(),
                    balanceAccountDTO.getId(),
                    balanceAccountDTO.getPriceId(),
                    parentId);
            topology.getEconomy().getBalanceAccountMap().put(balanceAccount.getId(),
                                                             balanceAccount);
        }
        // In the case where a region id is not present we want to set it to -1
        final long regionId = sourceContext.hasRegionId() ? sourceContext.getRegionId() : -1L;
        final Context context = new Context(regionId, sourceContext.getZoneId(), balanceAccount,
                sourceContext.getFamilyBasedCoverageList());
        destination.setContext(context);
    }

} // end ProtobufToAnalysis class
