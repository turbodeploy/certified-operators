package com.vmturbo.platform.analysis.ede;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.function.DoubleUnaryOperator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.Resize;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommodityResizeSpecification;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.EconomyConstants;
import com.vmturbo.platform.analysis.economy.EconomySettings;
import com.vmturbo.platform.analysis.economy.RawMaterials;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ledger.IncomeStatement;
import com.vmturbo.platform.analysis.ledger.Ledger;
import com.vmturbo.platform.analysis.pricefunction.PriceFunction;
import com.vmturbo.platform.analysis.utilities.Bisection;
import com.vmturbo.platform.analysis.utilities.DoubleTernaryOperator;
import com.vmturbo.platform.analysis.utilities.ResizeActionStateTracker;

/**
 * This class implements the resize decision logic.
 *
 */
public class Resizer {

    static final Logger logger = LogManager.getLogger(Resizer.class);
    static Map<CommoditySold, ResizeActionStateTracker> resizeImpact = new HashMap<>();
    private static final String NEGATIVE_QUANTITY_UPDATE = "Cap Negative Quantity Update to 0 for buyer {} with reverse {} and resold {}";
    private static final String NEGATIVE_PEAK_QUANTITY_UPDATE = "Cap Negative Peak Quantity Update to 0 for buyer {} with reverse {} and resold {}";
    public static int exceptionCounter = 0;

    /**
     * Return a list of actions to optimize the size of all eligible commodities in the economy.
     * <p>
     *  As a result of invoking this method, both the economy and the state that are passed as
     *  parameters to it, may be changed.
     *
     * @param economy The economy whose commodity we want to resize.
     * @param ledger The ledger to use for revenue and expense calculation.
     * @return A list of actions.
     */
    public static @NonNull List<@NonNull Action> resizeDecisions(@NonNull Economy economy,
                                                                 @NonNull Ledger ledger) {
        List<@NonNull Action> actions = new ArrayList<>();
        try {
            ConsistentResizer consistentResizer = new ConsistentResizer();
            for (Trader seller : economy.getTraders()) {
                try {
                    ledger.calculateCommodityExpensesAndRevenuesForTrader(economy, seller);
                    if (economy.getForceStop()) {
                        return actions;
                    }
                    if (seller.getSettings().isResizeThroughSupplier()) {
                        continue;
                    }
                    Basket basketSold = seller.getBasketSold();
                    boolean isDebugTrader = seller.isDebugEnabled();
                    String sellerDebugInfo = seller.getDebugInfoNeverUseInCode();
                    // do not resize if the basketSold is NULL or if the entity is INACTIVE
                    if (basketSold == null || !seller.getState().isActive()) {
                        if (logger.isTraceEnabled() || isDebugTrader) {
                            if (basketSold == null) {
                                logger.info("{" + sellerDebugInfo
                                        + "} will not be resized because basketSold is null.");
                            } else {
                                logger.info("{" + sellerDebugInfo
                                        + "} will not be resized because its state is "
                                        + seller.getState().toString() + ".");
                            }
                        }
                        continue;
                    }
                    List<IncomeStatement> incomeStatements = ledger.getCommodityIncomeStatements(seller);
                    for (int soldIndex = 0; soldIndex < basketSold.size(); soldIndex++) {
                        CommoditySold commoditySold = seller.getCommoditiesSold().get(soldIndex);
                        if (!commoditySold.getSettings().isResizable()) {
                            continue;
                        }
                        IncomeStatement incomeStatement = incomeStatements.get(soldIndex);
                        CommoditySpecification resizedCommodity = basketSold.get(soldIndex);
                        try {
                            boolean consistentResizing = seller.isInScalingGroup(economy);
                            boolean engage = evaluateEngageCriteria(economy, seller, commoditySold,
                                    incomeStatement);
                            /*
                             * All members of a scaling group are forced to generate a resize so that each
                             * trader can generate (but not take) a resize with a potential new capacity.
                             * These untaken resizes are called partial resizes, and are not taken until after
                             * all traders in the group have generated their own partial resizes.  The
                             * consistent resizer will then evaluate all of these partial resizes to determine
                             * the best new capacity for the entire group.
                             */
                            if (engage || consistentResizing) {
                                double expenses = incomeStatement.getExpenses();
                                if (consistentResizing || expenses > 0) {
                                    try {
                                        double desiredROI = getDesiredROI(incomeStatement);
                                        double newRevenue = expenses < incomeStatement.getMinDesiredExpenses()
                                                ? incomeStatement.getDesiredRevenues()
                                                : desiredROI * (Double.isInfinite(expenses)
                                                ? incomeStatement.getMaxDesiredExpenses()
                                                : expenses);
                                        double currentRevenue = incomeStatement.getRevenues();
                                        double desiredCapacity =
                                                calculateDesiredCapacity(commoditySold, newRevenue, seller, economy);
                                        Map<CommoditySold, Trader> rawMaterialMapping =
                                                RawMaterials.findSellerCommodityAndSupplier(economy, seller, soldIndex);
                                        Set<CommoditySold> rawMaterials = (rawMaterialMapping != null) ? rawMaterialMapping.keySet() : null;
                                        float rateOfResize = seller.getSettings().getRateOfResize();
                                        double newEffectiveCapacity = calculateEffectiveCapacity(economy, seller,
                                                resizedCommodity, desiredCapacity, commoditySold, rawMaterials, rateOfResize);
                                        boolean capacityChange = Double.compare(newEffectiveCapacity,
                                                commoditySold.getEffectiveCapacity()) != 0;
                                        if (consistentResizing || capacityChange) {
                                            double newCapacity = newEffectiveCapacity /
                                                    commoditySold.getSettings().getUtilizationUpperBound();
                                            Resize resizeAction = new Resize(economy, seller,
                                                    resizedCommodity, commoditySold, soldIndex, newCapacity);
                                            resizeAction.setImportance(currentRevenue - newRevenue);
                                            if (consistentResizing) {
                                                // If the action is the only one in a scaling group, we treat
                                                // it as a normal resize, so we need to pass in both engage
                                                // and capacity change to the delayed action generation logic.
                                                // See ConsistentResizer.ResizingGroup.generateResizes for
                                                // details.
                                                consistentResizer.addResize(resizeAction,
                                                        engage && capacityChange, rawMaterialMapping);
                                            } else {
                                                takeAndAddResizeAction(actions, resizeAction);
                                            }
                                        } else {
                                            if (logger.isTraceEnabled() || isDebugTrader) {
                                                logger.info("{" + sellerDebugInfo
                                                        + "} No resize for the commodity "
                                                        + resizedCommodity.getDebugInfoNeverUseInCode()
                                                        + " because the calculated capacity is the same to the"
                                                        + " current one.");
                                            }
                                        }
                                    } catch (Exception bisectionException) {
                                        /**
                                         *  This block is to catch checkArgument exceptions that may be
                                         *  generated by checkArgument in Bisection.solve.
                                         *  There are cases where revenueFunction passed to bisection
                                         *  calculates negative values for both end points of it.
                                         *  This is not valid and so we try to catch exception here and move
                                         *  to the next seller.
                                         *
                                         *  Also it catches null pointer exception that can happen when a VM on
                                         *  storage that is state UNKNOWN is trying size.
                                         */
                                        logger.info("Trader " + sellerDebugInfo
                                                + " while resizing threw exception "
                                                + bisectionException.getMessage() + " : Capacity "
                                                + commoditySold.getEffectiveCapacity() + " Quantity "
                                                + commoditySold.getQuantity() + " Revenues "
                                                + incomeStatement.getRevenues() + " Expenses "
                                                + incomeStatement.getExpenses());
                                    }
                                } else {
                                    if (logger.isTraceEnabled() || isDebugTrader) {
                                        logger.info("{" + sellerDebugInfo
                                                + "} No resize for the commodity "
                                                + resizedCommodity.getDebugInfoNeverUseInCode()
                                                + " because expenses are 0.");
                                    }
                                }
                            } else {
                                if (logger.isTraceEnabled() || isDebugTrader) {
                                    logger.info("{" + sellerDebugInfo
                                            + "} Resize engagement criteria are false for commodity "
                                            + resizedCommodity.getDebugInfoNeverUseInCode() + ".");
                                }
                            }
                        } catch (Exception e) {
                            if (exceptionCounter < EconomyConstants.EXCEPTION_PRINT_LIMIT) {
                                logger.error(EconomyConstants.EXCEPTION_MESSAGE, seller.getDebugInfoNeverUseInCode()
                                        + " Commodity " + resizedCommodity.getDebugInfoNeverUseInCode(),
                                        e.getMessage(), e);
                                exceptionCounter++;
                            }
                            economy.getExceptionTraders().add(seller.getOid());
                        }
                    }
                } catch (Exception e) {
                    if (exceptionCounter < EconomyConstants.EXCEPTION_PRINT_LIMIT) {
                        logger.error(EconomyConstants.EXCEPTION_MESSAGE, seller.getDebugInfoNeverUseInCode(),
                                e.getMessage(), e);
                        exceptionCounter++;
                    }
                    economy.getExceptionTraders().add(seller.getOid());
                }
            }
            // Consistently resize commodities in scaling groups
            consistentResizer.resizeScalingGroupCommodities(actions);
            if (!economy.getSettings().isResizeDependentCommodities()) {
                // rollback dependent commodity resize
                Lists.reverse(actions).stream().filter(Resize.class::isInstance)
                        .map(Resize.class::cast)
                        .forEach(a -> resizeDependentCommodities(economy, a.getSellingTrader(), a.getResizedCommodity(),
                                a.getSoldIndex(), a.getOldCapacity(), a.getResizedCommodity().isHistoricalQuantitySet(), true));
            }
            return actions;
        } catch (Exception e) {
            logger.error(EconomyConstants.EXCEPTION_MESSAGE, "Resizer",
                    e.getMessage(), e);
            return actions;
        } finally {
            resizeImpact.clear();
        }
    }

    /**
     * Take an untaken Resize action and generate debugging logging if configured to so do.  Also
     * adds the taken Resize action to the actions list.
     * @param actions list to add this action to
     * @param resizeAction the Resize action to take/log and add to actions list
     */
    static void takeAndAddResizeAction(final List<@NonNull Action> actions,
                                       final Resize resizeAction) {
        resizeAction.take(resizeAction.getResizedCommodity().isHistoricalQuantitySet());
        actions.add(resizeAction);
        Trader trader = resizeAction.getSellingTrader();
        if (logger.isTraceEnabled() || trader.isDebugEnabled()) {
            logger.info("{" + trader.getDebugInfoNeverUseInCode()
                + "} A resize action was generated for the commodity "
                + trader.getBasketSold().get(resizeAction.getSoldIndex())
                    .getDebugInfoNeverUseInCode()
                + " from " + resizeAction.getOldCapacity()
                + " to " + resizeAction.getNewCapacity() + ".");
        }
    }

    /**
     * Given the desired effective capacity find the new effective capacity taking into
     * consideration capacity increment and rightsizing rate.
     *
     * For a downward resize, the change is limited by observed use (considering both
     * maxQuantity and peakQuantity).
     *
     * In all cases, the the new size is not permitted to exceed the capacity of the seller.
     *
     * @param economy         The economy that the trader is a part of
     * @param seller          The seller whose commodity is being resized
     * @param commSpec        The commSpec being resized
     * @param desiredCapacity The calculated new desired effective capacity; may be greater or less than
     *                        than current capacity.
     * @param resizeCommodity The {@link CommoditySold commodity} being resized, to obtain peak usage,
     *                        current capacity and capacity increment.
     * @param rawMaterials    The source of raw material of {@link CommoditySold commoditySold}.
     * @param rateOfRightSize The user configured rateOfRightSize from {@link EconomySettings}.
     * @return The recommended new capacity.
     */
    private static double calculateEffectiveCapacity(Economy economy, Trader seller,
                                                     CommoditySpecification commSpec,
                                                     double desiredCapacity,
                                                     @NonNull CommoditySold resizeCommodity,
                                                     Set<CommoditySold> rawMaterials,
                                                     float rateOfRightSize) {
        checkArgument(rateOfRightSize > 0, "Expected rateOfRightSize to be > 0", rateOfRightSize);

        double currentCapacity = resizeCommodity.getEffectiveCapacity();
        double desiredAdjustment = desiredCapacity - currentCapacity;
        double capacityIncrement = resizeCommodity.getSettings().getCapacityIncrement() * resizeCommodity.getSettings().getUtilizationUpperBound();
        double adjustment = 0;
        if (desiredCapacity > currentCapacity) {
            adjustment = calculateResizeUpAmount(economy, seller, commSpec, desiredAdjustment, resizeCommodity, rawMaterials, capacityIncrement, rateOfRightSize);
        } else {
            // "amount" values are always non-negative, hence we negate the arg and the result for a downward resize
            adjustment = -calculateResizeDownAmount(economy, seller, commSpec, -desiredAdjustment, resizeCommodity, rawMaterials, capacityIncrement, rateOfRightSize);
        }
        return currentCapacity + adjustment;
    }

    /**
     * Compute the amount of increase for an upward resize.
     *
     * The desired amount is adjusted to conform to capacity increment and rightsizing rate.
     *
     * In addition, the amount is limited by available headroom in the seller.
     *
     * @param economy           The economy that the trader is a part of
     * @param seller            The seller
     * @param commSpec          The commSpec being resized
     * @param desiredAmount     The desired capacity increase; must be non-negative
     * @param resizeCommodity   The {@link CommoditySold commodity} being resized.
     * @param rawMaterials      The source of raw materials supplying the {@link CommoditySold commodity} being resized.
     * @param capacityIncrement The capacity increment for the {@link CommoditySold commodity} being resized.
     * @param rateOfRightSize   The user configured rightsizing rate from {@link EconomySettings}.
     * @return                  The recommended amount of increase for {@link CommoditySold commodity} being resized; always non-negative
     */
    private static double calculateResizeUpAmount(Economy economy, Trader seller, CommoditySpecification commSpec,
                                                  double desiredAmount,
                                                  @NonNull CommoditySold resizeCommodity,
                                                  Set<CommoditySold> rawMaterials,
                                                  double capacityIncrement,
                                                  double rateOfRightSize) {
        // If the current capacity is already above the raw material's capacity, do not resize up further.
        double finalDesiredIncrement = Double.MAX_VALUE;
        double capacityUpperBound = resizeCommodity.getSettings().getCapacityUpperBound();
        double maxAmount = capacityUpperBound - resizeCommodity.getEffectiveCapacity();
        if (logger.isTraceEnabled() || seller.isDebugEnabled()) {
            logger.info("The max amount we can resize up {}/{} is {}. This is derived from currentCapacity {} and capacity upper bound {}",
                    seller.getDebugInfoNeverUseInCode(), commSpec.getDebugInfoNeverUseInCode(),
                    maxAmount, resizeCommodity.getEffectiveCapacity(), capacityUpperBound);
        }

        if (maxAmount < desiredAmount) {
            desiredAmount = maxAmount;
        }
        double incrementsValue = desiredAmount / (capacityIncrement * rateOfRightSize);
        // Compute increments sufficient for desired amount.
        // Need to ensure that we do not go above the capacityUpperBound since we do
        // desiredAmount / (capacityIncrement * rateOfRightSize) and we take the ceil() of the
        // decimal. When we take the ceil(), rounding up can bring the capacity above the capacityUpperBound
        // which is why we need to take the floor() when constricted by the capacity upper bound.
        // The example where this happens is we have a capacity upper bound of 8 vCPUs worth of MHz
        // and we resize the VM from 6 to 9 vCPUs because we end up with a decimal for
        // desiredAmount/capacityIncrement.
        // Also need to ensure when capacity upper bound is larger than desired new capacity, the
        // final desired increment won't be changed.
        int numIncrements = (int)Math.ceil(incrementsValue) * capacityIncrement <= maxAmount
            ? (int)Math.ceil(incrementsValue)
            : (int)Math.floor(incrementsValue);

        for (CommoditySold rawMaterial : rawMaterials) {
            if (rawMaterial != null
                    && (resizeCommodity.getEffectiveCapacity() + capacityIncrement > rawMaterial.getEffectiveCapacity())) {
                if (logger.isTraceEnabled() || seller.isDebugEnabled()) {
                    logger.info("Cannot resize {}/{} further up. Current capacity {} + capacityIncrement {} "
                                    + "is above raw material capacity {}. Not resizing.", seller.getDebugInfoNeverUseInCode(),
                            commSpec.getDebugInfoNeverUseInCode(), resizeCommodity.getEffectiveCapacity(),
                            capacityIncrement, rawMaterial.getEffectiveCapacity());
                }
                return 0;
            }

            // make sure we don't exceed available headroom in seller
            double headroom = rawMaterial == null ? Double.MAX_VALUE :
                    resizeCommodity.getSettings().getUtilizationUpperBound()
                            * (rawMaterial.getEffectiveCapacity() - rawMaterial.getQuantity());
            // Need to consider capacity headroom as the comparison between the commodity to resize
            // and then provider's commodity (rawMaterial) as we can't generate a scale up to be higher
            // capacity than the seller's provider has.
            double capacityHeadroom = rawMaterial == null ? Double.MAX_VALUE :
                    rawMaterial.getEffectiveCapacity() - resizeCommodity.getEffectiveCapacity();
            if (headroom > 0 && capacityHeadroom >= capacityIncrement) {
                if (headroom < numIncrements * capacityIncrement) {
                    numIncrements = (int) Math.floor(headroom / capacityIncrement);
                }
                int availableIncrements = (int) Math.floor(capacityHeadroom / capacityIncrement);
                double desiredIncrement = Math.min(availableIncrements, numIncrements) * capacityIncrement;
                if (rawMaterial == null) {
                    // This is possible in case of portchannel where the raw material is dummy value.
                    logger.debug("Raw material is null. Resizing up by {} for {} on {}",
                            desiredIncrement, commSpec.getDebugInfoNeverUseInCode(),
                            seller.getDebugInfoNeverUseInCode());
                    return desiredIncrement;
                }
                finalDesiredIncrement = Math.min(desiredIncrement, finalDesiredIncrement);
            } else {
                return 0.0;
            }
        }
        // in the case of heap resizing up, we make sure that the sum of all heapCapacities sold by allServers
        // is less than the vMemSoldCapacity. This sum is not going to be same as vMemUsed and hence
        // this check is different from the rawMaterial validation.
        // TODO: avoid duplicating the calculation of CapacityOnCoConsumersForResizeUp when there are multiple rawMaterials
        double totalCapOnConsumers = calculateTotalCapacityOnCoConsumersForResizeUp(economy, commSpec, seller);
        for (CommoditySold rawMaterial : rawMaterials) {
            if (totalCapOnConsumers + finalDesiredIncrement > rawMaterial.getEffectiveCapacity()) {
                logger.debug("The sum of the effCap sold by all the customers is {}. This is "
                                + "much larger than the raw material {}'s effCap {} after resizing up by {} for {} on {}",
                        totalCapOnConsumers, rawMaterial.getEffectiveCapacity(),
                        finalDesiredIncrement, commSpec.getDebugInfoNeverUseInCode(), seller.getDebugInfoNeverUseInCode());
                return 0.0;
            }
        }
        return finalDesiredIncrement;
    }

    /**
     * When a rawMaterial like vMem tries resizingUp, return the dependant commodity sold by the customers.
     * These dependant commodities are heap and dbMem
     *
     * @param economy           The economy that the trader is a part of
     * @param commSpec          The commSpec being resized
     * @param seller            The seller
     * @return                  Total Capacity of commoditySold across all customers
     */
    private static double calculateTotalCapacityOnCoConsumersForResizeUp(Economy economy,
                                                                                   CommoditySpecification commSpec,
                                                                                   Trader seller) {
        // co-dependant commodity capacity validation
        // this means that there is a mapping for the commodity and the commodity is resizeUpAware
        double totalCap = 0;
        Optional<RawMaterials> rawMaterials = economy.getRawMaterials(commSpec.getBaseType());
        Set<ShoppingList> sls = economy.getMarketsAsBuyer(seller).keySet();
        if (rawMaterials.isPresent()) {
            for (int baseCommType : rawMaterials.get().getMaterials()) {
                // for dbMem scale up, vMem and dbCacheHitRate are in the rawMaterialsMap
                // consider just the commodity that has an entry in the producesMap. Its just vMem that passes this
                if (economy.getResizeProducesDependencyEntry(baseCommType) != null) {
                    // check if the supplier has the related commodity in its basket
                    Optional<Trader> optionalSupplier = sls.stream()
                            .map(ShoppingList::getSupplier)
                            .filter(Objects::nonNull)
                            .filter(trader -> trader.getBasketSold().indexOfBaseType(baseCommType) != -1).findFirst();
                    if (optionalSupplier.isPresent()) {
                        // a VM can host both appServer and a dbServer. In this case, we need to sum the usage
                        // of dbMemCap and heapCap sold by all customers
                        if (economy.getResizeProducesDependencyEntry(baseCommType) != null) {
                            for (int commType : economy.getResizeProducesDependencyEntry(baseCommType)) {
                                totalCap += sumUpCapacitiesSoldByCustomersForCommodity(optionalSupplier.get(), commType);
                            }
                        }
                    }
                }
            }
        }
        return totalCap;
    }

    /**
     * When a commodity like heap or dbMem tries resizingDown, return the sum of capacities of dependant commodity sold
     * by the co-customers. These dependant commodities are heap and dbMem.
     *
     * @param economy           The economy that the trader is a part of
     * @param commSpec          The commSpec being resized
     * @param seller            The seller
     * @return                  Total capacity of commoditySold across all customers
     */
    private static double calculateTotalCapacityOnCoConsumersForResizeDown(Economy economy,
                                                                                   CommoditySpecification commSpec,
                                                                                   Trader seller) {
        // co-dependant commodity capacity validation
        // this means that there is a mapping for the commodity and the commodity is resizeUpAware
        double totalCap = 0;
        if (economy.getResizeProducesDependencyEntry(commSpec.getBaseType()) != null) {
            // for vMem resizing down, the related commodities are heap or dbMem
            for (Integer commType : economy.getResizeProducesDependencyEntry(commSpec.getBaseType())) {
                // check if customer sells commType. eg, heap or dbMem in the case of an AppServer or dbServer respectively
                // and sum up the resourceCapacities
                totalCap += sumUpCapacitiesSoldByCustomersForCommodity(seller, commType);
            }
        }
        return totalCap;
    }

    /**
     * return the sum of capacities of commodity sold of a particular type sold by the customers of a seller.
     *
     * @param seller            The seller
     * @param commType          Type of commodity being sold by the customers
     * @return                  Total Capacity of commoditySold across all customers
     */
    private static double sumUpCapacitiesSoldByCustomersForCommodity(Trader seller,
                                                                        Integer commType) {
        // check if customer sells commType. eg, heap or dbMem in the case of an AppServer or dbServer respectively
        // and sum up the resourceCapacities
        return seller.getCustomers().stream().map(ShoppingList::getBuyer)
                .filter(trader -> trader.getBasketSold().indexOfBaseType(commType) != -1)
                .map(trader -> trader.getCommoditiesSold().get(trader.getBasketSold().indexOfBaseType(commType)))
                .map(CommoditySold::getCapacity)
                .mapToDouble(Double::doubleValue).sum();
    }

    /**
     * Compute the amount of decrease for a downward resize.
     * <p>
     * The desired amount is adjusted to conform to capacity increment and rightsizing rate.
     * <p>
     * In addition, the following limits are placed on the resize amount:
     *
     * <ul>
     * <li>The capacity is not allowed to drop below the commodity's historical percentile commodity
     *     if available. If not available, then the capacity is not allowed to drop below the max
     *     quantity. In both cases, it cannot drop below the peakQuantity.</li>
     * <li>The capacity is not allowed to change if there is no current or historical use data.</li>
     * <li>The capacity is not allowed to drop below a single capacity increment amount.</li>
     * </ul>
     *
     * @param economy           The economy that the trader is a part of
     * @param seller            the seller whose commodity is being resized
     * @param commSpec          the commSpec which is being resized
     * @param desiredAmount     the desired capacity decrease; must be non-negative.
     * @param resizeCommodity   the {@link CommoditySold commodity} being resized.
     * @param rawMaterials      the source of raw materials supplying the {@link CommoditySold commodity} being resized.
     * @param capacityIncrement the capacity increment for the {@link CommoditySold commodity} being resized.
     * @param rateOfRightSize   the user configured rightsizing rate from {@link EconomySettings}.
     * @return the recommended amount of decrease for {@link CommoditySold commodity} being resized; always non-negative.
     */
    private static double calculateResizeDownAmount(Economy economy, Trader seller, CommoditySpecification commSpec,
                                                    double desiredAmount,
                                                    @NonNull CommoditySold resizeCommodity,
                                                    Set<CommoditySold> rawMaterials,
                                                    double capacityIncrement,
                                                    double rateOfRightSize) {
        double currentCapacity = resizeCommodity.getEffectiveCapacity();
        if (logger.isTraceEnabled() || seller.isDebugEnabled()) {
            logger.info("calculateResizeDownAmount for trader {}"
                + " with isHistoricalQuantitySet={},"
                + " historicalOrElseCurrentQuantity={}"
                + " maxQuantity={}",
                () -> seller.getDebugInfoNeverUseInCode(),
                () -> resizeCommodity.isHistoricalQuantitySet(),
                () -> resizeCommodity.getHistoricalOrElseCurrentQuantity(),
                () -> resizeCommodity.getMaxQuantity());
        }
        final double historicalOrMaxQuantity;
        if (resizeCommodity.isHistoricalQuantitySet()) {
            historicalOrMaxQuantity = resizeCommodity.getHistoricalOrElseCurrentQuantity();
        } else {
            // On some commodities we do not track the historical percentile. As a result, we fall
            // back to max quantity to ensure we do not resize outside of the customer's expectation.
            historicalOrMaxQuantity = resizeCommodity.getMaxQuantity();
        }
        double peakQuantity = resizeCommodity.getPeakQuantity();

        // don't permit downward resize if there's no usage data
        if (resizeCommodity.getQuantity() == 0 && historicalOrMaxQuantity == 0 && peakQuantity == 0) {
            return 0.0;
        }
        double capacityLowerBound = resizeCommodity.getSettings().getCapacityLowerBound();
        // don't permit resize below historical max/peak or below capacity lower bound
        double maxAmount = currentCapacity - Math.max(Math.max(historicalOrMaxQuantity, peakQuantity), capacityLowerBound);
        if (logger.isTraceEnabled() || seller.isDebugEnabled()) {
            logger.info("The max amount we can resize down {}/{} is {}. This is derived from currentCapcity {}, max {}, peak {}, capcityLowerBound {}",
                seller.getDebugInfoNeverUseInCode(), commSpec.getDebugInfoNeverUseInCode(),
                maxAmount, currentCapacity, historicalOrMaxQuantity, peakQuantity, capacityLowerBound);
        }
        if (maxAmount < 0) {
            return 0.0;
        } else if (maxAmount < desiredAmount) {
            desiredAmount = maxAmount;
        }

        // compute # of increments that will keep capacity at or above desired level
        int numDecrements = (int) Math.floor(desiredAmount / (capacityIncrement * rateOfRightSize));
        // if resizing rate > 1, then we could compute zero increments even with resizing rate > 1.
        // So here we just make sure our resize doesn't turn into a no-op in that case.
        // See OM-34833 for more info.
        if (desiredAmount >= capacityIncrement && numDecrements == 0) {
            numDecrements = 1;
        }
        // Don't resize to a capacity less than the capacity increment
        double amount = numDecrements * capacityIncrement;
        if (currentCapacity - amount < capacityIncrement) {
            amount = currentCapacity >= 2 * capacityIncrement ? currentCapacity - capacityIncrement : 0.0;
        }
        // If the final number to go to is greater than the raw material capacity, and if the resize
        // commodity original capacity is less than the rawMaterial's, then adjusted amount
        // will be 0. When the resize commodity's original capacity is bigger than the rawMaterial's
        // probably due to hyper-threading, then allow the new capacity after resize down to be greater
        // than its rawMaterial.
        double newCapacity = currentCapacity - amount;
        for (CommoditySold rawMaterial : rawMaterials) {
            if (rawMaterial != null && resizeCommodity.getCapacity() < rawMaterial.getCapacity()
                    && newCapacity > rawMaterial.getEffectiveCapacity()) {
                amount = 0;
                if (logger.isTraceEnabled() || seller.isDebugEnabled()) {
                    logger.info("New resized capacity for {}/{} is {}. But this is above rawMaterial "
                                    + "capacity {}. Not resizing.", seller.getDebugInfoNeverUseInCode(),
                            commSpec.getDebugInfoNeverUseInCode(), newCapacity, rawMaterial.getEffectiveCapacity());
                }
            }
        }
        double totalCapOnCoConsumers = calculateTotalCapacityOnCoConsumersForResizeDown(economy, commSpec, seller);
        // if the total capacity on all the consumers for the dependant commodities is much larger than the newCapacity, dont resize
        if (totalCapOnCoConsumers > currentCapacity - amount) {
            logger.debug("The sum of the Capacities sold by all the customers is {}. This is "
                            + "much larger than {}'s Capacity {} after resizing down by {} for {}",
                            totalCapOnCoConsumers, commSpec.getDebugInfoNeverUseInCode(), currentCapacity,
                            amount, seller.getDebugInfoNeverUseInCode());
            return 0;
        }
        return amount;
    }

    /**
     * Returns the desired ROI for a commodity.
     *
     * @param incomeStatement The income statement of the commodity.
     * @return The desired ROI.
     */
    private static double getDesiredROI(IncomeStatement incomeStatement) {
        // approximate as the average of min and max desired ROIs
        return (incomeStatement.getMinDesiredROI() + incomeStatement.getMaxDesiredROI()) / 2;
    }

    /**
     * Checks the resize engagement criteria for a commodity.
     *
     * @param economy The {@link Economy}.
     * @param resizeCommodity The {@link CommoditySold commodity} that is to be resized.
     * @param commodityIS The {@link IncomeStatement income statement} of the commodity sold.
     * @return Whether the commodity meets the resize engagement criterion.
     */
    public static boolean evaluateEngageCriteria(Economy economy, Trader seller,
                                                 CommoditySold resizeCommodity,
                                                 IncomeStatement commodityIS) {
        boolean eligibleForResizeDown = seller.getSettings().isEligibleForResizeDown();
        double currentCapacity = resizeCommodity.getEffectiveCapacity();
        double currentUtilization = resizeCommodity.getHistoricalOrElseCurrentQuantity()
                        / currentCapacity;
        // do not resize if utilization is in acceptable range
        // or if resizeDown warm up interval not finish
        EconomySettings settings = economy.getSettings();

        boolean isDebugTrader = seller.isDebugEnabled();
        String sellerDebugInfo = seller.getDebugInfoNeverUseInCode();

        if (currentUtilization > settings.getRightSizeLower() &&
            currentUtilization < settings.getRightSizeUpper()) {
            if (logger.isTraceEnabled() || isDebugTrader) {
                logger.info("{" + sellerDebugInfo + "} will not be resized because the"
                            + " current utilization (" + currentUtilization + ") is bigger than"
                            + "the lower right size limit (" + settings.getRightSizeLower()
                            + ") and lower than the upper right size limit ("
                            + settings.getRightSizeUpper() + ").");
            }
            return false;
        }
        if ((commodityIS.getROI() > commodityIS.getMaxDesiredROI() &&
                currentUtilization > settings.getRightSizeUpper()) ||
               (commodityIS.getROI() < commodityIS.getMinDesiredROI() &&
                currentUtilization < settings.getRightSizeLower() &&
               eligibleForResizeDown)) {
            return true;
        } else {
            if (logger.isTraceEnabled() || isDebugTrader) {
                boolean foundReason = false;
                StringBuilder message = new StringBuilder("{" + sellerDebugInfo + "} will not be"
                                                        + " resized because");
                if (commodityIS.getROI() <= commodityIS.getMaxDesiredROI()) {
                    message.append(" current ROI (" + commodityIS.getROI() + ") is smaller than or"
                                    + " equal to max desired ROI (" + commodityIS.getMaxDesiredROI()
                                    + ")");
                    foundReason = true;
                }

                if (currentUtilization <= settings.getRightSizeUpper()) {
                    message.append((foundReason ? "," : "") + " current utilization ("
                                    + currentUtilization + ") is smaller than or equal to right"
                                    + " size upper limit (" + settings.getRightSizeUpper() + ")");
                }

                if (commodityIS.getROI() >= commodityIS.getMinDesiredROI()) {
                    message.append(", current ROI (" + commodityIS.getROI() + ") is bigger than or"
                                    + " equal to min desired ROI ("
                                    + commodityIS.getMinDesiredROI() + ")");
                }

                if (currentUtilization >= settings.getRightSizeLower()) {
                    message.append(", current utilization (" + currentUtilization + ") is bigger"
                                    + " than or equal to right size lower limit ("
                                    + settings.getRightSizeLower() + ")");
                }

                if (!eligibleForResizeDown) {
                    message.append(", it is not eligible for resize down");
                }

                message.append(".");
                logger.info(message);
            }
            return false;
        }
    }

    /**
     * Finds the new commodity capacity needed for the target revenue.
     *
     * @param resizeCommodity Commodity to be resized.
     * @param newRevenue The target revenue for the commodity after resize.
     * @param seller The seller that we are finding the desired capacity for.
     * @param economy The economy that this seller operates in.
     * @return The new capacity
     */
    private static double calculateDesiredCapacity(CommoditySold resizeCommodity, double newRevenue,
                                                   Trader seller, Economy economy) {
        double currentQuantity = resizeCommodity.getHistoricalOrElseCurrentQuantity();
        PriceFunction priceFunction = resizeCommodity.getSettings().getPriceFunction();

        // interval which is almost (0,1) to avoid divide by zero
        double intervalMin = Math.nextAfter(0.0, 1.0);
        double intervalMax = Math.nextAfter(1.0, 0.0);

        // solve revenueFunction(u) = newRevenue for u in (intervalMin,intervalMax)
        DoubleUnaryOperator revenueFunction = u -> u * priceFunction.unitPrice(u, null,
                                                     seller, resizeCommodity, economy) - newRevenue;
        return currentQuantity / Bisection.solve(revenueFunction, intervalMin, intervalMax);
    }

    /**
     * For resize, update the quantity of the dependent commodity.
     *
     * @param economy The {@link Economy}.
     * @param seller The {@link Trader} selling the {@link CommoditySold commodity}.
     * @param commoditySold The {@link CommoditySold commodity} sold that was resized
     *                      and may trigger resize of other commodities.
     * @param commoditySoldIndex The index of {@link CommoditySold commodity} sold in basket.
     * @param newCapacity The new capacity.
     * @param basedOnHistorical Is this action based on historical quantity? The simulation
     * @param reverse is true when we need to reverse the resize of the dependent commodities.
     * on dependent commodities changes based on the parameter.
     */
    public static void resizeDependentCommodities(@NonNull Economy economy, @NonNull Trader seller,
                                                  @NonNull CommoditySold commoditySold, int commoditySoldIndex,
                                                  double newCapacity, boolean basedOnHistorical, boolean reverse) {
        try {
            List<CommodityResizeSpecification> typeOfCommsBought = economy.getResizeDependency(
                                     seller.getBasketSold().get(commoditySoldIndex).getBaseType());
            if (typeOfCommsBought == null || typeOfCommsBought.isEmpty()) {
                return;
            }
            List<Integer> skippedCommodityTypes =
                            economy.getHistoryBasedResizeSkippedDependentCommodities(seller
                                            .getBasketSold().get(commoditySoldIndex).getBaseType());

            for (ShoppingList shoppingList : economy.getMarketsAsBuyer(seller).keySet()) {

                Trader supplier = shoppingList.getSupplier();
                Basket basketBought = shoppingList.getBasket();
                for (CommodityResizeSpecification typeOfCommBought : typeOfCommsBought) {
                    // If this resize is based on historical quantities and this commodity should
                    // be skipped when the resize is based on historical quantity continue
                    if (basedOnHistorical
                            && skippedCommodityTypes != null
                            && skippedCommodityTypes.contains(typeOfCommBought.getCommodityType())) {
                        continue;
                    }

                    int boughtIndex = basketBought.indexOfBaseType(typeOfCommBought.getCommodityType());
                    if (boughtIndex < 0) {
                        continue;
                    }
                    CommoditySpecification specOfSold = basketBought.get(boughtIndex);
                    CommoditySold commSoldBySupplier = supplier.getCommoditySold(specOfSold);
                    double changeInCapacity = newCapacity - commoditySold.getCapacity();

                    updateUsageOnBoughtAndSoldCommodities(commoditySold, commSoldBySupplier, shoppingList, boughtIndex,
                                                            typeOfCommBought, newCapacity, changeInCapacity, reverse, false);

                    if (commSoldBySupplier.getSettings().isResold()) {
                        resizeDependentCommoditiesOnReseller(commoditySold, economy, seller, supplier, specOfSold,
                                typeOfCommBought, newCapacity, changeInCapacity, reverse);
                    }
                }
            }
        } catch (Exception e) {
            if (exceptionCounter < EconomyConstants.EXCEPTION_PRINT_LIMIT) {
                logger.error(EconomyConstants.EXCEPTION_MESSAGE, seller.getDebugInfoNeverUseInCode()
                        + " Commodity " + seller.getBasketSold().get(commoditySoldIndex).getDebugInfoNeverUseInCode(),
                        e.getMessage(), e);
                exceptionCounter++;
            }
            economy.getExceptionTraders().add(seller.getOid());
        }
    }

    /**
     * Recursively identify the dependent commodities on the resellers and update usage on buyer and seller.
     *
     * @param resizingCommodity is the commodity that is scaling.
     * @param economy           The {@link Economy}.
     * @param consumer          The {@link Trader} buying the dependent commodity and being resized.
     * @param supplier          The {@link Trader} selling the dependent commodity.
     * @param commSpec          The specification of the dependent commodity being scaled.
     * @param typeOfCommBought  is the dependant CommoditySpecification.
     * @param newCapacity       is the new desired capacity.
     * @param changeInCapacity  The change in capacity.
     * @param reverse           true if reverse updating
     */
    private static void resizeDependentCommoditiesOnReseller(CommoditySold resizingCommodity,
                                                             Economy economy, Trader consumer,
                                                             Trader supplier, CommoditySpecification commSpec,
                                                             CommodityResizeSpecification typeOfCommBought,
                                                             double newCapacity, double changeInCapacity, boolean reverse) {
        Optional<ShoppingList> slOfSupplier = economy.getMarketsAsBuyer(supplier).keySet().stream()
                .filter(sl -> sl.getBasket().indexOfBaseType(commSpec.getBaseType()) != -1)
                .findFirst();
        slOfSupplier.ifPresent(sl -> {
            if (sl.getSupplier() == null) {
                logger.warn("The supplier of reseller {} is null when resizing {}.", supplier.getDebugInfoNeverUseInCode(),
                    consumer.getDebugInfoNeverUseInCode());
                return;
            }
            int soldIndex = sl.getSupplier().getBasketSold().indexOfBaseType(commSpec.getBaseType());
            CommoditySold commSoldBySupplier = sl.getSupplier().getCommoditiesSold().get(soldIndex);
            updateUsageOnBoughtAndSoldCommodities(resizingCommodity, commSoldBySupplier, sl,
                    sl.getBasket().indexOfBaseType(commSpec.getBaseType()),
                    typeOfCommBought, newCapacity, changeInCapacity, reverse, true);
            if (commSoldBySupplier.getSettings().isResold()) {
                resizeDependentCommoditiesOnReseller(resizingCommodity, economy, consumer, sl.getSupplier(),
                    commSpec, typeOfCommBought, newCapacity, changeInCapacity, reverse);
            }
        });
    }

    /**
     * update the usage of both bought quantity and sold quantity by changeInCapacity.
     *
     * @param resizingCommodity commodity resizing.
     * @param commSoldBySupplier The commoditySold by the seller whose usage we update.
     * @param shoppingList The {@link ShoppingList} of the consumer.
     * @param boughtIndex index of the bought commodity.
     * @param typeOfCommBought is the dependant CommoditySpecification.
     * @param newCapacity is the new desiredCapacity.
     * @param changeInCapacity change in capacity.
     * @param reverse reverses updation when true
     * @param isResold true if updating commodity on a reseller.
     */
    private static void updateUsageOnBoughtAndSoldCommodities(CommoditySold resizingCommodity,
                                                              CommoditySold commSoldBySupplier,
                                                              ShoppingList shoppingList, int boughtIndex,
                                                              CommodityResizeSpecification typeOfCommBought,
                                                              double newCapacity, double changeInCapacity,
                                                              boolean reverse, boolean isResold) {

        logger.trace("consumer : {}, boughtQnty : {}, peakQnty : {}, newCap : {}, capChange : {}",
                shoppingList.getBuyer().getDebugInfoNeverUseInCode(),
                shoppingList.getQuantity(boughtIndex), shoppingList.getPeakQuantity(boughtIndex),
                newCapacity, changeInCapacity);
        if (!reverse) {
            if (changeInCapacity < 0) {
                // resize down
                double oldQuantityBought = shoppingList.getQuantities()[boughtIndex];
                double oldPeakQuantityBought = shoppingList.getPeakQuantities()[boughtIndex];
                // If the commodity being updated is on a reseller, we just decrement the consumption by the changeInCapacity.
                if (isResold) {
                    double newQuantityBought = oldQuantityBought + changeInCapacity;
                    double newPeakQuantityBought = oldPeakQuantityBought + changeInCapacity;
                    shoppingList.getQuantities()[boughtIndex] = newQuantityBought;
                    shoppingList.getPeakQuantities()[boughtIndex] = newPeakQuantityBought;
                    if (commSoldBySupplier.getQuantity() + changeInCapacity < 0) {
                        logger.error(NEGATIVE_QUANTITY_UPDATE, shoppingList.getBuyer().getDebugInfoNeverUseInCode(),
                                reverse, isResold);
                    }
                    commSoldBySupplier.setQuantity(Math.max(0, commSoldBySupplier.getQuantity() + changeInCapacity));
                    if (commSoldBySupplier.getPeakQuantity() + changeInCapacity < 0) {
                        logger.warn(NEGATIVE_PEAK_QUANTITY_UPDATE, shoppingList.getBuyer().getDebugInfoNeverUseInCode(),
                                reverse, isResold);
                    }
                    commSoldBySupplier.setPeakQuantity(Math.max(0, commSoldBySupplier.getPeakQuantity() + changeInCapacity));
                } else {
                    DoubleTernaryOperator decrementFunction = typeOfCommBought.getDecrementFunction();
                    double decrementedQuantity = decrementFunction.applyAsDouble(
                            oldQuantityBought, newCapacity, 0);
                    double decrementedPeakQuantity = decrementFunction.applyAsDouble(
                            oldPeakQuantityBought, newCapacity, 0);
                    double newQuantityBought = commSoldBySupplier.getQuantity() - (oldQuantityBought - decrementedQuantity);
                    double newPeakQuantityBought = commSoldBySupplier.getPeakQuantity() - (oldPeakQuantityBought - decrementedPeakQuantity);
                    if (newQuantityBought < 0) {
                        logger.error(NEGATIVE_QUANTITY_UPDATE, shoppingList.getBuyer().getDebugInfoNeverUseInCode(),
                                reverse, isResold);
                    }
                    commSoldBySupplier.setQuantity(Math.max(0, newQuantityBought));
                    if (newPeakQuantityBought < 0) {
                        logger.warn(NEGATIVE_PEAK_QUANTITY_UPDATE, shoppingList.getBuyer().getDebugInfoNeverUseInCode(),
                                reverse, isResold);
                    }
                    commSoldBySupplier.setPeakQuantity(Math.max(0, newPeakQuantityBought));
                    shoppingList.getQuantities()[boughtIndex] = decrementedQuantity;
                    shoppingList.getPeakQuantities()[boughtIndex] = decrementedPeakQuantity;
                }
                resizeImpact.putIfAbsent(resizingCommodity, new ResizeActionStateTracker());
                resizeImpact.get(resizingCommodity).addEntry(commSoldBySupplier, oldQuantityBought, oldPeakQuantityBought);
            } else {
                // resize up
                DoubleTernaryOperator incrementFunction = typeOfCommBought.getIncrementFunction();
                double oldQuantityBought = shoppingList.getQuantities()[boughtIndex];
                double oldPeakQuantityBought = shoppingList.getPeakQuantities()[boughtIndex];
                double incrementedQuantity = incrementFunction.applyAsDouble(oldQuantityBought, changeInCapacity, 0);
                double incrementedPeakQuantity = incrementFunction.applyAsDouble(oldPeakQuantityBought, changeInCapacity, 0);
                shoppingList.getQuantities()[boughtIndex] = incrementedQuantity;
                shoppingList.getPeakQuantities()[boughtIndex] = incrementedPeakQuantity;
                commSoldBySupplier.setQuantity(commSoldBySupplier.getQuantity()
                        + (incrementedQuantity - oldQuantityBought));
                commSoldBySupplier.setPeakQuantity(commSoldBySupplier.getPeakQuantity()
                        + (incrementedPeakQuantity - oldPeakQuantityBought));
                resizeImpact.putIfAbsent(resizingCommodity, new ResizeActionStateTracker());
                resizeImpact.get(resizingCommodity).addEntry(commSoldBySupplier, oldQuantityBought, oldPeakQuantityBought);
            }
        } else {
            double oldQuantityBought = resizeImpact.get(resizingCommodity).getQuantity(commSoldBySupplier);
            double oldPeakQuantityBought = resizeImpact.get(resizingCommodity).getPeakQuantity(commSoldBySupplier);
            double currentQuantityBought = shoppingList.getQuantities()[boughtIndex];
            double currentPeakQuantityBought = shoppingList.getPeakQuantities()[boughtIndex];
            double newQuantityBought = commSoldBySupplier.getQuantity()
                    - (currentQuantityBought - oldQuantityBought);
            double newPeakQuantityBought = commSoldBySupplier.getPeakQuantity()
                    - (currentPeakQuantityBought - oldPeakQuantityBought);
            if (newQuantityBought < 0) {
                logger.error(NEGATIVE_QUANTITY_UPDATE, shoppingList.getBuyer().getDebugInfoNeverUseInCode(),
                        reverse, isResold);
            }
            commSoldBySupplier.setQuantity(Math.max(0, newQuantityBought));
            if (newPeakQuantityBought < 0) {
                logger.warn(NEGATIVE_PEAK_QUANTITY_UPDATE, shoppingList.getBuyer().getDebugInfoNeverUseInCode(),
                        reverse, isResold);
            }
            commSoldBySupplier.setPeakQuantity(Math.max(0, newPeakQuantityBought));
            shoppingList.getQuantities()[boughtIndex] = oldQuantityBought;
            shoppingList.getPeakQuantities()[boughtIndex] = oldPeakQuantityBought;
        }
    }
}
