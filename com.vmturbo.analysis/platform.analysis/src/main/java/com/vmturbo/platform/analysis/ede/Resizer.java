package com.vmturbo.platform.analysis.ede;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.DoubleUnaryOperator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.Resize;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommodityResizeSpecification;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.EconomySettings;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ledger.IncomeStatement;
import com.vmturbo.platform.analysis.ledger.Ledger;
import com.vmturbo.platform.analysis.pricefunction.PriceFunction;
import com.vmturbo.platform.analysis.utilities.Bisection;
import com.vmturbo.platform.analysis.utilities.DoubleTernaryOperator;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * This class implements the resize decision logic.
 *
 */
public class Resizer {

    static final Logger logger = LogManager.getLogger(Resizer.class);

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
        ConsistentResizer consistentResizer = new ConsistentResizer();
        float rateOfResize = economy.getSettings().getRateOfResize();
        for (Trader seller : economy.getTraders()) {

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
                boolean consistentResizing = seller.isInScalingGroup();
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
                            Pair<CommoditySold, Trader> p = findSellerCommodityAndSupplier(economy,
                                seller, soldIndex);
                            CommoditySold rawMaterial = (p != null) ? p.getFirst() : null;
                            double newEffectiveCapacity = calculateEffectiveCapacity(economy, seller,
                                resizedCommodity, desiredCapacity, commoditySold,
                                    rawMaterial, rateOfResize);
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
                                        engage && capacityChange, p);
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
            }
        }
        // Consistently resize commodities in scaling groups
        consistentResizer.resizeScalingGroupCommodities(actions);
        return actions;
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
     * @param rawMaterial     The source of raw material of {@link CommoditySold commoditySold}.
     * @param rateOfRightSize The user configured rateOfRightSize from {@link EconomySettings}.
     * @return The recommended new capacity.
     */
    private static double calculateEffectiveCapacity(Economy economy, Trader seller,
                                                     CommoditySpecification commSpec,
                                                     double desiredCapacity,
                                                     @NonNull CommoditySold resizeCommodity,
                                                     CommoditySold rawMaterial,
                                                     float rateOfRightSize) {
        checkArgument(rateOfRightSize > 0, "Expected rateOfRightSize to be > 0", rateOfRightSize);

        double currentCapacity = resizeCommodity.getEffectiveCapacity();
        double desiredAdjustment = desiredCapacity - currentCapacity;
        double capacityIncrement = resizeCommodity.getSettings().getCapacityIncrement() * resizeCommodity.getSettings().getUtilizationUpperBound();
        double adjustment = 0;
        if (desiredCapacity > currentCapacity) {
            adjustment = calculateResizeUpAmount(economy, seller, commSpec, desiredAdjustment, resizeCommodity, rawMaterial, capacityIncrement, rateOfRightSize);
        } else {
            // "amount" values are always non-negative, hence we negate the arg and the result for a downward resize
            adjustment = -calculateResizeDownAmount(economy, seller, commSpec, -desiredAdjustment, resizeCommodity, rawMaterial, capacityIncrement, rateOfRightSize);
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
     * @param rawMaterial       The source of raw material supplying the {@link CommoditySold commodity} being resized.
     * @param capacityIncrement The capacity increment for the {@link CommoditySold commodity} being resized.
     * @param rateOfRightSize   The user configured rightsizing rate from {@link EconomySettings}.
     * @return                  The recommended amount of increase for {@link CommoditySold commodity} being resized; always non-negative
     */
    private static double calculateResizeUpAmount(Economy economy, Trader seller, CommoditySpecification commSpec,
                                                  double desiredAmount,
                                                  @NonNull CommoditySold resizeCommodity,
                                                  CommoditySold rawMaterial,
                                                  double capacityIncrement,
                                                  double rateOfRightSize) {
        // If the current capacity is already above the raw material's capacity, do not resize up
        // further.
        if (rawMaterial != null &&
                resizeCommodity.getEffectiveCapacity() > rawMaterial.getEffectiveCapacity()) {
            if (logger.isTraceEnabled() || seller.isDebugEnabled()) {
                logger.info("Cannot resize {}/{} further up. Current capacity {} already " +
                        "above raw material capacity {}. Not resizing.", seller.getDebugInfoNeverUseInCode(),
                        commSpec.getDebugInfoNeverUseInCode(), resizeCommodity.getEffectiveCapacity(),
                        rawMaterial.getEffectiveCapacity());
            }
            return 0;
        }
        // If the current capacity is already greater than or equal to the capacity upper bound,
        // then the capacity upper bound does not matter. Make it Double.MAX_VALUE.
        double capacityUpperBound = resizeCommodity.getEffectiveCapacity() >= resizeCommodity.getSettings().getCapacityUpperBound()
            ? Double.MAX_VALUE
            : resizeCommodity.getSettings().getCapacityUpperBound();
        double maxAmount = capacityUpperBound - resizeCommodity.getEffectiveCapacity();
        if (logger.isTraceEnabled() || seller.isDebugEnabled()) {
            logger.info("The max amount we can resize up {}/{} is {}. This is derived from currentCapacity {} and capacity upper bound {}",
                seller.getDebugInfoNeverUseInCode(), commSpec.getDebugInfoNeverUseInCode(),
                maxAmount, resizeCommodity.getEffectiveCapacity(), capacityUpperBound);
        }
        if (maxAmount < desiredAmount) {
            desiredAmount = maxAmount;
        }
        // compute increments sufficient for desired amount
        int numIncrements = (int) Math.ceil(desiredAmount / (capacityIncrement * rateOfRightSize));

        // make sure we don't exceed available headroom in seller
        double headroom = rawMaterial == null ? Double.MAX_VALUE :
            resizeCommodity.getSettings().getUtilizationUpperBound() *
                (rawMaterial.getEffectiveCapacity() - rawMaterial.getQuantity());
        if (headroom > 0) {
            if (headroom < numIncrements * capacityIncrement) {
                numIncrements = (int) Math.floor(headroom / capacityIncrement);
            }
            double desiredIncrement = numIncrements * capacityIncrement;
            if (rawMaterial == null) {
                // This is possible in case of portchannel where the raw material is dummy value.
                logger.debug("Raw material is null. Resizing up by {} for {} on {}",
                        desiredIncrement, commSpec.getDebugInfoNeverUseInCode(),
                        seller.getDebugInfoNeverUseInCode());
                return desiredIncrement;
            }
            // in the case of heap resizing up, we make sure that the sum of all heapCapacities sold by allServers
            // is less than the vMemSoldCapacity. This sum is not going to be same as vMemUsed and hence
            // this check is different from the rawMaterial validation.
            double totalEffCapOnConsumers = calculateTotalEffectiveCapacityOnCoConsumersForResizeUp(economy, commSpec, seller);
            if (totalEffCapOnConsumers + desiredIncrement <= rawMaterial.getEffectiveCapacity()) {
                return desiredIncrement;
            } else {
                logger.debug("The sum of the effCap sold by all the customers is {}. This is " +
                        "much larger than the raw material {}'s effCap {} after resizing up by {} for {} on {}",
                        totalEffCapOnConsumers, rawMaterial.getEffectiveCapacity(),
                        desiredIncrement, commSpec.getDebugInfoNeverUseInCode(), seller.getDebugInfoNeverUseInCode());
                return 0.0;
            }
        } else {
            return 0.0;
        }
    }

    /**
     * When a rawMaterial like vMem tries resizingUp, return the dependant commodity sold by the customers.
     * These dependant commodities are heap and dbMem
     *
     * @param economy           The economy that the trader is a part of
     * @param commSpec          The commSpec being resized
     * @param seller            The seller
     * @return                  Total effectiveCapacity of commoditySold across all customers
     */
    private static double calculateTotalEffectiveCapacityOnCoConsumersForResizeUp(Economy economy,
                                                                                   CommoditySpecification commSpec,
                                                                                   Trader seller) {
        // co-dependant commodity capacity validation
        // this means that there is a mapping for the commodity and the commodity is resizeUpAware
        double totalEffCap = 0;
        List<Integer> rawMaterials = economy.getRawMaterials(commSpec.getBaseType());
        Set<ShoppingList> sls = economy.getMarketsAsBuyer(seller).keySet();
        if (rawMaterials != null) {
            for (int baseCommType : rawMaterials) {
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
                                totalEffCap += sumUpEffCapacitiesSoldByCustomersForCommodity(optionalSupplier.get(), commType);
                            }
                        }
                    }
                }
            }
        }
        return totalEffCap;
    }

    /**
     * When a commodity like heap or dbMem tries resizingDown, return the sum of capacities of dependant commodity sold by the co-customers.
     * These dependant commodities are heap and dbMem
     *
     * @param economy           The economy that the trader is a part of
     * @param commSpec          The commSpec being resized
     * @param seller            The seller
     * @return                  Total effectiveCapacity of commoditySold across all customers
     */
    private static double calculateTotalEffectiveCapacityOnCoConsumersForResizeDown(Economy economy,
                                                                                   CommoditySpecification commSpec,
                                                                                   Trader seller) {
        // co-dependant commodity capacity validation
        // this means that there is a mapping for the commodity and the commodity is resizeUpAware
        double totalEffCap = 0;
        if (economy.getResizeProducesDependencyEntry(commSpec.getBaseType()) != null) {
            // for vMem resizing down, the related commodities are heap or dbMem
            for (Integer commType : economy.getResizeProducesDependencyEntry(commSpec.getBaseType())) {
                // check if customer sells commType. eg, heap or dbMem in the case of an AppServer or dbServer respectively
                // and sum up the resourceCapacities
                totalEffCap += sumUpEffCapacitiesSoldByCustomersForCommodity(seller, commType);
            }
        }
        return totalEffCap;
    }

    /**
     * return the sum of effective capacities of commodity sold of a particular type sold by the customers of a seller.
     *
     * @param seller            The seller
     * @param commType          Type of commodity being sold by the customers
     * @return                  Total effectiveCapacity of commoditySold across all customers
     */
    private static double sumUpEffCapacitiesSoldByCustomersForCommodity(Trader seller,
                                                                        Integer commType) {
        // check if customer sells commType. eg, heap or dbMem in the case of an AppServer or dbServer respectively
        // and sum up the resourceCapacities
        return seller.getCustomers().stream().map(ShoppingList::getBuyer)
                .filter(trader -> trader.getBasketSold().indexOfBaseType(commType) != -1)
                .map(trader -> trader.getCommoditiesSold().get(trader.getBasketSold().indexOfBaseType(commType)))
                .map(CommoditySold::getEffectiveCapacity)
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
     * <li>The capacity is not allowed to drop below the commodity's maxQuantity or peakQuantity.</li>
     * <li>The capacity is not allowed to change if there is no current or historical use data.</li>
     * <li>The capacity is not allowed to drop below a single capacity increment amount.</li>
     * </ul>
     *
     * @param economy           The economy that the trader is a part of
     * @param seller            the seller whose commodity is being resized
     * @param commSpec          the commSpec which is being resized
     * @param desiredAmount     the desired capacity decrease; must be non-negative.
     * @param resizeCommodity   the {@link CommoditySold commodity} being resized.
     * @param rawMaterial       the source of raw material supplying the {@link CommoditySold commodity} being resized.
     * @param capacityIncrement the capacity increment for the {@link CommoditySold commodity} being resized.
     * @param rateOfRightSize   the user configured rightsizing rate from {@link EconomySettings}.
     * @return the recommended amount of decrease for {@link CommoditySold commodity} being resized; always non-negative.
     */
    private static double calculateResizeDownAmount(Economy economy, Trader seller, CommoditySpecification commSpec,
                                                    double desiredAmount,
                                                    @NonNull CommoditySold resizeCommodity,
                                                    CommoditySold rawMaterial,
                                                    double capacityIncrement,
                                                    double rateOfRightSize) {
        double currentCapacity = resizeCommodity.getEffectiveCapacity();
        double maxQuantity = resizeCommodity.getMaxQuantity();
        double peakQuantity = resizeCommodity.getPeakQuantity();

        // don't permit downward resize if there's no usage data
        if (resizeCommodity.getQuantity() == 0 && maxQuantity == 0 && peakQuantity == 0) {
            return 0.0;
        }
        // If the current capacity is already lesser than or equal to the capacity lower bound,
        // then the capacity lower bound does not matter. Make it 0.
        double capacityLowerBound = currentCapacity <= resizeCommodity.getSettings().getCapacityLowerBound()
            ? 0 : resizeCommodity.getSettings().getCapacityLowerBound();
        // don't permit resize below historical max/peak or below capacity lower bound
        double maxAmount = currentCapacity - Math.max(Math.max(maxQuantity, peakQuantity), capacityLowerBound);
        if (logger.isTraceEnabled() || seller.isDebugEnabled()) {
            logger.info("The max amount we can resize down {}/{} is {}. This is derived from currentCapcity {}, max {}, peak {}, capcityLowerBound {}",
                seller.getDebugInfoNeverUseInCode(), commSpec.getDebugInfoNeverUseInCode(),
                maxAmount, currentCapacity, maxQuantity, peakQuantity, capacityLowerBound);
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
        // If the final number to go to is greater than the raw material capacity, then restrict
        // it to the raw material capacity. (This can happen when the current capacity is greater
        // than the raw material's capacity)
        double newCapacity = currentCapacity - amount;
        if (rawMaterial != null && newCapacity > rawMaterial.getEffectiveCapacity()) {
            amount = 0;
            if (logger.isTraceEnabled() || seller.isDebugEnabled()) {
                logger.info("New resized capacity for {}/{} is {}. But this is above rawMaterial " +
                                "capacity {}. Not resizing.", seller.getDebugInfoNeverUseInCode(),
                        commSpec.getDebugInfoNeverUseInCode(), newCapacity, rawMaterial.getEffectiveCapacity());
            }
        }
        double totalEffectiveCapOnCoConsumers = calculateTotalEffectiveCapacityOnCoConsumersForResizeDown(economy, commSpec, seller);
        // if the total capacity on all the consumers for the dependant commodities is much larger than the newCapacity, dont resize
        if (totalEffectiveCapOnCoConsumers > currentCapacity - amount) {
            logger.debug("The sum of the effCap sold by all the customers is {}. This is " +
                            "much larger than {}'s effCap {} after resizing down by {} for {}",
                             totalEffectiveCapOnCoConsumers, commSpec.getDebugInfoNeverUseInCode(), currentCapacity,
                             amount, seller.getDebugInfoNeverUseInCode());
            return 0;
        }
        return amount;
    }
    /**
     * Find the commodity sold and supplier by the Seller which is raw material for the given commodity.
     *
     * @param economy The Economy.
     * @param buyer The Buyer of the commodity in the Economy.
     * @param commoditySoldIndex The index of commodity for which we need to find the raw materials.
     * @return Pair containing the commodity sold and its supplier
     */
    private static @Nullable Pair<CommoditySold, Trader>
        findSellerCommodityAndSupplier(@NonNull Economy economy,
                                               @NonNull Trader buyer, int commoditySoldIndex) {
        List<Integer> typeOfCommsBought = economy.getRawMaterials(buyer.getBasketSold()
                                                     .get(commoditySoldIndex).getBaseType());
        for (ShoppingList shoppingList : economy.getMarketsAsBuyer(buyer).keySet()) {

            Trader supplier = shoppingList.getSupplier();
            Basket basketBought = shoppingList.getBasket();
            for (Integer typeOfCommBought : typeOfCommsBought) {
                int boughtIndex = basketBought.indexOfBaseType(typeOfCommBought.intValue());
                if (boughtIndex < 0) {
                    continue;
                }
                CommoditySold commSoldBySeller = supplier.getCommoditySold(basketBought
                                                                           .get(boughtIndex));
                return new Pair(commSoldBySeller, supplier);
            }
        }
        return null;
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
     * on dependent commodities changes based on the parameter.
     */
    public static void resizeDependentCommodities(@NonNull Economy economy,
             @NonNull Trader seller, @NonNull CommoditySold commoditySold, int commoditySoldIndex,
                                                    double newCapacity, boolean basedOnHistorical) {
        if (!economy.getSettings().isResizeDependentCommodities()) {
            return;
        }
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
                CommoditySold commSoldBySupplier = supplier.getCommoditySold(basketBought
                                .get(boughtIndex));
                double changeInCapacity = newCapacity - commoditySold.getCapacity();
                if (changeInCapacity < 0) {
                    // resize down
                    DoubleTernaryOperator decrementFunction =
                                            typeOfCommBought.getDecrementFunction();
                    double oldQuantityBought = shoppingList.getQuantities()[boughtIndex];
                    double decrementedQuantity = decrementFunction.applyAsDouble(
                                                oldQuantityBought, newCapacity, 0);
                    double newQuantityBought = commSoldBySupplier.getQuantity() -
                                                        (oldQuantityBought - decrementedQuantity);
                    if (!supplier.isTemplateProvider()) {
                        if (newQuantityBought < 0) {
                            logger.warn("Expected new quantity bought {} to >= 0. Buyer is {}. " +
                                "Supplier is {}.", newQuantityBought, seller.getDebugInfoNeverUseInCode(),
                                supplier.getDebugInfoNeverUseInCode());
                            continue;
                        }
                        commSoldBySupplier.setQuantity(newQuantityBought);
                    }
                    shoppingList.getQuantities()[boughtIndex] = decrementedQuantity;
                } else {
                    // resize up
                    DoubleTernaryOperator incrementFunction =
                                    typeOfCommBought.getIncrementFunction();
                    double oldQuantityBought = shoppingList.getQuantities()[boughtIndex];
                    double incrementedQuantity = incrementFunction.applyAsDouble(
                                                           oldQuantityBought, changeInCapacity, 0);
                    shoppingList.getQuantities()[boughtIndex] = incrementedQuantity;
                    commSoldBySupplier.setQuantity(commSoldBySupplier.getQuantity() +
                                                 (incrementedQuantity - oldQuantityBought));
                }
            }
        }
    }
}
