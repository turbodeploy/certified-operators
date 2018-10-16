package com.vmturbo.platform.analysis.ede;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.List;
import java.util.function.DoubleBinaryOperator;
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
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.EconomySettings;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ledger.IncomeStatement;
import com.vmturbo.platform.analysis.ledger.Ledger;
import com.vmturbo.platform.analysis.pricefunction.PriceFunction;
import com.vmturbo.platform.analysis.utilities.Bisection;
import com.vmturbo.platform.analysis.utilities.DoubleTernaryOperator;

/**
 * This class implements the resize decision logic.
 *
 */
public class Resizer {

    static final Logger logger = LogManager.getLogger(Resizer.class);

    // Maximum number of iterations for Bisection method to ensure we never loop indefinitely
    // For the interval (0,1) Bisection will halve the interval on each iteration. Double use
    // 52 bits to represent the mantissa, so after 52 iterations the interval cannot be made
    // smaller.
    private static final int MAX_ITERATIONS = 53;

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
        float rateOfResize = economy.getSettings().getRateOfResize();

        ledger.calculateAllCommodityExpensesAndRevenues(economy);

        for (Trader seller : economy.getTraders()) {
            if (economy.getForceStop()) {
                return actions;
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
                if (evaluateEngageCriteria(economy, seller, commoditySold, incomeStatement)) {
                    double expenses = incomeStatement.getExpenses();
                    if (expenses > 0) {
                        try {
                            double desiredROI = getDesiredROI(incomeStatement);
                            double newRevenue = expenses < incomeStatement.getMinDesiredExpenses()
                                    ? incomeStatement.getDesiredRevenues()
                                    : desiredROI * (Double.isInfinite(expenses)
                                        ? incomeStatement.getMaxDesiredExpenses()
                                        : expenses);
                            double currentRevenue = incomeStatement.getRevenues();
                            double desiredCapacity =
                               calculateDesiredCapacity(commoditySold, newRevenue
                                                        , seller, economy);
                            CommoditySold rawMaterial = findSellerCommodity(economy, seller,
                                                                            soldIndex);
                            double newEffectiveCapacity =
                               calculateEffectiveCapacity(desiredCapacity, commoditySold,
                                                          rawMaterial, rateOfResize);
                            if (Double.compare(newEffectiveCapacity,
                                               commoditySold.getEffectiveCapacity()) != 0) {
                                double oldCapacity = commoditySold.getCapacity();
                                double newCapacity = newEffectiveCapacity /
                                           commoditySold.getSettings().getUtilizationUpperBound();
                                Resize resizeAction = new Resize(economy, seller,
                                                basketSold.get(soldIndex), commoditySold,
                                                soldIndex, newCapacity);
                                resizeAction.take();
                                resizeAction.setImportance(currentRevenue - newRevenue);
                                actions.add(resizeAction);
                                if (logger.isTraceEnabled() || isDebugTrader) {
                                    logger.info("{" + sellerDebugInfo +
                                            "} A resize action was generated for the commodity "
                                            + basketSold.get(soldIndex).getDebugInfoNeverUseInCode()
                                            + " from " + oldCapacity + " to " + newCapacity + ".");
                                }
                            } else {
                                if (logger.isTraceEnabled() || isDebugTrader) {
                                    logger.info("{" + sellerDebugInfo
                                            + "} No resize for the commodity "
                                            + basketSold.get(soldIndex).getDebugInfoNeverUseInCode()
                                            + " because the calculated capacity is the same to the"
                                            + " current one.");
                                }
                            }
                        } catch (Exception bisectionException) {
                            logger.info(bisectionException.getMessage() + " : Capacity "
                                         + commoditySold.getEffectiveCapacity() + " Quantity "
                                         + commoditySold.getQuantity() + " Revenues "
                                         + incomeStatement.getRevenues() + " Expenses "
                                         + incomeStatement.getExpenses());
                        }
                    } else {
                        if (logger.isTraceEnabled() || isDebugTrader) {
                            logger.info("{" + sellerDebugInfo
                                        + "} No resize for the commodity "
                                        + basketSold.get(soldIndex).getDebugInfoNeverUseInCode()
                                        + " because expenses are 0.");
                        }
                    }
                } else {
                    if (logger.isTraceEnabled() || isDebugTrader) {
                        logger.info("{" + sellerDebugInfo
                                    + "} Resize engagement criteria are false for commodity "
                                    + basketSold.get(soldIndex).getDebugInfoNeverUseInCode() + ".");
                    }
                }
            }
        }

        return actions;
    }

    /**
     * Given the desired effective capacity find the new effective capacity taking into
     * consideration capacity increment. For resize up limit the increase to what the seller
     * can provide. For resize down, limit the decrease to the maximum used (i.e.,
     * max(maxQuantity, peakQuantity)).
     *
     * @param desiredCapacity The calculated new desired effective capacity.
     * @param commoditySold The {@link CommoditySold commodity} sold to obtain peak usage,
     *                      current capacity and capacity increment.
     * @param rawMaterial The source of raw material of {@link CommoditySold commoditySold}.
     * @param rateOfRightSize The user configured rateOfRightSize from {@link EconomySettings}.
     * @return The recommended new capacity.
     */
    private static double calculateEffectiveCapacity(double desiredCapacity,
                                                     @NonNull CommoditySold commoditySold,
                                                     CommoditySold rawMaterial,
                                                     float rateOfRightSize) {
        checkArgument(rateOfRightSize > 0, "Expected rateOfRightSize to be > 0", rateOfRightSize);

        double maxQuantity = commoditySold.getMaxQuantity();
        double peakQuantity = commoditySold.getPeakQuantity();
        double capacityIncrement = commoditySold.getSettings().getCapacityIncrement() *
                                        commoditySold.getSettings().getUtilizationUpperBound();
        double currentCapacity = commoditySold.getEffectiveCapacity();
        double newCapacity = currentCapacity;
        double delta = desiredCapacity - currentCapacity;

        if (delta > 0) {
            // limit the increase to what the seller can provide
            double numIncrements = delta / capacityIncrement / rateOfRightSize;
            int ceilNumIncrements = (int) Math.ceil(numIncrements);
            double proposedIncrement = capacityIncrement * ceilNumIncrements;

            // if we do not specify a raw material, then we impose no restriction on the value
            // we resize to (regarding to what the seller can provide)
            double remaining = rawMaterial == null ? Double.MAX_VALUE :
                            commoditySold.getSettings().getUtilizationUpperBound() *
                                (rawMaterial.getEffectiveCapacity() - rawMaterial.getQuantity());
            if (remaining > 0) {
                if (remaining < proposedIncrement) {
                    int floorNumIncrements = (int) Math.floor(remaining / capacityIncrement);
                    newCapacity += capacityIncrement * floorNumIncrements;
                } else {
                    newCapacity += proposedIncrement;
                }
                if (newCapacity > rawMaterial.getEffectiveCapacity()) {
                    newCapacity = rawMaterial.getEffectiveCapacity();
                }
            }
        } else {
            if (commoditySold.getQuantity() == 0 && maxQuantity == 0 && peakQuantity == 0) {
                return currentCapacity;
            }

            // limit the decrease to be above max usage
            delta = -delta;
            double maxCapacityDecrement = currentCapacity - Math.max(maxQuantity, peakQuantity);
            if (maxCapacityDecrement < 0) {
                return currentCapacity;
            }
            if (maxCapacityDecrement < delta) {
                delta = maxCapacityDecrement;
            }
            double numDecrements = delta / capacityIncrement / rateOfRightSize;
            int floorNumDecrements = (int) Math.floor(numDecrements);
            // if the rate_of_resize > 1, then the floorNumDecrements can be 0 even if
            // delta > capacityIncrement. In this case we don't resize down
            // which is undesirable (bug OM-34833). Set floorNumDecrements to 1 in this case.
            if (delta >= capacityIncrement) {
                floorNumDecrements = Math.max(1, floorNumDecrements);
            }
            double proposedCapacityDecrement = capacityIncrement * floorNumDecrements;
            newCapacity -= proposedCapacityDecrement;
            // do not fall below 1 unit of capacity increment
            if (newCapacity < capacityIncrement) {
                if (currentCapacity >= 2 * capacityIncrement) {
                    newCapacity = capacityIncrement;
                } else {
                    return currentCapacity;
                }
            }
        }

        return newCapacity;
    }

    /**
     * Find the commodity sold by the Seller which is raw material for the given commodity.
     *
     * @param economy The Economy.
     * @param buyer The Buyer of the commodity in the Economy.
     * @param commoditySoldIndex The index of commodity for which we need to find the raw materials.
     * @return The commodity sold.
     */
    private static @Nullable CommoditySold findSellerCommodity(@NonNull Economy economy,
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
                return commSoldBySeller;
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
     * @param commoditySold The {@link CommoditySold commodity} that is to be resized.
     * @param commodityIS The {@link IncomeStatement income statement} of the commodity sold.
     * @return Whether the commodity meets the resize engagement criterion.
     */
    public static boolean evaluateEngageCriteria(Economy economy, Trader seller,
                                                 CommoditySold resizeCommodity,
                                                 IncomeStatement commodityIS) {
        boolean eligibleForResizeDown = seller.getSettings().isEligibleForResizeDown();
        double currentCapacity = resizeCommodity.getEffectiveCapacity();
        double currentQuantity = resizeCommodity.getQuantity();
        double currentUtilization = currentQuantity / currentCapacity;
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
     * @return The new capacity
     * @throws Exception If it cannot find the new capacity.
     */
    private static double calculateDesiredCapacity(CommoditySold resizeCommodity, double newRevenue,
                                                   Trader seller, Economy economy) {
        double currentQuantity = resizeCommodity.getQuantity();
        PriceFunction priceFunction = resizeCommodity.getSettings().getPriceFunction();

        // interval which is almost (0,1) to avoid divide by zero
        double intervalMin = Math.nextAfter(0.0, 1.0);
        double intervalMax = Math.nextAfter(1.0, 0.0);

        // solve revenueFunction(u) = newRevenue for u in (intervalMin,intervalMax)
        DoubleUnaryOperator revenueFunction = (u) -> u * priceFunction.unitPrice(u, null,
                                                         seller, resizeCommodity, economy) - newRevenue;
        // pass in a function to calculate error in capacity for utilization interval (x,y)
        DoubleBinaryOperator errorFunction = (x, y) -> currentQuantity / x - currentQuantity / y;
        // we will change capacity in steps of capacity increment so we can stop when error in
        // capacity is less than half of capacity increment
        double epsilon = resizeCommodity.getSettings().getCapacityIncrement() *
                                    resizeCommodity.getSettings().getUtilizationUpperBound() / 2;
        double newNormalizedUtilization = Bisection.solve(epsilon, errorFunction,
                                  MAX_ITERATIONS, revenueFunction, intervalMin, intervalMax);
        double newCapacity = currentQuantity / newNormalizedUtilization;
        return newCapacity;
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
     */
    public static void resizeDependentCommodities(@NonNull Economy economy,
             @NonNull Trader seller, @NonNull CommoditySold commoditySold, int commoditySoldIndex,
                                                                              double newCapacity) {
        if (!economy.getSettings().isResizeDependentCommodities()) {
            return;
        }
        List<CommodityResizeSpecification> typeOfCommsBought = economy.getResizeDependency(
                                 seller.getBasketSold().get(commoditySoldIndex).getBaseType());
        if (typeOfCommsBought == null || typeOfCommsBought.isEmpty()) {
            return;
        }
        for (ShoppingList shoppingList : economy.getMarketsAsBuyer(seller).keySet()) {

            Trader supplier = shoppingList.getSupplier();
            Basket basketBought = shoppingList.getBasket();
            for (CommodityResizeSpecification typeOfCommBought : typeOfCommsBought) {
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
                        checkArgument(newQuantityBought >= 0, "Expected new quantity bought %s to >= 0",
                                      newQuantityBought);
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
