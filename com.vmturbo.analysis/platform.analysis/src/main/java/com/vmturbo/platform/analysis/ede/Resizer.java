package com.vmturbo.platform.analysis.ede;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.List;
import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleUnaryOperator;

import org.apache.log4j.Logger;
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

/**
 * This class implements the resize decision logic.
 *
 */
public class Resizer {

    static final Logger logger = Logger.getLogger(Resizer.class);

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

        ledger.calculateAllCommodityExpensesAndRevenues(economy);

        for (Trader seller : economy.getTraders()) {
            Basket basketSold = seller.getBasketSold();
            if (basketSold == null) {
                continue;
            }
            List<IncomeStatement> incomeStatements = ledger.getCommodityIncomeStatements(seller);
            for (int soldIndex = 0; soldIndex < basketSold.size(); soldIndex++) {
                CommoditySold commoditySold = seller.getCommoditiesSold().get(soldIndex);
                if (!commoditySold.getSettings().isResizable()) {
                    continue;
                }
                IncomeStatement incomeStatement = incomeStatements.get(soldIndex);
                if (evaluateEngageCriteria(economy, commoditySold, incomeStatement)) {
                    double expenses = incomeStatement.getExpenses();
                    if (expenses > 0) {
                        try {
                            double desiredROI = getDesiredROI(incomeStatement);
                            double newRevenue = desiredROI * expenses;
                            double currentRevenue = incomeStatement.getRevenues();
                            double desiredCapacity =
                               calculateDesiredCapacity(commoditySold, currentRevenue, newRevenue);
                            CommoditySold rawMaterial = findSellerCommodity(economy, seller,
                                                                            soldIndex);
                            double newEffectiveCapacity =
                               calculateEffectiveCapacity(desiredCapacity, commoditySold,
                                                          rawMaterial);
                            if (Double.compare(newEffectiveCapacity,
                                               commoditySold.getEffectiveCapacity()) != 0) {
                                double newCapacity = newEffectiveCapacity /
                                           commoditySold.getSettings().getUtilizationUpperBound();
                                Resize resizeAction = new Resize(economy, seller,
                                                basketSold.get(soldIndex), commoditySold,
                                                soldIndex, newCapacity);
                                resizeAction.take();
                                resizeAction.setImportance(currentRevenue - newRevenue);
                                actions.add(resizeAction);
                            }
                        } catch (Exception bisectionException) {
                            logger.error(bisectionException.getMessage() + " : Capacity "
                                         + commoditySold.getEffectiveCapacity() + " Historical Quantity "
                                         + commoditySold.getHistoricalQuantity() + " Revenues "
                                         + incomeStatement.getRevenues() + " Expenses "
                                         + incomeStatement.getExpenses());
                        }
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
     * @return The recommended new capacity.
     */
    private static double calculateEffectiveCapacity(double desiredCapacity,
                                                     @NonNull CommoditySold commoditySold,
                                                     @NonNull CommoditySold rawMaterial) {
        checkArgument(rawMaterial != null, "Expected raw material for commodity %s to be non-null",
                                            commoditySold);
        double maxQuantity = commoditySold.getMaxQuantity();
        double peakQuantity = commoditySold.getHistoricalPeakQuantity();
        double capacityIncrement = commoditySold.getSettings().getCapacityIncrement();
        double currentCapacity = commoditySold.getEffectiveCapacity();
        double newCapacity = currentCapacity;
        double delta = desiredCapacity - currentCapacity;

        if (delta > 0) {
            // limit the increase to what the seller can provide
            double numIncrements = delta / capacityIncrement;
            int ceilNumIncrements = (int) Math.ceil(numIncrements);
            double proposedIncrement = capacityIncrement * ceilNumIncrements;
            double remaining = rawMaterial.getEffectiveCapacity() - rawMaterial.getQuantity();
            if (remaining > 0) {
                if (remaining < proposedIncrement) {
                    int floorNumIncrements = (int) Math.floor(remaining / capacityIncrement);
                    newCapacity += capacityIncrement * floorNumIncrements;
                } else {
                    newCapacity += proposedIncrement;
                }
            }
        } else {
            // limit the decrease to be above max usage
            delta = -delta;
            double maxCapacityDecrement = currentCapacity - Math.max(maxQuantity, peakQuantity);
            if (maxCapacityDecrement < delta) {
                delta = maxCapacityDecrement;
            }
            double numDecrements = delta / capacityIncrement;
            int floorNumDecrements = (int) Math.floor(numDecrements);
            double proposedCapacityDecrement = capacityIncrement * floorNumDecrements;
            newCapacity -= proposedCapacityDecrement;
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
    public static boolean evaluateEngageCriteria(Economy economy, CommoditySold resizeCommodity,
                                                 IncomeStatement commodityIS) {
        double currentCapacity = resizeCommodity.getEffectiveCapacity();
        double currentQuantity = resizeCommodity.getQuantity();
        double currentUtilization = currentQuantity / currentCapacity;
        // do not resize if utilization is in acceptable range
        EconomySettings settings = economy.getSettings();
        if (currentUtilization > settings.getRightSizeLower() &&
            currentUtilization < settings.getRightSizeUpper()) {
            return false;
        }
        return (commodityIS.getROI() > commodityIS.getMaxDesiredROI()) ||
                        (commodityIS.getROI() < commodityIS.getMinDesiredROI());
    }

    /**
     * Finds the new commodity capacity needed for the target revenue.
     *
     * @param resizeCommodity Commodity to be resized.
     * @param currentRevenue Current revenue for the commodity to be resized.
     * @param newRevenue The target revenue for the commodity after resize.
     * @return The new capacity
     * @throws Exception If it cannot find the new capacity.
     */
    private static double calculateDesiredCapacity(CommoditySold resizeCommodity,
                                                   double currentRevenue, double newRevenue) {
        double currentQuantity = resizeCommodity.getHistoricalQuantity();
        PriceFunction priceFunction = resizeCommodity.getSettings().getPriceFunction();

        // interval which is almost (0,1) to avoid divide by zero
        double intervalMin = Math.nextAfter(0.0, 1.0);
        double intervalMax = Math.nextAfter(1.0, 0.0);

        // solve revenueFunction(u) = newRevenue for u in (intervalMin,intervalMax)
        DoubleUnaryOperator revenueFunction = (u) -> u * priceFunction.unitPrice(u) - newRevenue;
        // pass in a function to calculate error in capacity for utilization interval (x,y)
        DoubleBinaryOperator errorFunction = (x, y) -> currentQuantity / x - currentQuantity / y;
        // we will change capacity in steps of capacity increment so we can stop when error in
        // capacity is less than half of capacity increment
        double epsilon = resizeCommodity.getSettings().getCapacityIncrement() / 2;
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
                CommoditySold commSoldBySeller = supplier.getCommoditySold(basketBought
                                .get(boughtIndex));
                double changeInCapacity = newCapacity - commoditySold.getCapacity();
                if (changeInCapacity < 0) {
                    // resize down
                    DoubleBinaryOperator decrementFunction =
                                            typeOfCommBought.getDecrementFunction();
                    double oldQuantityBought = shoppingList.getQuantities()[boughtIndex];
                    double decrementedQuantity = decrementFunction.applyAsDouble(
                                                oldQuantityBought, newCapacity);
                    double newQuantityBought = commSoldBySeller.getQuantity() -
                                                        (oldQuantityBought - decrementedQuantity);
                    checkArgument(newQuantityBought >= 0, "Expected new quantity bought %s to >= 0",
                                  newQuantityBought);
                    shoppingList.getQuantities()[boughtIndex] = decrementedQuantity;
                    commSoldBySeller.setQuantity(newQuantityBought);
                } else {
                    // resize up
                    DoubleBinaryOperator incrementFunction =
                                    typeOfCommBought.getIncrementFunction();
                    double oldQuantityBought = shoppingList.getQuantities()[boughtIndex];
                    double incrementedQuantity = incrementFunction.applyAsDouble(
                                                           oldQuantityBought, changeInCapacity);
                    shoppingList.getQuantities()[boughtIndex] = incrementedQuantity;
                    commSoldBySeller.setQuantity(commSoldBySeller.getQuantity() +
                                                 (incrementedQuantity - oldQuantityBought));
                }
            }
        }
    }
}
