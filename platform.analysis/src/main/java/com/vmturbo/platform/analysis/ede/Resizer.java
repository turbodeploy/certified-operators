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

    // Maximum number of iterations to use for Bisection method to ensure we never loop indefinitely
    private static final int MAX_ITERATIONS = 53;
    // Accuracy for convergence of Bisection method
    private static final double ROOT_ACCURACY = 1.0E-15;
    // The low end of range for the normalized utilization
    private static final double UTILIZATION_LOW = 1.0E-12;
    // The high end of range for the normalized utilization
    private static final double UTILIZATION_HIGH = .999;

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
    public static @NonNull List<@NonNull Action> resizeDecisions(@NonNull Economy economy, @NonNull Ledger ledger) {

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
                if (evaluateEngageCriteria(incomeStatement)) {
                    double expenses = incomeStatement.getExpenses();
                    if (expenses > 0) {
                        try {
                            double desiredROI = getDesiredROI(incomeStatement);
                            double newRevenue = desiredROI * expenses;
                            double currentRevenue = incomeStatement.getRevenues();
                            double desiredCapacity =
                               calculateDesiredCapacity(commoditySold, currentRevenue, newRevenue);
                            CommoditySold rawMaterial = findSellerCommodity(economy, seller, soldIndex);
                            double newEffectiveCapacity =
                               calculateEffectiveCapacity(desiredCapacity, commoditySold, rawMaterial);
                            if (Double.compare(newEffectiveCapacity, commoditySold.getEffectiveCapacity()) != 0) {
                                double capacityFactor =
                                         commoditySold.getEffectiveCapacity() / commoditySold.getCapacity();
                                double newCapacity = newEffectiveCapacity / capacityFactor;
                                Resize resizeAction = new Resize(seller, basketSold.get(soldIndex), newCapacity);
                                actions.add(resizeAction);
                                updateResizeDependency(economy, seller, commoditySold, soldIndex, newCapacity);
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
     * Take peak used and capacity increment parameter into consideration for the recommended
     * new capacity.
     *
     * @param desiredCapacity The calculated new desired capacity.
     * @param commoditySold The commodity sold to obtain peak usage, current capacity and capacity increment.
     * @param rawMaterial The raw material of commoditySold.
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
            if (remaining < proposedIncrement) {
                int floorNumIncrements = (int) Math.floor(remaining / capacityIncrement);
                newCapacity += capacityIncrement * floorNumIncrements;
            } else {
                newCapacity += proposedIncrement;
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
     * @param commodityIS The income statement of the commodity sold.
     * @return Whether the commodity meets the resize engagement criterion.
     */
    public static boolean evaluateEngageCriteria(IncomeStatement commodityIS) {
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
        double currentCapacity = resizeCommodity.getEffectiveCapacity();
        double currentQuantity = resizeCommodity.getHistoricalQuantity();
        PriceFunction priceFunction = resizeCommodity.getSettings().getPriceFunction();

        double intervalMin;
        double intervalMax;
        // assuming monotonically increasing price function
        double currentUtilization = currentQuantity / currentCapacity;
        checkArgument(currentUtilization < 1,
                      "Expected currentUtilization %s < 1", currentUtilization);
        if (newRevenue < currentRevenue) {
            intervalMin = (currentUtilization > UTILIZATION_LOW) ? UTILIZATION_LOW :
                                                           currentUtilization * ROOT_ACCURACY;
            intervalMax = currentUtilization;
        } else {
            intervalMin = currentUtilization;
            intervalMax = (currentUtilization < UTILIZATION_HIGH) ? UTILIZATION_HIGH :
                           currentUtilization + (1.0 - currentUtilization) *  UTILIZATION_HIGH;
        }

        // solve revenueFunction(u) = newRevenue for u in (intervalMin,intervalMax)
        DoubleUnaryOperator revenueFunction = (u) -> u * priceFunction.unitPrice(u) - newRevenue;
        DoubleBinaryOperator errorFunction = (x, y) -> currentQuantity / x - currentQuantity / y;
        double epsilon = resizeCommodity.getSettings().getCapacityIncrement() / 2;
        double newNormalizedUtilization = Bisection.solve(epsilon, errorFunction,
                                  MAX_ITERATIONS, revenueFunction, intervalMin, intervalMax);
        double newCapacity = currentQuantity / newNormalizedUtilization;
        return newCapacity;
    }

    /**
     * For resize down, update the quantity of the dependent commodity.
     *
     * @param economy The Economy.
     * @param seller The Trader selling the commodity.
     * @param commoditySold The commodity sold.
     * @param commoditySoldIndex The index of commodity sold in basket.
     * @param newCapacity The new capacity.
     */
    private static void updateResizeDependency(@NonNull Economy economy, Trader seller,
                                               CommoditySold commoditySold, int commoditySoldIndex,
                                               double newCapacity) {
        if (newCapacity > commoditySold.getCapacity()) {
            return;
        }
        List<CommodityResizeSpecification> typeOfCommsBought = economy.getResizeDependency(
                                 seller.getBasketSold().get(commoditySoldIndex).getBaseType());
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
                DoubleBinaryOperator limitFunction = typeOfCommBought.getLimitFunction();
                double adjustedQuantity = limitFunction.applyAsDouble(commSoldBySeller.getQuantity(),
                                                                      newCapacity);
                commSoldBySeller.setQuantity(adjustedQuantity);
            }
        }
    }
}
