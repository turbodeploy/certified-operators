package com.vmturbo.platform.analysis.ede;

import java.util.ArrayList;
import java.util.List;
import java.util.function.DoubleUnaryOperator;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ledger.IncomeStatement;
import com.vmturbo.platform.analysis.ledger.Ledger;
import com.vmturbo.platform.analysis.pricefunction.PriceFunction;
import com.vmturbo.platform.analysis.utilities.Bisection;

/**
 * This class implements the resize decision logic.
 *
 */
public class Resize {

    // Maximum number of iterations to use for Bisection method to ensure we never loop indefinitely
    private static final int MAX_ITERATIONS = 100;
    // Accuracy for convergence of Bisection method
    private static final double ROOT_ACCURACY = 1.0E-3;
    // The low end of range for the normalized utilization
    private static final double UTILIZATION_LOW = 1.0E-12;
    // The high end of range for the normalized utilization
    private static final double UTILIZATION_HIGH = .99;

    /**
     * Return a list of recommendations to optimize the resize of all eligible commodities in the economy.
     *
     * <p>
     *  As a result of invoking this method, both the economy and the state that are passed as
     *  parameters to it, may be changed.
     * </p>
     *
     * @param economy - the economy whose commodity we want to resize
     */
    public static @NonNull List<@NonNull Action> resizeDecisions(@NonNull Economy economy) {

        @NonNull List<Action> actions = new ArrayList<>();

        Ledger ledger = new Ledger(economy);
        ledger.calculateAllCommodityExpensesAndRevenues(economy);

        for (Trader seller : economy.getTraders()) {
            Basket basketSold = seller.getBasketSold();
            if (basketSold == null) {
                continue;
            }
            List<IncomeStatement> incomeStatements = ledger.getCommodityIncomeStatements(seller);
            for (int soldIndex = 0; soldIndex < basketSold.size(); soldIndex++) {
                CommoditySpecification basketCommSpec = basketSold.get(soldIndex);
                CommoditySold commoditySold = seller.getCommoditiesSold().get(soldIndex);
                boolean isResizeable = commoditySold.getSettings().isResizable();
                if (!isResizeable) {
                    continue;
                }
                if (checkEngageCriteriaForCommodity(ledger, seller, soldIndex)) {
                    IncomeStatement incomeStatement = incomeStatements.get(soldIndex);
                    double expenses = incomeStatement.getExpenses();
                    if (expenses > 0) {
                        double desiredROI = getDesiredROI(incomeStatement);
                        double newRevenue = desiredROI * expenses;
                        double currentRevenue = incomeStatement.getRevenues();
                        double newCapacity = calculateNewCapacity(commoditySold, currentRevenue, newRevenue);
                        com.vmturbo.platform.analysis.actions.Resize resizeAction =
                          new com.vmturbo.platform.analysis.actions.Resize(seller, basketCommSpec, newCapacity);
                        actions.add(resizeAction);
                    }
                }
            }
        }

        return actions;
    }

    /**
     * Returns the desired ROI for a commodity.
     *
     * @param incomeStatement The income statement of the commodity.
     * @return
     */
    private static double getDesiredROI(IncomeStatement incomeStatement) {
        return (incomeStatement.getMinDesiredROI() + incomeStatement.getMaxDesiredROI()) / 2;
    }

    /**
     * Checks the resize engagement criterion for a commodity.
     *
     * @param ledger The ledger containing the income statements of commodities.
     * @param seller The trader that sells the commodity.
     * @param commoditySoldIndex The index of the commodity sold.
     * @return true if commodity meets the resize engagement criterion.
     */
    public static boolean checkEngageCriteriaForCommodity(Ledger ledger, Trader seller, int commoditySoldIndex) {

        IncomeStatement commodityIS = ledger.getCommodityIncomeStatements(seller).get(commoditySoldIndex);
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
    private static double calculateNewCapacity(CommoditySold resizeCommodity, double currentRevenue, double newRevenue) {
        double currentCapacity = resizeCommodity.getEffectiveCapacity();
        double currentQuantity = resizeCommodity.getQuantity();
        PriceFunction priceFunction = resizeCommodity.getSettings().getPriceFunction();

        double intervalMin;
        double intervalMax;
        // assuming monotonically increasing price function
        if (newRevenue < currentRevenue) {
            intervalMin = UTILIZATION_LOW;
            intervalMax = currentQuantity/(currentCapacity);
        } else {
            intervalMin = currentQuantity/(currentCapacity);
            intervalMax = UTILIZATION_HIGH;
        }

        // solve p(x) = target for x in (a,b)
        DoubleUnaryOperator revenueFunction = (u) -> u * priceFunction.unitPrice(u) - newRevenue;
        double newNormalizedUtilization = Bisection.solve(ROOT_ACCURACY, MAX_ITERATIONS, revenueFunction, intervalMin, intervalMax);

        double newCapacity = currentQuantity / newNormalizedUtilization;
        return newCapacity;
    }
}
