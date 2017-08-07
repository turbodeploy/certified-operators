package com.vmturbo.platform.analysis.utilities;

import com.vmturbo.platform.analysis.economy.CommoditySold;

public class FunctionalOperatorUtil {

    public static FunctionalOperator ADD_COMM = (buyer, boughtIndex, commSold, seller, economy, take)
                    -> new double[]{buyer.getQuantities()[boughtIndex] + commSold.getQuantity(),
                                    buyer.getPeakQuantities()[boughtIndex] + commSold.getPeakQuantity()};

    public static FunctionalOperator SUB_COMM = (buyer, boughtIndex, commSold, seller, economy, take)
                    -> new double[]{Math.max(0, commSold.getQuantity() - buyer.getQuantities()[boughtIndex]),
                                    Math.max(0, commSold.getPeakQuantity() - buyer.getPeakQuantities()[boughtIndex])};

    // Return commSoldUsed (template-cost) when taking the action and
    // (spent - oldTemplateCost + newTemplateCost) when not taking the action
    public static FunctionalOperator UPDATE_EXPENSES = (buyer, boughtIndex, commSold, seller,
                        economy, take)
                    -> {
                        if (take) {
                            // updating the action spent when taking it
                            CommoditySold commSoldByCurrSeller = buyer.getSupplier() != null ? buyer.getSupplier()
                                            .getCommoditySold(buyer.getBasket().get(boughtIndex)) : null;
                            double currCost = commSoldByCurrSeller == null ? 0 : commSoldByCurrSeller.getQuantity();
                            economy.setSpent((float)(economy.getSpent() - currCost + commSold.getQuantity()));
                            // do not update the usedValues of the soldCommodities when the action is being taken
                            return new double[]{commSold.getQuantity(), commSold.getPeakQuantity()};
                        } else {
                            return new double[]{economy.getSpent() - buyer.getQuantities()[boughtIndex] +
                                                commSold.getQuantity(), 0};
                    }};

    // Return commSoldUsed when taking the action and add commBought with commSold when not taking the action
    public static FunctionalOperator IGNORE_CONSUMPTION = (buyer, boughtIndex, commSold, seller, economy, take)
                    -> {if (take) {
                            return new double[]{commSold.getQuantity(), commSold.getPeakQuantity()};
                        } else {
                            return new double[]{buyer.getQuantities()[boughtIndex] + commSold.getQuantity(),
                                                buyer.getPeakQuantities()[boughtIndex] + commSold.getPeakQuantity()};
                        }
                    };

    public static FunctionalOperator AVG_COMMS = (buyer, boughtIndex, commSold, seller, economy, take)
                    -> new double[]{(commSold.getQuantity() + buyer.getQuantities()[boughtIndex])
                                    /seller.getCustomers().size(),
                                    (commSold.getPeakQuantity() - buyer.getPeakQuantities()[boughtIndex])
                                    /seller.getCustomers().size()};

    public static FunctionalOperator MAX_COMM = (buyer, boughtIndex, commSold, seller, economy, take)
                    -> new double[]{Math.max(buyer.getQuantities()[boughtIndex], commSold.getQuantity()),
                                    Math.max(buyer.getPeakQuantities()[boughtIndex], commSold.getPeakQuantity())};

    public static FunctionalOperator MIN_COMM = (buyer, boughtIndex, commSold, seller, economy, take)
                    -> new double[]{Math.min(buyer.getQuantities()[boughtIndex], commSold.getQuantity()),
                                    Math.min(buyer.getPeakQuantities()[boughtIndex], commSold.getPeakQuantity())};

    public static FunctionalOperator RETURN_BOUGHT_COMM = (buyer, boughtIndex, commSold, seller, economy, take)
                    -> new double[]{buyer.getQuantities()[boughtIndex],
                                    buyer.getPeakQuantities()[boughtIndex]};
}
