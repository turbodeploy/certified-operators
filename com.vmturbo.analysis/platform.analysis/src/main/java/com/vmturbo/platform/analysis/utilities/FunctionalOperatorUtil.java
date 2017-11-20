package com.vmturbo.platform.analysis.utilities;

import com.vmturbo.platform.analysis.economy.BalanceAccount;
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
        BalanceAccount ba = buyer.getBuyer().getSettings().getBalanceAccount();
                        if (take) {
                            // updating the action spent when taking it
                            CommoditySold commSoldByCurrSeller = buyer.getSupplier() != null ? buyer.getSupplier()
                                            .getCommoditySold(buyer.getBasket().get(boughtIndex)) : null;
                            double currCost = commSoldByCurrSeller == null ? 0 : commSoldByCurrSeller.getQuantity();
                            ba.setSpent((float)(ba.getSpent() - currCost + commSold.getQuantity()));
                            // do not update the usedValues of the soldCommodities when the action is being taken
                            return new double[]{commSold.getQuantity(), commSold.getPeakQuantity()};
                        } else {
                            return new double[]{ba.getSpent() - buyer.getQuantities()[boughtIndex] +
                                                commSold.getQuantity(), 0};
                    }};

    // when taking the action, Return commSoldUsed
    // when not taking the action, return 0 if the buyer fits or INFINITY otherwise
    public static FunctionalOperator IGNORE_CONSUMPTION = (buyer, boughtIndex, commSold, seller, economy, take)
                    -> {if (take) {
                            return new double[]{commSold.getQuantity(), commSold.getPeakQuantity()};
                        } else {
                            return ((buyer.getQuantities()[boughtIndex] <= commSold.getCapacity()) &&
                                    (buyer.getPeakQuantities()[boughtIndex] <= commSold.getCapacity())) ?
                                    new double[]{0, 0} : new double[]{Double.POSITIVE_INFINITY,
                                                    Double.POSITIVE_INFINITY};
                        }
                    };

    public static FunctionalOperator AVG_COMMS = (buyer, boughtIndex, commSold, seller, economy, take)
                    -> {int numCustomers = seller.getCustomers().size();
                    // if we take the move, we have already moved and we dont need to assume a new
                    // customer. If we are not taking the move, we want to update the used considering
                    // an incoming customer. In which case, we need to increase the custoemrCount by 1
                        return new double[]{(commSold.getQuantity() * numCustomers +
                                                buyer.getQuantities()[boughtIndex])
                                                    /(numCustomers + (take ? 0 : 1)),
                                            (commSold.getPeakQuantity() * numCustomers +
                                                buyer.getPeakQuantities()[boughtIndex])
                                                    /(numCustomers + (take ? 0 : 1))
                                            };
                    };

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
