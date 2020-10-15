package com.vmturbo.platform.analysis.utilities;

import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.UnmodifiableEconomy;

@FunctionalInterface
public interface FunctionalOperator {
    double[] operate(ShoppingList buyer, int boughtIndex, CommoditySold commSold, Trader seller,
                     UnmodifiableEconomy economy, boolean take, double overhead);
}