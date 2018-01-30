package com.vmturbo.platform.analysis.utilities;

import java.io.Serializable;

import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;

@FunctionalInterface
public interface CostFunction extends Serializable {
    double calculateCost(ShoppingList buyer, Trader seller, boolean validate, Economy economy);

}