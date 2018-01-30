package com.vmturbo.platform.analysis.utilities;

import java.io.Serializable;

import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.UnmodifiableEconomy;

@FunctionalInterface
public interface CostFunction extends Serializable {
    double calculateCost(ShoppingList buyer, Trader seller, boolean validate, UnmodifiableEconomy economy);

}