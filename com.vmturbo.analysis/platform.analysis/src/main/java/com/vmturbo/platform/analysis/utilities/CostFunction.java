package com.vmturbo.platform.analysis.utilities;

import java.io.Serializable;

import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.UnmodifiableEconomy;
import com.vmturbo.platform.analysis.utilities.Quote.MutableQuote;

@FunctionalInterface
public interface CostFunction extends Serializable {
    MutableQuote calculateCost(ShoppingList buyer, Trader seller, boolean validate, UnmodifiableEconomy economy);
}