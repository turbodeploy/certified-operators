package com.vmturbo.platform.analysis.pricefunction;

import java.io.Serializable;

import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.utilities.Quote.MutableQuote;

@FunctionalInterface
public interface QuoteFunction extends Serializable {
    MutableQuote calculateQuote(ShoppingList buyer, Trader seller, double bestQuoteSoFar,
                                boolean forTraderIncomeStmt, Economy economy);

}
