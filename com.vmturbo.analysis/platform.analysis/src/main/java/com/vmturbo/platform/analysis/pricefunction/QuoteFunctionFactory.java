package com.vmturbo.platform.analysis.pricefunction;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.analysis.economy.BalanceAccount;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ede.EdeCommon;
import com.vmturbo.platform.analysis.utilities.CostFunction;

public class QuoteFunctionFactory {

    private static final Logger logger = LogManager.getLogger(QuoteFunctionFactory.class);

    /**
     * Create a quote function which sums up the cost from each commodity in the buyer's basket.
     *
     * @return {@link QuoteFunction}
     */
    public static @Nonnull QuoteFunction sumOfCommodityQuoteFunction() {
        QuoteFunction qf = (buyer, seller, bestQuoteSoFar, forTraderIncomeStmt, economy) -> {
            Basket basket = buyer.getBasket();
            double[] quote = {0.0, 0.0, 0.0};
            // go over all commodities in basket
            int boughtIndex = 0;
            for (int soldIndex = 0; boughtIndex < basket.size()
                            && quote[0] < bestQuoteSoFar && Double.isFinite(quote[0]);
                            boughtIndex++, soldIndex++) {
                CommoditySpecification basketCommSpec = basket.get(boughtIndex);
                // Find corresponding commodity sold. Commodities sold are ordered the same way as the
                // basket commodities, so iterate once (O(N)) as opposed to searching each time (O(NLog(N))
                while (!basketCommSpec.isSatisfiedBy(seller.getBasketSold().get(soldIndex))) {
                    soldIndex++;
                }
                double[] tempQuote = EdeCommon.computeCommodityCost(economy, buyer, seller,
                                                                    soldIndex, boughtIndex, forTraderIncomeStmt);
                quote[0] += tempQuote[0];
                if (forTraderIncomeStmt) {
                    quote[1] += tempQuote[1];
                    quote[2] += tempQuote[2];
                }
            }
            if (seller.isDebugEnabled() && buyer.getSupplier() == seller && Double.isInfinite(quote[0])) {
                CommoditySpecification basketCommSpec = basket.get(boughtIndex - 1);
                logger.info("{" + buyer.getBuyer().getDebugInfoNeverUseInCode()
                            + "} The commodity causing the infinite quote: "
                            + basketCommSpec.getDebugInfoNeverUseInCode());
            }
            return quote;
        };
        return qf;
    }

    /**
     * Create a quote function which computes the risk of running out of budget if the buyer
     * move from current supplier to a new seller.
     *
     * @return {@link QuoteFunction}
     */
    public static @Nonnull QuoteFunction budgetDepletionRiskBasedQuoteFunction() {
        QuoteFunction qf = (buyer, seller, bestQuoteSoFar, forTraderIncomeStmt, economy) -> {
            double[] quote = {0.0, 0.0, 0.0};
            double costOnNewSeller = computeCost(buyer, seller);
            double costOnCurrentSupplier = computeCost(buyer, buyer.getSupplier());
            BalanceAccount ba = buyer.getBuyer().getSettings().getBalanceAccount();
            // TODO: if the buyer is on the wrong supplier, costOnSupplier may be infinity
            // now I added this to workaround such a case
            if (Double.isInfinite(costOnCurrentSupplier)) {
                costOnCurrentSupplier = 0;
            }
			double spent = (ba == null ? 0 : ba.getSpent());
			double budget = (ba == null ? 1 : ba.getBudget());
			double budgetUtil = (spent - costOnCurrentSupplier + costOnNewSeller) / budget;
            quote[0] = (budgetUtil >= 1) ? Double.POSITIVE_INFINITY :
                    1 / ((1 - budgetUtil) * (1 - budgetUtil));
            return quote;
        };
        return qf;
    }

    /**
     * Computes the cost of shopping list on a trader by applying the trader's {@link CostFunction}.
     *
     * @param shoppingList the shopping list as buyer
     * @param seller the trader which charges the buyer
     * @return the cost
     */
    private static double computeCost(ShoppingList shoppingList, Trader seller) {
        return (seller == null || seller.getSettings().getCostFunction() == null) ? 0
                        : seller.getSettings().getCostFunction().calculateCost(shoppingList, seller);
    }
}
