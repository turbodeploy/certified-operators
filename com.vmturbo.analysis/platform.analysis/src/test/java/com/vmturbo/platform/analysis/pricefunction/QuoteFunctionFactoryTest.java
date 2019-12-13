package com.vmturbo.platform.analysis.pricefunction;

import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;

public class QuoteFunctionFactoryTest {

    private final QuoteFunction qf = QuoteFunctionFactory.sumOfCommodityQuoteFunction();
    private final Basket basketBought = new Basket(
        new CommoditySpecification(1),
        new CommoditySpecification(2),
        new CommoditySpecification(3));

    private Trader buyer;
    private Trader seller;
    private ShoppingList shoppingList;

    private final Economy economy = new Economy();

    @Before
    public void setup() {
        buyer = economy.addTrader(0, TraderState.ACTIVE, new Basket(), basketBought);
        seller = economy.addTrader(0, TraderState.ACTIVE, basketBought, new Basket());
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();
        shoppingList = economy.getMarketsAsBuyer(buyer).keySet().iterator().next();

        shoppingList.setQuantity(0, 100.0);
        shoppingList.setPeakQuantity(0, 100.0);
        shoppingList.setQuantity(1, 100.0);
        shoppingList.setPeakQuantity(1, 100.0);
        shoppingList.setQuantity(2, 100.0);
        shoppingList.setPeakQuantity(2, 100.0);

        seller.getCommoditySold(new CommoditySpecification(1)).setCapacity(300.0);
        seller.getCommoditySold(new CommoditySpecification(2)).setCapacity(300.0);
        seller.getCommoditySold(new CommoditySpecification(3)).setCapacity(300.0);
    }

    // The current supplier should compute the complete quote even if we determine
    // it is more expensive than the best quote so that the value can be reused.
    @Test
    public void testSumOfCommodityQuoteFunctionCurrentSupplier() {
        // Move onto the seller.
        shoppingList.move(seller);

        final double quote[] = qf.calculateQuote(shoppingList, seller, 0, false, economy)
            .getQuoteValues();
        assertTrue(quote[0] > 0.5); // Each commodity adds 0.3333... to the quote. So the sum of the 3 is 1.0
    }

    // We can early-exit for new suppliers if they are more expensive than the best quote.
    @Test
    public void testSumOfCommodityQuoteFunctionNewSupplier() {
        final double quote[] = qf.calculateQuote(shoppingList, seller, 0, false, economy)
            .getQuoteValues();
        // Because best quote is 0, as soon as we get above this value we should early exit and not compute
        // the full quote since we are getting the quote from the current supplier.
        assertTrue(quote[0] < 0.5);
    }
}