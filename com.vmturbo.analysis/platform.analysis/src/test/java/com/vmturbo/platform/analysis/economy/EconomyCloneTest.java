package com.vmturbo.platform.analysis.economy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.platform.analysis.ede.EdeCommon;
import com.vmturbo.platform.analysis.pricefunction.PriceFunctionFactory;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;

public class EconomyCloneTest {

    Economy economy;
    Economy clone;
    int TOTAL_SELLERS = 20;
    int INACTIVE_SELLER = 17; // an arbitrary number

    @Before
    public void setupEconomy() {
        economy = new Economy();
        int sellerNumber = 0;
        Basket basket = new Basket(new CommoditySpecification(1));
        Basket empty = new Basket();
        Trader seller = economy.addTrader(1, TraderState.ACTIVE, basket);
        seller.setDebugInfoNeverUseInCode(String.valueOf(sellerNumber++));
        economy.addTrader(1, TraderState.ACTIVE, basket)
            .setDebugInfoNeverUseInCode(String.valueOf(sellerNumber++));
        CommoditySold commSold = seller.getCommoditiesSold().get(0);
        commSold.setCapacity(100);
        commSold.setQuantity(20);
        commSold.setPeakQuantity(30);
        commSold.getSettings().setPriceFunction(PriceFunctionFactory.createStandardWeightedPriceFunction(7.0));

        Trader buyer = economy.addTrader(2, TraderState.ACTIVE, empty);
        buyer.setDebugInfoNeverUseInCode(String.valueOf(sellerNumber++));
        ShoppingList shoppingList = economy.addBasketBought(buyer, basket);
        shoppingList.setQuantity(0, 11);
        shoppingList.setPeakQuantity(0, 15);
        shoppingList.move(seller);

        // Set the state of one trader to INACTIVE, then verify that the INACTIVE one in
        // the clone appears in the same location in the list.
        while (economy.getTraders().size() < TOTAL_SELLERS) {
            TraderState state = economy.getTraders().size() == INACTIVE_SELLER
                            ? TraderState.INACTIVE
                            : TraderState.ACTIVE;
            economy.addTrader(1, state, basket)
                .setDebugInfoNeverUseInCode(String.valueOf(sellerNumber++));
        }

        clone = economy.simulationClone();
    }

    /**
     * Test that a quote in the cloned economy is equal to a quote in the original economy.
     */
    @Test
    public void  testCloneEconomy() {
        Trader seller = economy.getTraders().get(0);
        ShoppingList shoppingList = seller.getCustomers().get(0);
        double quote = EdeCommon.quote(economy, shoppingList, seller, Double.POSITIVE_INFINITY, false)
            .getQuoteValue();

        Trader cloneSeller = clone.getTraders().get(0);
        ShoppingList cloneShoppingList = cloneSeller.getCustomers().get(0);
        double cloneQuote = EdeCommon.quote(clone, cloneShoppingList, cloneSeller, Double.POSITIVE_INFINITY, false)
            .getQuoteValue();
        assertTrue(quote > 0); // Just to be sure
        assertEquals(quote, cloneQuote, TestUtils.FLOATING_POINT_DELTA);
    }

    /**
     * Verify that the order of traders in the cloned economy is the same as in the original,
     * by verifying that the one trader which is INACTIVE is in the same place in the clone as
     * the one in the original economy.
     */
    @Test
    public void testCloneSellerOrder() {
        assertEquals(TraderState.INACTIVE, clone.getTraders().get(INACTIVE_SELLER).getState());
        clone.getTraders().stream()
            .forEach(t -> assertEquals(t.getDebugInfoNeverUseInCode(),
                    t.getEconomyIndex() + Economy.SIM_CLONE_SUFFIX));
    }
}
