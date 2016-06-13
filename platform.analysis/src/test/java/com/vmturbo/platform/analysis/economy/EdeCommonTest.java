package com.vmturbo.platform.analysis.economy;

import java.util.function.DoubleBinaryOperator;

import org.junit.Test;
import org.junit.runner.RunWith;

import com.vmturbo.platform.analysis.ede.EdeCommon;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import static org.junit.Assert.*;

/**
 * A test case for the {@link Basket} class.
 */
@RunWith(JUnitParamsRunner.class)
public class EdeCommonTest {

    // fields
    private static final int commodityCapacity = 10;
    private static final int commodityUsed = 5;
    private static final double expectedQuote = 5.11;

    // CommoditySpecifications to use in tests
    private static final CommoditySpecification CPU_ANY = new CommoditySpecification(0, 1, Integer.MAX_VALUE);
    private static final CommoditySpecification MEM = new CommoditySpecification(1);
    private static final CommoditySpecification ST_OVER1000 = new CommoditySpecification(2, 1000, Integer.MAX_VALUE);
    private static final CommoditySpecification ST_LAT = new CommoditySpecification(3, 0, 100);
    private static final CommoditySpecification CLUSTER_A = new CommoditySpecification(4, 0, 0);


    // Baskets to use in tests
    private static final Basket PM_ANY = new Basket(CPU_ANY, MEM);
    private static final Basket ST_SELL = new Basket(ST_OVER1000, ST_LAT);
    private static final Basket PM_A = new Basket(CPU_ANY, MEM, CLUSTER_A);

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestQuote_Basket_Basket_Boolean_Boolean() {
        return new Object[][] {
            {PM_ANY, PM_ANY, true, true},
            {PM_ANY, PM_A, true, true},
            {PM_ANY, PM_ANY, true, false},
            {PM_ANY, PM_A, true, false}
        };
    }

    // verify quote when the provider sells varying sizes of baskets which could be inadequate sometimes (the consumer should not shop in such markets)
    @Test
    @Parameters
    @TestCaseName("Test #{index}: Quote({0},{1},{2},{3},{4})")
    public final void testQuote_Basket_Basket_Boolean_Boolean(Basket bought, Basket sold, boolean providerPresent,
                                                                    boolean oldSupplierPresent) {
        Economy economy = new Economy();
        Trader providingTrader = economy.addTrader(0, TraderState.ACTIVE, sold); // u can create a trader only by adding it to the market
        Basket basketSold = providingTrader.getBasketSold();
        // change the capacity of the commSold to commodityCapacity of 10
        basketSold.forEach(item->{providingTrader.getCommoditySold(item).setCapacity(commodityCapacity);
                           if (oldSupplierPresent) {
                               providingTrader.getCommoditiesSold().forEach(item1->{item1.setQuantity(5).setPeakQuantity(7);});
                           }
            }
        );

        ShoppingList consumerShoppingList = economy.addBasketBought(economy.addTrader(1, TraderState.ACTIVE, bought), bought);
        consumerShoppingList.setQuantity(0, 5).setPeakQuantity(0, 7);
        consumerShoppingList.setQuantity(1, 5).setPeakQuantity(1, 7);

        consumerShoppingList.setSupplier(oldSupplierPresent ? providingTrader : null);
        assertTrue(expectedQuote ==  Math.round(EdeCommon.quote(economy, consumerShoppingList, providingTrader,
                        Double.POSITIVE_INFINITY, false)[0]*100d)/100d);

    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestQuote_Basket_Basket_Boolean_Invalid () {
        return new Object[][] {
            {PM_A, PM_ANY, true},
            {PM_A, PM_ANY, false},
        };
    }

    // finding a quote for a consumer that isnt sufficiently provided by any provider... We throw an outOfBoundsException in this case
    // verify quote when the provider sells varying sizes of baskets which could be inadequate sometimes (the consumer should not shop in such markets)
    @Test(expected = Exception.class)
    @Parameters
    @TestCaseName("Test #{index}: Quote({0},{1},{2})")
    public final void testQuote_Basket_Basket_Boolean_Invalid(Basket bought, Basket sold,
                                                                    boolean oldSupplierPresent) {
        Economy economy = new Economy();
        Trader providingTrader = economy.addTrader(0, TraderState.ACTIVE, sold); // u can create a trader only by adding it to the market
        Basket basketSold = providingTrader.getBasketSold();
        // change the capacity of the commSold to 10
        basketSold.forEach(item->providingTrader.getCommoditySold(item).setCapacity(commodityCapacity));

        ShoppingList consumerShoppingList = economy.addBasketBought(economy.addTrader(1, TraderState.ACTIVE, bought), bought);
        consumerShoppingList.setQuantity(0, 5).setPeakQuantity(0, 7);
        consumerShoppingList.setQuantity(1, 5).setPeakQuantity(1, 7);

        consumerShoppingList.setSupplier(oldSupplierPresent ? providingTrader : null);

        EdeCommon.quote(economy, consumerShoppingList, providingTrader, Double.POSITIVE_INFINITY, false);

    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestQuote_quantityFunction_Double_boolean() {
        return new Object[][]{
            {new DoubleBinaryOperator[]{(sold, bought) -> sold + bought, Math::max}, new double[]{4,9},
                                        new double[] {5,9}, true},
            {new DoubleBinaryOperator[]{(sold, bought) -> sold + bought, Math::max}, new double[]{7,9},
                                        new double[] {5,9}, false},// large quantity for commodity1
            {new DoubleBinaryOperator[]{(sold, bought) -> sold + bought, (sold, bought) -> sold + bought},
                                        new double[]{9,4}, new double[] {9,4}, false},// large quantity for commodity1
            {new DoubleBinaryOperator[]{(sold, bought) -> sold + bought, (sold, bought) -> sold + bought},
                                        new double[]{4,4}, new double[] {9,4}, false},// large peakQuantity for commodity1
            {new DoubleBinaryOperator[]{(sold, bought) -> sold + bought, (sold, bought) -> sold + bought},
                                        new double[]{4,4}, new double[] {4,5}, true},
        };
    }

    // verify quote when the provider sells varying sizes of baskets, sometimes inadequate
    @Test
    @Parameters
    @TestCaseName("Test #{index}: Quote({0},{1},{2},{3})")
    public final void testQuote_quantityFunction_Double_boolean(DoubleBinaryOperator quantityFunction[], double quantity[],
                                                                double peakQuantity[], boolean isCorrect) {
        Economy economy = new Economy();
        Basket basket = ST_SELL;
        Trader seller = economy.addTrader(0, TraderState.ACTIVE, basket); // u can create a trader only by adding it to the market
        ShoppingList consumerShoppingList = economy.addBasketBought(economy.addTrader(1, TraderState.ACTIVE, basket), basket);

        Basket basketSold = seller.getBasketSold();
        basketSold.forEach(item->{
            int indexOfItem = basketSold.indexOf(item);
            // change the capacity of the commSold to 12 of which 5 units are used
            seller.getCommoditySold(item).setCapacity(commodityCapacity).setQuantity(commodityUsed).setPeakQuantity(commodityUsed);
            economy.getModifiableQuantityFunctions().put(item, quantityFunction[indexOfItem]);
            consumerShoppingList.setQuantity(indexOfItem, quantity[indexOfItem]).setPeakQuantity(indexOfItem, peakQuantity[indexOfItem]);
        });

        boolean qInf = Double.isInfinite(EdeCommon.quote(economy, consumerShoppingList, seller, Double.POSITIVE_INFINITY, false)[0]);
        assertTrue(isCorrect != qInf);
    }


} // end class QuoteTest
