package com.vmturbo.platform.analysis;

import static com.google.common.base.Preconditions.*;

import java.util.Map;
import java.util.function.Supplier;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.BuyerParticipation;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;

/**
 * The Main class for the application.
 *
 * <p>
 *  Currently it only serves to test that the project is build correctly,
 *  but later it will serve as the entry point for the application that
 *  will run alongside OperationsManager to test the Market 2 prototype
 *  on select customers.
 * </p>
 */
public final class Main {

    public static void main(String[] args) {
        Supplier<String> greeter = () -> "Hello Java 8 world!!!";
        System.out.println(greeter.get());

        CommoditySpecification cpu = new CommoditySpecification(0);
        Basket basket1 = new Basket(cpu,new CommoditySpecification(1),
                                    new CommoditySpecification(2),new CommoditySpecification(3));
        Basket basket2 = new Basket(new CommoditySpecification(4),new CommoditySpecification(5));

        Economy economy = new Economy();
        Trader trader1 = economy.addTrader(0, TraderState.ACTIVE, new Basket(), basket1, basket2, basket2);
        Trader trader2 = economy.addTrader(1, TraderState.ACTIVE, basket1);
        Trader trader3 = economy.addTrader(2, TraderState.ACTIVE, basket2);
        Trader trader4 = economy.addTrader(2, TraderState.ACTIVE, basket2);
        economy.moveTrader(economy.getMarketsAsBuyer(trader1).get(economy.getMarket(basket1)).get(0), trader2);
        economy.moveTrader(economy.getMarketsAsBuyer(trader1).get(economy.getMarket(basket2)).get(0), trader3);
        economy.moveTrader(economy.getMarketsAsBuyer(trader1).get(economy.getMarket(basket2)).get(1), trader4);

        trader2.getCommoditySold(cpu).setCapacity(100);
        economy.getCommodityBought(economy.getMarketsAsBuyer(trader1).get(economy.getMarket(basket1)).get(0),cpu).setQuantity(42);

        for (Trader supplier : economy.getSuppliers(trader1)) {
            System.out.println(supplier.getType());
        }

        economy.moveTrader(economy.getMarketsAsBuyer(trader1).get(economy.getMarket(basket1)).get(0), null);
        economy.moveTrader(economy.getMarketsAsBuyer(trader1).get(economy.getMarket(basket2)).get(0), null);
        economy.moveTrader(economy.getMarketsAsBuyer(trader1).get(economy.getMarket(basket2)).get(1), null);

        for (Map.Entry<Market, BuyerParticipation> entry : economy.getMarketsAsBuyer(trader1).entries()) {
            if (economy.getSupplier(entry.getValue()) == null) {
                System.out.print("Trader1 is not currently buying basket ");
                System.out.print(entry.getKey().getBasket());
                System.out.println(" from anyone!");
            }
        }

        economy = new Economy();
        trader1 = economy.addTrader(0, TraderState.ACTIVE, new Basket());
        trader2 = economy.addTrader(1, TraderState.ACTIVE, basket1);
        trader3 = economy.addTrader(2, TraderState.ACTIVE, basket2);
        trader4 = economy.addTrader(2, TraderState.ACTIVE, basket2);
        economy.moveTrader(economy.addBasketBought(trader1, basket1), trader2);
        economy.moveTrader(economy.addBasketBought(trader1, basket2), trader3);
        economy.moveTrader(economy.addBasketBought(trader1, basket2), trader4);
    }

    /**
     * A simple factorial method, whose purpose is to test that Maven runs tests properly.
     *
     * @param n The natural number whose factorial should be returned.
     * @return n!
     */
    public static long factorial(int n) {
        checkArgument(n >= 0);

        long p = 1;
        while (n > 1) {
            p *= n;
            --n;
        }

        return p;
    }
}
