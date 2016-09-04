package com.vmturbo.platform.analysis.benchmarks;

import java.util.List;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommodityBought;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.ede.Ede;

/**
 * A benchmark to determine the performance of the 'market algorithms' on Economies of varying sizes
 * with only one market.
 *
 * <p>
 *  May be useful to run with one or more of the following JVM options:
 *  -verbose:gc -verbose:class -verbose:jni
 *  -Xdiag -XX:+UnlockDiagnosticVMOptions -XX:+PrintCompilation -XX:+PrintInlining
 *  -Xmx16G -Xms16G -Xmn6G
 * </p>
 */
public final class GeneratingRecommendations {
    // Methods

    public static void main(String[] args) {
        // Parameters
        final int nOrdersOfMagnitude = 20;
        final int nCommodities = 10;
        final int nIterations = 5;

        // Generate a number of large economies
        System.out.println("Populating Economies:");
        final CommoditySpecification[] commoditySpecifications = new CommoditySpecification[nCommodities];
        for (int i = 0 ; i < commoditySpecifications.length ; ++i) {
            commoditySpecifications[i] = new CommoditySpecification(i);
        }
        final @NonNull Basket emptyBasket = new Basket();
        final @NonNull Basket tradedBasket = new Basket(commoditySpecifications);

        final @NonNull Economy[] economies = new Economy[nOrdersOfMagnitude];
        for (int i = 0, nBuyers = 500, nSellers = 1 ; i < nOrdersOfMagnitude ; ++i, nSellers *= 2) {
            long start = System.nanoTime();
            economies[i] = new Economy();

            for (int c = 0 ; c < nBuyers ; ++c) {
                Trader trader = economies[i].addTrader(1, TraderState.ACTIVE, emptyBasket, tradedBasket);

                for (CommodityBought commodityBought : economies[i].getCommoditiesBought(
                        economies[i].getMarketsAsBuyer(trader).keySet().iterator().next())) {
                    commodityBought.setQuantity(10);
                    commodityBought.setPeakQuantity(15);
                }
            }

            for (int c = 0 ; c < nSellers ; ++c) {
                Trader trader = economies[i].addTrader(0, TraderState.ACTIVE, tradedBasket);

                for (@NonNull @ReadOnly CommoditySold commoditySold : trader.getCommoditiesSold()) {
                    commoditySold.setCapacity(100);
                    commoditySold.setQuantity(50);
                    commoditySold.setPeakQuantity(60);
                }
            }

            System.out.println(System.nanoTime()-start);
        }

        // Warm up
        final @NonNull Ede ede = new Ede();
        System.out.println("Warming up:");
        long start = System.nanoTime();
        List<Action> actions = ede.generateActions(economies[nOrdersOfMagnitude - 1], false,
                        true, true, true);
        System.out.println(System.nanoTime()-start);


        // Run placement
        for (int i = 0 ; i < nOrdersOfMagnitude ; ++i) {
            for (int j = 0 ; j < nIterations ; ++j) {
                start = System.nanoTime();
                actions = ede.generateActions(economies[i], false, true, true, true);
                System.out.print(System.nanoTime()-start + "\t");
            }
            System.out.println();
        }
        System.out.println("Done!");
    }

} // end GeneratingRecommendations benchmark
