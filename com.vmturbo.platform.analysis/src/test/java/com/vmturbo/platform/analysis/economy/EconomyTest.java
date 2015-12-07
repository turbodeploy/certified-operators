package com.vmturbo.platform.analysis.economy;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

/**
 * A test case for the {@link Economy} class.
 */
@RunWith(JUnitParamsRunner.class)
public class EconomyTest {
    // Fields

    // CommoditySpecifications to use in tests
    private static final CommoditySpecification CPU_ANY = new CommoditySpecification((short)0,1,Integer.MAX_VALUE);
    private static final CommoditySpecification CPU_4 = new CommoditySpecification((short)0,4,4);
    private static final CommoditySpecification CPU_1to8 = new CommoditySpecification((short)0,1,8);
    private static final CommoditySpecification MEM = new CommoditySpecification((short)1);
    private static final CommoditySpecification ST_OVER1000 = new CommoditySpecification((short)2,1000,Integer.MAX_VALUE);
    private static final CommoditySpecification ST_UPTO1200 = new CommoditySpecification((short)2,0,1200);
    private static final CommoditySpecification VM1 = new CommoditySpecification((short)3,0,0);
    private static final CommoditySpecification VM2 = new CommoditySpecification((short)3,1,1);
    private static final CommoditySpecification CLUSTER_A = new CommoditySpecification((short)4,0,0);
    private static final CommoditySpecification CLUSTER_B = new CommoditySpecification((short)4,1,1);
    private static final CommoditySpecification SEGMENT_1 = new CommoditySpecification((short)5,0,0);
    private static final CommoditySpecification SEGMENT_2 = new CommoditySpecification((short)5,1,1);

    // Baskets to use in tests
    private static final Basket EMPTY = new Basket();
    private static final Basket PM_ANY = new Basket(CPU_ANY,MEM);
    private static final Basket PM_4CORE = new Basket(CPU_4,MEM);
    private static final Basket PM_SELL = new Basket(CPU_1to8,MEM);
    private static final Basket ST_BUY = new Basket(ST_OVER1000);
    private static final Basket ST_SELL = new Basket(ST_UPTO1200);
    private static final Basket APP_1 = new Basket(VM1);
    private static final Basket APP_2 = new Basket(VM2);
    private static final Basket PM_A = new Basket(CPU_ANY,MEM,CLUSTER_A);
    private static final Basket PM_B = new Basket(CPU_ANY,MEM,CLUSTER_B);
    private static final Basket PM_SELL_A = new Basket(CPU_1to8,MEM,CLUSTER_A);
    private static final Basket PM_SELL_B = new Basket(CPU_1to8,MEM,CLUSTER_B);
    private static final Basket ST_A1 = new Basket(ST_OVER1000,CLUSTER_A,SEGMENT_1);
    private static final Basket ST_A2 = new Basket(ST_OVER1000,CLUSTER_A,SEGMENT_2);
    private static final Basket ST_SELL_A1 = new Basket(ST_UPTO1200,CLUSTER_A,SEGMENT_1);
    private static final Basket ST_SELL_A2 = new Basket(ST_UPTO1200,CLUSTER_A,SEGMENT_2);

    // Methods

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] generateEconomies() {
        final List<@NonNull Object @NonNull []> output = new ArrayList<>();

        // 0 nodes, 0 edges
        Economy economy = new Economy();
        output.add(new Object[]{economy,new Basket[]{}});

        // 1 node, 0 edges
        economy = new Economy();
        economy.addTrader(0, TraderState.ACTIVE, EMPTY);
        output.add(new Object[]{economy,new Basket[]{}});

        Basket[] baskets = {EMPTY,PM_SELL};
        Trader[] traders = new Trader[2];

        // 1 node, 1 edge (2 placements x 2 baskets sold x 2 baskets bought)
        for (Basket basketSold : baskets) {
            for (Basket basketBought1 : baskets) {
                for (int dst1 = 0 ; dst1 < 2 ; ++dst1) {
                    economy = new Economy();
                    traders[0] = economy.addTrader(0, TraderState.ACTIVE, basketSold);
                    economy.moveTrader(economy.addBasketBought(traders[0], basketBought1), traders[dst1]);
                    output.add(new Object[]{economy,new Basket[]{basketBought1}});
                }
            }
        }

        // 1 node, 2 edges (2x2 placements x 2 baskets sold x 2x2 baskets bought)
        for (Basket basketSold : baskets) {
            for (Basket basketBought1 : baskets) {
                for (Basket basketBought2 : baskets) {
                    for (int dst1 = 0 ; dst1 < 2 ; ++dst1) {
                        for (int dst2 = 0 ; dst2 < 2 ; ++dst2) {
                            economy = new Economy();
                            traders[0] = economy.addTrader(0, TraderState.ACTIVE, basketSold);
                            economy.moveTrader(economy.addBasketBought(traders[0], basketBought1), traders[dst1]);
                            economy.moveTrader(economy.addBasketBought(traders[0], basketBought2), traders[dst2]);
                            output.add(new Object[]{economy, basketBought1 == basketBought2
                                            ? new Basket[]{basketBought1} : baskets});
                        }
                    }
                }
            }
        }

        // 2 nodes, 0 edges (x2 baskets sold)
        for (Basket basketSold : baskets) {
            economy = new Economy();
            economy.addTrader(0, TraderState.ACTIVE, EMPTY);
            economy.addTrader(0, TraderState.ACTIVE, basketSold);
            output.add(new Object[]{economy,new Basket[]{}});
        }

        traders = new Trader[3];
        // 2 nodes, 1 edge (6 placements x 2 baskets bought x 2 baskets sold)
        for (Basket basketSold : baskets) {
            for (Basket basketBought1 : baskets) {
                for (int src1 = 0 ; src1 < 2 ; ++src1) {
                    for (int dst1 = 0 ; dst1 < 3 ; ++dst1) {
                        economy = new Economy();
                        traders[0] = economy.addTrader(0, TraderState.ACTIVE, EMPTY);
                        traders[1] = economy.addTrader(0, TraderState.ACTIVE, basketSold);
                        economy.moveTrader(economy.addBasketBought(traders[src1], basketBought1), traders[dst1]);
                        output.add(new Object[]{economy,new Basket[]{basketBought1}});
                    }
                }
            }
        }

        // 2 nodes, 2 edges (6x6 placements x 2x2 baskets bought x 2 baskets sold)
        long startAll = System.nanoTime();
        long economyCounter = 0;
        for (Basket basketSold : baskets) {
            for (Basket basketBought1 : baskets) {
                for (Basket basketBought2 : baskets) {
                    for (int src1 = 0 ; src1 < 2 ; ++src1) {
                        for (int dst1 = 0 ; dst1 < 3 ; ++dst1) {
                            for (int src2 = 0 ; src2 < 2 ; ++src2) {
                                for (int dst2 = 0 ; dst2 < 3 ; ++dst2) {
                                    long start = System.nanoTime();
                                    economy = new Economy();
                                    traders[0] = economy.addTrader(0, TraderState.ACTIVE, EMPTY);
                                    traders[1] = economy.addTrader(0, TraderState.ACTIVE, basketSold);
                                    economy.moveTrader(economy.addBasketBought(traders[src1], basketBought1), traders[dst1]);
                                    economy.moveTrader(economy.addBasketBought(traders[src2], basketBought2), traders[dst2]);
                                    economyCounter += System.nanoTime() - start;

                                    output.add(new Object[]{economy, basketBought1 == basketBought2
                                        ? new Basket[]{basketBought1} : baskets});
                                }
                            }
                        }
                    }
                }
            }
        }

        // TODO (Vaptistis): add representative economies of larger size
        return output.toArray();
    }

    @Test
    public final void testEconomy() {
        Economy economy = new Economy();
        assertTrue(economy.getMarkets().isEmpty());
        assertTrue(economy.getTraders().isEmpty());
    }

    // TODO (Vaptistis): This is not the recommended way to write tests. Normally there would be
    // one test case method for each tested method and this would make it easier to see what went
    // wrong when a test fails. Unfortunately there seems to be a problem with the framework that
    // makes it spend most of its time invoking the test cases instead of running them. This means
    // that by breaking the tests into 20 different methods we would make the test take ~20 times
    // more time, and by then we would be risking for people not running them because they would
    // take too long. This needs to be refactored into distinct test cases as soon as the
    // performance issues of the testing framework are solved!
    @Test
    @Parameters(method = "generateEconomies")
    public final void testEconomyMethods(@NonNull Economy economy, @NonNull Basket @NonNull [] baskets) {
        // Test Economy.getMarkets()
        assertEquals(baskets.length, economy.getMarkets().size());

        int i = 0;
        for (@NonNull @ReadOnly Market market : economy.getMarkets()) {
            assertEquals(0, market.getBasket().compareTo(baskets[i++]));
        }

        // Test Economy.getMarket(Basket)
        for (Basket basket : baskets) {
            assertEquals(0, economy.getMarket(basket).getBasket().compareTo(basket));
        }

        // Test Economy.getMarket(BuyerParticipation)
        for (@NonNull @ReadOnly Market market : economy.getMarkets()) {
            for (@NonNull BuyerParticipation participation : market.getBuyers()) {
                assertSame(market, economy.getMarket(participation));
                assertEquals(market.getBasket().size(), participation.getQuantities().length);
                // Could have also tested getPeakQuantities here, but the property
                // getQuantities().length == getPeakQuantities().length is tested in
                // BuyerParticipationTest
            }
        }

        // Test Economy.getIndex(Trader)
        for (i = 0 ; i < economy.getTraders().size() ; ++i) {
            assertEquals(i, economy.getIndex(economy.getTraders().get(i)));
            // This also implicitly checks that all traders in economy are distinct.
        }

        // Test Economy.getCommoditiesBought(BuyerParticipation)
        // and  Economy.getCommodityBought(BuyerParticipation,CommoditySpecification)
        for (@NonNull @ReadOnly Market market : economy.getMarkets()) {
            for (@NonNull BuyerParticipation participation : market.getBuyers()) {
                final @NonNull @ReadOnly List<@NonNull CommodityBought> commoditiesBought =
                                                        economy.getCommoditiesBought(participation);
                assertEquals(market.getBasket().size(), commoditiesBought.size());
                for (i = 0 ; i < commoditiesBought.size() ; ++i) {
                    final @NonNull CommodityBought commodityBought1 = commoditiesBought.get(i);
                    final @NonNull CommodityBought commodityBought2 =
                        economy.getCommodityBought(participation, market.getBasket().get(i));

                    // The API doesn't guarantee that you'll get the same commodity bought object
                    // each time you call getCommodityBought or that it will match one of the
                    // objects in getCommoditiesBought, but the objects must refer to the same
                    // quantity and peak quantity values.

                    // test quantity between commodities
                    double quantity = commodityBought1.getQuantity();
                    assertEquals(quantity, commodityBought2.getQuantity(), 0);
                    commodityBought1.setQuantity(quantity += 1.5);
                    assertEquals(quantity, commodityBought2.getQuantity(), 0);
                    commodityBought2.setQuantity(quantity += 1.5);
                    assertEquals(quantity, commodityBought1.getQuantity(), 0);

                    // test peak quantity between commodities
                    double peakQuantity = commodityBought1.getPeakQuantity();
                    assertEquals(peakQuantity, commodityBought2.getPeakQuantity(), 0);
                    commodityBought1.setPeakQuantity(peakQuantity += 1.5);
                    assertEquals(peakQuantity, commodityBought2.getPeakQuantity(), 0);
                    commodityBought2.setPeakQuantity(peakQuantity += 1.5);
                    assertEquals(peakQuantity, commodityBought1.getPeakQuantity(), 0);

                    // TODO (Vaptistis): the current API allows changing the bought quantity and
                    // peak quantity through the commodity bought but not through the quantity and
                    // peak quantity vectors. The rational was that these vectors are intended to be
                    // used in contexts where changing these values would be wrong while the
                    // commodities bought are intended to be used by the message handlers accepting
                    // changes coming from Mediation. Yet this behavior is surprising!

                    // test quantity between commodity and vector
                    quantity = commodityBought1.getQuantity();
                    assertEquals(quantity, participation.getQuantities()[i], 0);
                    commodityBought1.setQuantity(quantity += 1.5);
                    assertEquals(quantity, participation.getQuantities()[i], 0);

                    // test peak quantity between commodity and vector
                    peakQuantity = commodityBought1.getPeakQuantity();
                    assertEquals(peakQuantity, participation.getPeakQuantities()[i], 0);
                    commodityBought1.setPeakQuantity(peakQuantity += 1.5);
                    assertEquals(peakQuantity, participation.getPeakQuantities()[i], 0);
                }
            }
        }

    }

    @Test
    @Ignore
    public final void testGetTraders() {
        fail("Not yet implemented");// TODO
    }

    @Test
    @Ignore
    public final void testAddTrader() {
        fail("Not yet implemented");// TODO
    }

    @Test
    @Ignore
    public final void testRemoveTrader() {
        fail("Not yet implemented");// TODO
    }

    @Test
    @Ignore
    public final void testMoveTrader() {
        fail("Not yet implemented");// TODO
    }

    @Test
    @Ignore
    public final void testGetCustomers() {
        fail("Not yet implemented");// TODO
    }

    @Test
    @Ignore
    public final void testGetCustomerParticipations() {
        fail("Not yet implemented");// TODO
    }

    @Test
    @Ignore
    public final void testGetSuppliers() {
        fail("Not yet implemented");// TODO
    }

    @Test
    @Ignore
    public final void testGetMarketsAsBuyer() {
        fail("Not yet implemented");// TODO
    }

    @Test
    @Ignore
    public final void testGetMarketsAsSeller() {
        fail("Not yet implemented");// TODO
    }

    @Test
    @Ignore
    public final void testAddCommodityBought() {
        fail("Not yet implemented");// TODO
    }

    @Test
    @Ignore
    public final void testRemoveCommodityBought() {
        fail("Not yet implemented");// TODO
    }

    @Test
    @Ignore
    public final void testClone() {
        fail("Not yet implemented");// TODO
    }

} // end EconomyTest class
