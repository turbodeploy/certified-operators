package com.vmturbo.platform.analysis.economy;

import static com.vmturbo.platform.analysis.testUtilities.TestUtils.VM_TYPE;
import static org.junit.Assert.*;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.platform.analysis.utilities.FunctionalOperator;
import com.vmturbo.platform.analysis.utilities.FunctionalOperatorUtil;
import com.vmturbo.platform.analysis.utility.CollectionTests;
import com.vmturbo.platform.analysis.utility.ListTests;
import com.vmturbo.platform.analysis.utility.MapTests;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;

/**
 * A test case for the {@link Economy} class.
 */
@RunWith(Enclosed.class)
public class EconomyTest {
    // Fields

    // CommoditySpecifications to use in tests
    private static final CommoditySpecification CPU = new CommoditySpecification(0);
    private static final CommoditySpecification MEM = new CommoditySpecification(1);
    private static final CommoditySpecification CLUSTER_A = new CommoditySpecification(4,1004);
    private static final CommoditySpecification SEGMENT_1 = new CommoditySpecification(5,1005);
    private static final CommoditySpecification CPU_1 = new CommoditySpecification(6,1000);
    private static final CommoditySpecification CPU_2 = new CommoditySpecification(7,1000);
    private static final CommoditySpecification CPU_4 = new CommoditySpecification(8,1000);
    private static final CommoditySpecification CPU_8 = new CommoditySpecification(9,1000);

    // Baskets to use in tests
    private static final Basket EMPTY = new Basket();
    private static final Basket PM_ANY = new Basket(CPU,MEM);
    private static final Basket PM_4CORE = new Basket(CPU_4,MEM);
    private static final Basket PM_SELL = new Basket(CPU_1, CPU_2, CPU_4, CPU_8,MEM);
    private static final Basket PMtoVM = new Basket(CPU,
        new CommoditySpecification(1), // MEM
        new CommoditySpecification(10), // Datastore commodity with key 1
        new CommoditySpecification(11));// Datastore commodity with key 2
    private static final Basket STtoVM = new Basket(
        new CommoditySpecification(12), // Storage Amount (no key)
        new CommoditySpecification(13));// DSPM access commodity with key A

    private static final int[] types = {0,1};
    private static final TraderState[] states = {TraderState.ACTIVE,TraderState.INACTIVE};
    private static final Basket[] basketsSold = {EMPTY,PM_SELL};
    private static final Basket[][] basketsBoughtLists = {{},{EMPTY},{EMPTY,PM_4CORE}};
    private static final Market independentMarket = new Market(EMPTY);
    private static final TraderWithSettings independentTrader = new TraderWithSettings(0, 0, TraderState.ACTIVE, EMPTY);
    private static final ShoppingList independentShoppingList = new ShoppingList(independentTrader, EMPTY);
    private static final @NonNull FunctionalOperator DUMMY_FUNCTION = FunctionalOperatorUtil.ADD_COMM;

    // TODO (Vaptistis): Eventually, all parameterized tests that share the same parameters can be
    // refactored in a single parameterized test, but until we implement copying of Economies the
    // tests that modify their arguments must be run in their own parameterized test classes.
    @Ignore // This class contains code common to all parameterized tests in EconomyTest. One gets
    @RunWith(Parameterized.class) // an error without these annotations...
    public static class CommonMembersOfParameterizedTests {
        // Fields needed by parameterized runner
        @Parameter(value = 0) public @NonNull Economy economy;
        @Parameter(value = 1) public @NonNull Basket @NonNull [] baskets;
        @Parameter(value = 2) public @NonNull Trader @NonNull [] traders;

        @Parameters(name = "Test #{index}")
        public static Collection<Object[]> generateEconomies() {
            final List<@NonNull Object @NonNull []> output = new ArrayList<>();
            final Trader @NonNull [] traders = new Trader[3]; // Up to 2 nodes plus 1 for null.
            final Basket[] baskets = {EMPTY,PM_SELL};

            // 0 nodes, 0 edges
            Economy economy = new Economy();
            output.add(new Object[]{economy,new Basket[]{},new Trader[]{}});

            // 1 node, 0 edges
            for (TraderState state : states) {
                economy = new Economy();
                traders[0] = economy.addTrader(0, state, EMPTY);
                output.add(new Object[]{economy,new Basket[]{},Arrays.copyOf(traders, 1)});
            }

            // 1 node, 1 edge (2 placements x 2 baskets sold x 2 baskets bought)
            for (TraderState state : states) {
                for (Basket basketSold : baskets) {
                    for (Basket basketBought1 : baskets) {
                        for (int dst1 = 0 ; dst1 < 2 ; ++dst1) {
                            economy = new Economy();
                            traders[0] = economy.addTrader(0, state, basketSold);
                            economy.addBasketBought(traders[0], basketBought1).move(traders[dst1]);

                            output.add(new Object[]{economy,new Basket[]{basketBought1},Arrays.copyOf(traders, 1)});
                        }
                    }
                }
            }

            // 1 node, 2 edges (2x2 placements x 2 baskets sold x 2x2 baskets bought)
            for (TraderState state : states) {
                for (Basket basketSold : baskets) {
                    for (Basket basketBought1 : baskets) {
                        for (Basket basketBought2 : baskets) {
                            for (int dst1 = 0 ; dst1 < 2 ; ++dst1) {
                                for (int dst2 = 0 ; dst2 < 2 ; ++dst2) {
                                    economy = new Economy();
                                    traders[0] = economy.addTrader(0, state, basketSold);
                                    economy.addBasketBought(traders[0], basketBought1).move(traders[dst1]);
                                    economy.addBasketBought(traders[0], basketBought2).move(traders[dst2]);

                                    output.add(new Object[]{economy, basketBought1 == basketBought2
                                                    ? new Basket[]{basketBought1} : baskets, Arrays.copyOf(traders, 1)});
                                }
                            }
                        }
                    }
                }
            }

            // 2 nodes, 0 edges (x2 baskets sold)
            for (TraderState state : states) {
                for (Basket basketSold : baskets) {
                    economy = new Economy();
                    traders[0] = economy.addTrader(0, state, EMPTY);
                    traders[1] = economy.addTrader(0, TraderState.ACTIVE, basketSold);
                    output.add(new Object[]{economy,new Basket[]{}, Arrays.copyOf(traders, 2)});
                }
            }

            // 2 nodes, 1 edge (6 placements x 2 baskets bought x 2 baskets sold)
            for (TraderState state : states) {
                for (Basket basketSold : baskets) {
                    for (Basket basketBought1 : baskets) {
                        for (int src1 = 0 ; src1 < 2 ; ++src1) {
                            for (int dst1 = 0 ; dst1 < 3 ; ++dst1) {
                                economy = new Economy();
                                traders[0] = economy.addTrader(0, state, EMPTY);
                                traders[1] = economy.addTrader(0, TraderState.ACTIVE, basketSold);
                                economy.addBasketBought(traders[src1], basketBought1).move(traders[dst1]);

                                output.add(new Object[]{economy,new Basket[]{basketBought1}, Arrays.copyOf(traders, 2)});
                            }
                        }
                    }
                }
            }

            // 2 nodes, 2 edges (6x6 placements x 2x2 baskets bought x 2 baskets sold)
            for (Basket basketSold : baskets) {
                for (Basket basketBought1 : baskets) {
                    for (Basket basketBought2 : baskets) {
                        for (int src1 = 0 ; src1 < 2 ; ++src1) {
                            for (int dst1 = 0 ; dst1 < 3 ; ++dst1) {
                                for (int src2 = 0 ; src2 < 2 ; ++src2) {
                                    for (int dst2 = 0 ; dst2 < 3 ; ++dst2) {
                                        economy = new Economy();
                                        traders[0] = economy.addTrader(0, TraderState.ACTIVE, EMPTY);
                                        traders[1] = economy.addTrader(0, TraderState.ACTIVE, basketSold);
                                        economy.addBasketBought(traders[src1], basketBought1).move(traders[dst1]);
                                        economy.addBasketBought(traders[src2], basketBought2).move(traders[dst2]);

                                        output.add(new Object[]{economy, basketBought1 == basketBought2
                                            ? new Basket[]{basketBought1} : baskets, Arrays.copyOf(traders, 2)});
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Handwritten topologies of larger size:

            // Economy 1
            economy = new Economy();
            Trader vm = economy.addTrader(0, TraderState.ACTIVE, new Basket(), PMtoVM, STtoVM, STtoVM);
            Trader pm = economy.addTrader(1, TraderState.ACTIVE, PMtoVM);
            Trader st1 = economy.addTrader(2, TraderState.ACTIVE, STtoVM);
            Trader st2 = economy.addTrader(2, TraderState.ACTIVE, STtoVM);
            ShoppingList[] shoppingLists = economy.getMarketsAsBuyer(vm).keySet().toArray(new ShoppingList[3]);
            shoppingLists[0].move(pm);
            shoppingLists[1].move(st1);
            shoppingLists[2].move(st2);
            pm.getCommoditySold(CPU).setCapacity(100);
            economy.getCommodityBought(shoppingLists[0],CPU).setQuantity(42);
            output.add(new Object[]{economy,new Basket[]{PMtoVM,STtoVM},new Trader[]{vm,pm,st1,st2}});

            // Economy 2
            economy = new Economy();
            vm = economy.addTrader(0, TraderState.ACTIVE, new Basket());
            pm = economy.addTrader(1, TraderState.ACTIVE, PMtoVM);
            st1 = economy.addTrader(2, TraderState.ACTIVE, STtoVM);
            st2 = economy.addTrader(2, TraderState.ACTIVE, STtoVM);
            economy.addBasketBought(vm, PMtoVM).move(pm);
            economy.addBasketBought(vm, STtoVM).move(st1);
            economy.addBasketBought(vm, STtoVM).move(st2);
            output.add(new Object[]{economy,new Basket[]{PMtoVM,STtoVM},new Trader[]{vm,pm,st1,st2}});

            // TODO (Vaptistis): add more economies of larger size
            return output;
        }
    } // end CommonMembersOfParameterizedTests class

    // Inner test classes

    public static class NonParameterizedTests {
        @Test
        public final void testEconomy() {
            UnmodifiableEconomy economy = new Economy();
            assertTrue(economy.getMarkets().isEmpty());
            assertTrue(economy.getTraders().isEmpty());
            assertNotNull(economy.getSettings());
        }

        @Test
        public void testResetMarketsPopulatedFlag () throws NoSuchFieldException, SecurityException,
                IllegalArgumentException, IllegalAccessException {
            Economy eco = new Economy();
            eco.populateMarketsWithSellersAndMergeConsumerCoverage();
            Field marketsPopulated = Economy.class.getDeclaredField("marketsPopulated");
            marketsPopulated.setAccessible(true);
            assertTrue((boolean)marketsPopulated.get(eco));
            eco.resetMarketsPopulatedFlag();
            assertFalse((boolean)marketsPopulated.get(eco));
        }

        @Test
        public void testGetPeerShoppingLists() {
            int[] config = {2, 4};
            Economy economy = new Economy();
            Trader[] traders = makeTraders(economy, config);
            // traders[0] is in sg-1 and traders[2] is in sg-2
            assertEquals(1, economy.getPeerShoppingLists(getSl(economy, traders[0])).size());
            assertEquals(3, economy.getPeerShoppingLists(getSl(economy, traders[2])).size());
        }
    } // end TestEconomy class

    public static class TraderAndPopulateTests {
        private final Economy economy = new Economy();
        private final Basket emptyBasket = new Basket();
        private final Basket cpuBasket = new Basket(new CommoditySpecification(0));
        private final Basket memBasket = new Basket(new CommoditySpecification(1));
        private final Trader cpuBuyer = economy.addTrader(0, TraderState.ACTIVE, emptyBasket, cpuBasket);
        private final Trader memBuyer = economy.addTrader(0, TraderState.ACTIVE, emptyBasket, memBasket);
        private Trader modelSeller;

        @Rule
        public ExpectedException expectedException = ExpectedException.none();

        @Before
        public void setup() {
            modelSeller = economy.addTrader(0, TraderState.ACTIVE, cpuBasket);
        }

        @Test
        public void testAddTraderByModelSellerSameBasketSold() {
            assertEquals(2, economy.getMarkets().size());
            economy.populateMarketsWithSellersAndMergeConsumerCoverage();
            assertEquals(1, getActiveSellerCountForBuyerMarkets(cpuBuyer));
            assertEquals(0, getActiveSellerCountForBuyerMarkets(memBuyer));

            economy.addTraderByModelSeller(modelSeller, TraderState.ACTIVE, cpuBasket,
                            modelSeller.getCliques());
            assertEquals(2, getActiveSellerCountForBuyerMarkets(cpuBuyer));
            assertEquals(0, getActiveSellerCountForBuyerMarkets(memBuyer));
        }

        @Test
        public void testAddTraderByModelSellerDifferentBasketSold() {
            economy.populateMarketsWithSellersAndMergeConsumerCoverage();
            assertEquals(1, getActiveSellerCountForBuyerMarkets(cpuBuyer));
            assertEquals(0, getActiveSellerCountForBuyerMarkets(memBuyer));

            economy.addTraderByModelSeller(modelSeller, TraderState.ACTIVE, memBasket,
                            modelSeller.getCliques());
            assertEquals(1, getActiveSellerCountForBuyerMarkets(cpuBuyer));
            assertEquals(1, getActiveSellerCountForBuyerMarkets(memBuyer));
        }

        @Test
        public void testPopulateMarketsWithSellersAndMergeConsumerCoverage() {
            assertEquals(0, getActiveSellerCountForBuyerMarkets(cpuBuyer));
            assertEquals(0, getActiveSellerCountForBuyerMarkets(memBuyer));

            economy.populateMarketsWithSellersAndMergeConsumerCoverage();
            assertEquals(1, getActiveSellerCountForBuyerMarkets(cpuBuyer));
            assertEquals(0, getActiveSellerCountForBuyerMarkets(memBuyer));
        }

        @Test
        public void testPopulateMarketsWithSellersThrowsOnMultipleCalls() {
            economy.populateMarketsWithSellersAndMergeConsumerCoverage();

            expectedException.expect(IllegalArgumentException.class);
            economy.populateMarketsWithSellersAndMergeConsumerCoverage();
        }

        private int getActiveSellerCountForBuyerMarkets(final Trader buyer) {
            return economy.getMarketsAsBuyer(buyer).values().iterator().next().getActiveSellers().size();
        }
    }

    @RunWith(Parameterized.class)
    public static class EconomyReadOnlyMethods extends CommonMembersOfParameterizedTests{
        @Test
        public final void testGetMarkets() {
            assertEquals(baskets.length, economy.getMarkets().size());
            CollectionTests.verifyUnmodifiableValidOperations(economy.getMarkets(), independentMarket);
            CollectionTests.verifyUnmodifiableInvalidOperations(economy.getMarkets(), independentMarket);

            int i = 0;
            for (@NonNull @ReadOnly Market market : economy.getMarkets()) {
                assertEquals(0, market.getBasket().compareTo(baskets[i++]));
            }
        }

        @Test
        public final void testGetMarket_Basket() {
            for (Basket basket : baskets) {
                assertEquals(0, economy.getMarket(basket).getBasket().compareTo(basket));
            }
        }

        @Test
        public final void testGetMarket_ShoppingList() {
            for (@NonNull @ReadOnly Market market : economy.getMarkets()) {
                for (@NonNull ShoppingList shoppingList : market.getBuyers()) {
                    assertSame(market, economy.getMarket(shoppingList));
                    assertEquals(market.getBasket().size(), shoppingList.getQuantities().length);
                    // Could have also tested getPeakQuantities here, but the property
                    // getQuantities().length == getPeakQuantities().length is tested in
                    // ShoppingListTest
                }
            }
        }

        @Test
        public final void testGetIndex_Trader() {
            for (int i = 0 ; i < economy.getTraders().size() ; ++i) {
                assertEquals(i, economy.getTraders().get(i).getEconomyIndex());
                // This also implicitly checks that all traders in economy are distinct.
            }
        }

        @Test
        public final void testGetCommoditiesBought_And_GetCommodityBought() {
            for (@NonNull @ReadOnly Market market : economy.getMarkets()) {
                for (@NonNull ShoppingList shoppingList : market.getBuyers()) {
                    final @NonNull @ReadOnly List<@NonNull CommodityBought> commoditiesBought =
                                    economy.getCommoditiesBought(shoppingList);
                    assertEquals(market.getBasket().size(), commoditiesBought.size());
                    for (int i = 0 ; i < commoditiesBought.size() ; ++i) {
                        final @NonNull CommodityBought commodityBought1 = commoditiesBought.get(i);
                        final @NonNull CommodityBought commodityBought2 =
                                        economy.getCommodityBought(shoppingList, market.getBasket().get(i));

                        // The API doesn't guarantee that you'll get the same commodity bought object
                        // each time you call getCommodityBought or that it will match one of the
                        // objects in getCommoditiesBought, but the objects must refer to the same
                        // quantity and peak quantity values.

                        // test quantity between commodities
                        double quantity = commodityBought1.getQuantity();
                        assertEquals(quantity, commodityBought2.getQuantity(), TestUtils.FLOATING_POINT_DELTA);
                        commodityBought1.setQuantity(quantity += 1.5);
                        assertEquals(quantity, commodityBought2.getQuantity(), TestUtils.FLOATING_POINT_DELTA);
                        commodityBought2.setQuantity(quantity += 1.5);
                        assertEquals(quantity, commodityBought1.getQuantity(), TestUtils.FLOATING_POINT_DELTA);

                        // resetting quantity. If qnty > peakQnty, we reset peakQnty to qnty
                        quantity=0;
                        commodityBought1.setQuantity(quantity);
                        commodityBought2.setQuantity(quantity);
                        // test peak quantity between commodities
                        double peakQuantity = commodityBought1.getPeakQuantity();
                        assertEquals(peakQuantity, commodityBought2.getPeakQuantity(), TestUtils.FLOATING_POINT_DELTA);
                        commodityBought1.setPeakQuantity(peakQuantity += 1.5);
                        assertEquals(peakQuantity, commodityBought2.getPeakQuantity(), TestUtils.FLOATING_POINT_DELTA);
                        commodityBought2.setPeakQuantity(peakQuantity += 1.5);
                        assertEquals(peakQuantity, commodityBought1.getPeakQuantity(), TestUtils.FLOATING_POINT_DELTA);

                        // test quantity between commodity and vector
                        quantity = commodityBought1.getQuantity();
                        assertEquals(quantity, shoppingList.getQuantities()[i], TestUtils.FLOATING_POINT_DELTA);
                        commodityBought1.setQuantity(quantity += 1.5);
                        assertEquals(quantity, shoppingList.getQuantities()[i], TestUtils.FLOATING_POINT_DELTA);
                        shoppingList.getQuantities()[i] = quantity += 1.5;
                        assertEquals(quantity, commodityBought1.getQuantity(), TestUtils.FLOATING_POINT_DELTA);

                        // test peak quantity between commodity and vector
                        peakQuantity = commodityBought1.getPeakQuantity();
                        assertEquals(peakQuantity, shoppingList.getPeakQuantities()[i], TestUtils.FLOATING_POINT_DELTA);
                        commodityBought1.setPeakQuantity(peakQuantity += 1.5);
                        assertEquals(peakQuantity, shoppingList.getPeakQuantities()[i], TestUtils.FLOATING_POINT_DELTA);
                        shoppingList.getPeakQuantities()[i] = quantity += 1.5;
                        assertEquals(quantity, commodityBought1.getPeakQuantity(), TestUtils.FLOATING_POINT_DELTA);
                    }
                }
            }
        }

        @Test
        public final void testGetTrader() {
            assertEquals(traders.length, economy.getTraders().size());
            ListTests.verifyUnmodifiableValidOperations(economy.getTraders(),independentTrader);
            ListTests.verifyUnmodifiableInvalidOperations(economy.getTraders(),independentTrader);

            int i = 0;
            for (@NonNull @ReadOnly Trader trader : economy.getTraders()) {
                assertSame(traders[i++], trader);
            }
        }

        @Test
        public final void testGetSuppliers_Trader() {
            for (Trader trader : traders) {
                ListTests.verifyUnmodifiableValidOperations(economy.getSuppliers(trader),independentTrader);
                ListTests.verifyUnmodifiableInvalidOperations(economy.getSuppliers(trader),independentTrader);
            }
        }

        @Test
        public final void testGetMarketsAsBuyer_Trader() {
            for (Trader trader : traders) {
                MapTests.verifyUnmodifiableValidOperations(economy.getMarketsAsBuyer(trader),
                                                           independentShoppingList,independentMarket);
                MapTests.verifyUnmodifiableInvalidOperations(economy.getMarketsAsBuyer(trader),
                                                             independentShoppingList,independentMarket);
            }
        }

        @Test
        public final void testGetMarketsAsSeller_Trader() {
            for (Trader trader : traders) {
                ListTests.verifyUnmodifiableValidOperations(economy.getMarketsAsSeller(trader),independentMarket);
                ListTests.verifyUnmodifiableInvalidOperations(economy.getMarketsAsSeller(trader),independentMarket);
            }
        }
    } // end EconomyReadOnlyMethods class

    @RunWith(Parameterized.class)
    public static class AddTrader extends CommonMembersOfParameterizedTests {
        @Test
        public final void testAddTrader() {
            for (int type : types) {
                for (TraderState state : states) {
                    for (Basket basketSold : basketsSold) {
                        for (Basket[] basketsBought : basketsBoughtLists) {
                            Trader trader = economy.addTrader(type, state, basketSold, basketsBought);
                            assertEquals(type, trader.getType());
                            assertSame(state, trader.getState());
                            assertSame(basketSold, trader.getBasketSold());
                            assertEquals(basketSold.size(), trader.getCommoditiesSold().size());

                            assertEquals(basketsBought.length, economy.getMarketsAsBuyer(trader).size());
                            assertArrayEquals(basketsBought, economy.getMarketsAsBuyer(trader).values()
                                .stream().map(Market::getBasket).toArray());
                        }
                    }
                }
            }
        }
    } // end AddTrader class

    @RunWith(Parameterized.class)
    public static class RemoveTrader extends CommonMembersOfParameterizedTests {
        @Test
        public final void testRemoveTrader() {
            try {
                economy.removeTrader(independentTrader);
                fail();
            } catch(IllegalArgumentException e) {
                // ignore
            }
            for (Trader trader : traders) {
                assertSame(economy, economy.removeTrader(trader));
                assertFalse(economy.getTraders().contains(trader));
                for (@NonNull @ReadOnly Market market : economy.getMarkets()) {
                    assertFalse(market.getActiveSellers().contains(trader));
                    assertEquals(0, market.getBuyers().stream().filter(p->p.getBuyer() == trader).count());
                }
            }
            assertTrue(economy.getTraders().isEmpty());
        }
    } // end RemoveTrader class

    @RunWith(Parameterized.class)
    public static class AddBasketBought extends CommonMembersOfParameterizedTests {
        @Test
        public final void testAddBasketBought() {
            final Basket[] basketsBought = {EMPTY,PM_4CORE};
            // TODO (Vaptistis): have a separate list of baskets that are not in the Economy yet.

            for (Trader trader : traders) {
                for (@NonNull Basket basket : basketsBought) {
                    ShoppingList shoppingList = economy.addBasketBought(trader, basket);
                    assertSame(trader, shoppingList.getBuyer());
                    assertNull(shoppingList.getSupplier());
                    assertEquals(shoppingList.getBuyer().getState().isActive(),
                                 economy.getMarket(shoppingList).getBuyers().contains(shoppingList));
                }
            }
        }
    } // end AddBasketBought class

    @RunWith(Parameterized.class)
    public static class RemoveBasketBought extends CommonMembersOfParameterizedTests {
        @Test
        public final void testRemoveBasketBought() {
            for (@NonNull @ReadOnly Market market : economy.getMarkets()) {
                for (@NonNull ShoppingList shoppingList : new ArrayList<>(market.getBuyers())) {
                    Basket removedBasket = economy.removeBasketBought(shoppingList);
                    assertEquals(0, market.getBasket().compareTo(removedBasket));
                    assertFalse(market.getBuyers().contains(shoppingList));
                }
                assertTrue(market.getBuyers().isEmpty());
            }
        }
    } // end RemoveBasketBought class

    @RunWith(Parameterized.class)
    public static class AddCommodityBought extends CommonMembersOfParameterizedTests {
        @Test
        public final void testAddCommodityBought() {
            final CommoditySpecification[] specifications = {CLUSTER_A,SEGMENT_1,CPU};
            double quantity = 45;
            double peakQuantity = 45;

            for (@NonNull @ReadOnly Market market : new ArrayList<>(economy.getMarkets())) {
                for (@NonNull ShoppingList shoppingList : new ArrayList<>(market.getBuyers())) {
                    ShoppingList newShoppingList = shoppingList;

                    for (CommoditySpecification specification : specifications) {
                        for (CommodityBought commodityBought : economy.getCommoditiesBought(newShoppingList)) {
                            commodityBought.setQuantity(quantity);
                            commodityBought.setPeakQuantity(peakQuantity);
                        }

                        newShoppingList = economy.addCommodityBought(newShoppingList, specification);
                        assertTrue(economy.getMarket(newShoppingList).getBasket().contains(specification));

                        CommodityBought addedCommodity = economy.getCommodityBought(newShoppingList, specification);
                        addedCommodity.setQuantity(quantity);
                        addedCommodity.setPeakQuantity(peakQuantity);

                        for (CommodityBought commodityBought : economy.getCommoditiesBought(newShoppingList)) {
                            assertEquals(quantity, commodityBought.getQuantity(), TestUtils.FLOATING_POINT_DELTA);
                            assertEquals(peakQuantity, commodityBought.getPeakQuantity(), TestUtils.FLOATING_POINT_DELTA);
                        }

                        quantity += 1.1;
                        peakQuantity += 1.2;
                    }
                }
            }
        }
    } // end AddCommodityBought class

    @RunWith(Parameterized.class)
    public static class RemoveCommodityBought extends CommonMembersOfParameterizedTests {
        @Test
        public final void testRemoveCommodityBought() {
            final CommoditySpecification[] specifications = {CLUSTER_A, CPU_1, CPU_2, CPU_4, CPU_8, MEM};
            double quantity = 45;
            double peakQuantity = 45;

            for (@NonNull @ReadOnly Market market : new ArrayList<>(economy.getMarkets())) {
                for (@NonNull ShoppingList shoppingList : new ArrayList<>(market.getBuyers())) {
                    ShoppingList newShoppingList = shoppingList;

                    for (CommoditySpecification specification : specifications) {
                        for (CommodityBought commodityBought : economy.getCommoditiesBought(newShoppingList)) {
                            commodityBought.setQuantity(quantity);
                            commodityBought.setPeakQuantity(peakQuantity);
                        }

                        newShoppingList = economy.removeCommodityBought(newShoppingList, specification);
                        assertFalse(economy.getMarket(newShoppingList).getBasket().contains(specification));

                        for (CommodityBought commodityBought : economy.getCommoditiesBought(newShoppingList)) {
                            assertEquals(quantity, commodityBought.getQuantity(), TestUtils.FLOATING_POINT_DELTA);
                            assertEquals(peakQuantity, commodityBought.getPeakQuantity(), TestUtils.FLOATING_POINT_DELTA);
                        }

                        quantity += 1.1;
                        peakQuantity += 1.2;
                    }
                }
            }
        }
    } // end RemoveCommodityBought class

    @RunWith(Parameterized.class)
    public static class Clear extends CommonMembersOfParameterizedTests {
        @Test
        public final void testClear() {
            economy.clear();
            assertTrue(economy.getTraders().isEmpty());
            assertTrue(economy.getMarkets().isEmpty());
            // TODO: compare with newly constructed object when we implement equals.
        }
    } // end Clear class

    /*
     * Support routines for scaling group tests
     */
    private static ShoppingList getSl(Economy economy, Trader trader) {
        // Return the first (and only) ShoppingList for buyer
        return economy.getMarketsAsBuyer(trader).keySet().iterator().next();
    }

    private static Trader makeTrader(Economy economy, int id, String groupName, int groupFactor) {
        Trader trader = economy.addTrader(VM_TYPE, TraderState.ACTIVE, EMPTY);
        trader.setDebugInfoNeverUseInCode("buyer-" + (id + 1));
        trader.setScalingGroupId(groupName);
        trader.getSettings().setQuoteFactor(0.999).setMoveCostFactor(0);
        ShoppingList shoppingList = economy.addBasketBought(trader, PM_ANY).setMovable(true);
        shoppingList.setGroupFactor(groupFactor);
        economy.registerShoppingListWithScalingGroup(groupName, shoppingList);
        return trader;
    }

    /**
     * Create a group of Traders in a scaling group.
     * @param economy the economy
     * @param baseId base trader ID
     * @param numTraders number of Traders
     * @param groupNumber scaling group number, for naming purposes
     * @return list of Traders
     */
    private static List<Trader> makeGroupOfTraders(Economy economy, int baseId,
                                            int numTraders, int groupNumber) {
        String groupName = "sg=" + groupNumber;
        return IntStream.range(0, numTraders)
            .mapToObj(tn -> makeTrader(economy, baseId + tn, groupName, numTraders))
            .collect(Collectors.toList());
    }

    /**
     * Create some traders in some number of different scaling groups.
     * @param economy economy
     * @param traderList list of traders.  Each element of the list will represent the number of
     *                  traders in each scaling group.  Each trader is named "trader-n" and each
     *                   scaling group is named "sg-n".
     */
    private static Trader[] makeTraders(Economy economy, int[] traderList) {
        int groupNum = 1;
        int baseId = 0;
        List<Trader> traders = new ArrayList<>();
        for (int groupSize : traderList) {
            traders.addAll(makeGroupOfTraders(economy, baseId, groupSize, groupNum++));
            baseId += groupSize;
        }
        return traders.toArray(new Trader[0]);
    }
} // end EconomyTest class
