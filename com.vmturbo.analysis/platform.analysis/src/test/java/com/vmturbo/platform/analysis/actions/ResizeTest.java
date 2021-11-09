package com.vmturbo.platform.analysis.actions;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;
import com.vmturbo.platform.analysis.topology.LegacyTopology;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

/**
 * A test case for the {@link Resize} class.
 */
@RunWith(JUnitParamsRunner.class)
public class ResizeTest {
    // Fields
    private static final Basket EMPTY = new Basket();
    private static final double[] validCapacities = {0.0,0.1,1.5};
    private static final double[] invalidCapacities = {-0.1,-1.5};
    private static final CommoditySpecification[] specifications = {
        new CommoditySpecification(0), new CommoditySpecification(2)
    };

    // Methods

    @Test
    @Parameters(method = "parametersForTestResize_NormalInput")
    @TestCaseName("Test #{index}: new Resize({0},{1},{2},{3})")
    public final void testResize_4_NormalInput(@NonNull Economy economy,
            @NonNull Trader sellingTrader, @NonNull CommoditySpecification resizedCommodity,
            double oldCapacity, double newCapacity) {
        @NonNull Resize resize = new Resize(economy, sellingTrader, resizedCommodity,
                                            oldCapacity, newCapacity);

        assertSame(sellingTrader, resize.getSellingTrader());
        assertSame(resizedCommodity, resize.getResizedCommoditySpec());
        assertEquals(oldCapacity, resize.getOldCapacity(), TestUtils.FLOATING_POINT_DELTA);
        assertEquals(newCapacity, resize.getNewCapacity(), TestUtils.FLOATING_POINT_DELTA);
    }

    @Test
    @Parameters(method = "parametersForTestResize_NormalInput")
    @TestCaseName("Test #{index}: new Resize({0},{1},{3}) with old capacity = {2}")
    public final void testResize_3_NormalInput(@NonNull Economy economy,
            @NonNull Trader sellingTrader, @NonNull CommoditySpecification resizedCommodity,
            double oldCapacity, double newCapacity) {
        sellingTrader.getCommoditySold(resizedCommodity).setCapacity(oldCapacity);

        @NonNull Resize resize = new Resize(economy, sellingTrader, resizedCommodity, newCapacity);

        assertSame(sellingTrader, resize.getSellingTrader());
        assertSame(resizedCommodity, resize.getResizedCommoditySpec());
        assertEquals(oldCapacity, resize.getOldCapacity(), TestUtils.FLOATING_POINT_DELTA);
        assertEquals(newCapacity, resize.getNewCapacity(), TestUtils.FLOATING_POINT_DELTA);
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestResize_NormalInput() {
        List<Object[]> testCases = new ArrayList<>();
        @NonNull Economy economy = new Economy();

        for (TraderState state : TraderState.values()) {
            for (int size = 0 ; size < specifications.length ; ++size) {
                @NonNull Basket basketSold = new Basket(Arrays.copyOf(specifications, size));
                @NonNull Trader trader = economy.addTrader(0, state, basketSold);

                for (CommoditySpecification specification : basketSold) {
                    for (double oldCapacity : validCapacities) {
                        for (double newCapacity : validCapacities) {
                            testCases.add(new Object[]{economy,trader,specification,oldCapacity,newCapacity});
                        }
                    }
                }
            }
        }

        return testCases.toArray();
    }

    @Test(expected = IllegalArgumentException.class)
    @Parameters(method = "parametersForTestResize_InvalidInput")
    @TestCaseName("Test #{index}: new Resize({0},{1},{2},{3})")
    public final void testResize_4_InvalidInput(@NonNull Economy economy, @NonNull Trader sellingTrader,
            @NonNull CommoditySpecification resizedCommodity, double oldCapacity, double newCapacity) {
        new Resize(economy, sellingTrader, resizedCommodity, oldCapacity, newCapacity);
    }

    @Test(expected = RuntimeException.class) // It should be either IllegalArgumentException or NullPointerException.
    @Parameters(method = "parametersForTestResize_InvalidInput")
    @TestCaseName("Test #{index}: new Resize({0},{1},{3}) with old capacity = {2}")
    public final void testResize_3_InvalidInput(@NonNull Economy economy, @NonNull Trader sellingTrader,
            @NonNull CommoditySpecification resizedCommodity, double oldCapacity, double newCapacity) {
        sellingTrader.getCommoditySold(resizedCommodity).setCapacity(oldCapacity);
        new Resize(economy, sellingTrader, resizedCommodity, newCapacity);
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestResize_InvalidInput() {
        List<Object[]> testCases = new ArrayList<>();
        @NonNull Economy economy = new Economy();

        for (TraderState state : TraderState.values()) {
            for (int size = 0 ; size < specifications.length ; ++size) {
                @NonNull Basket basketSold = new Basket(Arrays.copyOf(specifications, size));
                @NonNull Trader trader = economy.addTrader(0, state, basketSold);

                for (int invalidSpec = size ; invalidSpec < specifications.length ; ++invalidSpec) {
                    for (double oldCapacity : validCapacities) {
                        for (double newCapacity : validCapacities) {
                            testCases.add(new Object[]{economy,trader,specifications[invalidSpec],oldCapacity,newCapacity});
                        }
                    }
                }
            }
        }

        for (TraderState state : TraderState.values()) {
            for (int size = 0 ; size < specifications.length ; ++size) {
                @NonNull Basket basketSold = new Basket(Arrays.copyOf(specifications, size));
                @NonNull Trader trader = economy.addTrader(0, state, basketSold);

                for (CommoditySpecification specification : basketSold) {
                    for (double oldCapacity : invalidCapacities) {
                        for (double newCapacity : validCapacities) {
                            testCases.add(new Object[]{economy,trader,specification,oldCapacity,newCapacity});
                        }
                    }
                }
            }
        }

        for (TraderState state : TraderState.values()) {
            for (int size = 0 ; size < specifications.length ; ++size) {
                @NonNull Basket basketSold = new Basket(Arrays.copyOf(specifications, size));
                @NonNull Trader trader = economy.addTrader(0, state, basketSold);

                for (CommoditySpecification specification : basketSold) {
                    for (double oldCapacity : validCapacities) {
                        for (double newCapacity : invalidCapacities) {
                            testCases.add(new Object[]{economy,trader,specification,oldCapacity,newCapacity});
                        }
                    }
                }
            }
        }

        return testCases.toArray();
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: {0}.serialize({1}) == {2}")
    public final void testSerialize(@NonNull Resize resize,
            @NonNull Function<@NonNull Trader, @NonNull String> oid, @NonNull String serialized) {
        assertEquals(resize.serialize(oid), serialized);
    }

    // TODO (Vaptistis): add more tests once semantics are clear.
    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestSerialize() {
        @NonNull Map<@NonNull Trader, @NonNull String> oids = new HashMap<>();
        @NonNull Function<@NonNull Trader, @NonNull String> oid = oids::get;

        Economy e1 = new Economy();
        Trader t1 = e1.addTrader(0, TraderState.ACTIVE, new Basket(specifications), EMPTY);
        Trader t2 = e1.addTrader(0, TraderState.INACTIVE, new Basket(specifications), EMPTY);

        oids.put(t1, "id1");
        oids.put(t2, "id2");

        return new Object[][]{
            {new Resize(e1, t1, specifications[0], 10, 15),oid,"<action type=\"resize\" sellingTrader=\"id1\" "
                + "commoditySpecification=\"<0>\" oldCapacity=\"10.0\" newCapacity=\"15.0\" />"},
            {new Resize(e1, t2, specifications[1], 0, 1),oid,"<action type=\"resize\" sellingTrader=\"id2\" "
                + "commoditySpecification=\"<2>\" oldCapacity=\"0.0\" newCapacity=\"1.0\" />"}
        };
    }

    @Test
    @Parameters(method = "parametersForTestResize_NormalInput")
    @TestCaseName("Test #{index}: new Resize({0},{1},{2},{3}).take().rollback()")
    public final void testTakeRollback(@NonNull Economy economy, @NonNull Trader sellingTrader,
            @NonNull CommoditySpecification resizedCommodity, double oldCapacity, double newCapacity) {
        @NonNull Resize resize = new Resize(economy, sellingTrader, resizedCommodity, oldCapacity, newCapacity);

        assertSame(resize, resize.take());
        assertEquals(newCapacity, sellingTrader.getCommoditySold(resizedCommodity).getCapacity(), TestUtils.FLOATING_POINT_DELTA);

        assertSame(resize, resize.rollback());
        assertEquals(oldCapacity, sellingTrader.getCommoditySold(resizedCommodity).getCapacity(), TestUtils.FLOATING_POINT_DELTA);
        // TODO: test that sellingTrader is otherwise left unchanged by both operations.
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: {0}.debugDescription({1}) == {2}")
    public final void testDebugDescription(@NonNull Resize resize, @NonNull LegacyTopology topology,
                                           @NonNull String description) {
        assertEquals(description, resize.debugDescription(topology.getUuids()::get,
            topology.getNames()::get, topology.getCommodityTypes()::getName, topology.getTraderTypes()::getName));
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestDebugDescription() {
        @NonNull LegacyTopology topology1 = new LegacyTopology();
        @NonNull Economy e1 = (Economy) topology1.getEconomy();

        Trader t1 = topology1.addTrader("id1","VM1","VM",TraderState.ACTIVE, Arrays.asList("VCPU"));
        CommoditySpecification cs1 = new CommoditySpecification(topology1.getCommodityTypes().getId("VCPU"));
        t1.getCommoditySold(cs1).setCapacity(2);
        Trader t2 = topology1.addTrader("id2","Host2","PM",TraderState.INACTIVE, Arrays.asList("CPU","MEM"));
        CommoditySpecification cs2 = new CommoditySpecification(topology1.getCommodityTypes().getId("MEM"));
        t2.getCommoditySold(cs2).setCapacity(100);

        return new Object[][]{
            {new Resize(e1,t1,cs1,5),topology1,
                "Resize VCPU of VM1 [id1] (#0) up from 2.0 to 5.0."},
            {new Resize(e1,t1,cs1,1),topology1,
                "Resize VCPU of VM1 [id1] (#0) down from 2.0 to 1.0."},
            {new Resize(e1,t2,cs2,200),topology1,
                "Resize MEM of Host2 [id2] (#1) up from 100.0 to 200.0."},
            {new Resize(e1,t2,cs2,50),topology1,
                "Resize MEM of Host2 [id2] (#1) down from 100.0 to 50.0."},
            // TODO: update test when we figure out how to get correct type!
        };
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: {0}.debugReason({1}) == {2}")
    public final void testDebugReason(@NonNull Resize resize, @NonNull LegacyTopology topology,
                                      @NonNull String reason) {
        assertEquals(reason, resize.debugReason(topology.getUuids()::get,
            topology.getNames()::get, topology.getCommodityTypes()::getName, topology.getTraderTypes()::getName));
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestDebugReason() {
        @NonNull LegacyTopology topology1 = new LegacyTopology();
        @NonNull Economy e1 = (Economy) topology1.getEconomy();

        Trader t1 = topology1.addTrader("id1","VM1","VM",TraderState.ACTIVE, Arrays.asList("VCPU"));
        CommoditySpecification cs1 = new CommoditySpecification(topology1.getCommodityTypes().getId("VCPU"));
        t1.getCommoditySold(cs1).setCapacity(2);
        Trader t2 = topology1.addTrader("id2","Host2","PM",TraderState.INACTIVE, Arrays.asList("CPU","MEM"));
        CommoditySpecification cs2 = new CommoditySpecification(topology1.getCommodityTypes().getId("MEM"));
        t2.getCommoditySold(cs2).setCapacity(100);

        return new Object[][]{
            {new Resize(e1,t1,cs1,5),topology1, "To ensure performance."},
            {new Resize(e1,t1,cs1,1),topology1, "To improve efficiency."},
            {new Resize(e1,t2,cs2,200),topology1, "To ensure performance."},
            {new Resize(e1,t2,cs2,50),topology1, "To improve efficiency."},
            // TODO: update test when we figure out how to get correct type!
        };
    }

    @SuppressWarnings("unused")
    private static Object[] parametersForTestEquals_and_HashCode() {
        Economy e = new Economy();
        Basket b1 = new Basket(new CommoditySpecification(100));
        Basket b2 = new Basket(new CommoditySpecification(200));
        Basket b3 = new Basket(new CommoditySpecification(300));
        Trader t1 = e.addTrader(0, TraderState.ACTIVE, b1, b2);
        Trader t2 = e.addTrader(0, TraderState.ACTIVE, b1, b2);
        Trader t3 = e.addTrader(0, TraderState.ACTIVE, b2, b3);
        Trader t4 = e.addTrader(0, TraderState.ACTIVE, b2, b3);

        ShoppingList shop1 = e.addBasketBought(t1, b2);
        shop1.move(t3);
        ShoppingList shop2 = e.addBasketBought(t2, b2);
        shop2.move(t3);

        Resize resize1 = new Resize(e, t3, new CommoditySpecification(200), 200);
        Resize resize2 = new Resize(e, t3, new CommoditySpecification(200), 200);
        Resize resize3 = new Resize(e, t1, new CommoditySpecification(100), 200);
        return new Object[][] {{resize1, resize2, true}, {resize1, resize3, false}};
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: equals and hashCode for {0}, {1} == {2}")
    public final void testEquals_and_HashCode(@NonNull Resize resize1, @NonNull Resize resize2,
                    boolean expect) {
        assertEquals(expect, resize1.equals(resize2));
        assertEquals(expect, resize1.hashCode() == resize2.hashCode());
    }

    @SuppressWarnings("unused")
    private static Object[] parametersForTestResize_Combine() {
        Economy e = new Economy();
        Basket b1 = new Basket(new CommoditySpecification(100));
        Trader t1 = e.addTrader(0, TraderState.ACTIVE, b1);

        Resize resize1 = new Resize(e, t1, new CommoditySpecification(100), 200);
        return new Object[] {e, t1, resize1};
    }

    @Test
    @Parameters(method = "parametersForTestResize_Combine")
    @TestCaseName("Test #{index}: Resize Combine for {0}, {1}, {2}")
    public final void testResize_Combine(@NonNull Economy economy,
        @NonNull Trader sellingTrader, @NonNull Resize resize) {
        sellingTrader.getSettings().setResizeThroughSupplier(true);
        Trader provider1 = economy.addTrader(1, TraderState.ACTIVE, new Basket(specifications));
        Trader provider2 = economy.addTrader(1, TraderState.ACTIVE, new Basket(specifications));
        resize.getResizeTriggerTraders().put(provider1, new HashSet<>(specifications[0].getBaseType()));

        @NonNull Resize resize2 = new Resize(economy, sellingTrader, resize.getResizedCommoditySpec(), resize.getNewCapacity() + 100);
        resize2.getResizeTriggerTraders().put(provider2, new HashSet<>(specifications[1].getBaseType()));

        Resize combined = (Resize)resize.combine(resize2);

        assertSame(sellingTrader, combined.getSellingTrader());
        assertTrue(combined.getOldCapacity() == resize.getOldCapacity());
        assertTrue(combined.getNewCapacity() == resize2.getNewCapacity());
        assertTrue(combined.getResizeTriggerTraders().size() == 2);
    }
} // end ResizeTest class
