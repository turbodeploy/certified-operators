package com.vmturbo.platform.analysis.actions;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
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
    public final void testResize_4_NormalInput(@NonNull Trader sellingTrader,
            @NonNull CommoditySpecification resizedCommodity, double oldCapacity, double newCapacity) {
        @NonNull Resize resize = new Resize(sellingTrader, resizedCommodity, oldCapacity, newCapacity);

        assertSame(sellingTrader, resize.getSellingTrader());
        assertSame(resizedCommodity, resize.getResizedCommodity());
        assertEquals(oldCapacity, resize.getOldCapacity(), 0f);
        assertEquals(newCapacity, resize.getNewCapacity(), 0f);
    }

    @Test
    @Parameters(method = "parametersForTestResize_NormalInput")
    @TestCaseName("Test #{index}: new Resize({0},{1},{3}) with old capacity = {2}")
    public final void testResize_3_NormalInput(@NonNull Trader sellingTrader,
            @NonNull CommoditySpecification resizedCommodity, double oldCapacity, double newCapacity) {
        sellingTrader.getCommoditySold(resizedCommodity).setCapacity(oldCapacity);

        @NonNull Resize resize = new Resize(sellingTrader, resizedCommodity, newCapacity);

        assertSame(sellingTrader, resize.getSellingTrader());
        assertSame(resizedCommodity, resize.getResizedCommodity());
        assertEquals(oldCapacity, resize.getOldCapacity(), 0f);
        assertEquals(newCapacity, resize.getNewCapacity(), 0f);
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
                            testCases.add(new Object[]{trader,specification,oldCapacity,newCapacity});
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
    public final void testResize_4_InvalidInput(@NonNull Trader sellingTrader,
            @NonNull CommoditySpecification resizedCommodity, double oldCapacity, double newCapacity) {
        new Resize(sellingTrader, resizedCommodity, oldCapacity, newCapacity);
    }

    @Test(expected = RuntimeException.class) // It should be either IllegalArgumentException or NullPointerException.
    @Parameters(method = "parametersForTestResize_InvalidInput")
    @TestCaseName("Test #{index}: new Resize({0},{1},{3}) with old capacity = {2}")
    public final void testResize_3_InvalidInput(@NonNull Trader sellingTrader,
            @NonNull CommoditySpecification resizedCommodity, double oldCapacity, double newCapacity) {
        sellingTrader.getCommoditySold(resizedCommodity).setCapacity(oldCapacity);
        new Resize(sellingTrader, resizedCommodity, newCapacity);
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
                            testCases.add(new Object[]{trader,specifications[invalidSpec],oldCapacity,newCapacity});
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
                            testCases.add(new Object[]{trader,specification,oldCapacity,newCapacity});
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
                            testCases.add(new Object[]{trader,specification,oldCapacity,newCapacity});
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
            {new Resize(t1, specifications[0], 10, 15),oid,"<action type=\"resize\" sellingTrader=\"id1\" "
                + "commoditySpecification=\"<0, 0, MAX_VALUE>\" oldCapacity=\"10.0\" newCapacity=\"15.0\" />"},
            {new Resize(t2, specifications[1], 0, 1),oid,"<action type=\"resize\" sellingTrader=\"id2\" "
                + "commoditySpecification=\"<2, 0, MAX_VALUE>\" oldCapacity=\"0.0\" newCapacity=\"1.0\" />"}
        };
    }

    @Test
    @Parameters(method = "parametersForTestResize_NormalInput")
    @TestCaseName("Test #{index}: new Resize({0},{1},{2},{3}).take().rollback()")
    public final void testTakeRollback(@NonNull Trader sellingTrader,
            @NonNull CommoditySpecification resizedCommodity, double oldCapacity, double newCapacity) {
        @NonNull Resize resize = new Resize(sellingTrader, resizedCommodity, oldCapacity, newCapacity);

        assertSame(resize, resize.take());
        assertEquals(newCapacity, sellingTrader.getCommoditySold(resizedCommodity).getCapacity(), 0f);

        assertSame(resize, resize.rollback());
        assertEquals(oldCapacity, sellingTrader.getCommoditySold(resizedCommodity).getCapacity(), 0f);
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

        Trader t1 = topology1.addTrader("id1","VM1","VM",TraderState.ACTIVE, Arrays.asList("VCPU"));
        CommoditySpecification cs1 = new CommoditySpecification(topology1.getCommodityTypes().getId("VCPU"));
        t1.getCommoditySold(cs1).setCapacity(2);
        Trader t2 = topology1.addTrader("id2","Host2","PM",TraderState.INACTIVE, Arrays.asList("CPU","MEM"));
        CommoditySpecification cs2 = new CommoditySpecification(topology1.getCommodityTypes().getId("MEM"));
        t2.getCommoditySold(cs2).setCapacity(100);

        return new Object[][]{
            {new Resize(t1,cs1,5),topology1,
                "Resize VCPU of VM1 [id1] (#0) up from 2.0 to 5.0."},
            {new Resize(t1,cs1,1),topology1,
                "Resize VCPU of VM1 [id1] (#0) down from 2.0 to 1.0."},
            {new Resize(t2,cs2,200),topology1,
                "Resize MEM of Host2 [id2] (#1) up from 100.0 to 200.0."},
            {new Resize(t2,cs2,50),topology1,
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

        Trader t1 = topology1.addTrader("id1","VM1","VM",TraderState.ACTIVE, Arrays.asList("VCPU"));
        CommoditySpecification cs1 = new CommoditySpecification(topology1.getCommodityTypes().getId("VCPU"));
        t1.getCommoditySold(cs1).setCapacity(2);
        Trader t2 = topology1.addTrader("id2","Host2","PM",TraderState.INACTIVE, Arrays.asList("CPU","MEM"));
        CommoditySpecification cs2 = new CommoditySpecification(topology1.getCommodityTypes().getId("MEM"));
        t2.getCommoditySold(cs2).setCapacity(100);

        return new Object[][]{
            {new Resize(t1,cs1,5),topology1, "To ensure performance."},
            {new Resize(t1,cs1,1),topology1, "To improve efficiency."},
            {new Resize(t2,cs2,200),topology1, "To ensure performance."},
            {new Resize(t2,cs2,50),topology1, "To improve efficiency."},
            // TODO: update test when we figure out how to get correct type!
        };
    }

} // end ResizeTest class
