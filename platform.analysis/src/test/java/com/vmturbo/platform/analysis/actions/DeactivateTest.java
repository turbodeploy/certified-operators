package com.vmturbo.platform.analysis.actions;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.topology.LegacyTopology;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

/**
 * A test case for the {@link Deactivate} class.
 */
@RunWith(JUnitParamsRunner.class)
public class DeactivateTest {
    // Fields
    private static final Basket EMPTY = new Basket();

    // Methods

    @Test
    @Parameters
    @TestCaseName("Test #{index}: new Deactivate({0},{1},{3})")
    public final void testDeactivate(@NonNull Trader target, @NonNull Market sourceMarket, boolean unusedFlag) {
        @NonNull Deactivate deactivation = new Deactivate(target, sourceMarket);

        assertSame(target, deactivation.getTarget());
        assertSame(sourceMarket, deactivation.getSourceMarket());
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestDeactivate() {
        Economy e1 = new Economy();
        Trader t1 = e1.addTrader(0, TraderState.ACTIVE, EMPTY, EMPTY);
        Trader t2 = e1.addTrader(0, TraderState.INACTIVE, EMPTY, EMPTY);

        return new Object[][]{{t1,e1.getMarket(EMPTY),true},{t2,e1.getMarket(EMPTY),false}};
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: {0}.serialize({1}) == {2}")
    public final void testSerialize(@NonNull Deactivate deactivation,
            @NonNull Function<@NonNull Trader, @NonNull String> oid, @NonNull String serialized) {
        assertEquals(deactivation.serialize(oid), serialized);
    }

    // TODO (Vaptistis): add more tests once semantics are clear.
    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestSerialize() {
        @NonNull Map<@NonNull Trader, @NonNull String> oids = new HashMap<>();
        @NonNull Function<@NonNull Trader, @NonNull String> oid = oids::get;

        Economy e1 = new Economy();
        Trader t1 = e1.addTrader(0, TraderState.ACTIVE, EMPTY, EMPTY);
        Trader t2 = e1.addTrader(0, TraderState.INACTIVE, EMPTY, EMPTY);

        oids.put(t1, "id1");
        oids.put(t2, "id2");

        return new Object[][]{
            {new Deactivate(t1, e1.getMarket(EMPTY)),oid,"<action type=\"deactivate\" target=\"id1\" />"},
            {new Deactivate(t2, e1.getMarket(EMPTY)),oid,"<action type=\"deactivate\" target=\"id2\" />"}
        };
    }

    @Test
    @Parameters(method = "parametersForTestDeactivate")
    @TestCaseName("Test #{index}: new Deactivate({0},{1}).take() throw == {3}")
    public final void testTake(@NonNull Trader target, @NonNull Market sourceMarket, boolean valid) {
        @NonNull Deactivate deactivation = new Deactivate(target,sourceMarket);

        try {
            deactivation.take();
            assertTrue(valid);
        } catch (IllegalArgumentException e){
            assertFalse(valid);
        }
        assertFalse(target.getState().isActive());
    }

    @Test
    @Parameters(method = "parametersForTestDeactivate")
    @TestCaseName("Test #{index}: new Deactivate({0},{1}).rollback() throw == {3}")
    public final void testRollback(@NonNull Trader target, @NonNull Market sourceMarket, boolean invalid) {
        @NonNull Deactivate deactivation = new Deactivate(target,sourceMarket);
        // TODO: normally, we should take the action before rolling back...
        try {
            deactivation.rollback();
            assertFalse(invalid);
        } catch (IllegalArgumentException e){
            assertTrue(invalid);
        }
        assertTrue(target.getState().isActive());
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: {0}.debugDescription({1}) == {2}")
    public final void testDebugDescription(@NonNull Deactivate deactivation, @NonNull LegacyTopology topology,
                                           @NonNull String description) {
        assertEquals(description, deactivation.debugDescription(topology.getUuids()::get,
            topology.getNames()::get, topology.getCommodityTypes()::getName, topology.getTraderTypes()::getName));
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestDebugDescription() {
        @NonNull LegacyTopology topology1 = new LegacyTopology();

        Trader t1 = topology1.addTrader("id1","name1","type1",TraderState.ACTIVE, Arrays.asList());
        topology1.addBasketBought(t1, Arrays.asList());
        Trader t2 = topology1.addTrader("id2","name2","type2",TraderState.INACTIVE, Arrays.asList());
        topology1.addBasketBought(t2, Arrays.asList("a"));

        return new Object[][]{
            {new Deactivate(t1, topology1.getEconomy().getMarket(EMPTY)),topology1,"Deactivate name1 [id1] (#0)."},
            {new Deactivate(t2, topology1.getEconomy().getMarket(EMPTY)),topology1,"Deactivate name2 [id2] (#1)."},
        };
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: {0}.debugReason({1}) == {2}")
    public final void testDebugReason(@NonNull Deactivate deactivation, @NonNull LegacyTopology topology,
                                      @NonNull String reason) {
        assertEquals(reason, deactivation.debugReason(topology.getUuids()::get,
            topology.getNames()::get, topology.getCommodityTypes()::getName, topology.getTraderTypes()::getName));
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestDebugReason() {
        @NonNull LegacyTopology topology1 = new LegacyTopology();

        Trader t1 = topology1.addTrader("id1","name1","type1",TraderState.ACTIVE, Arrays.asList());
        topology1.addBasketBought(t1, Arrays.asList());
        Trader t2 = topology1.addTrader("id2","name2","type2",TraderState.INACTIVE, Arrays.asList());
        topology1.addBasketBought(t2, Arrays.asList("a"));

        return new Object[][]{
            {new Deactivate(t1, topology1.getEconomy().getMarket(EMPTY)),topology1,"Because of insufficient demand for []."},
            {new Deactivate(t2, topology1.getEconomy().getMarket(EMPTY)),topology1,"Because of insufficient demand for []."},
        };
    }

} // end DeactivateTest class
