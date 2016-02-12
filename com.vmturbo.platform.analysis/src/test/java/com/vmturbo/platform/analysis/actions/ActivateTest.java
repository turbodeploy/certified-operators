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
 * A test case for the {@link Activate} class.
 */
@RunWith(JUnitParamsRunner.class)
public class ActivateTest {
    // Fields
    private static final Basket EMPTY = new Basket();

    // Methods

    @Test
    @Parameters
    @TestCaseName("Test #{index}: new Activate({0},{1})")
    public final void testActivate(@NonNull Trader target, @NonNull Market sourceMarket) {
        @NonNull Activate activation = new Activate(target, sourceMarket);

        assertSame(target, activation.getTarget());
        assertSame(sourceMarket, activation.getSourceMarket());
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestActivate() {
        Economy e1 = new Economy();
        Trader t1 = e1.addTrader(0, TraderState.ACTIVE, EMPTY, EMPTY);
        Trader t2 = e1.addTrader(0, TraderState.INACTIVE, EMPTY, EMPTY);

        return new Object[][]{{t1,e1.getMarket(EMPTY)},{t2,e1.getMarket(EMPTY)}};
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: {0}.serialize({1}) == {2}")
    public final void testSerialize(@NonNull Activate activation,
            @NonNull Function<@NonNull Trader, @NonNull String> oid, @NonNull String serialized) {
        assertEquals(activation.serialize(oid), serialized);
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
            {new Activate(t1, e1.getMarket(EMPTY)),oid,"<action type=\"activate\" target=\"id1\" />"},
            {new Activate(t2, e1.getMarket(EMPTY)),oid,"<action type=\"activate\" target=\"id2\" />"}
        };
    }

    @Test
    @Parameters(method = "parametersForTestActivate")
    @TestCaseName("Test #{index}: new Activate({0},{1}).take()")
    public final void testTake(@NonNull Trader target, @NonNull Market sourceMarket) {
        new Activate(target,sourceMarket).take();
        assertTrue(target.getState().isActive());
    }

    @Test
    @Parameters(method = "parametersForTestActivate")
    @TestCaseName("Test #{index}: new Activate({0},{1}).rollback()")
    public final void testRollback(@NonNull Trader target, @NonNull Market sourceMarket) {
        // TODO: normally, we should take the action before rolling back...
        new Activate(target,sourceMarket).rollback();
        assertFalse(target.getState().isActive());
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: {0}.debugDescription({1}) == {2}")
    public final void testDebugDescription(@NonNull Activate activation, @NonNull LegacyTopology topology,
                                           @NonNull String description) {
        assertEquals(description, activation.debugDescription(topology.getUuids()::get,
            topology.getNames()::get, topology.getTraderTypes()::getName, topology.getCommodityTypes()::getName));
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestDebugDescription() {
        @NonNull LegacyTopology topology1 = new LegacyTopology();

        Trader t1 = topology1.addTrader("id1","name1","type1",TraderState.ACTIVE, Arrays.asList());
        topology1.addBasketBought(t1, Arrays.asList());
        Trader t2 = topology1.addTrader("id2","name2","type2",TraderState.INACTIVE, Arrays.asList());
        topology1.addBasketBought(t2, Arrays.asList("a"));

        return new Object[][]{
            {new Activate(t1, topology1.getEconomy().getMarket(EMPTY)),topology1,"Activate name1 [id1] (#0)."},
            {new Activate(t2, topology1.getEconomy().getMarket(EMPTY)),topology1,"Activate name2 [id2] (#1)."},
        };
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: {0}.debugReason({1}) == {2}")
    public final void testDebugReason(@NonNull Activate activation, @NonNull LegacyTopology topology,
                                      @NonNull String reason) {
        assertEquals(reason, activation.debugReason(topology.getUuids()::get,
            topology.getNames()::get, topology.getTraderTypes()::getName, topology.getCommodityTypes()::getName));
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestDebugReason() {
        @NonNull LegacyTopology topology1 = new LegacyTopology();

        Trader t1 = topology1.addTrader("id1","name1","type1",TraderState.ACTIVE, Arrays.asList());
        topology1.addBasketBought(t1, Arrays.asList());
        Trader t2 = topology1.addTrader("id2","name2","type2",TraderState.INACTIVE, Arrays.asList());
        topology1.addBasketBought(t2, Arrays.asList("a"));

        return new Object[][]{
            {new Activate(t1, topology1.getEconomy().getMarket(EMPTY)),topology1,"To satisfy increased demand for []."},
            {new Activate(t2, topology1.getEconomy().getMarket(EMPTY)),topology1,"To satisfy increased demand for []."},
        };
    }

} // end ActivateTest class
