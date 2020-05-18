package com.vmturbo.platform.analysis.actions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.topology.LegacyTopology;

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
    @TestCaseName("Test #{index}: new Deactivate({0},{1},{2})")
    public final void testDeactivate(@NonNull Economy economy, @NonNull Trader target,
                                     @NonNull Basket triggeringBasket, boolean unusedFlag) {
        @NonNull Deactivate deactivation = new Deactivate(economy, target, triggeringBasket);

        assertSame(target, deactivation.getTarget());
        assertSame(triggeringBasket, deactivation.getTriggeringBasket());
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestDeactivate() {
        Economy e1 = new Economy();
        Trader t1 = e1.addTrader(0, TraderState.ACTIVE, EMPTY, EMPTY);
        Trader t2 = e1.addTrader(0, TraderState.INACTIVE, EMPTY, EMPTY);

        return new Object[][]{
            {e1, t1, EMPTY, true},
            {e1, t2, EMPTY, false}
        };
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestWithGuaranteedBuyer() {
        Economy e1 = new Economy();
        Basket basket = new Basket(new CommoditySpecification(0));
        Trader t1 = e1.addTrader(0, TraderState.ACTIVE, basket, EMPTY);
        Trader t2 = e1.addTrader(0, TraderState.INACTIVE, basket, EMPTY);

        Trader b1 = e1.addTrader(1, TraderState.ACTIVE, EMPTY, basket);
        b1.getSettings().setGuaranteedBuyer(true);
        ShoppingList s1 = e1.addBasketBought(b1, basket);
        s1.move(t1);

        return new Object[][]{
            {e1, t1, EMPTY, true},
            {e1, t2, EMPTY, false}
        };
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
            {new Deactivate(e1, t1, EMPTY), oid, "<action type=\"deactivate\" target=\"id1\" />"},
            {new Deactivate(e1, t2, EMPTY), oid, "<action type=\"deactivate\" target=\"id2\" />"}
        };
    }

    @Test
    @Parameters(method = "parametersForTestDeactivate")
    @TestCaseName("Test #{index}: new Deactivate({0},{1},{2}).take() throw == {3}")
    public final void testTake(@NonNull Economy economy, @NonNull Trader target,
                               @NonNull Basket triggeringBasket, boolean valid) {
        @NonNull Deactivate deactivation = new Deactivate(economy, target, triggeringBasket);

        try {
            assertSame(deactivation, deactivation.take());
            assertTrue(valid);
        } catch (IllegalArgumentException e){
            assertFalse(valid);
        }
        assertFalse(target.getState().isActive());
    }

    @Test
    @Parameters(method = "parametersForTestDeactivate")
    @TestCaseName("Test #{index}: new Deactivate({0},{1},{2}).rollback()  throw == {3}")
    public final void testRollback(@NonNull Economy economy, @NonNull Trader target,
                                   @NonNull Basket triggeringBasket, boolean invalid) {
        @NonNull Deactivate deactivation = new Deactivate(economy, target, triggeringBasket);
        // mock the actionTaken flag as if it is being taken
        try {
            Field actionTakenField = ActionImpl.class.getDeclaredField("actionTaken");
            actionTakenField.setAccessible(true);
            actionTakenField.setBoolean(deactivation, true);
        } catch (Exception e) {
            fail();
        }
        try {
            assertSame(deactivation, deactivation.rollback());
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

        Economy e1 = new Economy();
        Trader t1 = topology1.addTrader("id1","name1","type1",TraderState.ACTIVE, Arrays.asList());
        topology1.addBasketBought(t1, Arrays.asList());
        Trader t2 = topology1.addTrader("id2","name2","type2",TraderState.INACTIVE, Arrays.asList());
        topology1.addBasketBought(t2, Arrays.asList("a"));

        return new Object[][]{
            {new Deactivate(e1, t1, EMPTY), topology1, "Deactivate name1 [id1] (#0)."},
            {new Deactivate(e1, t2, EMPTY), topology1, "Deactivate name2 [id2] (#1)."},
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

        Economy e1 = new Economy();
        Trader t1 = topology1.addTrader("id1","name1","type1",TraderState.ACTIVE, Arrays.asList());
        topology1.addBasketBought(t1, Arrays.asList());
        Trader t2 = topology1.addTrader("id2","name2","type2",TraderState.INACTIVE, Arrays.asList());
        topology1.addBasketBought(t2, Arrays.asList("a"));


        return new Object[][]{
            {new Deactivate(e1, t1, EMPTY), topology1, "Because of insufficient demand for []."},
            {new Deactivate(e1, t2, EMPTY), topology1, "Because of insufficient demand for []."},
        };
    }

    @SuppressWarnings("unused")
    private static Object[] parametersForTestEquals_and_HashCode() {
        Economy e = new Economy();
        Basket b1 = new Basket(new CommoditySpecification(100));
        Basket b2 = new Basket(new CommoditySpecification(200));
        Trader t1 = e.addTrader(0, TraderState.ACTIVE, b1, b1);
        Trader t2 = e.addTrader(0, TraderState.ACTIVE, b1, b2);

        Deactivate deactivate1 = new Deactivate(e, t1, b1);
        Deactivate deactivate2 = new Deactivate(e, t1, b1);
        Deactivate deactivate3 = new Deactivate(e, t2, b1);
        Deactivate deactivate4 = new Deactivate(e, t1, b2);
        Deactivate deactivate5 = new Deactivate(e, t2, b2);
        return new Object[][]{
            {deactivate1, deactivate1, true},

            {deactivate1, deactivate2, true},
            {deactivate1, deactivate3, false},
            {deactivate1, deactivate4, false},
            {deactivate1, deactivate5, false},

            {deactivate2, deactivate1, true},
            {deactivate3, deactivate1, false},
            {deactivate4, deactivate1, false},
            {deactivate5, deactivate1, false},
        };
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: equals and hashCode for {0}, {1} == {2}")
    public final void testEquals_and_HashCode(@NonNull Deactivate deactivate1,
                    @NonNull Deactivate deactivate2, boolean expect) {
        assertEquals(expect, deactivate1.equals(deactivate2));
        assertEquals(expect, deactivate1.hashCode() == deactivate2.hashCode());
    }
} // end DeactivateTest class
