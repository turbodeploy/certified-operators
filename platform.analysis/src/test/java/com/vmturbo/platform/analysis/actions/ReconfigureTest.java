package com.vmturbo.platform.analysis.actions;

import static org.junit.Assert.*;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

/**
 * A test case for the {@link Reconfigure} class.
 */
@RunWith(JUnitParamsRunner.class)
public class ReconfigureTest {
    // Fields
    private static final Basket EMPTY = new Basket();

    // Methods
    @Test
    @Parameters
    @TestCaseName("Test #{index}: new Reconfigure({0},{1})")
    public final void testReconfigure(@NonNull Economy economy, @NonNull ShoppingList target) {
        Reconfigure reconfiguration = new Reconfigure(economy,target);

        assertSame(economy, reconfiguration.getEconomy());
        assertSame(target, reconfiguration.getTarget());
        assertSame(target.getSupplier(), reconfiguration.getSource());
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestReconfigure() {
        Economy e1 = new Economy();
        ShoppingList p1 = e1.addBasketBought(e1.addTrader(0, TraderState.ACTIVE, EMPTY), EMPTY);

        Economy e2 = new Economy();
        ShoppingList p2 = e2.addBasketBought(e2.addTrader(0, TraderState.ACTIVE, EMPTY), EMPTY);
        p2.move(e2.addTrader(1, TraderState.ACTIVE, EMPTY));

        return new Object[][]{{e1,p1},{e2,p2}};
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: {0}.serialize({1}) == {2}")
    public final void testSerialize(@NonNull Reconfigure reconfiguration, @NonNull Function<@NonNull Trader, @NonNull String> oid,
                                    @NonNull String serialized) {
        assertEquals(reconfiguration.serialize(oid), serialized);
    }

    // TODO (Vaptistis): add more tests once semantics are clear.
    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestSerialize() {
        @NonNull Map<@NonNull Trader, @NonNull String> oids = new HashMap<>();
        @NonNull Function<@NonNull Trader, @NonNull String> oid = oids::get;

        Economy e1 = new Economy();
        Trader t1 = e1.addTrader(0, TraderState.ACTIVE, EMPTY);
        ShoppingList p1 = e1.addBasketBought(t1, EMPTY);
        Trader s1 = e1.addTrader(0, TraderState.ACTIVE, EMPTY);
        p1.move(s1);
        oids.put(t1, "id1");
        oids.put(s1, "id2");

        return new Object[][]{{new Reconfigure(e1, p1),oid,"<action type=\"reconfigure\" target=\"id1\" source=\"id2\" />"}};
    }

    @Test
    @Parameters(method = "parametersForTestReconfigure")
    @TestCaseName("Test #{index}: new Reconfigure({0},{1}).take()")
    public final void testTake(@NonNull Economy economy, @NonNull ShoppingList target) {
        Trader oldSupplier = target.getSupplier();
        @NonNull Reconfigure reconfiguration = new Reconfigure(economy, target);
        // TODO: take a copy of the economy and assert it remained unchanged when copying gets
        // implemented
        assertSame(reconfiguration, reconfiguration.take());
        assertSame(oldSupplier, target.getSupplier());
    }

    @Test
    @Parameters(method = "parametersForTestReconfigure")
    @TestCaseName("Test #{index}: new Reconfigure({0},{1}).rollback()")
    public final void testRollback(@NonNull Economy economy, @NonNull ShoppingList target) {
        Trader oldSupplier = target.getSupplier();
        @NonNull Reconfigure reconfiguration = new Reconfigure(economy, target);
        // mock the actionTaken flag as if it is being taken
        try {
            Field actionTakenField = ActionImpl.class.getDeclaredField("actionTaken");
            actionTakenField.setAccessible(true);
            actionTakenField.setBoolean(reconfiguration, true);
        } catch (Exception e) {
            fail();
        }
        // TODO: take a copy of the economy and assert it remained unchanged when copying gets
        // implemented
        assertSame(reconfiguration, reconfiguration.rollback());
        assertSame(oldSupplier, target.getSupplier());
    }

    @Test
    @Ignore
    public final void testDebugDescription() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    @Ignore
    public final void testDebugReason() {
        fail("Not yet implemented"); // TODO
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

        Reconfigure reconfigure1 = new Reconfigure(e, shop1);
        Reconfigure reconfigure2 = new Reconfigure(e, shop2);
        Reconfigure reconfigure3 = new Reconfigure(e, shop1);
        return new Object[][] {{reconfigure1, reconfigure2, false},
                        {reconfigure1, reconfigure3, true}};
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: equals and hashCode for {0}, {1} == {2}")
    public final void testEquals_and_HashCode(@NonNull Reconfigure reconfigure1,
                    @NonNull Reconfigure reconfigure2, boolean expect) {
        assertEquals(expect, reconfigure1.equals(reconfigure2));
        assertEquals(expect, reconfigure1.hashCode() == reconfigure2.hashCode());
    }
} // end ReconfigureTest class
