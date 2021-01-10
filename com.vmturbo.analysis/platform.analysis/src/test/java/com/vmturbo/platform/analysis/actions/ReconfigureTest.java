package com.vmturbo.platform.analysis.actions;

import static org.junit.Assert.*;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.economy.TraderWithSettings;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

/**
 * A test case for the {@link ReconfigureConsumer} class.
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
        ReconfigureConsumer reconfiguration = new ReconfigureConsumer(economy, target);

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
    public final void testSerialize(@NonNull ReconfigureConsumer reconfiguration, @NonNull Function<@NonNull Trader, @NonNull String> oid,
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

        return new Object[][]{{new ReconfigureConsumer(e1, p1), oid, "<action type=\"reconfigure\" target=\"id1\" source=\"id2\" />"}};
    }

    @Test
    @Parameters(method = "parametersForTestReconfigure")
    @TestCaseName("Test #{index}: new Reconfigure({0},{1}).take()")
    public final void testTake(@NonNull Economy economy, @NonNull ShoppingList target) {
        Trader oldSupplier = target.getSupplier();
        @NonNull ReconfigureConsumer reconfiguration = new ReconfigureConsumer(economy, target);
        // TODO: take a copy of the economy and assert it remained unchanged when copying gets
        // implemented
        assertSame(reconfiguration, reconfiguration.take());
        assertSame(oldSupplier, target.getSupplier());
        assertFalse(target.isMovable());
    }

    @Test
    @Parameters(method = "parametersForTestReconfigure")
    @TestCaseName("Test #{index}: new Reconfigure({0},{1}).rollback()")
    public final void testRollback(@NonNull Economy economy, @NonNull ShoppingList target) {
        Trader oldSupplier = target.getSupplier();
        @NonNull ReconfigureConsumer reconfiguration = new ReconfigureConsumer(economy, target);
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
        assertTrue(target.isMovable());
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
    private static Object[] parametersForTestEqualsAndHashCode() {
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

        ReconfigureConsumer reconfigure1 = new ReconfigureConsumer(e, shop1);
        ReconfigureConsumer reconfigure2 = new ReconfigureConsumer(e, shop2);
        ReconfigureConsumer reconfigure3 = new ReconfigureConsumer(e, shop1);
        return new Object[][] {{reconfigure1, reconfigure2, false},
                        {reconfigure1, reconfigure3, true}};
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: equals and hashCode for {0}, {1} == {2}")
    public final void testEqualsAndHashCode(@NonNull ReconfigureConsumer reconfigure1,
                    @NonNull ReconfigureConsumer reconfigure2, boolean expect) {
        assertEquals(expect, reconfigure1.equals(reconfigure2));
        assertEquals(expect, reconfigure1.hashCode() == reconfigure2.hashCode());
    }

    /**
     * Test that we are removing software license commodity successfully from a trader.
     */
    @Test
    public final void testReconfigureProviderRemoval() {
        Economy e = new Economy();
        Basket basket1 = new Basket(TestUtils.VMEM);
        Trader vm = e.addTrader(0, TraderState.ACTIVE, basket1);
        CommoditySpecification removalComm = TestUtils.SOFTWARE_LICENSE_COMMODITY;
        CommoditySpecification regularComm = TestUtils.MEM;
        Basket basket2 = new Basket(removalComm, regularComm);
        Trader pm = e.addTrader(1, TraderState.ACTIVE, basket2);
        TestUtils.createAndPlaceShoppingList(e, Arrays.asList(TestUtils.MEM), vm,
            new double[] {100}, pm);
        Map<CommoditySpecification, CommoditySold> comm = new HashMap<CommoditySpecification, CommoditySold>() {{
            put(removalComm, pm.getCommoditySold(removalComm));
        }};
        ReconfigureProviderRemoval removalAction = new ReconfigureProviderRemoval(e,
            (TraderWithSettings)pm, comm);
        removalAction.take();
        assertTrue(pm.getCommoditiesSold().size() == 1);
        assertTrue(pm.getBasketSold().size() == 1);
        assertTrue(pm.getCommoditySold(removalComm) == null);
        assertTrue(pm.getCommoditySold(regularComm) != null);
        removalAction.rollback();
        assertTrue(pm.getCommoditiesSold().size() == 2);
        assertTrue(pm.getBasketSold().size() == 2);
        assertTrue(pm.getCommoditySold(removalComm) != null);
        assertTrue(pm.getCommoditySold(regularComm) != null);
    }

    /**
     * Test that we are adding software license commodity successfully to a trader.
     */
    @Test
    public final void testReconfigureProviderAddition() {
        Economy e = new Economy();
        Basket basket1 = new Basket(TestUtils.VMEM);
        Trader vm = e.addTrader(0, TraderState.ACTIVE, basket1);
        CommoditySpecification additionComm = TestUtils.SOFTWARE_LICENSE_COMMODITY;
        CommoditySpecification regularComm1 = TestUtils.MEM;
        CommoditySpecification regularComm2 = TestUtils.MEM;
        Basket basket2 = new Basket(additionComm, regularComm1);
        Basket basket3 = new Basket(regularComm2);
        Trader pm = e.addTrader(1, TraderState.ACTIVE, basket2);
        TestUtils.createAndPlaceShoppingList(e, Arrays.asList(TestUtils.MEM), vm,
            new double[] {100}, pm);
        Map<CommoditySpecification, CommoditySold> comm = new HashMap<CommoditySpecification, CommoditySold>() {{
            put(additionComm, pm.getCommoditySold(additionComm));
        }};
        Trader pm2 = e.addTrader(2, TraderState.ACTIVE, basket3);
        ReconfigureProviderAddition additionAction = new ReconfigureProviderAddition(e,
                (TraderWithSettings)pm2, comm);
        additionAction.take();
        assertTrue(pm2.getCommoditiesSold().size() == 2);
        assertTrue(pm2.getBasketSold().size() == 2);
        assertTrue(pm2.getCommoditySold(regularComm2) != null);
        assertTrue(pm2.getCommoditySold(additionComm) != null);
        additionAction.rollback();
        assertTrue(pm2.getCommoditiesSold().size() == 1);
        assertTrue(pm2.getBasketSold().size() == 1);
        assertTrue(pm2.getCommoditySold(regularComm2) != null);
        assertTrue(pm2.getCommoditySold(additionComm) == null);
    }
} // end ReconfigureTest class
