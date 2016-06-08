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
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
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
    @TestCaseName("Test #{index}: new Activate({0},{1},{2},{3})")
    public final void testActivate(@NonNull Economy economy, @NonNull Trader target, @NonNull Market sourceMarket, @NonNull Trader modelSeller, boolean unusedFlag) {
        @NonNull Activate activation = new Activate(economy, target, sourceMarket, modelSeller);

        assertSame(target, activation.getTarget());
        assertSame(sourceMarket, activation.getSourceMarket());
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestActivate() {
        Economy e1 = new Economy();
        Trader t1 = e1.addTrader(0, TraderState.ACTIVE, EMPTY, EMPTY);
        Trader t2 = e1.addTrader(0, TraderState.INACTIVE, EMPTY, EMPTY);
        Trader t3 = e1.addTrader(0, TraderState.ACTIVE, EMPTY, EMPTY);
        Trader t4 = e1.addTrader(0, TraderState.INACTIVE, EMPTY, EMPTY);

        return new Object[][]{{e1,t1,e1.getMarket(EMPTY),t3, false},{e1,t2,e1.getMarket(EMPTY),t4, true}};
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestWithGuaranteedBuyer() {
        Economy e1 = new Economy();
        Basket basket = new Basket(new CommoditySpecification(0));
        Trader t1 = e1.addTrader(0, TraderState.INACTIVE, basket, EMPTY);
        Trader t2 = e1.addTrader(0, TraderState.ACTIVE, basket, EMPTY);

        //t3 is the model seller
        Trader t3 = e1.addTrader(0, TraderState.ACTIVE, basket, EMPTY);

        Trader b1 = e1.addTrader(0, TraderState.ACTIVE, EMPTY, basket);
        b1.getSettings().setGuaranteedBuyer(true);
        //b1 is a guaranteedBuyer for t2 and t3
        ShoppingList s2 = e1.addBasketBought(b1, basket);
        s2.move(t2);
        ShoppingList s3 = e1.addBasketBought(b1, basket);
        s3.move(t3);

        return new Object[][]{{e1,t1,e1.getMarket(EMPTY),t3, b1, true}, {e1,t2,e1.getMarket(EMPTY),t3, b1, false}};
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
        Trader t3 = e1.addTrader(0, TraderState.ACTIVE, EMPTY, EMPTY);
        Trader t4 = e1.addTrader(0, TraderState.INACTIVE, EMPTY, EMPTY);

        oids.put(t1, "id1");
        oids.put(t2, "id2");

        return new Object[][]{
            {new Activate(e1, t1, e1.getMarket(EMPTY), t3),oid,"<action type=\"activate\" target=\"id1\" />"},
            {new Activate(e1, t2, e1.getMarket(EMPTY), t4),oid,"<action type=\"activate\" target=\"id2\" />"}
        };
    }

    @Test
    @Parameters(method = "parametersForTestActivate")
    @TestCaseName("Test #{index}: new Activate({0},{1},{2},{3}).take()  throw == {4}")
    public final void testTake(@NonNull Economy economy, @NonNull Trader target, @NonNull Market sourceMarket, @NonNull Trader modelSeller, boolean valid) {
        @NonNull Activate activation = new Activate(economy, target,sourceMarket, modelSeller);

        try {
            assertSame(activation, activation.take());
            assertTrue(valid);
        } catch (IllegalArgumentException e){
            assertFalse(valid);
        }
        assertTrue(target.getState().isActive());
    }

    @Test
    @Parameters(method = "parametersForTestActivate")
    @TestCaseName("Test #{index}: new Activate({0},{1},{2},{3}).rollback()  throw == {4}")
    public final void testRollback(@NonNull Economy economy, @NonNull Trader target, @NonNull Market sourceMarket, @NonNull Trader modelSeller, boolean invalid) {
        @NonNull Activate activation = new Activate(economy, target,sourceMarket, modelSeller);
        // TODO: normally, we should take the action before rolling back...
        try {
            assertSame(activation, activation.rollback());
            assertFalse(invalid);
        } catch (IllegalArgumentException e){
            assertTrue(invalid);
        }
        assertFalse(target.getState().isActive());
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: {0}.debugDescription({1}) == {2}")
    public final void testDebugDescription(@NonNull Activate activation, @NonNull LegacyTopology topology,
                                           @NonNull String description) {
        assertEquals(description, activation.debugDescription(topology.getUuids()::get,
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
        Trader t3 = topology1.addTrader("id3","name3","type3",TraderState.ACTIVE, Arrays.asList());
        topology1.addBasketBought(t3, Arrays.asList());
        Trader t4 = topology1.addTrader("id4","name4","type4",TraderState.INACTIVE, Arrays.asList());
        topology1.addBasketBought(t4, Arrays.asList("b"));

        return new Object[][]{
            {new Activate(e1, t1, topology1.getEconomy().getMarket(EMPTY), t3),topology1,"Activate name1 [id1] (#0)."},
            {new Activate(e1, t2, topology1.getEconomy().getMarket(EMPTY), t4),topology1,"Activate name2 [id2] (#1)."},
        };
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: {0}.debugReason({1}) == {2}")
    public final void testDebugReason(@NonNull Activate activation, @NonNull LegacyTopology topology,
                                      @NonNull String reason) {
        assertEquals(reason, activation.debugReason(topology.getUuids()::get,
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
        Trader t3 = topology1.addTrader("id3","name3","type3",TraderState.ACTIVE, Arrays.asList());
        topology1.addBasketBought(t3, Arrays.asList());
        Trader t4 = topology1.addTrader("id4","name4","type4",TraderState.INACTIVE, Arrays.asList());
        topology1.addBasketBought(t4, Arrays.asList("b"));

        return new Object[][]{
            {new Activate(e1, t1, topology1.getEconomy().getMarket(EMPTY), t3),topology1,"To satisfy increased demand for []."},
            {new Activate(e1, t2, topology1.getEconomy().getMarket(EMPTY), t4),topology1,"To satisfy increased demand for []."},
        };
    }

    @Test
    @Parameters(method = "parametersForTestWithGuaranteedBuyer")
    @TestCaseName("Test #{index}: new Activate({0}, {1}, {2}, {3}).take() throw == {5}")
    public final void testTakeWithGuaranteedBuyer(@NonNull Economy economy, @NonNull Trader target, @NonNull Market sourceMarket, @NonNull Trader modelSeller, @NonNull Trader expectedBuyer, boolean valid) {
        @NonNull Activate activation = new Activate(economy, target, sourceMarket, modelSeller);

        try {
            assertSame(activation, activation.take());
            assertTrue(valid);
        } catch (IllegalArgumentException e) {
            assertFalse(valid);
        }
        assertTrue(target.getState().isActive());

        // check for shoppingList for guaranteedBuyer and trader to activate
        assertEquals(1, target.getCustomers().size());
        // check the buyer-seller relation is being set for guaranteed buyer and newly activated trader
        assertEquals(expectedBuyer, target.getCustomers().get(0).getBuyer());
    }

    @Test
    @Parameters(method = "parametersForTestWithGuaranteedBuyer")
    @TestCaseName("Test #{index}: new Activate({0},{1},{2},{3}).rollback() throw == {5}")
    public final void testRollbackWithGuaranteedBuyer(@NonNull Economy economy, @NonNull Trader target, @NonNull Market sourceMarket, @NonNull Trader modelSeller, @ NonNull Trader expectedBuyer, boolean invalid) {
        @NonNull Activate activation = new Activate(economy, target,sourceMarket, modelSeller);
        try {
            assertSame(activation, activation.rollback());
            assertFalse(invalid);
        } catch (IllegalArgumentException e) {
            assertTrue(invalid);
        }
        assertFalse(target.getState().isActive());
        // check the buyer-seller relation is being cancelled
        assertEquals(0, target.getCustomers().size());
    }

    @SuppressWarnings("unused")
    private static Object[] parametersForTestEquals_and_HashCode() {
        Economy e = new Economy();
        Basket b1 = new Basket(new CommoditySpecification(100));
        Basket b2 = new Basket(new CommoditySpecification(200));
        Basket b3 = new Basket();
        // t1 t2 are two different traders with same contents, while t3 is another different
        // trader with different contents
        Trader t1 = e.addTrader(0, TraderState.ACTIVE, b1, b2);
        Trader t2 = e.addTrader(0, TraderState.ACTIVE, b1, b2);
        Trader t3 = e.addTrader(0, TraderState.INACTIVE, b2, b3);
        Trader t4 = e.addTrader(0, TraderState.ACTIVE, b2, b3);

        Market m1 = e.getMarket(b2);
        Market m3 = e.getMarket(b3);

        Activate activate1 = new Activate(e, t3, m1, t4);
        Activate activate2 = new Activate(e, t3, m1, t4);
        Activate activate3 = new Activate(e, t4, m1, t3);
        Activate activate4 = new Activate(e, t4, m1, t4);
        return new Object[][] {{activate1, activate2, true}, {activate1, activate3, false},
                        {activate1, activate4, false}};
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: equals and hashCode for {0}, {1} == {2}")
    public final void testEquals_and_HashCode(@NonNull Activate activate1,
                    @NonNull Activate activate2, boolean expect) {
        assertEquals(expect, activate1.equals(activate2));
        assertEquals(expect, activate1.hashCode() == activate2.hashCode());
    }

} // end ActivateTest class
