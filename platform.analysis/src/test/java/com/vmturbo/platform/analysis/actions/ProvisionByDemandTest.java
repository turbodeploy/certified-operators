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
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.ede.EdeCommon;
import com.vmturbo.platform.analysis.topology.LegacyTopology;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

/**
 * A test case for the {@link ProvisionByDemand} class.
 */
@RunWith(JUnitParamsRunner.class)
public class ProvisionByDemandTest {
    // Fields
    private static final Basket EMPTY = new Basket();

    // Methods

    @Test
    @Parameters
    @TestCaseName("Test #{index}: new ProvisionByDemand({0},{1})")
    public final void testProvisionByDemand(@NonNull Economy economy, @NonNull ShoppingList modelBuyer) {
        @NonNull ProvisionByDemand provision = new ProvisionByDemand(economy, modelBuyer);

        assertSame(economy, provision.getEconomy());
        assertSame(modelBuyer, provision.getModelBuyer());
        assertNull(provision.getProvisionedSeller());
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestProvisionByDemand() {
        Economy e1 = new Economy();

        ShoppingList b1 = e1.addBasketBought(e1.addTrader(0, TraderState.ACTIVE, EMPTY), EMPTY);
        ShoppingList b2 = e1.addBasketBought(e1.addTrader(0, TraderState.INACTIVE, EMPTY), EMPTY);
        ShoppingList b3 = e1.addBasketBought(e1.addTrader(0, TraderState.ACTIVE, EMPTY),
            new Basket(new CommoditySpecification(0)));
        b3.setQuantity(0, 5).setPeakQuantity(0, 6.5);

        ShoppingList b4 = e1.addBasketBought(e1.addTrader(0, TraderState.ACTIVE, EMPTY),
            new Basket(new CommoditySpecification(0),new CommoditySpecification(1)));
        b4.setQuantity(0, 2.2).setPeakQuantity(0, 6.5);
        b4.setQuantity(1, 100).setPeakQuantity(1, 101.3);

        return new Object[][]{{e1,b1},{e1,b2},{e1,b3},{e1,b4}};
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: {0}.serialize({1}) == {2}")
    public final void testSerialize(@NonNull ProvisionByDemand provision,
            @NonNull Function<@NonNull Trader, @NonNull String> oid, @NonNull String serialized) {
        assertEquals(provision.serialize(oid), serialized);
    }

    // TODO (Vaptistis): add more tests once semantics are clear.
    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestSerialize() {
        @NonNull Map<@NonNull Trader, @NonNull String> oids = new HashMap<>();
        @NonNull Function<@NonNull Trader, @NonNull String> oid = oids::get;

        Economy e1 = new Economy();
        ShoppingList b1 = e1.addBasketBought(e1.addTrader(0, TraderState.ACTIVE, EMPTY), EMPTY);
        ShoppingList b2 = e1.addBasketBought(e1.addTrader(0, TraderState.INACTIVE, EMPTY), EMPTY);

        oids.put(b1.getBuyer(), "id1");
        oids.put(b2.getBuyer(), "id2");

        return new Object[][]{
            {new ProvisionByDemand(e1,b1),oid,"<action type=\"provisionByDemand\" modelBuyer=\"id1\" />"},
            {new ProvisionByDemand(e1,b2),oid,"<action type=\"provisionByDemand\" modelBuyer=\"id2\" />"}
        };
    }

    @Test
    @Parameters(method = "parametersForTestProvisionByDemand")
    @TestCaseName("Test #{index}: new ProvisionByDemand({0},{1}).take().rollback()")
    public final void testTakeRollback(@NonNull Economy economy, @NonNull ShoppingList modelBuyer) {
        final int oldSize = economy.getTraders().size();
        @NonNull ProvisionByDemand provision = new ProvisionByDemand(economy, modelBuyer);

        assertSame(provision, provision.take());
        assertNotNull(provision.getProvisionedSeller());
        assertTrue(provision.getProvisionedSeller().getState().isActive());
        assertTrue(economy.getTraders().contains(provision.getProvisionedSeller()));
        assertEquals(oldSize+1, economy.getTraders().size());
        assertTrue(EdeCommon.quote(economy, modelBuyer, null,
            provision.getProvisionedSeller()) < Double.POSITIVE_INFINITY); // assert that it can fit.

        assertSame(provision, provision.rollback());
        assertNull(provision.getProvisionedSeller());
        assertFalse(economy.getTraders().contains(provision.getProvisionedSeller()));
        assertEquals(oldSize, economy.getTraders().size());
        // TODO: can compare economy for equality (once implemented) to test that rolling back
        // indeed completely restores it.
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: {0}.debugDescription({1}) == {2}")
    public final void testDebugDescription(@NonNull ProvisionByDemand provision, @NonNull LegacyTopology topology,
                                           @NonNull String description) {
        assertEquals(description, provision.debugDescription(topology.getUuids()::get,
            topology.getNames()::get, topology.getCommodityTypes()::getName, topology.getTraderTypes()::getName));
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestDebugDescription() {
        @NonNull LegacyTopology topology1 = new LegacyTopology();

        Trader t1 = topology1.addTrader("id1","name1","VM",TraderState.ACTIVE, Arrays.asList());
        ShoppingList b1 = topology1.addBasketBought(t1, Arrays.asList());
        Trader t2 = topology1.addTrader("id2","name2","Container",TraderState.INACTIVE, Arrays.asList());
        ShoppingList b2 = topology1.addBasketBought(t2, Arrays.asList("CPU"));

        return new Object[][]{
            {new ProvisionByDemand((Economy)topology1.getEconomy(), b1),topology1,
                "Provision a new VM with the following characteristics: "},
            {new ProvisionByDemand((Economy)topology1.getEconomy(), b2),topology1,
                "Provision a new Container with the following characteristics: "},
            // TODO: update test when we figure out how to get correct type!
        };
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: {0}.debugReason({1}) == {2}")
    public final void testDebugReason(@NonNull ProvisionByDemand provision, @NonNull LegacyTopology topology,
                                      @NonNull String reason) {
        assertEquals(reason, provision.debugReason(topology.getUuids()::get,
            topology.getNames()::get, topology.getCommodityTypes()::getName, topology.getTraderTypes()::getName));
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestDebugReason() {
        @NonNull LegacyTopology topology1 = new LegacyTopology();

        Trader t1 = topology1.addTrader("id1","VM1","VM",TraderState.ACTIVE, Arrays.asList());
        ShoppingList b1 = topology1.addBasketBought(t1, Arrays.asList());
        Trader t2 = topology1.addTrader("id2","Container1","Container",TraderState.INACTIVE, Arrays.asList());
        ShoppingList b2 = topology1.addBasketBought(t2, Arrays.asList("CPU"));

        return new Object[][]{
            {new ProvisionByDemand((Economy)topology1.getEconomy(), b1),topology1,
                "No VM has enough capacity for VM1 [id1] (#0)."},
            {new ProvisionByDemand((Economy)topology1.getEconomy(), b2),topology1,
                "No Container has enough capacity for Container1 [id2] (#1)."},
            // TODO: update test when we figure out how to get correct type!
        };
    }

} // end ProvisionByDemandTest class
