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
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.topology.LegacyTopology;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

/**
 * A test case for the {@link ProvisionBySupply} class.
 */
@RunWith(JUnitParamsRunner.class)
public class ProvisionBySupplyTest {
    // Fields
    private static final CommoditySpecification CPU = new CommoditySpecification(0);
    private static final CommoditySpecification MEM = new CommoditySpecification(1);
    private static final CommoditySpecification VCPU = new CommoditySpecification(0);
    private static final CommoditySpecification VMEM = new CommoditySpecification(1);

    private static final Basket EMPTY = new Basket();
    private static final Basket[] basketsSold = {EMPTY, new Basket(VCPU), new Basket(VCPU, VMEM)};
    private static final Basket[] basketsBought = {EMPTY, new Basket(CPU), new Basket(CPU, MEM)};

    private static final String DEBUG_INFO = "trader name";

    // Methods

    @Test
    @Parameters
    @TestCaseName("Test #{index}: new ProvisionBySupply({0},{1})")
    public final void testProvisionBySupply(@NonNull Economy economy, @NonNull Trader modelSeller) {
        @NonNull ProvisionBySupply provision = new ProvisionBySupply(economy, modelSeller);

        assertSame(economy, provision.getEconomy());
        assertSame(modelSeller, provision.getModelSeller());
        assertNull(provision.getProvisionedSeller());
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestProvisionBySupply() {
        Economy e1 = new Economy();
        List<Object[]> testCases = new ArrayList<>();

        for (TraderState state : TraderState.values()) {
            for (Basket basketSold : basketsSold) {
                for (Basket basketBought1 : basketsBought) {
                    testCases.add(new Object[]{e1, e1.addTrader(0, state, basketSold, basketBought1)});
                    // TODO (Vaptistis): also add quantities bought and sold

                    for (Basket basketBought2 : basketsBought) {
                        testCases.add(new Object[]{e1, e1.addTrader(0, state, basketSold, basketBought1, basketBought2)});
                    }
                }
            }

        }

        return testCases.toArray();
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestWithGuaranteedBuyer() {
        Economy e1 = new Economy();

        Trader modelSeller = e1.addTrader(0, TraderState.ACTIVE, basketsSold[1], basketsBought[0]);
        Trader b1 = e1.addTrader(1, TraderState.ACTIVE, basketsSold[1], basketsSold[1]);
        b1.getSettings().setGuaranteedBuyer(true);
        ShoppingList s1 = e1.addBasketBought(b1, basketsSold[1]);
        s1.move(modelSeller);
        return new Object[]{e1, modelSeller};
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: {0}.serialize({1}) == {2}")
    public final void testSerialize(@NonNull ProvisionBySupply provision,
            @NonNull Function<@NonNull Trader, @NonNull String> oid, @NonNull String serialized) {
        assertEquals(provision.serialize(oid), serialized);
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
            {new ProvisionBySupply(e1, t1), oid, "<action type=\"provisionBySupply\" modelSeller=\"id1\" />"},
            {new ProvisionBySupply(e1, t2), oid, "<action type=\"provisionBySupply\" modelSeller=\"id2\" />"}
        };
    }

    @Test
    @Parameters(method = "parametersForTestProvisionBySupply")
    @TestCaseName("Test #{index}: new ProvisionBySupply({0},{1}).take().rollback()")
    public final void testTakeRollback(@NonNull Economy economy, @NonNull Trader modelSeller) {
        final int oldSize = economy.getTraders().size();
        @NonNull ProvisionBySupply provision = new ProvisionBySupply(economy, modelSeller);
        modelSeller.setDebugInfoNeverUseInCode(DEBUG_INFO);

        assertSame(provision, provision.take());
        Trader provisionedSeller = provision.getProvisionedSeller();
        assertNotNull(provisionedSeller);
        assertTrue(provisionedSeller.getState().isActive());
        assertEquals(provisionedSeller.getDebugInfoNeverUseInCode(),
                DEBUG_INFO + " clone #" + provisionedSeller.getEconomyIndex());
        assertTrue(economy.getTraders().contains(provisionedSeller));
        assertEquals(oldSize+1, economy.getTraders().size());
        // TODO: test that provisioned seller is indeed identical to the model one when Trader
        // equality testing is implemented.

        assertSame(provision, provision.rollback());
        assertNull(provision.getProvisionedSeller());
        assertFalse(economy.getTraders().contains(provisionedSeller));
        assertEquals(oldSize, economy.getTraders().size());
        // TODO: can compare economy for equality (once implemented) to test that rolling back
        // indeed completely restores it.
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: {0}.debugDescription({1}) == {2}")
    public final void testDebugDescription(@NonNull ProvisionBySupply provision, @NonNull LegacyTopology topology,
                                           @NonNull String description) {
        assertEquals(description, provision.debugDescription(topology.getUuids()::get,
            topology.getNames()::get, topology.getCommodityTypes()::getName, topology.getTraderTypes()::getName));
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestDebugDescription() {
        @NonNull LegacyTopology topology1 = new LegacyTopology();

        Trader t1 = topology1.addTrader("id1", "VM1", "VM", TraderState.ACTIVE, Arrays.asList());
        t1.setDebugInfoNeverUseInCode(DEBUG_INFO);
        ShoppingList b1 = topology1.addBasketBought(t1, Arrays.asList());
        Trader t2 = topology1.addTrader("id2", "Container2", "Container", TraderState.INACTIVE, Arrays.asList());
        t2.setDebugInfoNeverUseInCode(DEBUG_INFO);
        ShoppingList b2 = topology1.addBasketBought(t2, Arrays.asList("CPU"));

        return new Object[][]{
            {new ProvisionBySupply((Economy)topology1.getEconomy(), t1), topology1,
                "Provision a new VM similar to VM1 [id1] (#0)."},
            {new ProvisionBySupply((Economy)topology1.getEconomy(), t2), topology1,
                "Provision a new Container similar to Container2 [id2] (#1)."},
            // TODO: update test when we figure out how to get correct type!
        };
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: {0}.debugReason({1}) == {2}")
    public final void testDebugReason(@NonNull ProvisionBySupply provision, @NonNull LegacyTopology topology,
                                      @NonNull String reason) {
        assertEquals(reason, provision.debugReason(topology.getUuids()::get,
            topology.getNames()::get, topology.getCommodityTypes()::getName, topology.getTraderTypes()::getName));
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestDebugReason() {
        @NonNull LegacyTopology topology1 = new LegacyTopology();

        Trader t1 = topology1.addTrader("id1", "VM1", "VM", TraderState.ACTIVE, Arrays.asList());
        ShoppingList b1 = topology1.addBasketBought(t1, Arrays.asList());
        Trader t2 = topology1.addTrader("id2", "Container1", "Container", TraderState.INACTIVE, Arrays.asList());
        ShoppingList b2 = topology1.addBasketBought(t2, Arrays.asList("CPU"));

        return new Object[][]{
            {new ProvisionBySupply((Economy)topology1.getEconomy(), t1), topology1,
                "No VM has enough leftover capacity for [buyer]."},
            {new ProvisionBySupply((Economy)topology1.getEconomy(), t2), topology1,
                "No Container has enough leftover capacity for [buyer]."},
            // TODO: update test when we figure out how to get correct type!
        };
    }

    @Test
    @Parameters(method = "parametersForTestWithGuaranteedBuyer")
    @TestCaseName("Test #{index}: new ProvisionBySupply({0},{1}).take().rollback()")
    public final void testTakeRollbackWithGuranteedBuyer(@NonNull Economy economy, @NonNull Trader modelSeller) {
        @NonNull ProvisionBySupply provision = new ProvisionBySupply(economy, modelSeller);
        provision.take();
        Trader provisionedSeller = provision.getProvisionedSeller();

        List<Trader> guranteedBuyer = new ArrayList<>();
        for (ShoppingList shoppingList : modelSeller.getCustomers()) {
            if (shoppingList.getBuyer().getSettings().isGuaranteedBuyer()) {
                guranteedBuyer.add(shoppingList.getBuyer());
            }
        }

        provision.rollback();

        assertNull(provision.getProvisionedSeller());
        for (Trader buyer : guranteedBuyer) {
            assertTrue(!economy.getSuppliers(buyer).contains(provisionedSeller));
        }


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

        ProvisionBySupply provisionBySupply1 = new ProvisionBySupply(e, t2);
        ProvisionBySupply provisionBySupply2 = new ProvisionBySupply(e, t2);
        ProvisionBySupply provisionBySupply3 = new ProvisionBySupply(e, t3);
        return new Object[][] {{provisionBySupply1, provisionBySupply2, true},
                        {provisionBySupply1, provisionBySupply3, false}};
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: equals and hashCode for {0}, {1} == {2}")
    public final void testEquals_and_HashCode(@NonNull ProvisionBySupply provisionBySupply1,
                    @NonNull ProvisionBySupply provisionBySupply2, boolean expect) {
        assertEquals(expect, provisionBySupply1.equals(provisionBySupply2));
        assertEquals(expect, provisionBySupply1.hashCode() == provisionBySupply2.hashCode());
    }

} // end ProvisionBySupplyTest class
