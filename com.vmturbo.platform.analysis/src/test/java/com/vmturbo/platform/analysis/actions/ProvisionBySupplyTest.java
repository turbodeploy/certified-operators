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
import com.vmturbo.platform.analysis.economy.BuyerParticipation;
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
    private static final Basket[] basketsSold = {EMPTY, new Basket(VCPU), new Basket(VCPU,VMEM)};
    private static final Basket[] basketsBought = {EMPTY, new Basket(CPU), new Basket(CPU,MEM)};

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
                    testCases.add(new Object[]{e1,e1.addTrader(0, state, basketSold, basketBought1)});
                    // TODO (Vaptistis): also add quantities bought and sold

                    for (Basket basketBought2 : basketsBought) {
                        testCases.add(new Object[]{e1,e1.addTrader(0, state, basketSold, basketBought1, basketBought2)});
                    }
                }
            }

        }

        return testCases.toArray();
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
            {new ProvisionBySupply(e1,t1),oid,"<action type=\"provisionBySupply\" modelSeller=\"id1\" />"},
            {new ProvisionBySupply(e1,t2),oid,"<action type=\"provisionBySupply\" modelSeller=\"id2\" />"}
        };
    }

    @Test
    @Parameters(method = "parametersForTestProvisionBySupply")
    @TestCaseName("Test #{index}: new ProvisionBySupply({0},{1}).take().rollback()")
    public final void testTakeRollback(@NonNull Economy economy, @NonNull Trader modelSeller) {
        final int oldSize = economy.getTraders().size();
        @NonNull ProvisionBySupply provision = new ProvisionBySupply(economy, modelSeller);

        provision.take();
        assertNotNull(provision.getProvisionedSeller());
        assertTrue(provision.getProvisionedSeller().getState().isActive());
        assertTrue(economy.getTraders().contains(provision.getProvisionedSeller()));
        assertEquals(oldSize+1, economy.getTraders().size());
        // TODO: test that provisioned seller is indeed identical to the model one when Trader
        // equality testing is implemented.

        provision.rollback();
        assertNull(provision.getProvisionedSeller());
        assertFalse(economy.getTraders().contains(provision.getProvisionedSeller()));
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

        Trader t1 = topology1.addTrader("id1","VM1","VM",TraderState.ACTIVE, Arrays.asList());
        BuyerParticipation b1 = topology1.addBasketBought(t1, Arrays.asList());
        Trader t2 = topology1.addTrader("id2","Container2","Container",TraderState.INACTIVE, Arrays.asList());
        BuyerParticipation b2 = topology1.addBasketBought(t2, Arrays.asList("CPU"));

        return new Object[][]{
            {new ProvisionBySupply((Economy)topology1.getEconomy(), t1),topology1,
                "Provision a new VM similar to VM1 [id1] (#0)."},
            {new ProvisionBySupply((Economy)topology1.getEconomy(), t2),topology1,
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

        Trader t1 = topology1.addTrader("id1","VM1","VM",TraderState.ACTIVE, Arrays.asList());
        BuyerParticipation b1 = topology1.addBasketBought(t1, Arrays.asList());
        Trader t2 = topology1.addTrader("id2","Container1","Container",TraderState.INACTIVE, Arrays.asList());
        BuyerParticipation b2 = topology1.addBasketBought(t2, Arrays.asList("CPU"));

        return new Object[][]{
            {new ProvisionBySupply((Economy)topology1.getEconomy(), t1),topology1,
                "No VM has enough leftover capacity for [buyer]."},
            {new ProvisionBySupply((Economy)topology1.getEconomy(), t2),topology1,
                "No Container has enough leftover capacity for [buyer]."},
            // TODO: update test when we figure out how to get correct type!
        };
    }

} // end ProvisionBySupplyTest class
