package com.vmturbo.platform.analysis.actions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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
import com.vmturbo.platform.analysis.economy.TraderSettings;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.ede.EdeCommon;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;
import com.vmturbo.platform.analysis.topology.LegacyTopology;

/**
 * A test case for the {@link ProvisionByDemand} class.
 */
@RunWith(JUnitParamsRunner.class)
public class ProvisionByDemandTest {
    // Fields
    private static final Basket EMPTY = new Basket();

    private static final String DEBUG_INFO = "trader name";

    // Methods

    @Test
    @Parameters
    @TestCaseName("Test #{index}: new ProvisionByDemand({0},{1},{2},{3])")
    public final void testProvisionByDemand(@NonNull Economy economy,
                    @NonNull ShoppingList modelBuyer, @NonNull Trader modelSeller, boolean isProvisionUseful) {
        double[] quoteBefore = EdeCommon.quote(economy, modelBuyer, modelSeller, Double.POSITIVE_INFINITY, false)
            .getQuoteValues();

        if (isProvisionUseful) {
            assertTrue(Double.isInfinite(quoteBefore[0]));
        }
        TraderSettings modelSellerSettings = modelSeller.getSettings();
        modelSellerSettings.setMinDesiredUtil(0.65);
        modelSellerSettings.setMinDesiredUtil(0.75);
        @NonNull
        ProvisionByDemand provision = (ProvisionByDemand)(new ProvisionByDemand(economy, modelBuyer, modelSeller)).take();
        TraderSettings provisionedTraderSettings = provision.getActionTarget().getSettings();


        // verify that the settings are updated correctly on the provisionedTrader
        assertEquals(modelSellerSettings.getMaxDesiredUtil(), provisionedTraderSettings.getMaxDesiredUtil(), TestUtils.FLOATING_POINT_DELTA);
        assertEquals(modelSellerSettings.getMinDesiredUtil(), provisionedTraderSettings.getMinDesiredUtil(), TestUtils.FLOATING_POINT_DELTA);
        assertTrue(provisionedTraderSettings.isSuspendable());
        assertEquals(modelSellerSettings.isGuaranteedBuyer(), provisionedTraderSettings.isGuaranteedBuyer());
        assertFalse(provisionedTraderSettings.isCloneable());

        // verify if all the modified commodities are added to commodityNewCapacityMap_
        assertEquals(provision.getCommodityNewCapacityMap().isEmpty(), !isProvisionUseful);

        double[] quoteAfter = EdeCommon.quote(economy, modelBuyer, provision.getProvisionedSeller(), 0, false)
            .getQuoteValues();
        assertTrue(Double.isFinite(quoteAfter[0]));

        assertSame(economy, provision.getEconomy());
        assertSame(modelBuyer, provision.getModelBuyer());
        assertSame(modelSeller, provision.getModelSeller());
        assertNotNull(provision.getProvisionedSeller());
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestProvisionByDemand() {
        Economy e1 = new Economy();

        ShoppingList b1 = e1.addBasketBought(e1.addTrader(0, TraderState.ACTIVE, EMPTY), EMPTY);
        ShoppingList b2 = e1.addBasketBought(e1.addTrader(0, TraderState.INACTIVE, EMPTY), EMPTY);
        ShoppingList b3 = e1.addBasketBought(e1.addTrader(0, TraderState.ACTIVE, EMPTY),
            new Basket(new CommoditySpecification(0)));
        // model that sells the commodities that the buyers shop for (though not the needed amount)
        Trader model = e1.addTrader(
                0,
                TraderState.ACTIVE,
                new Basket(new CommoditySpecification(0),
                new CommoditySpecification(1))
        );
        model.getCommoditiesSold().get(0).setCapacity(75).setQuantity(0);
        model.getCommoditiesSold().get(1).setCapacity(75).setQuantity(0);
        model.setDebugInfoNeverUseInCode(DEBUG_INFO);
        b3.setQuantity(0, 100).setPeakQuantity(0, 6.5);

        ShoppingList b4 = e1.addBasketBought(e1.addTrader(0, TraderState.ACTIVE, EMPTY),
            new Basket(new CommoditySpecification(0), new CommoditySpecification(1)));
        b4.setQuantity(0, 2.2).setPeakQuantity(0, 6.5);
        b4.setQuantity(1, 100).setPeakQuantity(1, 101.3);

        return new Object[][] {{e1, b1, model, false}, {e1, b2, model, false}, {e1, b3, model, true}, {e1, b4, model, true}};
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
        Trader model = e1.addTrader(0, TraderState.ACTIVE, EMPTY, EMPTY);
        oids.put(b1.getBuyer(), "id1");
        oids.put(b2.getBuyer(), "id2");

        return new Object[][]{
                        {new ProvisionByDemand(e1, b1, model), oid,
                                        "<action type=\"provisionByDemand\" modelBuyer=\"id1\" />"},
                        {new ProvisionByDemand(e1, b2, model), oid,
                                        "<action type=\"provisionByDemand\" modelBuyer=\"id2\" />"}
        };
    }

    @Test
    @Parameters(method = "parametersForTestProvisionByDemand")
    @TestCaseName("Test #{index}: new ProvisionByDemand({0},{1},{2}).take().rollback()")
    public final void testTakeRollback(@NonNull Economy economy, @NonNull ShoppingList modelBuyer, @NonNull Trader modelSeller
                    , boolean isProvisionUseful) {
        final int oldSize = economy.getTraders().size();
        @NonNull ProvisionByDemand provision = new ProvisionByDemand(economy, modelBuyer, modelSeller);

        assertEquals(modelSeller, provision.getModelSeller());
        assertSame(provision, provision.take());
        Trader provisionedSeller = provision.getProvisionedSeller();
        assertNotNull(provisionedSeller);
        assertTrue(provisionedSeller.getState().isActive());
        assertEquals(provisionedSeller.getDebugInfoNeverUseInCode(),
                DEBUG_INFO + " clone #" + provisionedSeller.getEconomyIndex());
        assertTrue(economy.getTraders().contains(provisionedSeller));
        assertEquals(oldSize+1, economy.getTraders().size());
        assertTrue(EdeCommon.quote(economy, modelBuyer, provisionedSeller, Double.POSITIVE_INFINITY, false)
            .getQuoteValue() < Double.POSITIVE_INFINITY);
        // assert that it can fit.

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
    public final void testDebugDescription(@NonNull ProvisionByDemand provision, @NonNull LegacyTopology topology,
                                           @NonNull String description) {
        assertEquals(description, provision.debugDescription(topology.getUuids()::get,
            topology.getNames()::get, topology.getCommodityTypes()::getName, topology.getTraderTypes()::getName));
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestDebugDescription() {
        @NonNull LegacyTopology topology1 = new LegacyTopology();

        Trader t1 = topology1.addTrader("id1", "name1", "VM",TraderState.ACTIVE, Arrays.asList());
        ShoppingList b1 = topology1.addBasketBought(t1, Arrays.asList());
        Trader t2 = topology1.addTrader("id2", "name2", "Container", TraderState.INACTIVE, Arrays.asList());
        ShoppingList b2 = topology1.addBasketBought(t2, Arrays.asList("CPU"));
        Trader t3 = topology1.addTrader("id3", "name3", "VM", TraderState.ACTIVE, Arrays.asList());
        Trader t4 = topology1.addTrader("id4", "name4", "Container", TraderState.ACTIVE,
                        Arrays.asList());

        return new Object[][]{
                        {new ProvisionByDemand((Economy)topology1.getEconomy(), b1, t3), topology1,
                "Provision a new VM with the following characteristics: "},
                        {new ProvisionByDemand((Economy)topology1.getEconomy(), b2, t4), topology1,
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

        Trader t1 = topology1.addTrader("id1", "VM1", "VM", TraderState.ACTIVE, Arrays.asList());
        ShoppingList b1 = topology1.addBasketBought(t1, Arrays.asList());
        Trader t2 = topology1.addTrader("id2", "Container1", "Container", TraderState.INACTIVE, Arrays.asList());
        ShoppingList b2 = topology1.addBasketBought(t2, Arrays.asList("CPU"));
        Trader t3 = topology1.addTrader("id3", "VM2", "VM", TraderState.ACTIVE, Arrays.asList());
        Trader t4 = topology1.addTrader("id4", "Container2", "Container", TraderState.ACTIVE,
                        Arrays.asList());
        return new Object[][]{
                        {new ProvisionByDemand((Economy)topology1.getEconomy(), b1, t3), topology1,
                "No VM has enough capacity for VM1 [id1] (#0)."},
                        {new ProvisionByDemand((Economy)topology1.getEconomy(), b2, t4), topology1,
                "No Container has enough capacity for Container1 [id2] (#1)."},
            // TODO: update test when we figure out how to get correct type!
        };
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

        ProvisionByDemand provisionByDemand1 = new ProvisionByDemand(e, shop1, t4);
        ProvisionByDemand provisionByDemand2 = new ProvisionByDemand(e, shop2, t4);
        ProvisionByDemand provisionByDemand3 = new ProvisionByDemand(e, shop1, t4);
        return new Object[][] {{provisionByDemand1, provisionByDemand2, false},
                        {provisionByDemand1, provisionByDemand3, true}};
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: equals and hashCode for {0}, {1} == {2}")
    public final void testEquals_and_HashCode(@NonNull ProvisionByDemand provisionByDemand1,
                    @NonNull ProvisionByDemand provisionByDemand2,
                    boolean expect) {
        assertEquals(expect, provisionByDemand1.equals(provisionByDemand2));
        assertEquals(expect, provisionByDemand1.hashCode() == provisionByDemand2.hashCode());
    }

    @Test
    public final void testTake_checkModelSellerWithCliques() {
        Set<Long> cliques = new HashSet<>(Arrays.asList(0l));
        Economy e = new Economy();
        Basket b1 = new Basket(new CommoditySpecification(100));
        Basket b2 = new Basket(new CommoditySpecification(200));
        Trader t1 = e.addTrader(0, TraderState.ACTIVE, b1, b2);
        Trader t2 = e.addTrader(0, TraderState.ACTIVE, b2, cliques);
        ShoppingList shop1 = e.addBasketBought(t1, b2);
        shop1.move(t2);
        e.populateMarketsWithSellersAndMergeConsumerCoverage();

        Action action = new ProvisionByDemand(e, shop1, t2).take();
        // check cliques are cloned
        assertEquals(cliques, ((ProvisionByDemand)action).getProvisionedSeller().getCliques());
    }

    /**
     * We set up a shopping list request 100 units of commSpec0. There is a model seller which sells
     * 75 units of commSpec0, 75 units of commSpec1 and 5 units of commSpec2.
     * The test checks that the newly provisioned seller sells greater than 75 units of commSpec0.
     * The remaining commSpecs (1 and 2) should be the same capacity as of model seller.
     *
     */
    @Test
    public void testCapacitiesOfProvSeller() {
        //Test setup
        Economy e1 = new Economy();
        ShoppingList sl = e1.addBasketBought(e1.addTrader(0, TraderState.ACTIVE, EMPTY),
                new Basket(new CommoditySpecification(0)));
        sl.setQuantity(0, 100).setPeakQuantity(0, 100);
        Trader modelSeller = e1.addTrader(0, TraderState.ACTIVE,
                new Basket(new CommoditySpecification(0),
                        new CommoditySpecification(1),
                        new CommoditySpecification(2))
        );
        modelSeller.getCommoditiesSold().get(0).setCapacity(75).setQuantity(0);
        modelSeller.getCommoditiesSold().get(1).setCapacity(75).setQuantity(0);
        modelSeller.getCommoditiesSold().get(2).setCapacity(5).setQuantity(0);
        modelSeller.setDebugInfoNeverUseInCode(DEBUG_INFO);
        // call provision by demand
        ProvisionByDemand provision = (ProvisionByDemand)(new
                ProvisionByDemand(e1, sl, modelSeller)).take();
        Trader provSeller = provision.getProvisionedSeller();
        //Asserts
        assertEquals(true,provSeller.getCommoditiesSold().get(0).getCapacity() >
                modelSeller.getCommoditiesSold().get(0).getCapacity());
        assertEquals(modelSeller.getCommoditiesSold().get(1).getCapacity(),
                provSeller.getCommoditiesSold().get(1).getCapacity(), TestUtils.FLOATING_POINT_DELTA);
        assertEquals(modelSeller.getCommoditiesSold().get(2).getCapacity(),
                provSeller.getCommoditiesSold().get(2).getCapacity(), TestUtils.FLOATING_POINT_DELTA);
    }

    /**
     * Check to see that we specify ProvisionByDemand reason commodity in map when peak overhead
     * being high is what leads to the provision by demand.
     */
    @Test
    public void testPeakOverHeadReason() {
        double oldCap = 100;
        double highPeak = 150;
        CommoditySpecification cs = new CommoditySpecification(0);
        Economy e = new Economy();
        e.getCommsToAdjustOverhead().add(cs);
        ShoppingList sl = e.addBasketBought(e.addTrader(0, TraderState.ACTIVE, EMPTY),
                new Basket(cs));
        sl.setQuantity(0, 10).setPeakQuantity(0, 20);
        Trader seller = e.addTrader(0, TraderState.ACTIVE,
                new Basket(cs));
        seller.getCommoditiesSold().get(0).setCapacity(oldCap).setQuantity(20)
            .setPeakQuantity(highPeak);
        ProvisionByDemand provision = new ProvisionByDemand(e, sl, seller);
        assertTrue(provision.getCommodityNewCapacityMap().isEmpty());
        provision.take();
        assertTrue(!provision.getCommodityNewCapacityMap().isEmpty());
        double newCap = provision.getCommodityNewCapacityMap().get(0);
        assertTrue(newCap > oldCap);
    }
} // end ProvisionByDemandTest class
