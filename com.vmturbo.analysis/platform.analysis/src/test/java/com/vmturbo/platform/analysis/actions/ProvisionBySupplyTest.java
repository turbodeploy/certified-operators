package com.vmturbo.platform.analysis.actions;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import com.vmturbo.platform.analysis.testUtilities.TestUtils;
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
    private static final CommoditySpecification STORAGE_AMOUNT = new CommoditySpecification(2);

    private static final Basket EMPTY = new Basket();
    private static final Basket[] basketsSold = {EMPTY, new Basket(VCPU), new Basket(VCPU, VMEM),
                    new Basket(VCPU, VMEM, STORAGE_AMOUNT)};
    private static final Basket[] basketsBought = {EMPTY, new Basket(CPU), new Basket(CPU, MEM),
                    new Basket(STORAGE_AMOUNT)};

    private static final String DEBUG_INFO = "trader name";

    // Methods

    @Test
    @Parameters
    @TestCaseName("Test #{index}: new ProvisionBySupply({0},{1})")
    public final void testProvisionBySupply(@NonNull Economy economy, @NonNull Trader modelSeller) {
        @NonNull ProvisionBySupply provision = new ProvisionBySupply(economy, modelSeller, CPU);

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
        e1.populateMarketsWithSellersAndMergeConsumerCoverage();

        return testCases.toArray();
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestWithGuaranteedBuyer() {
        Economy e1 = new Economy();

        Trader modelSeller = e1.addTrader(0, TraderState.ACTIVE, basketsSold[1], basketsBought[0]);
        Trader b1 = e1.addTrader(1, TraderState.ACTIVE, basketsSold[1]);
        b1.getSettings().setGuaranteedBuyer(true);
        ShoppingList s1 = e1.addBasketBought(b1, basketsSold[1]);
        s1.move(modelSeller);
        e1.populateMarketsWithSellersAndMergeConsumerCoverage();

        return new Object[]{e1, modelSeller};
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestWithResizeThroughSupplier() {
        Economy e1 = new Economy();

        Trader modelSeller = e1.addTrader(0, TraderState.ACTIVE, basketsSold[3], basketsBought[0]);
        Trader b1 = e1.addTrader(1, TraderState.ACTIVE, basketsSold[3]);
        b1.getSettings().setResizeThroughSupplier(true);
        ShoppingList s1 = e1.addBasketBought(b1, basketsSold[3]);
        s1.move(modelSeller);

        Trader modelSeller2 = e1.addTrader(0, TraderState.ACTIVE, basketsSold[3], basketsBought[0]);
        Trader b2 = e1.addTrader(1, TraderState.ACTIVE, basketsBought[3]);
        b2.getSettings().setResizeThroughSupplier(true);
        ShoppingList s2 = e1.addBasketBought(b2, basketsBought[3]);
        s2.move(modelSeller2);
        e1.populateMarketsWithSellersAndMergeConsumerCoverage();
        return new Object[]{e1, modelSeller, modelSeller2};
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
        CommoditySpecification commSpec = CPU;
        Trader t2 = e1.addTrader(0, TraderState.INACTIVE, EMPTY, EMPTY);
        e1.populateMarketsWithSellersAndMergeConsumerCoverage();

        oids.put(t1, "id1");
        oids.put(t2, "id2");

        return new Object[][]{
            {new ProvisionBySupply(e1, t1, commSpec), oid, "<action type=\"provisionBySupply\" modelSeller=\"id1\" />"},
            {new ProvisionBySupply(e1, t2, commSpec), oid, "<action type=\"provisionBySupply\" modelSeller=\"id2\" />"}
        };
    }

    @Test
    @Parameters(method = "parametersForTestProvisionBySupply")
    @TestCaseName("Test #{index}: new ProvisionBySupply({0},{1}).take().rollback()")
    public final void testTakeRollback(@NonNull Economy economy, @NonNull Trader modelSeller) {
        final int oldSize = economy.getTraders().size();
        @NonNull ProvisionBySupply provision = new ProvisionBySupply(economy, modelSeller, CPU);
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
        topology1.populateMarketsWithSellersAndMergeConsumerCoverage();

        return new Object[][]{
            {new ProvisionBySupply((Economy)topology1.getEconomy(), t1, CPU), topology1,
                "Provision a new VM similar to VM1 [id1] (#0)."},
            {new ProvisionBySupply((Economy)topology1.getEconomy(), t2, CPU), topology1,
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
        topology1.populateMarketsWithSellersAndMergeConsumerCoverage();

        return new Object[][]{
            {new ProvisionBySupply((Economy)topology1.getEconomy(), t1, CPU), topology1,
                "No VM has enough leftover capacity for [buyer]."},
            {new ProvisionBySupply((Economy)topology1.getEconomy(), t2, CPU), topology1,
                "No Container has enough leftover capacity for [buyer]."},
            // TODO: update test when we figure out how to get correct type!
        };
    }

    @Test
    @Parameters(method = "parametersForTestWithGuaranteedBuyer")
    @TestCaseName("Test #{index}: new ProvisionBySupply({0},{1}).take().rollback()")
    public final void testTakeRollbackWithGuranteedBuyer(@NonNull Economy economy, @NonNull Trader modelSeller) {
        @NonNull ProvisionBySupply provision = new ProvisionBySupply(economy, modelSeller, CPU);
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

    @Test
    @Parameters(method = "parametersForTestWithResizeThroughSupplier")
    @TestCaseName("Test #{index}: GenerateResizeThroughSupplier")
    public final void testWithResizeThroughSupplier(@NonNull Economy economy,
                    @NonNull Trader modelSeller, @NonNull Trader modelSeller2) {
        @NonNull ProvisionBySupply provision1 = (ProvisionBySupply) new ProvisionBySupply(economy,
                        modelSeller, CPU).take();
        assertTrue(provision1.getSubsequentActions().stream().filter(a -> a instanceof Resize)
                        .count() == 3);
        assertTrue(provision1.getSubsequentActions().stream().filter(a -> a instanceof Resize)
                        .filter(resize -> ((Resize)resize).getResizeTriggerTraders().get(modelSeller)
                                        .contains(CPU.getBaseType())).count() == 3);
        @NonNull ProvisionBySupply provision2 = (ProvisionBySupply) new ProvisionBySupply(economy,
                        modelSeller, STORAGE_AMOUNT).take();
        assertTrue(provision2.getSubsequentActions().stream().filter(a -> a instanceof Resize)
                        .count() == 3);
        assertTrue(provision2.getSubsequentActions().stream().filter(a -> a instanceof Resize)
                        .filter(resize -> ((Resize)resize).getResizeTriggerTraders().get(modelSeller)
                                        .contains(STORAGE_AMOUNT.getBaseType())).count() == 3);
        @NonNull ProvisionBySupply provision3 = (ProvisionBySupply) new ProvisionBySupply(economy,
                        modelSeller2, CPU).take();
        assertTrue(provision3.getSubsequentActions().stream().filter(a -> a instanceof Resize)
                        .count() == 1);
        assertTrue(provision3.getSubsequentActions().stream().filter(a -> a instanceof Resize)
                        .filter(resize -> ((Resize)resize).getResizeTriggerTraders().get(modelSeller2)
                                        .contains(CPU.getBaseType())).count() == 1);
        @NonNull ProvisionBySupply provision4 = (ProvisionBySupply) new ProvisionBySupply(economy,
                        modelSeller2, STORAGE_AMOUNT).take();
        assertTrue(provision4.getSubsequentActions().stream().filter(a -> a instanceof Resize)
                        .count() == 1);
        assertTrue(provision4.getSubsequentActions().stream().filter(a -> a instanceof Resize)
                        .filter(resize -> ((Resize)resize).getResizeTriggerTraders().get(modelSeller2)
                                        .contains(STORAGE_AMOUNT.getBaseType())).count() == 1);
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
        e.populateMarketsWithSellersAndMergeConsumerCoverage();

        ProvisionBySupply provisionBySupply1 = new ProvisionBySupply(e, t2, CPU);
        ProvisionBySupply provisionBySupply2 = new ProvisionBySupply(e, t2, CPU);
        ProvisionBySupply provisionBySupply3 = new ProvisionBySupply(e, t3, CPU);
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

        Action action = new ProvisionBySupply(e, t2, CPU).take();
        // check cliques are cloned
        assertEquals(cliques, ((ProvisionBySupply)action).getProvisionedSeller().getCliques());
    }

    /**
     * Case: One vapp consume on two apps, each hosted by a container.
     * Each app sells 150 transactions. The vapp requests 150 transaction
     * from each app.
     * Expected: if provisionBySupply of app occurs, then one container should also be cloned to host
     * the new app. Vapp requests 100 transaction from each of the three apps.
     * If the provisionBySuply is rolled back, then economy contains original traders
     * and vapp requests 150 from each of the two apps.
     */
    @Test
    public void testTakeAndRollback_provisionBySupplyWithGuaranteedBuyer() {
       Economy e = new Economy();
       Basket b1 = new Basket(TestUtils.VCPU);
       Basket b2 = new Basket(TestUtils.TRANSACTION);
       Trader c1 = TestUtils.createTrader(e, TestUtils.CONTAINER_TYPE, Arrays.asList(0l),
                        Arrays.asList(TestUtils.VCPU),
                        new double[]{200}, true, false);
       Trader c2 = TestUtils.createTrader(e, TestUtils.CONTAINER_TYPE, Arrays.asList(0l),
                                          Arrays.asList(TestUtils.VCPU),
                                          new double[]{200}, true, false);
       Trader app1 = TestUtils.createTrader(e, TestUtils.APP_TYPE, Arrays.asList(0l),
                                            Arrays.asList(TestUtils.TRANSACTION),
                                            new double[]{150}, true, false);
       app1.getSettings().setProviderMustClone(true);
       ShoppingList sl1 = e.addBasketBought(app1, b1);
       TestUtils.moveSlOnSupplier(e, sl1, c1, new double[]{70});
       Trader app2 = TestUtils.createTrader(e, TestUtils.APP_TYPE, Arrays.asList(0l),
                                            Arrays.asList(TestUtils.TRANSACTION),
                                            new double[]{150}, true, false);
       app2.getSettings().setProviderMustClone(true);
       ShoppingList sl2 = e.addBasketBought(app2, b1);
       TestUtils.moveSlOnSupplier(e, sl2, c2, new double[]{70});
       Trader vapp = TestUtils.createTrader(e, TestUtils.VAPP_TYPE, Arrays.asList(0l),
                                            Arrays.asList(), new double[]{}, true, true);
       ShoppingList sl3 = e.addBasketBought(vapp, b2);
       TestUtils.moveSlOnSupplier(e, sl3, app1, new double[]{150});
       ShoppingList sl4 = e.addBasketBought(vapp, b2);
       TestUtils.moveSlOnSupplier(e, sl4, app2, new double[]{150});

       ProvisionBySupply provision1 = (ProvisionBySupply)new ProvisionBySupply(e, app1, CPU).take();

       assertTrue(e.getTraders().size() == 7);
       assertTrue(e.getMarketsAsBuyer(vapp).keySet().size() == 3);
       assertTrue(e.getTraders().stream().filter(t -> t.getType() == TestUtils.CONTAINER_TYPE)
                  .count() == 3);
       assertTrue(e.getTraders().stream().filter(t -> t.getType() == TestUtils.APP_TYPE)
                  .count() == 3);
       assertTrue(provision1.getActionTarget().getCustomers().size() == 1);
       assertEquals(100.0, provision1.getActionTarget().getCustomers().get(0).getQuantities()[0], TestUtils.FLOATING_POINT_DELTA);
       assertEquals(100.0, sl3.getQuantities()[0], TestUtils.FLOATING_POINT_DELTA);
       assertEquals(100.0, sl4.getQuantities()[0], TestUtils.FLOATING_POINT_DELTA);

       provision1.rollback();
       assertTrue(e.getTraders().size() == 5);
       assertTrue(e.getMarketsAsBuyer(vapp).keySet().size() == 2);
       assertTrue(e.getTraders().stream().filter(t -> t.getType() == TestUtils.CONTAINER_TYPE)
                  .count() == 2);
       assertTrue(e.getTraders().stream().filter(t -> t.getType() == TestUtils.APP_TYPE)
                  .count() == 2);
       assertTrue(e.getMarketsAsBuyer(vapp).keySet().stream()
                  .allMatch(sl -> sl.getQuantities()[0] == 150));
    }

    /**
     * Case: Two containers on one pm, each container hosts one app.Both containers sell
     * 100 VCPU and buy 100 CPU from pm. The pm sells only 250 CPU.
     *
     * Expected: if provisionBySupply of app occurs, a container will be cloned to
     * host the app. The new container will bootstrap a new clone of pm to place it.
     * If provisionBySupply is rolled back, economy contains original traders.
     */

    @Test
    public void testTakeAndRollback_provisionBySupplyWithUnplacedClones() {
        Economy e = new Economy();
        Basket b1 = new Basket(TestUtils.VCPU);
        Basket b2 = new Basket(TestUtils.CPU);
        Trader pm1 = TestUtils.createTrader(e, TestUtils.PM_TYPE, Arrays.asList(0l),
                                           Arrays.asList(TestUtils.CPU),
                                           new double[]{250}, true, false);
        Trader c1 = TestUtils.createTrader(e, TestUtils.CONTAINER_TYPE, Arrays.asList(0l),
                                           Arrays.asList(TestUtils.VCPU),
                                           new double[]{100}, true, false);
        ShoppingList sl1 = e.addBasketBought(c1, b2);
        TestUtils.moveSlOnSupplier(e, sl1, pm1, new double[]{100});
        Trader c2 = TestUtils.createTrader(e, TestUtils.CONTAINER_TYPE, Arrays.asList(0l),
                                           Arrays.asList(TestUtils.VCPU),
                                           new double[]{100}, true, false);
        ShoppingList sl2 = e.addBasketBought(c2, b2);
        TestUtils.moveSlOnSupplier(e, sl2, pm1, new double[]{100});
        Trader app1 = TestUtils.createTrader(e, TestUtils.APP_TYPE, Arrays.asList(0l),
                                             Arrays.asList(TestUtils.TRANSACTION),
                                             new double[]{50}, true, false);
        app1.getSettings().setProviderMustClone(true);
        ShoppingList sl3 = e.addBasketBought(app1, b1);
        TestUtils.moveSlOnSupplier(e, sl3, c1, new double[]{50});
        Trader app2 = TestUtils.createTrader(e, TestUtils.APP_TYPE, Arrays.asList(0l),
                                             Arrays.asList(TestUtils.TRANSACTION),
                                             new double[]{50}, true, false);
        app2.getSettings().setProviderMustClone(true);
        ShoppingList sl4 = e.addBasketBought(app2, b1);
        TestUtils.moveSlOnSupplier(e, sl4, c2, new double[]{50});
        e.populateMarketsWithSellersAndMergeConsumerCoverage();

        ProvisionBySupply provision = (ProvisionBySupply)new ProvisionBySupply(e, app1, CPU).take();
        // assert that clone is suspendable
        assertTrue(((ProvisionBySupply)provision).getProvisionedSeller().getSettings().isSuspendable());
        assertTrue(e.getTraders().size() == 8);
        assertTrue(e.getTraders().stream().filter(t -> t.getType() == TestUtils.CONTAINER_TYPE)
                   .count() == 3);
        assertTrue(e.getTraders().stream().filter(t -> t.getType() == TestUtils.PM_TYPE)
                   .count() == 2);

        provision.rollback();
        assertTrue(e.getTraders().size() == 5);
        assertTrue(e.getTraders().stream().filter(t -> t.getType() == TestUtils.CONTAINER_TYPE)
                   .count() == 2);
        assertTrue(e.getTraders().stream().filter(t -> t.getType() == TestUtils.PM_TYPE)
                   .count() == 1);
    }

    @Test
    public void testTakeWithCloneCommodityWithNewType() {
        Economy e = new Economy();
        int appCommBaseType = 100;
        int appCommType = 200;
        // appCommodity has cloneWithNewType = true
        CommoditySpecification appCs = new CommoditySpecification(appCommType, appCommBaseType, true);
        Basket b1 = new Basket(appCs, TestUtils.VCPU);
        Basket b2 = new Basket(TestUtils.TRANSACTION);
        Trader c1 = TestUtils.createTrader(e, TestUtils.CONTAINER_TYPE, Arrays.asList(0l),
                                           Arrays.asList(TestUtils.VCPU, appCs),
                                           new double[]{100, 1000}, true, false);
        Trader app1 = TestUtils.createTrader(e, TestUtils.APP_TYPE, Arrays.asList(0l),
                                             Arrays.asList(TestUtils.TRANSACTION),
                                             new double[]{150}, true, false);
        app1.getSettings().setProviderMustClone(true);
        // application buys the appCommodity
        ShoppingList sl1 = e.addBasketBought(app1, b1);
        TestUtils.moveSlOnSupplier(e, sl1, c1, new double[]{70, 1});
        Trader vapp = TestUtils.createTrader(e, TestUtils.VAPP_TYPE, Arrays.asList(0l),
                                             Arrays.asList(), new double[]{}, true, true);
        ShoppingList sl2 = e.addBasketBought(vapp, b2);
        TestUtils.moveSlOnSupplier(e, sl2, app1, new double[]{70});
        e.populateMarketsWithSellersAndMergeConsumerCoverage();

        ProvisionBySupply provision = (ProvisionBySupply)new ProvisionBySupply(e, app1, CPU).take();
        boolean findAppCommInContainerClone = false;
        for (Action a : provision.getSubsequentActions()) {
            if (a instanceof ProvisionBySupply) {
                // clone of container hosting the new application clone should have a different appComm
                // type but same base type
                for (CommoditySpecification cs : a.getActionTarget().getBasketSold()) {
                    if (cs.getBaseType() == appCommBaseType) {
                        findAppCommInContainerClone = true;
                        assertTrue(cs.getType() != appCommType);
                    } else {
                        assertTrue(((ProvisionBySupply)a).getModelSeller().getBasketSold().contains(cs));
                    }
                }
            }
        }
        assertTrue(findAppCommInContainerClone);
    }
} // end ProvisionBySupplyTest class
