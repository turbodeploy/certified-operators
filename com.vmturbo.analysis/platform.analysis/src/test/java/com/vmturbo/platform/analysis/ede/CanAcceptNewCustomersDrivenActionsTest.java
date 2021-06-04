package com.vmturbo.platform.analysis.ede;

import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.Deactivate;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ledger.Ledger;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;
import com.vmturbo.platform.analysis.updatingfunction.UpdatingFunctionFactory;

/**
 * A collection of tests to evaluate the decisions for entities with canAcceptNewCustomers true or false.
 */
public class CanAcceptNewCustomersDrivenActionsTest {

    /*
     *
     *   When canAcceptNewCustomers is false, we arent going to bring in new customers.
     *   We are fine with moving consumers out. However we do not force evacuations.
     *   Such a trader can suspend (when supendable)
     *   Such a trader cannot clone (even when clonable) - because when we clone, what does it mean to move customers into a clone with sellerAvailableForPlacement false.
     */
    Economy testEconomy;
    Trader consumer;
    Trader targetSeller;
    Trader seller2;
    Trader seller1;
    Trader smallSeller;
    Ledger ledger;
    CommoditySpecification commodity = new CommoditySpecification(0).setDebugInfoNeverUseInCode("COMM");

    private static final double RIGHT_SIZE_LOWER = 0.3;
    private static final double RIGHT_SIZE_UPPER = 0.7;

    private static final Logger logger = LogManager.getLogger(UpdatingFunctionFactory.class);

    /**
     * Create a Economy during setup.
     */
    @Before
    public void setUp() {
        testEconomy = new Economy();
    }

    /**
     * Test that we dont provision trader that cannotAcceptNewCustomers due to RoI.
     */
    @Test
    public void testProvision() {
        testEconomy.getSettings().setEstimatesEnabled(false);
        // Create targetSeller
        targetSeller = TestUtils.createTrader(testEconomy, 1,
                Arrays.asList(0L), Arrays.asList(commodity),
                new double[]{100}, true, false);
        targetSeller.getSettings().setCanAcceptNewCustomers(false);
        targetSeller.setDebugInfoNeverUseInCode("TARGET_SELLER");
        targetSeller.getSettings().setMinDesiredUtil(0.65).setMaxDesiredUtil(0.75);
        targetSeller.getCommoditiesSold().stream().forEach(cs -> cs.getSettings().setResizable(false));

        // Create consumer
        consumer = TestUtils.createTrader(testEconomy, 2,
                Arrays.asList(0L), Lists.newArrayList(),
                new double[]{}, false, false);
        consumer.getSettings().setMinDesiredUtil(0.65).setMaxDesiredUtil(0.75);
        consumer.setDebugInfoNeverUseInCode("CONSUMER");

        // place sl1 of consumer on targetSeller
        ShoppingList sl1 = TestUtils.createAndPlaceShoppingList(testEconomy,
                Arrays.asList(commodity), consumer,
                new double[]{40}, targetSeller).setMovable(true);

        // place sl2 of consumer on targetSeller
        TestUtils.createAndPlaceShoppingList(testEconomy,
                Arrays.asList(commodity), consumer,
                new double[]{40}, targetSeller).setMovable(true);

        // reduce capacity of commodity sold by targetSeller
        targetSeller.getCommoditySold(commodity).getSettings().setUtilizationUpperBound(0.7);

        // Create another seller and consumer to force provisioning
        // seller with canAcceptNewCustomers = True
        seller1 = TestUtils.createTrader(testEconomy, 1,
                Arrays.asList(0L), Arrays.asList(commodity),
                new double[]{100}, true, false);
        seller1.setDebugInfoNeverUseInCode("CO-SELLER");
        seller1.getSettings().setMinDesiredUtil(0.65).setMaxDesiredUtil(0.75);
        seller1.getCommoditiesSold().stream().forEach(cs -> cs.getSettings().setResizable(false));

        // place sl2 of consumer on seller1
        TestUtils.createAndPlaceShoppingList(testEconomy,
                Arrays.asList(commodity), consumer,
                new double[]{40}, seller1).setMovable(true);

        ledger = new Ledger(testEconomy);

        testEconomy.populateMarketsWithSellersAndMergeConsumerCoverage();

        // do not provision targetSeller as clone wont accept new customers
        List<Action> actions = Provision.provisionDecisions(testEconomy, ledger);

        Assert.assertTrue(!actions.stream().filter(a -> a.getActionTarget() == targetSeller).findAny().isPresent());

    }

    /**
     * Test that we dont move to trader with canAcceptNewCustomers=false.
     */
    @Test
    public void testMove() {
        // Create targetSeller
        targetSeller = TestUtils.createTrader(testEconomy, 1,
                Arrays.asList(0L), Arrays.asList(commodity),
                new double[]{100}, true, false);
        targetSeller.getSettings().setCanAcceptNewCustomers(false);
        targetSeller.setDebugInfoNeverUseInCode("TARGET_SELLER");
        targetSeller.getSettings().setMinDesiredUtil(0.65).setMaxDesiredUtil(0.75);
        targetSeller.getCommoditiesSold().stream().forEach(cs -> cs.getSettings().setResizable(false));

        // seller with canAcceptNewCustomers = True
        seller1 = TestUtils.createTrader(testEconomy, 1,
                Arrays.asList(0L), Arrays.asList(commodity),
                new double[]{100}, true, false);
        seller1.setDebugInfoNeverUseInCode("TARGET_SELLER");
        seller1.getSettings().setMinDesiredUtil(0.65).setMaxDesiredUtil(0.75);
        seller1.getCommoditiesSold().stream().forEach(cs -> cs.getSettings().setResizable(false));

        // Create consumer
        consumer = TestUtils.createTrader(testEconomy, 2,
                Arrays.asList(0L), Lists.newArrayList(),
                new double[]{}, false, false);
        consumer.getSettings().setMinDesiredUtil(0.65).setMaxDesiredUtil(0.75);
        consumer.setDebugInfoNeverUseInCode("CONSUMER");

        // place consumer on targetSeller
        ShoppingList sl = TestUtils.createAndPlaceShoppingList(testEconomy,
                Arrays.asList(commodity), consumer,
                new double[]{70}, targetSeller).setMovable(true);

        ledger = new Ledger(testEconomy);

        testEconomy.populateMarketsWithSellersAndMergeConsumerCoverage();

        // verify no moves were generated during shopAlone
        Assert.assertEquals(0, Placement.generatePlacementDecisions(testEconomy, Lists.newArrayList(sl)).getActions().size());

        consumer.getSettings().setIsShopTogether(true);
        // verify no moves were generated during shopTogether
        Assert.assertEquals(0, Placement.generatePlacementDecisions(testEconomy, Lists.newArrayList(sl)).getActions().size());

        // validate consumer would move if there was a larger seller that will give a cheaper quote.
        seller1.getCommoditySold(commodity).setCapacity(200);

        // verify 1 moves was generated during shopAlone
        consumer.getSettings().setIsShopTogether(false);
        List<Action> actions = Placement.generatePlacementDecisions(testEconomy, Lists.newArrayList(sl)).getActions();
        Assert.assertEquals(1, actions.size());

        // rollback action
        actions.get(0).rollback();

        // verify 1 move was generated during shopTogether
        consumer.getSettings().setIsShopTogether(true);
        actions = Placement.generatePlacementDecisions(testEconomy, Lists.newArrayList(sl)).getActions();
        Assert.assertEquals(1, actions.size());
        Assert.assertEquals(sl.getSupplier(), seller1);

        actions.get(0).rollback();

        // Deactivate the large seller and add another seller that is the same in size as the largeSeller
        // (however marked canAcceptNewCustomer=false) and validate that we dont move.
        // seller with canAcceptNewCustomers = False
        new Deactivate(testEconomy, seller1, seller1.getBasketSold()).take();
        seller2 = TestUtils.createTrader(testEconomy, 1,
                Arrays.asList(0L), Arrays.asList(commodity),
                new double[]{200}, true, false);
        seller2.setDebugInfoNeverUseInCode("TARGET_SELLER");
        seller2.getSettings().setMinDesiredUtil(0.65).setMaxDesiredUtil(0.75).setCanAcceptNewCustomers(false);
        seller2.getCommoditiesSold().stream().forEach(cs -> cs.getSettings().setResizable(false));

        // verify no moves were generated during shopAlone
        Assert.assertEquals(0, Placement.generatePlacementDecisions(testEconomy, Lists.newArrayList(sl)).getActions().size());
    }

    /**
     * Test that we dont provision during bootstrap.
     */
    @Test
    public void testBootstrap() {
        testEconomy.getSettings().setEstimatesEnabled(false);
        // Create targetSeller
        targetSeller = TestUtils.createTrader(testEconomy, 1,
                Arrays.asList(0L), Arrays.asList(commodity),
                new double[]{100}, true, false);
        targetSeller.getSettings().setCanAcceptNewCustomers(false);
        targetSeller.setDebugInfoNeverUseInCode("TARGET_SELLER");
        targetSeller.getSettings().setMinDesiredUtil(0.65).setMaxDesiredUtil(0.75);
        targetSeller.getCommoditiesSold().stream().forEach(cs -> cs.getSettings().setResizable(false));

        // Create consumer
        consumer = TestUtils.createTrader(testEconomy, 2,
                Arrays.asList(0L), Lists.newArrayList(),
                new double[]{}, false, false);
        consumer.getSettings().setMinDesiredUtil(0.65).setMaxDesiredUtil(0.75);
        consumer.setDebugInfoNeverUseInCode("CONSUMER");

        // place sl1 of consumer on targetSeller
        ShoppingList sl1 = TestUtils.createAndPlaceShoppingList(testEconomy,
                Arrays.asList(commodity), consumer,
                new double[]{40}, targetSeller).setMovable(true);

        // place sl2 of consumer on targetSeller
        TestUtils.createAndPlaceShoppingList(testEconomy,
                Arrays.asList(commodity), consumer,
                new double[]{40}, targetSeller).setMovable(true);

        // reduce capacity of commodity sold by targetSeller
        targetSeller.getCommoditySold(commodity).getSettings().setUtilizationUpperBound(0.7);

        testEconomy.populateMarketsWithSellersAndMergeConsumerCoverage();

        Assert.assertTrue(BootstrapSupply.bootstrapSupplyDecisions(testEconomy).isEmpty());
    }
}

