package com.vmturbo.platform.analysis.ede;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.Deactivate;
import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySoldSettings;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.ledger.Ledger;
import com.vmturbo.platform.analysis.pricefunction.PriceFunction;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;

/**
 * Tests for the Suspension class.
 */
public class SuspensionTest {
    private static final int VM_TYPE = 0;
    private static final int PM_TYPE = 1;
    private static final double CAPACITY = 111;
    private static final double UTILIZATION_UPPER_BOUND = 0.9;

    // CommoditySpecifications to use in tests
    private static final CommoditySpecification CPU = TestUtils.CPU;
    private static final CommoditySpecification MEM = TestUtils.MEM;

    // Baskets to use in tests
    private static final Basket EMPTY = new Basket();
    private static final Basket PM_SMALL = new Basket(CPU, MEM);
    private static final Basket APPLICATION_BASKET =
        new Basket(TestUtils.RESPONSE_TIME, TestUtils.TRANSACTION);
    private static final Basket CONTAINER_BASKET =
        new Basket(TestUtils.VCPU, TestUtils.VMEM, TestUtils.createNewCommSpec());
    private static final Basket POD_BASKET =
        new Basket(TestUtils.VCPU, TestUtils.VMEM, TestUtils.createNewCommSpec());
    private static final Basket VM_BASKET =
        new Basket(TestUtils.VCPU, TestUtils.VMEM, TestUtils.createNewCommSpec());

    /**
     * Verify that we do not suspend non-suspendaable traders.
     */
    @Test
    public void suspensionDecisionsDontSuspendNonSuspendableTraders() {
        Economy economy = new Economy();
        // adding 2 non-suspendable traders
        economy.addTrader(PM_TYPE, TraderState.ACTIVE, PM_SMALL);
        economy.addTrader(PM_TYPE, TraderState.ACTIVE, PM_SMALL);
        economy.addTrader(VM_TYPE, TraderState.ACTIVE, EMPTY, new Basket(CPU));

        economy.populateMarketsWithSellersAndMergeConsumerCoverage();
        Ledger ledger = new Ledger(economy);
        Ede ede = new Ede();

        Suspension suspension = new Suspension();
        List<Action> suspAxns = suspension.suspensionDecisions(economy, ledger);

        assertTrue(suspAxns.isEmpty());
    }


    /**
     * Do not suspend non-suspendable traders.
     */
    @Test
    public void suspensionDecisionsDontSuspendWhenNoActiveSellers() {
        Economy economy = new Economy();
        // adding 1 non-suspendable traders
        economy.addTrader(PM_TYPE, TraderState.ACTIVE, PM_SMALL);

        Suspension suspension = new Suspension();
        // check if returned action list is empty
        assertTrue(suspension.suspensionDecisions(economy, new Ledger(economy)).isEmpty());
    }

    /**
     * Test suspending a trader after its only remaining customer moves off of it.
     */
    @Test
    public void suspensionDecisionsSuspensionActions() {
        Economy economy = new Economy();
        // adding 2 non-suspendable traders
        economy.addTrader(PM_TYPE, TraderState.ACTIVE, PM_SMALL).getSettings().setCanAcceptNewCustomers(true);
        Trader seller = economy.addTrader(PM_TYPE, TraderState.ACTIVE, PM_SMALL);
        seller.getSettings().setCanAcceptNewCustomers(true);
        seller.getCommoditiesSold().stream()
            .forEach(cs -> cs.setCapacity(CAPACITY)
                .getSettings().setUtilizationUpperBound(UTILIZATION_UPPER_BOUND));
        seller.getSettings().setSuspendable(true);

        seller.getSettings().setMinDesiredUtil(0.6);

        Trader vm = economy.addTrader(VM_TYPE, TraderState.ACTIVE, EMPTY);
        ShoppingList vmSl = economy.addBasketBought(vm, new Basket(CPU)).setMovable(true);
        for (int index = 0; index < vmSl.getBasket().size(); index++) {
            vmSl.setQuantity(index, 10);
        }
        // placing the VM on the 2nd PM. This leaves the 1st PM empty without customers and
        // hence will subsequently be suspended
        for (ShoppingList buyer : economy.getMarkets().iterator().next().getBuyers()) {
            Action axn = (new Move(economy, buyer, seller));
            axn.take();
        }

        economy.populateMarketsWithSellersAndMergeConsumerCoverage();
        Ledger ledger = new Ledger(economy);
        Ede ede = new Ede();

        Suspension suspension = new Suspension();
        List<Action> suspAxns = suspension.suspensionDecisions(economy, ledger);

        // verify that the we suspend extra seller
        assertTrue(!suspAxns.isEmpty());

    }

    /**
     * Verify that we do not suspend the only seller.
     */
    @Test
    public void suspensionDecisionsNoActions() {
        Economy economy = new Economy();
        Trader seller  = economy.addTrader(PM_TYPE, TraderState.ACTIVE, PM_SMALL);
        seller.getSettings().setSuspendable(true);
        economy.addTrader(VM_TYPE, TraderState.ACTIVE, EMPTY, new Basket(CPU));
        economy.addTrader(VM_TYPE, TraderState.ACTIVE, EMPTY, new Basket(CPU));
        economy.getMarkets().forEach(mkt -> mkt.getBuyers().stream().forEach(buyer -> buyer.move(seller)));
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();
        Ledger ledger = new Ledger(economy);
        Ede ede = new Ede();

        Suspension suspension = new Suspension();
        List<Action> suspAxns = suspension.suspensionDecisions(economy, ledger);

        assertTrue(suspAxns.isEmpty());
    }

    /**
     * adjustUtilThreshold test.
     */
    @Test
    public void adjustUtilThreshold() {
        Economy economy = new Economy();
        Trader seller = economy.addTrader(VM_TYPE, TraderState.ACTIVE, PM_SMALL);

        CommoditySoldSettings commSoldSett = seller.getCommoditiesSold().get(0).getSettings();
        commSoldSett.setUtilizationUpperBound(0.7).setOrigUtilizationUpperBound(0.7);

        seller.getSettings().setMaxDesiredUtil(0.5);

        Suspension suspension = new Suspension();
        suspension.adjustUtilThreshold(economy, true);

        // verify that the utilUpperBound has changed to maxDesiredUtil
        assertTrue(commSoldSett.getUtilizationUpperBound() == seller.getSettings().getMaxDesiredUtil()
                        * commSoldSett.getOrigUtilizationUpperBound());
        suspension.adjustUtilThreshold(economy, false);

        // verify that the utilUpperBound has changed to origUtilDesiredUtil
        assertTrue(commSoldSett.getUtilizationUpperBound() == commSoldSett.getOrigUtilizationUpperBound());

        // verify that UtilizationUpperBound does not change for commodity using constantPriceFunction
        PriceFunction pfunc = PriceFunction.Cache.createConstantPriceFunction(1.0);
        commSoldSett.setPriceFunction(pfunc);
        suspension.adjustUtilThreshold(economy, true);
        assertTrue(commSoldSett.getUtilizationUpperBound() == commSoldSett.getOrigUtilizationUpperBound());

    }

    /**
     * Verify that we have exactly one deactivate action when suspendTrader is called.
     */
    @Test
    public void suspendTrader() {
        Economy economy = new Economy();
        // add buyer and seller to create a market
        economy.addTrader(VM_TYPE, TraderState.ACTIVE, EMPTY, new Basket(CPU));
        Trader seller = economy.addTrader(VM_TYPE, TraderState.ACTIVE, PM_SMALL);
        seller.getSettings().setCanAcceptNewCustomers(true);
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        List<Action> actions = new ArrayList<>();
        Suspension suspension = new Suspension();
        suspension.suspendTrader(economy, economy.getMarketsAsSeller(seller).iterator().next(),
            seller, actions);

        // verify that we have 1 deactivate action when suspendTrader is called
        assertTrue(actions.size() == 1);
        assertTrue(actions.get(0) instanceof Deactivate);
    }

    /**
     * No seller and hence no sole provider.
     */
    @Test
    public void findSoleProvidersNoSeller() {
        //Arrange
        Economy economy = getEconomyWithGivenSellersAndBuyers(0, 1);
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        //Act
        Suspension suspension = new Suspension();
        suspension.findSoleProviders(economy);

        //Assert
        assertTrue(suspension.getSoleProviders().isEmpty());
    }

    /**
     * No buyer and hence no sole provider.
     */
    @Test
    public void findSoleProvidersNoBuyer() {
        //Arrange
        Economy economy = getEconomyWithGivenSellersAndBuyers(1, 0);
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        //Act
        Suspension suspension = new Suspension();
        suspension.findSoleProviders(economy);

        //Assert
        assertTrue(suspension.getSoleProviders().isEmpty());
    }

    /**
     * One active seller and buyer. Hence one sole provider.
     */
    @Test
    public void findSoleProvidersOneActiveSeller() {
        //Arrange
        Economy economy = getEconomyWithGivenSellersAndBuyers(1, 1);
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        //Act
        Suspension suspension = new Suspension();
        suspension.findSoleProviders(economy);

        //Assert
        assertEquals(1, suspension.getSoleProviders().size());
    }

    /**
     * Inactive seller and hence no sole provider.
     */
    @Test
    public void findSoleProvidersInactiveSeller() {
        //Arrange
        Economy economy = getEconomyWithGivenSellersAndBuyers(0, 1);
        economy.addTrader(PM_TYPE, TraderState.INACTIVE, PM_SMALL);
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        //Act
        Suspension suspension = new Suspension();
        suspension.findSoleProviders(economy);

        //Assert
        assertTrue(suspension.getSoleProviders().isEmpty());
    }

    /**
     * Inactive buyer and hence no sole provider.
     */
    @Test
    public void findSoleProvidersInActiveBuyer() {
        //Arrange
        Economy economy = getEconomyWithGivenSellersAndBuyers(1, 0);
        economy.addTrader(VM_TYPE, TraderState.INACTIVE, EMPTY, new Basket(CPU));
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        //Act
        Suspension suspension = new Suspension();
        suspension.findSoleProviders(economy);

        //Assert
        assertEquals(0, suspension.getSoleProviders().size());
    }

    /**
     * More than one seller and hence no sole provider.
     */
    @Test
    public void findSoleProvidersMoreThanOneActiveSeller() {
        //Arrange
        Economy economy = getEconomyWithGivenSellersAndBuyers(2, 1);
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        //Act
        Suspension suspension = new Suspension();
        suspension.findSoleProviders(economy);

        //Assert
        assertTrue(suspension.getSoleProviders().isEmpty());
    }

    /**
     * One seller and two buyers. Hence one sole provider.
     */
    @Test
    public void findSoleProvidersWithTwoBuyers() {
        //Arrange
        Economy economy = getEconomyWithGivenSellersAndBuyers(1, 2);
        Trader guaranteedBuyer = economy.addTrader(VM_TYPE, TraderState.ACTIVE, EMPTY, new Basket(CPU));
        guaranteedBuyer.getSettings().setGuaranteedBuyer(true);
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        //Act
        Suspension suspension = new Suspension();
        suspension.findSoleProviders(economy);

        //Assert
        assertEquals(1, suspension.getSoleProviders().size());
    }

    /**
     * Two seller and two buyers. Hence no sole provider.
     */
    @Test
    public void findSoleProvidersWithMultipleSelleraAndBuyers() {
        //Arrange
        Economy economy = getEconomyWithGivenSellersAndBuyers(2, 2);
        Trader guaranteedBuyer = economy.addTrader(VM_TYPE, TraderState.ACTIVE, EMPTY, new Basket(CPU));
        guaranteedBuyer.getSettings().setGuaranteedBuyer(true);
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        //Act
        Suspension suspension = new Suspension();
        suspension.findSoleProviders(economy);

        //Assert
        assertTrue(suspension.getSoleProviders().isEmpty());
    }

    /**
     * One seller and one guaranteed buyer. Hence sole provider.
     */
    @Test
    public void findSoleProvidersWithGuaranteedBuyer() {
        //Arrange
        Economy economy = getEconomyWithGivenSellersAndBuyers(1, 0);
        Trader buyer = economy.addTrader(VM_TYPE, TraderState.ACTIVE, EMPTY, new Basket(CPU));
        buyer.getSettings().setGuaranteedBuyer(true);
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        //Act
        Suspension suspension = new Suspension();
        suspension.findSoleProviders(economy);

        //Assert
        assertTrue(suspension.getSoleProviders().isEmpty());
    }

    /**
     * One seller and two buyers but one guaranteed. Hence one sole provider.
     */
    @Test
    public void findSoleProvidersWithTwoBuyersAndOneGuaranteed() {
        //Arrange
        Economy economy = getEconomyWithGivenSellersAndBuyers(1, 1);
        Trader guaranteedBuyer = economy.addTrader(VM_TYPE, TraderState.ACTIVE, EMPTY, new Basket(CPU));
        guaranteedBuyer.getSettings().setGuaranteedBuyer(true);
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        //Act
        Suspension suspension = new Suspension();
        suspension.findSoleProviders(economy);

        //Assert
        assertEquals(1, suspension.getSoleProviders().size());
    }

    /**
     * Ensure that there are no sole providers detected when there are multiple sellers
     * in all markets.
     */
    @Test
    public void findSoleProvidersWithMoreThanOneActiveSeller() {
        //Arrange
        Economy economy = getEconomyWithGivenSellersAndBuyers(2, 1);
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        //Act
        Suspension suspension = new Suspension();
        suspension.findSoleProviders(economy);

        //Assert
        assertEquals(0, suspension.getSoleProviders().size());
    }

    /**
     * Set one seller such that it is a sole provider in only one market.
     * Two sellers : {PM1 selling (CPU,MEM) + Storage}, {PM2 -> selling (CPU,MEM)}, and two buyers
     * {VM1 buying (CPU,MEM)}, {VM1 buying (Storage)}. Hence, PM1 sole provider of storage.
     */
    @Test
    public void findSoleProvidersSellerSoleProviderInOneMarket() {
        //Arrange
        Economy economy = new Economy();

        //sellers
        final Basket cpuMemStorageBasket = new Basket(CPU, MEM, TestUtils.ST_AMT);
        final Basket storageBasket = new Basket(TestUtils.ST_AMT);
        final Trader soleProviderForStorage =
                economy.addTrader(PM_TYPE, TraderState.ACTIVE, cpuMemStorageBasket, EMPTY);
        soleProviderForStorage.getSettings().setCanAcceptNewCustomers(true);
        economy.addTrader(PM_TYPE, TraderState.ACTIVE, PM_SMALL, EMPTY)
                    .getSettings().setCanAcceptNewCustomers(true);
        //buyers
        Trader buyerVM1 = economy.addTrader(VM_TYPE, TraderState.ACTIVE, EMPTY);
        economy.addBasketBought(buyerVM1, PM_SMALL);
        Trader buyerVM2 = economy.addTrader(VM_TYPE, TraderState.ACTIVE, EMPTY);
        economy.addBasketBought(buyerVM2, storageBasket);
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        //Act
        Suspension suspension = new Suspension();
        suspension.findSoleProviders(economy);

        //Assert
        assertEquals(1, suspension.getSoleProviders().size());
        assertTrue(suspension.getSoleProviders().contains(soleProviderForStorage));
    }


   /**
    * Creates an economy with given number of sellers (PMs) and buyers (VMs).
    * @param numSellers number of sellers to create.
    * @param numBuyers number of buyers to create.
    * @return economy with requested traders populated.
    */
    private Economy getEconomyWithGivenSellersAndBuyers(int numSellers, int numBuyers) {
        Economy economy = new Economy();
        //Add sellers
        IntStream.range(0, numSellers)
            .forEach($ -> economy.addTrader(PM_TYPE, TraderState.ACTIVE, PM_SMALL)
                        .getSettings().setCanAcceptNewCustomers(true));
        //Add buyers
        IntStream.range(0, numBuyers)
            .forEach($ -> economy.addTrader(VM_TYPE, TraderState.ACTIVE, EMPTY, new Basket(CPU)));
        return economy;
    }

    /**
     * Test suspension of orphaned daemons.  Create an App/Container/Pod/VM chain and mark the pod
     * a daemon.  Since the VM has no non-daemon customers, it should suspend, and the resulting
     * list of actions should contain four deactivates.
     */
    @Test
    public void suspendOrphans() {
        // No guaranteed buyers, two application/container/pod over a single VM.
        Economy economy = createDaemonScenario(false, 2);
        Assert.assertNotNull(economy);
        Ledger ledger = new Ledger(economy);
        Ede ede = new Ede();
        Suspension suspension = new Suspension();
        List<Action> actions = suspension.suspensionDecisions(economy, ledger);

        assertEquals(7, actions.size());  // 1 VM, 2 pods, 2 containers, and 2 apps should suspend
        assertEquals(1, actions.stream().filter(a -> a.isExecutable()).count());
        Optional<Action> vmAction = actions.stream()
                        .filter(a -> a.getActionTarget().getDebugInfoNeverUseInCode().equals("VM"))
                        .findFirst();
        assertTrue(vmAction.isPresent());
        assertTrue(vmAction.get().isExecutable());
    }

    /**
     * Same test as above, with a guaranteed buyer on top.
     */
    @Test
    public void testSuspendOrphansWithGuaranteedBuyer() {
        // No guaranteed buyers, two application/container/pod over a single VM.
        Economy economy = createDaemonScenario(true, 2);
        Assert.assertNotNull(economy);
        Ledger ledger = new Ledger(economy);
        Ede ede = new Ede();
        Suspension suspension = new Suspension();
        List<Action> actions = suspension.suspensionDecisions(economy, ledger);

        // 1 VM, 1 GB, 2 pods, 2 containers, and 2 apps should suspend
        assertEquals(8, actions.size());
        assertEquals(1, actions.stream().filter(a -> a.isExecutable()).count());
        Optional<Action> vmAction = actions.stream()
                        .filter(a -> a.getActionTarget().getDebugInfoNeverUseInCode().equals("VM"))
                        .findFirst();
        assertTrue(vmAction.isPresent());
        assertTrue(vmAction.get().isExecutable());
    }

    /**
     * Create an economy to be used for testing orphaned daemon suspension testing.
     * @param addGuaranteedBuyer Add a guaranteed buyer if true
     * @param numApplications Number of applications to add.  If a guaranteed buyer was added, the
     *                        guaranteed buyer will buy from all of these applications.  This will
     *                        also create matching containers and pods, which in turn will be
     *                        hosted on the single VM that is also created.
     * @return economy with requested traders populated.
     */
    private Economy createDaemonScenario(final boolean addGuaranteedBuyer,
                                         final int numApplications) {
        Economy economy = new Economy();
        // Add a VM
        Trader vm = economy.addTrader(TestUtils.VM_TYPE, TraderState.ACTIVE, VM_BASKET);
        vm.setDebugInfoNeverUseInCode("VM").getSettings().setSuspendable(true);
        // Add a guaranteed buyer if requested
        Trader gb = addGuaranteedBuyer
                        ? economy.addTrader(TestUtils.VAPP_TYPE, TraderState.ACTIVE, EMPTY,
                                            APPLICATION_BASKET)
                            .setDebugInfoNeverUseInCode("GuaranteedBuyer")
                        : null;
        // Add applications with their respective containers and pods
        for (int i = 0, type = 0; i < numApplications; i++, type += 100) {
            Trader pod = economy.addTrader(TestUtils.VM_TYPE, TraderState.ACTIVE, POD_BASKET,
                                           VM_BASKET);
            pod.setDebugInfoNeverUseInCode("POD-" + i).getSettings().setDaemon(true);
            // Move the pod onto the VM
            economy.getMarketsAsBuyer(pod).keySet().forEach(sl -> sl.move(vm));

            Trader container = economy.addTrader(TestUtils.VM_TYPE, TraderState.ACTIVE,
                                                 CONTAINER_BASKET, POD_BASKET);
            container.setDebugInfoNeverUseInCode("CONTAINER-" + i);
            // Move the container onto the pod
            economy.getMarketsAsBuyer(container).keySet().forEach(sl -> sl.move(pod));
            Trader app = economy.addTrader(TestUtils.VM_TYPE, TraderState.ACTIVE,
                                           APPLICATION_BASKET, CONTAINER_BASKET);
            app.setDebugInfoNeverUseInCode("APP-" + i);
            // Move the app onto the container
            economy.getMarketsAsBuyer(app).keySet().forEach(sl -> sl.move(container));
            if (gb != null) {
                economy.getMarketsAsBuyer(gb).keySet().forEach(sl -> sl.move(app));
            }
        }

        economy.populateMarketsWithSellersAndMergeConsumerCoverage();
        return economy;
    }
}
