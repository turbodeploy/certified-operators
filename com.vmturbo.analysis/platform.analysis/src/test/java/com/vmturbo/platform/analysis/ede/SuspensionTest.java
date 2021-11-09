package com.vmturbo.platform.analysis.ede;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.ActionType;
import com.vmturbo.platform.analysis.actions.Deactivate;
import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.actions.Resize;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySoldSettings;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.ledger.Ledger;
import com.vmturbo.platform.analysis.pricefunction.PriceFunction;
import com.vmturbo.platform.analysis.pricefunction.PriceFunctionFactory;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.SuspensionsThrottlingConfig;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;

/**
 * Tests for the Suspension class.
 */
public class SuspensionTest {
    private static final int VM_TYPE = 0;
    private static final int PM_TYPE = 1;
    private static final int ST_TYPE = 2;
    private static final double CAPACITY = 111;
    private static final double UTILIZATION_UPPER_BOUND = 0.9;

    // CommoditySpecifications to use in tests
    private static final CommoditySpecification CPU = TestUtils.CPU;
    private static final CommoditySpecification MEM = TestUtils.MEM;
    private static final CommoditySpecification STORAGE = TestUtils.STORAGE;

    // Baskets to use in tests
    private static final Basket EMPTY = new Basket();
    private static final Basket PM_SMALL = new Basket(CPU, MEM);
    private static final Basket ST_SMALL = new Basket(STORAGE);

    /**
     * Initialize IdentityGenerator.
     */
    @BeforeClass
    public static void init() {
        IdentityGenerator.initPrefix(0);
    }

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

        Suspension suspension = new Suspension(SuspensionsThrottlingConfig.DEFAULT);
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

        Suspension suspension = new Suspension(SuspensionsThrottlingConfig.DEFAULT);
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

        Suspension suspension = new Suspension(SuspensionsThrottlingConfig.DEFAULT);
        List<Action> suspAxns = suspension.suspensionDecisions(economy, ledger);

        // verify that the we suspend extra seller
        assertTrue(!suspAxns.isEmpty());

    }

    /**
     * Test suspending a trader hosting a ResizeThroughSupplier consumer while the trader trader has an overutilized commodity.
     */
    @Test
    public void suspensionDecisionsWithOverhead() {
        Economy economy = new Economy();
        // adding 1 suspendable trader
        Trader seller = economy.addTrader(PM_TYPE, TraderState.ACTIVE, PM_SMALL);
        seller.setDebugInfoNeverUseInCode("Provider");
        seller.getSettings().setCanAcceptNewCustomers(true);
        seller.getCommoditiesSold().stream()
                .forEach(cs -> cs.setCapacity(CAPACITY)
                        .getSettings().setUtilizationUpperBound(UTILIZATION_UPPER_BOUND));
        // mem overhead on provider
        seller.getCommoditySold(MEM).setQuantity(CAPACITY * 2);

        // mock numConsumers with a non-zero number so that this commodity is considered in revenue calculation.
        assertEquals(seller.getCommoditySold(MEM).getNumConsumers(), 0);
        assertEquals(seller.getCommoditySold(CPU).getNumConsumers(), 0);
        seller.getCommoditySold(MEM).setNumConsumers(1);

        seller.getSettings().setSuspendable(true);
        seller.getSettings().setMinDesiredUtil(0.6).setMaxDesiredUtil(0.7);

        Trader consumer = economy.addTrader(VM_TYPE, TraderState.ACTIVE, EMPTY);
        consumer.setDebugInfoNeverUseInCode("Consumer").getSettings().setSuspendable(false);
        // buyer that is RTS-true can be ignored during suspension and is movable false.
        consumer.getSettings().setResizeThroughSupplier(true).setGuaranteedBuyer(true);
        ShoppingList consumerSl = economy.addBasketBought(consumer, new Basket(CPU)).setMovable(false);
        for (int index = 0; index < consumerSl.getBasket().size(); index++) {
            consumerSl.setQuantity(index, 10);
        }

        // placing the consumerSl on the host.
        (new Move(economy, consumerSl, seller)).take();
        // assert numConsumers changed to 1 for CPU after move
        assertEquals(seller.getCommoditySold(CPU).getNumConsumers(), 1);

        economy.populateMarketsWithSellersAndMergeConsumerCoverage();
        Ledger ledger = new Ledger(economy);

        Suspension suspension = new Suspension(SuspensionsThrottlingConfig.DEFAULT);
        suspension.suspensionDecisions(economy, ledger);

        // verify that we do not generate suspension of the provider when there is an overutilized commodity.
        assertTrue(seller.getState().isActive());

        seller.getCommoditySold(MEM).setNumConsumers(0);

        suspension.suspensionDecisions(economy, ledger);

        // verify that we genarate suspension of the provider even though there is an overutilized commodity.
        assertTrue(!seller.getState().isActive());
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

        Suspension suspension = new Suspension(SuspensionsThrottlingConfig.DEFAULT);
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

        Suspension suspension = new Suspension(SuspensionsThrottlingConfig.DEFAULT);
        suspension.adjustUtilThreshold(economy, true);

        // verify that the utilUpperBound has changed to maxDesiredUtil
        assertTrue(commSoldSett.getUtilizationUpperBound() == seller.getSettings().getMaxDesiredUtil()
                        * commSoldSett.getOrigUtilizationUpperBound());
        suspension.adjustUtilThreshold(economy, false);

        // verify that the utilUpperBound has changed to origUtilDesiredUtil
        assertTrue(commSoldSett.getUtilizationUpperBound() == commSoldSett.getOrigUtilizationUpperBound());

        // verify that UtilizationUpperBound does not change for commodity using constantPriceFunction
        PriceFunction pfunc = PriceFunctionFactory.createConstantPriceFunction(1.0);
        commSoldSett.setPriceFunction(pfunc);
        suspension.adjustUtilThreshold(economy, true);
        assertTrue(commSoldSett.getUtilizationUpperBound() == commSoldSett.getOrigUtilizationUpperBound());

    }

    /**
     * Resize Through Supplier deactivate test.
     */
    @Test
    public void suspendProviderOfResizeThroughSupplier() {
        Economy economy = new Economy();
        Trader consumer = economy.addTrader(ST_TYPE, TraderState.ACTIVE, ST_SMALL);
        consumer.getSettings().setResizeThroughSupplier(true);
        consumer.getCommoditiesSold().get(0).setCapacity(200);
        consumer.getCommoditiesSold().get(0).getSettings().setResizable(true);
        ShoppingList consumerSL = economy.addBasketBought(consumer, ST_SMALL);
        Trader seller = economy.addTrader(PM_TYPE, TraderState.ACTIVE, ST_SMALL);
        seller.getCommoditiesSold().get(0).setCapacity(100);
        seller.getCommoditiesSold().get(0).getSettings().setResizable(true);
        consumerSL.move(seller);

        CommoditySoldSettings commSoldSett = seller.getCommoditiesSold().get(0).getSettings();
        commSoldSett.setUtilizationUpperBound(1.0).setOrigUtilizationUpperBound(1.0);

        seller.getSettings().setMaxDesiredUtil(0.5);

        Suspension suspension = new Suspension(SuspensionsThrottlingConfig.DEFAULT);
        suspension.adjustUtilThreshold(economy, true);

        Deactivate deactivate = (Deactivate)new Deactivate(economy, seller, seller.getBasketSold()).take();

        List<Action> resizeList = deactivate.getSubsequentActions().stream()
            .filter(action -> action.getType() == ActionType.RESIZE).collect(Collectors.toList());
        assertTrue(resizeList.size() == 1);
        Resize resize = (Resize)resizeList.get(0);
        assertTrue(resize.getNewCapacity() == 100.0);
        assertTrue(consumer.getCommoditiesSold().get(0).getCapacity() == 100.0);
    }

    /**
     * Resize Through Supplier deactivate test. If no consumers exist then all Providers should be
     * able to suspend and cause the resize through supplier trader to suspend as well.
     */
    @Test
    public void suspendAllProvidersOfResizeThroughSupplier() {
        Economy economy = new Economy();
        Trader consumer = economy.addTrader(ST_TYPE, TraderState.ACTIVE, ST_SMALL);
        consumer.getSettings().setResizeThroughSupplier(true).setGuaranteedBuyer(true);
        consumer.getCommoditiesSold().get(0).setCapacity(200);
        consumer.getCommoditiesSold().get(0).getSettings().setResizable(true);
        ShoppingList consumerSL1 = economy.addBasketBought(consumer, ST_SMALL);
        Trader seller1 = economy.addTrader(PM_TYPE, TraderState.ACTIVE, ST_SMALL);
        seller1.getCommoditiesSold().get(0).setCapacity(200);
        seller1.getCommoditiesSold().get(0).getSettings().setResizable(true);
        consumerSL1.move(seller1);
        CommoditySoldSettings commSoldSett = seller1.getCommoditiesSold().get(0).getSettings();
        commSoldSett.setUtilizationUpperBound(0.5).setOrigUtilizationUpperBound(0.5);
        seller1.getSettings().setMaxDesiredUtil(0.75).setMaxDesiredUtil(0.65).setSuspendable(true)
            .setCanAcceptNewCustomers(true);

        ShoppingList consumerSL2 = economy.addBasketBought(consumer, ST_SMALL);
        Trader seller2 = economy.addTrader(PM_TYPE, TraderState.ACTIVE, ST_SMALL);
        seller2.getCommoditiesSold().get(0).setCapacity(200);
        seller2.getCommoditiesSold().get(0).getSettings().setResizable(true);
        consumerSL2.move(seller2);
        CommoditySoldSettings commSoldSett2 = seller2.getCommoditiesSold().get(0).getSettings();
        commSoldSett2.setUtilizationUpperBound(0.5).setOrigUtilizationUpperBound(0.5);
        seller2.getSettings().setMaxDesiredUtil(0.75).setMaxDesiredUtil(0.65).setSuspendable(true)
            .setCanAcceptNewCustomers(true);

        economy.populateMarketsWithSellersAndMergeConsumerCoverage();
        Suspension suspension = new Suspension(SuspensionsThrottlingConfig.DEFAULT);
        Ledger ledger = new Ledger(economy);
        List<Action> actions = suspension.suspensionDecisions(economy, ledger);

        assertTrue(actions.stream().filter(action -> action instanceof Deactivate).count() == 3);
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
        Suspension suspension = new Suspension(SuspensionsThrottlingConfig.DEFAULT);
        suspension.suspendTrader(economy,
            economy.getMarketsAsSeller(seller).iterator().next().getBasket(), seller, actions);

        // verify that we have 1 deactivate action when suspendTrader is called
        assertTrue(actions.size() == 1);
        assertTrue(actions.get(0) instanceof Deactivate);
    }

    /**
     * Verify that we suspend as many traders as needed with Suspension Throttling as DEFAULT.
     */
    @Test
    public void suspendTraderDefaultThrottling() {
        Economy economy = new Economy();
        Trader vm = economy.addTrader(VM_TYPE, TraderState.ACTIVE, EMPTY, new Basket(CPU));
        Trader seller1 = economy.addTrader(PM_TYPE, TraderState.ACTIVE, PM_SMALL);
        Trader seller2 = economy.addTrader(PM_TYPE, TraderState.ACTIVE, PM_SMALL);
        Trader seller3 = economy.addTrader(PM_TYPE, TraderState.ACTIVE, PM_SMALL);
        vm.setDebugInfoNeverUseInCode("vm");
        seller1.setDebugInfoNeverUseInCode("pm1").getSettings().setCanAcceptNewCustomers(true).setSuspendable(true);
        seller2.setDebugInfoNeverUseInCode("pm2").getSettings().setCanAcceptNewCustomers(true).setSuspendable(true);
        seller3.setDebugInfoNeverUseInCode("pm3").getSettings().setCanAcceptNewCustomers(true).setSuspendable(true);
        ShoppingList sl = economy.getMarketsAsBuyer(vm).keySet().iterator().next();
        sl.move(seller1);
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();
        Ledger ledger = new Ledger(economy);
        Suspension suspension = new Suspension(SuspensionsThrottlingConfig.DEFAULT);
        List<Action> actions = suspension.suspensionDecisions(economy, ledger);

        // verify that we have 2 deactivate actions when DEFAULT suspension throttling is set.
        assertTrue(actions.stream().filter(action -> action instanceof Deactivate)
                        .collect(Collectors.toList()).size() == 2);
    }

    /**
     * Verify that we suspend only 1 trader with Suspension Throttling as Cluster.
     */
    @Test
    public void suspendTraderClusterThrottling() {
        Economy economy = new Economy();
        Trader vm = economy.addTrader(VM_TYPE, TraderState.ACTIVE, EMPTY, new Basket(CPU));
        Trader seller1 = economy.addTrader(PM_TYPE, TraderState.ACTIVE, PM_SMALL);
        Trader seller2 = economy.addTrader(PM_TYPE, TraderState.ACTIVE, PM_SMALL);
        Trader seller3 = economy.addTrader(PM_TYPE, TraderState.ACTIVE, PM_SMALL);
        vm.setDebugInfoNeverUseInCode("vm");
        seller1.setDebugInfoNeverUseInCode("pm1").getSettings().setCanAcceptNewCustomers(true).setSuspendable(true);
        seller2.setDebugInfoNeverUseInCode("pm2").getSettings().setCanAcceptNewCustomers(true).setSuspendable(true);
        seller3.setDebugInfoNeverUseInCode("pm3").getSettings().setCanAcceptNewCustomers(true).setSuspendable(true);
        ShoppingList sl = economy.getMarketsAsBuyer(vm).keySet().iterator().next();
        sl.move(seller1);
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();
        Ledger ledger = new Ledger(economy);
        Suspension suspension = new Suspension(SuspensionsThrottlingConfig.CLUSTER);
        List<Action> actions = suspension.suspensionDecisions(economy, ledger);

        // verify that we have 1 deactivate action when CLUSTER suspension throttling is set.
        assertTrue(actions.stream().filter(action -> action instanceof Deactivate)
                        .collect(Collectors.toList()).size() == 1);
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
        Suspension suspension = new Suspension(SuspensionsThrottlingConfig.DEFAULT);
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
        Suspension suspension = new Suspension(SuspensionsThrottlingConfig.DEFAULT);
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
        Suspension suspension = new Suspension(SuspensionsThrottlingConfig.DEFAULT);
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
        Suspension suspension = new Suspension(SuspensionsThrottlingConfig.DEFAULT);
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
        Suspension suspension = new Suspension(SuspensionsThrottlingConfig.DEFAULT);
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
        Suspension suspension = new Suspension(SuspensionsThrottlingConfig.DEFAULT);
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
        Suspension suspension = new Suspension(SuspensionsThrottlingConfig.DEFAULT);
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
        Suspension suspension = new Suspension(SuspensionsThrottlingConfig.DEFAULT);
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
        Suspension suspension = new Suspension(SuspensionsThrottlingConfig.DEFAULT);
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
        Suspension suspension = new Suspension(SuspensionsThrottlingConfig.DEFAULT);
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
        Suspension suspension = new Suspension(SuspensionsThrottlingConfig.DEFAULT);
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
        Suspension suspension = new Suspension(SuspensionsThrottlingConfig.DEFAULT);
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
        Suspension suspension = new Suspension(SuspensionsThrottlingConfig.DEFAULT);
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
        Suspension suspension = new Suspension(SuspensionsThrottlingConfig.DEFAULT);
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
     * Case: One service consume on two apps, each hosted by a container.
     * Each app sells 300(ms) response time. The service requests 50(ms) response time from each app.
     * The containers sells 575(MHz) VCPU. The two applications requests 200(MHz) and 150(MHz) VCPU
     * respectively.
     * Test that if setting the min replicas to 2, the suspension doesn't happen
     * Test that if setting the min replicas to 1, there will be 2 suspensions, 1 app, and 1 container
     * due to providerMustClone
     */
    @Test
    public void testSuspensionWithMinReplicas() {
        Economy e = new Economy();
        // App1
        Trader app1 = TestUtils.createTrader(e, TestUtils.APP_TYPE, Collections.singletonList(0L),
                Collections.singletonList(TestUtils.RESPONSE_TIME),
                new double[]{300}, true, false);
        app1.setDebugInfoNeverUseInCode("application1");
        app1.getSettings()
                .setProviderMustClone(true)
                .setSuspendable(true)
                .setMaxDesiredUtil(0.75)
                .setMinDesiredUtil(0.65)
                .setMaxReplicas(2);
        // Container1
        Trader container1 = TestUtils.createTrader(e, TestUtils.CONTAINER_TYPE, Collections.singletonList(0L),
                Collections.singletonList(TestUtils.VCPU),
                new double[]{575}, true, false);
        container1.getSettings()
            .setMinDesiredUtil(0.65)
            .setMaxDesiredUtil(0.75);
        container1.setDebugInfoNeverUseInCode("container1");
        // Move App1 on Container1
        Basket basketVCPU = new Basket(TestUtils.VCPU);
        ShoppingList slApp1Container1 = e.addBasketBought(app1, basketVCPU);
        TestUtils.moveSlOnSupplier(e, slApp1Container1, container1, new double[]{200});
        // App2
        Trader app2 = TestUtils.createTrader(e, TestUtils.APP_TYPE, Collections.singletonList(0L),
                Collections.singletonList(TestUtils.RESPONSE_TIME),
                new double[]{300}, true, false);
        app2.setDebugInfoNeverUseInCode("application2");
        app2.getSettings()
                .setProviderMustClone(true)
                .setSuspendable(true)
                .setMaxDesiredUtil(0.75)
                .setMinDesiredUtil(0.65)
                .setMaxReplicas(2);
        // Container2
        Trader container2 = TestUtils.createTrader(e, TestUtils.CONTAINER_TYPE, Collections.singletonList(0L),
                Collections.singletonList(TestUtils.VCPU),
                new double[]{575}, true, false);
        container2.getSettings()
            .setMinDesiredUtil(0.65)
            .setMaxDesiredUtil(0.75);
        container2.setDebugInfoNeverUseInCode("container2");
        // Move App2 on Container2
        ShoppingList slApp2Container2 = e.addBasketBought(app2, basketVCPU);
        TestUtils.moveSlOnSupplier(e, slApp2Container2, container2, new double[]{150});
        // Service
        Trader svc = TestUtils.createTrader(e, TestUtils.SERVICE_TYPE, Collections.singletonList(0L),
                Collections.emptyList(), new double[]{}, true, true);
        svc.setDebugInfoNeverUseInCode("service");
        // Move Service on App1/App2/App3
        Basket basketResponseTime = new Basket(TestUtils.RESPONSE_TIME);
        ShoppingList slSvcApp1 = e.addBasketBought(svc, basketResponseTime);
        TestUtils.moveSlOnSupplier(e, slSvcApp1, app1, new double[]{50});
        ShoppingList slSvcApp2 = e.addBasketBought(svc, basketResponseTime);
        TestUtils.moveSlOnSupplier(e, slSvcApp2, app2, new double[]{50});
        // Populate market
        e.populateMarketsWithSellersAndMergeConsumerCoverage();
        Ledger ledger = new Ledger(e);
        Suspension suspension = new Suspension(SuspensionsThrottlingConfig.DEFAULT);
        // Setting the min replicas as the current replicas
        app1.getSettings().setMinReplicas(2);
        app2.getSettings().setMinReplicas(2);
        // Assert that suspension won't happen
        assertEquals(0, suspension.suspensionDecisions(e, ledger).size());
        // Setting the min replicas to be 1 fewer than the current replicas
        app1.getSettings().setMinReplicas(1);
        app2.getSettings().setMinReplicas(1);
        // Assert that suspension can happen (1 app, and 1 container due to providerMustClone)
        assertEquals(2, suspension.suspensionDecisions(e, ledger).size());
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
        Trader vm = economy.addTrader(TestUtils.VM_TYPE, TraderState.ACTIVE, TestCommon.VM_BASKET);
        vm.setDebugInfoNeverUseInCode("VM").getSettings().setSuspendable(true);
        // Add a guaranteed buyer if requested
        Trader gb = addGuaranteedBuyer
                        ? economy.addTrader(TestUtils.VAPP_TYPE, TraderState.ACTIVE, EMPTY,
                TestCommon.APPLICATION_BASKET)
                            .setDebugInfoNeverUseInCode("GuaranteedBuyer")
                        : null;
        if (gb != null) {
            gb.getSettings().setDaemon(true);
        }
        // Add applications with their respective containers and pods
        for (int i = 0, type = 0; i < numApplications; i++, type += 100) {
            Trader pod = economy.addTrader(TestUtils.VM_TYPE, TraderState.ACTIVE, TestCommon.POD_BASKET,
                    TestCommon.VM_BASKET);
            pod.getSettings().setDaemon(true);
            pod.setDebugInfoNeverUseInCode("POD-" + i).getSettings().setDaemon(true);
            // Move the pod onto the VM
            economy.getMarketsAsBuyer(pod).keySet().forEach(sl -> sl.move(vm));

            Trader container = economy.addTrader(TestUtils.VM_TYPE, TraderState.ACTIVE,
                    TestCommon.CONTAINER_BASKET, TestCommon.POD_BASKET);
            container.getSettings().setDaemon(true);
            container.setDebugInfoNeverUseInCode("CONTAINER-" + i);
            // Move the container onto the pod
            economy.getMarketsAsBuyer(container).keySet().forEach(sl -> sl.move(pod));
            Trader app = economy.addTrader(TestUtils.VM_TYPE, TraderState.ACTIVE,
                    TestCommon.APPLICATION_BASKET, TestCommon.CONTAINER_BASKET);
            app.getSettings().setDaemon(true);
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
