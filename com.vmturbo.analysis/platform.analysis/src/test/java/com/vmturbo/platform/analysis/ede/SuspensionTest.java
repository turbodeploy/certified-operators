package com.vmturbo.platform.analysis.ede;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

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
    private static final Basket PM_SMALL = new Basket(CPU,MEM);


    @Test
    public void test_suspensionDecisions_dontSuspendNonSuspendableTraders() {
        Economy economy = new Economy();
        // adding 2 non-suspendable traders
        economy.addTrader(PM_TYPE, TraderState.ACTIVE, PM_SMALL);
        economy.addTrader(PM_TYPE, TraderState.ACTIVE, PM_SMALL);
        economy.addTrader(VM_TYPE, TraderState.ACTIVE, EMPTY, new Basket(CPU));

        economy.populateMarketsWithSellersAndMergeConsumerCoverage();
        Ledger ledger = new Ledger(economy);
        Ede ede = new Ede();

        Suspension suspension = new Suspension();
        List<Action> suspAxns = suspension.suspensionDecisions(economy, ledger, ede);

        // verify that we do not suspend non-suspendaable traders
        assertTrue(suspAxns.isEmpty());
    }


    @Test
    public void test_suspensionDecisions_dontSuspendWhenNoActiveSellers() {
        Economy economy = new Economy();
        // adding 1 non-suspendable traders
        economy.addTrader(PM_TYPE, TraderState.ACTIVE, PM_SMALL);

        Suspension suspension = new Suspension();
        // check if returned action list is empty
        assertTrue(suspension.suspensionDecisions(economy, new Ledger(economy), new Ede())
                        .isEmpty());
    }

    @Test
    public void test_suspensionDecisions_suspensionActions() {
        Economy economy = new Economy();
        // adding 2 non-suspendable traders
        economy.addTrader(PM_TYPE, TraderState.ACTIVE, PM_SMALL).getSettings().setCanAcceptNewCustomers(true);
        Trader seller = economy.addTrader(PM_TYPE, TraderState.ACTIVE, PM_SMALL);
        seller.getSettings().setCanAcceptNewCustomers(true);
        seller.getCommoditiesSold().stream().forEach(cs -> cs.setCapacity(CAPACITY).getSettings().setUtilizationUpperBound(UTILIZATION_UPPER_BOUND));
        seller.getSettings().setSuspendable(true);

        seller.getSettings().setMinDesiredUtil(0.6);

        Trader vm = economy.addTrader(VM_TYPE, TraderState.ACTIVE, EMPTY);
        ShoppingList vmSl = economy.addBasketBought(vm, new Basket(CPU)).setMovable(true);
        for (int index = 0; index<vmSl.getBasket().size(); index++) {
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
        List<Action> suspAxns = suspension.suspensionDecisions(economy, ledger, ede);

        // verify that the we suspend extra seller
        assertTrue(!suspAxns.isEmpty());

    }

    @Test
    public void test_suspensionDecisions_noActions() {
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
        List<Action> suspAxns = suspension.suspensionDecisions(economy, ledger, ede);

        // verify that we do not suspend the only seller
        assertTrue(suspAxns.isEmpty());
    }

    @Test
    public void test_adjustUtilThreshold() {
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

    @Test
    public void test_suspendTrader() {
        Economy economy = new Economy();
        // add buyer and seller to create a market
        economy.addTrader(VM_TYPE, TraderState.ACTIVE, EMPTY, new Basket(CPU));
        Trader seller = economy.addTrader(VM_TYPE, TraderState.ACTIVE, PM_SMALL);
        seller.getSettings().setCanAcceptNewCustomers(true);
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        List<Action> actions = new ArrayList<>();
        Suspension suspension = new Suspension();
        suspension.suspendTrader(economy,
            economy.getMarketsAsSeller(seller).iterator().next().getBasket(), seller, actions);

        // verify that we have 1 deactivate action when suspendTrader is called
        assertTrue(actions.size() == 1);
        assertTrue(actions.get(0) instanceof Deactivate);
    }

    /*
     * No seller and hence no sole provider.
     */
    @Test
    public void test_findSoleProviders_noSeller() {
        //Arrange
        Economy economy = getEconomyWithGivenSellersAndBuyers(0, 1);
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        //Act
        Suspension suspension = new Suspension();
        suspension.findSoleProviders(economy);

        //Assert
        assertTrue(suspension.getSoleProviders().isEmpty());
    }

    /*
     * No buyer and hence no sole provider.
     */
    @Test
    public void test_findSoleProviders_noBuyer() {
        //Arrange
        Economy economy = getEconomyWithGivenSellersAndBuyers(1, 0);
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        //Act
        Suspension suspension = new Suspension();
        suspension.findSoleProviders(economy);

        //Assert
        assertTrue(suspension.getSoleProviders().isEmpty());
    }

    /*
     * One active seller and buyer. Hence one sole provider.
     */
    @Test
    public void test_findSoleProviders_oneActiveSeller() {
        //Arrange
        Economy economy = getEconomyWithGivenSellersAndBuyers(1, 1);
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        //Act
        Suspension suspension = new Suspension();
        suspension.findSoleProviders(economy);

        //Assert
        assertEquals(1, suspension.getSoleProviders().size());
    }

    /*
     * Inactive seller and hence no sole provider.
     */
    @Test
    public void test_findSoleProviders_inactiveSeller() {
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

    /*
     * Inactive buyer and hence no sole provider.
     */
    @Test
    public void test_findSoleProviders_inActiveBuyer() {
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

    /*
     * More than one seller and hence no sole provider.
     */
    @Test
    public void test_findSoleProviders_moreThanOneActiveSeller() {
        //Arrange
        Economy economy = getEconomyWithGivenSellersAndBuyers(2, 1);
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        //Act
        Suspension suspension = new Suspension();
        suspension.findSoleProviders(economy);

        //Assert
        assertTrue(suspension.getSoleProviders().isEmpty());
    }

    /*
     * One seller and two buyers. Hence one sole provider.
     */
    @Test
    public void test_findSoleProviders_withTwoBuyers() {
        //Arrange
        Economy economy = getEconomyWithGivenSellersAndBuyers(1, 2);
        Trader gurreanteedBuyer = economy.addTrader(VM_TYPE, TraderState.ACTIVE, EMPTY, new Basket(CPU));
        gurreanteedBuyer.getSettings().setGuaranteedBuyer(true);
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        //Act
        Suspension suspension = new Suspension();
        suspension.findSoleProviders(economy);

        //Assert
        assertEquals(1, suspension.getSoleProviders().size());
    }

    /*
     * Two seller and two buyers. Hence no sole provider.
     */
    @Test
    public void test_findSoleProviders_withMultipleSelleraAndBuyers() {
        //Arrange
        Economy economy = getEconomyWithGivenSellersAndBuyers(2, 2);
        Trader gurreanteedBuyer = economy.addTrader(VM_TYPE, TraderState.ACTIVE, EMPTY, new Basket(CPU));
        gurreanteedBuyer.getSettings().setGuaranteedBuyer(true);
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        //Act
        Suspension suspension = new Suspension();
        suspension.findSoleProviders(economy);

        //Assert
        assertTrue(suspension.getSoleProviders().isEmpty());
    }

    /*
     * One seller and one guaranteed buyer. Hence sole provider.
     */
    @Test
    public void test_findSoleProviders_withGuaranteedBuyer() {
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

    /*
     * One seller and two buyers but one guaranteed. Hence one sole provider.
     */
    @Test
    public void test_findSoleProviders_withTwoBuyersAndOneGurranteed() {
        //Arrange
        Economy economy = getEconomyWithGivenSellersAndBuyers(1, 1);
        Trader gurreanteedBuyer = economy.addTrader(VM_TYPE, TraderState.ACTIVE, EMPTY, new Basket(CPU));
        gurreanteedBuyer.getSettings().setGuaranteedBuyer(true);
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        //Act
        Suspension suspension = new Suspension();
        suspension.findSoleProviders(economy);

        //Assert
        assertEquals(1, suspension.getSoleProviders().size());
    }

    @Test
    public void test_findSoleProviders_withMoreThanOneActiveSeller() {
        //Arrange
        Economy economy = getEconomyWithGivenSellersAndBuyers(2, 1);
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        //Act
        Suspension suspension = new Suspension();
        suspension.findSoleProviders(economy);

        //Assert
        assertEquals(0, suspension.getSoleProviders().size());
    }

    /*
     * Set one seller s.t it is a sole provider in only one market.
     * Two sellers : {PM1 selling (CPU,MEM) + Storage}, {PM2 -> selling (CPU,MEM)}, and two buyers {VM1 buying (CPU,MEM)}, {VM1 buying (Storage)}
     * Hence, PM1 sole provider of storage.
     */
    @Test
    public void test_findSoleProviders_SellerSoleProviderInOneMarket() {
        //Arrange
        Economy economy = new Economy();

        //sellers
        Basket cpuMemStorageBasket= new Basket(CPU, MEM, TestUtils.ST_AMT);
        Basket storageBasket = new Basket(TestUtils.ST_AMT);
        Trader soleProviderForStorage = economy.addTrader(PM_TYPE, TraderState.ACTIVE, cpuMemStorageBasket, EMPTY);
        soleProviderForStorage.getSettings().setCanAcceptNewCustomers(true);
        economy.addTrader(PM_TYPE, TraderState.ACTIVE, PM_SMALL, EMPTY)
                    .getSettings().setCanAcceptNewCustomers(true);;
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


   /*
    * Creates an economy with given number of sellers (PMs) and buyers (VMs).
    */
    private Economy getEconomyWithGivenSellersAndBuyers(int numSellers, int numBuyers) {
        Economy economy = new Economy();
        //Add sellers
        IntStream.range(0, numSellers).forEach($ -> economy.addTrader(PM_TYPE, TraderState.ACTIVE, PM_SMALL)
                        .getSettings().setCanAcceptNewCustomers(true));
        //Add buyers
        IntStream.range(0, numBuyers).forEach($ -> economy.addTrader(VM_TYPE, TraderState.ACTIVE, EMPTY, new Basket(CPU)));
        return economy;
    }
}
