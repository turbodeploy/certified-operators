package com.vmturbo.platform.analysis.ledger;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.pricefunction.PriceFunction;
import com.vmturbo.platform.analysis.topology.LegacyTopology;
import com.vmturbo.platform.analysis.utility.ListTests;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

/**
 * Tests for the {@link IncomeStatement} class.
 */
@RunWith(JUnitParamsRunner.class)
public class LedgerTest {
    // Fields

    // CommoditySpecifications to use in tests
    private static final CommoditySpecification CPU_ANY = new CommoditySpecification(0, 1000, 1, Integer.MAX_VALUE);
    private static final CommoditySpecification MEM = new CommoditySpecification(1);
    private static final CommoditySpecification V_CPU = new CommoditySpecification(2);
    private static final CommoditySpecification V_MEM = new CommoditySpecification(3);
    private static final CommoditySpecification CLUSTER_A = new CommoditySpecification(4, 1004, 0, 0);

    // Baskets to use in tests
    private static final Basket PM_ANY = new Basket(CPU_ANY, MEM);
    private static final Basket PM_A = new Basket(CPU_ANY, MEM, CLUSTER_A);
    private static final Basket VM = new Basket(V_CPU, V_MEM);

    // Methods

    @Test
    public final void testLedger() {
        @NonNull String[] ids = {"id1","id2","id3"};
        @NonNull String[] names = {"name1", "name2", "name2"};
        @NonNull String[] tTypes = {"type1", "type2", "type2"};
        @NonNull TraderState[] states = {TraderState.ACTIVE,TraderState.INACTIVE,TraderState.ACTIVE};
        @NonNull String[][] cTypeGroups = {{"a","b","c"},{"d","e"},{"b","d","f"}};
        @NonNull Trader[] traders = new Trader[ids.length];

        @NonNull LegacyTopology topology = new LegacyTopology();
        for (int i = 0 ; i < ids.length ; ++i) {
            traders[i] = topology.addTrader(ids[i], names[i], tTypes[i], states[i], Arrays.asList(cTypeGroups[i]));
        }
        Economy economy = (Economy)topology.getEconomy();
        Ledger ledger = new Ledger(economy);

        assertTrue(ledger.getTraderIncomeStatements().size() == economy.getTraders().size());

        economy.getTraders().forEach(trader->{
            assertTrue(ledger.getCommodityIncomeStatements(trader).size() == trader.getCommoditiesSold().size());
        });

    }

    @Test
    public final void testGetUnmodifiableTraderIncomeStatements() {
        Economy economy = new Economy();
        economy.addTrader(1, TraderState.ACTIVE, new Basket(new CommoditySpecification(0,1000,4,8)));
        Ledger ledger = new Ledger(economy);
        List<IncomeStatement> isList = ledger.getTraderIncomeStatements();
        ListTests.verifyUnmodifiableValidOperations(isList, new IncomeStatement());
        ListTests.verifyUnmodifiableInvalidOperations(isList, new IncomeStatement());

    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: Ledger({0}).calculateCommExpensesAndRevenues() == {1}")
    public final void testCalculateExpensesAndRevenues(Basket bought, Basket sold) {
        Economy economy = new Economy();
        // create supplier(PM)
        Trader pm = economy.addTrader(0, TraderState.ACTIVE, sold); // u can create a trader only by adding it to the market
        pm.getCommoditiesSold().get(0).setQuantity(5).setPeakQuantity(7);
        pm.getCommoditiesSold().get(1).setQuantity(5).setPeakQuantity(7);

        // create consumer(VM)
        Trader vm = economy.addTrader(1, TraderState.ACTIVE, VM);
        economy.addBasketBought(vm, bought)
            .setQuantity(0, 5).setPeakQuantity(0, 7)
            .setQuantity(1, 5).setPeakQuantity(1, 7).setMovable(true).move(pm);

        // populate rawMaterialMap
        economy.getModifiableRawCommodityMap().put(V_CPU.getType(), Arrays.asList(CPU_ANY.getType()));
        economy.getModifiableRawCommodityMap().put(V_MEM.getType(), Arrays.asList(MEM.getType()));

        Ledger ledger = new Ledger(economy);
        // TODO: check expenses and revenues generated
        economy.getTraders().forEach(trader->{
            assertTrue(ledger.getCommodityIncomeStatements(trader).size() == trader.getCommoditiesSold().size());
        });
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestCalculateExpensesAndRevenues() {
        return new Object[][] {
            {PM_ANY, PM_ANY},
            {PM_ANY, PM_A},
        };
    }

    /**
     * Test that a Sellers revenues are the summation of the revenues from top two commodities sold.
     *
     */
    @Test
    public final void testCalculateRevenuesForSeller_Economy_Trader() {
        Economy economy = new Economy();

        List<CommoditySpecification> commSpecSold = new ArrayList<>();
        commSpecSold.add(new CommoditySpecification(0, 4, 10));
        commSpecSold.add(new CommoditySpecification(1, 10, 20));
        commSpecSold.add(new CommoditySpecification(2, 5, 5));

        Basket basketSold = new Basket(commSpecSold);

        // create supplier
        Trader seller = economy.addTrader(0, TraderState.ACTIVE, basketSold);
        seller.getCommoditiesSold().get(0).setQuantity(4).setPeakQuantity(10).setCapacity(100);
        seller.getCommoditiesSold().get(1).setQuantity(10).setPeakQuantity(20).setCapacity(20);
        seller.getCommoditiesSold().get(2).setQuantity(5).setPeakQuantity(5).setCapacity(500);

        // create consumer
        Trader buyer = economy.addTrader(1, TraderState.ACTIVE, basketSold);
        economy.addBasketBought(buyer, basketSold)
            .setQuantity(0, 4).setPeakQuantity(0, 10)
            .setQuantity(1, 10).setPeakQuantity(1, 20)
            .setQuantity(2, 5).setPeakQuantity(2, 5).setMovable(true).move(seller);


        Ledger ledger = new Ledger(economy);

        List<Double> revFromComm = new ArrayList<>();
        // get revenues from individual commodities
        PriceFunction pf0 = seller.getCommoditiesSold().get(0).getSettings().getPriceFunction();
        double commSoldUtil_0 = seller.getCommoditiesSold().get(0).getQuantity()/seller.getCommoditiesSold().get(0).getEffectiveCapacity();
        revFromComm.add(pf0.unitPrice(commSoldUtil_0) * commSoldUtil_0);

        PriceFunction pf1 = seller.getCommoditiesSold().get(1).getSettings().getPriceFunction();
        double commSoldUtil_1 = seller.getCommoditiesSold().get(1).getQuantity()/seller.getCommoditiesSold().get(1).getEffectiveCapacity();
        revFromComm.add(pf1.unitPrice(commSoldUtil_1) * commSoldUtil_1);

        PriceFunction pf2 = seller.getCommoditiesSold().get(2).getSettings().getPriceFunction();
        double commSoldUtil_2 = seller.getCommoditiesSold().get(2).getQuantity()/seller.getCommoditiesSold().get(2).getEffectiveCapacity();
        revFromComm.add(pf2.unitPrice(commSoldUtil_2) * commSoldUtil_2);
        Collections.sort(revFromComm, Collections.reverseOrder());

        IncomeStatement sellerIncomeStmt = ledger.getTraderIncomeStatements().get(seller.getEconomyIndex());

        // get seller's revenue
        for (Market market : economy.getMarkets()) {
            ledger.calculateExpAndRevForSellersInMarket(economy, market);
        }

        // assert seller's revenue equals revenues from top 2 commodities
        double totalRev = revFromComm.get(0) + revFromComm.get(1);
        assertEquals(sellerIncomeStmt.getRevenues(), totalRev, 0);


        // change max, min desired util
        revFromComm.clear();
        seller.getCommoditiesSold().get(0).getSettings().setUtilizationUpperBound(0.7);
        seller.getCommoditiesSold().get(1).getSettings().setUtilizationUpperBound(0.6);
        seller.getCommoditiesSold().get(2).getSettings().setUtilizationUpperBound(0.5);
        commSoldUtil_0 = seller.getCommoditiesSold().get(0).getQuantity()/seller.getCommoditiesSold().get(0).getEffectiveCapacity();
        revFromComm.add(pf0.unitPrice(commSoldUtil_0) * commSoldUtil_0);
        commSoldUtil_1 = seller.getCommoditiesSold().get(1).getQuantity()/seller.getCommoditiesSold().get(1).getEffectiveCapacity();
        revFromComm.add(pf1.unitPrice(commSoldUtil_1) * commSoldUtil_1);
        commSoldUtil_2 = seller.getCommoditiesSold().get(2).getQuantity()/seller.getCommoditiesSold().get(2).getEffectiveCapacity();
        revFromComm.add(pf2.unitPrice(commSoldUtil_2) * commSoldUtil_2);
        Collections.sort(revFromComm, Collections.reverseOrder());

        // get seller's revenue again
        for (Market market : economy.getMarkets()) {
            ledger.calculateExpAndRevForSellersInMarket(economy, market);
        }

        totalRev = revFromComm.get(0) + revFromComm.get(1);
        assertEquals(sellerIncomeStmt.getRevenues(), totalRev, 0);


        // apply a step price function
        // change back max, min desired util
        revFromComm.clear();
        seller.getSettings().setMaxDesiredUtil(1.0);
        seller.getSettings().setMinDesiredUtil(0.0);
        seller.getCommoditiesSold().get(0).getSettings().setPriceFunction(PriceFunction.Cache.createStepPriceFunction(0.3, 50.0, 20000.0));
        pf0 = seller.getCommoditiesSold().get(0).getSettings().getPriceFunction();
        commSoldUtil_0 = seller.getCommoditiesSold().get(0).getQuantity()/seller.getCommoditiesSold().get(0).getEffectiveCapacity();
        revFromComm.add(pf0.unitPrice(commSoldUtil_0) * commSoldUtil_0);

        seller.getCommoditiesSold().get(1).getSettings().setPriceFunction(PriceFunction.Cache.createStepPriceFunction(3.0, 50.0, 20000.0));
        pf1 = seller.getCommoditiesSold().get(1).getSettings().getPriceFunction();
        commSoldUtil_1 = seller.getCommoditiesSold().get(1).getQuantity()/seller.getCommoditiesSold().get(1).getEffectiveCapacity();
        revFromComm.add(pf1.unitPrice(commSoldUtil_1) * commSoldUtil_1);

        seller.getCommoditiesSold().get(2).getSettings().setPriceFunction(PriceFunction.Cache.createStepPriceFunction(2.0, 50.0, 20000.0));
        pf2 = seller.getCommoditiesSold().get(2).getSettings().getPriceFunction();
        commSoldUtil_2 = seller.getCommoditiesSold().get(2).getQuantity()/seller.getCommoditiesSold().get(2).getEffectiveCapacity();
        revFromComm.add(pf2.unitPrice(commSoldUtil_2) * commSoldUtil_2);
        Collections.sort(revFromComm, Collections.reverseOrder());

        // get seller's revenue again
        for (Market market : economy.getMarkets()) {
            ledger.calculateExpAndRevForSellersInMarket(economy, market);
        }

        totalRev = revFromComm.get(0) + revFromComm.get(1);
        assertEquals(sellerIncomeStmt.getRevenues(), totalRev, 0);

    }

    /**
     * Test some edge cases for Sellers revenues.
     *
     */
    @Test
    public final void testCalculateRevenuesForSellersEdgeCases_Economy_Trader() {
        Economy economy = new Economy();

        List<CommoditySpecification> commSpecSold = new ArrayList<>();
        commSpecSold.add(new CommoditySpecification(0, 0, 10));
        commSpecSold.add(new CommoditySpecification(1, 0, 20));
        commSpecSold.add(new CommoditySpecification(2, 0, 5));

        Basket basketSold = new Basket(commSpecSold);

        //1. Test all commodities having a used of 0
        // create supplier
        Trader seller = economy.addTrader(0, TraderState.ACTIVE, basketSold);
        seller.getCommoditiesSold().get(0).setQuantity(0).setPeakQuantity(10).setCapacity(100);
        seller.getCommoditiesSold().get(1).setQuantity(0).setPeakQuantity(20).setCapacity(20);
        seller.getCommoditiesSold().get(2).setQuantity(0).setPeakQuantity(5).setCapacity(500);

        // create consumer
        Trader buyer = economy.addTrader(1, TraderState.ACTIVE, basketSold);
        economy.addBasketBought(buyer, basketSold)
            .setQuantity(0, 0).setPeakQuantity(0, 10)
            .setQuantity(1, 0).setPeakQuantity(1, 20)
            .setQuantity(2, 0).setPeakQuantity(2, 5).setMovable(true).move(seller);


        Ledger ledger = new Ledger(economy);

        List<Double> revFromComm = new ArrayList<>();
        // get revenues from individual commodities
        PriceFunction pf0 = seller.getCommoditiesSold().get(0).getSettings().getPriceFunction();
        double commSoldUtil_0 = seller.getCommoditiesSold().get(0).getQuantity()/seller.getCommoditiesSold().get(0).getEffectiveCapacity();
        revFromComm.add(pf0.unitPrice(commSoldUtil_0) * commSoldUtil_0);

        PriceFunction pf1 = seller.getCommoditiesSold().get(1).getSettings().getPriceFunction();
        double commSoldUtil_1 = seller.getCommoditiesSold().get(1).getQuantity()/seller.getCommoditiesSold().get(1).getEffectiveCapacity();
        revFromComm.add(pf1.unitPrice(commSoldUtil_1) * commSoldUtil_1);

        PriceFunction pf2 = seller.getCommoditiesSold().get(2).getSettings().getPriceFunction();
        double commSoldUtil_2 = seller.getCommoditiesSold().get(2).getQuantity()/seller.getCommoditiesSold().get(2).getEffectiveCapacity();
        revFromComm.add(pf2.unitPrice(commSoldUtil_2) * commSoldUtil_2);
        Collections.sort(revFromComm, Collections.reverseOrder());

        IncomeStatement sellerIncomeStmt = ledger.getTraderIncomeStatements().get(seller.getEconomyIndex());

        // get seller's revenue
        for (Market market : economy.getMarkets()) {
            ledger.calculateExpAndRevForSellersInMarket(economy, market);
        }

        // assert seller's revenue equals revenues from top 2 commodities
        double totalRev = revFromComm.get(0) + revFromComm.get(1);
        assertEquals(sellerIncomeStmt.getRevenues(), totalRev, 0);

        //2.  Test some commodities over max desired utilization
        commSpecSold.clear();
        commSpecSold.add(new CommoditySpecification(0, 4, 10));
        commSpecSold.add(new CommoditySpecification(1, 10, 20));
        commSpecSold.add(new CommoditySpecification(2, 5, 5));

        basketSold = new Basket(commSpecSold);

        // create supplier
        Trader seller1 = economy.addTrader(0, TraderState.ACTIVE, basketSold);
        seller1.getCommoditiesSold().get(0).setQuantity(4).setPeakQuantity(10).setCapacity(10);
        seller1.getCommoditiesSold().get(1).setQuantity(20).setPeakQuantity(20).setCapacity(20);
        seller1.getCommoditiesSold().get(2).setQuantity(5).setPeakQuantity(5).setCapacity(5);

        // create consumer
        Trader buyer1 = economy.addTrader(1, TraderState.ACTIVE, basketSold);
        economy.addBasketBought(buyer1, basketSold)
            .setQuantity(0, 4).setPeakQuantity(0, 10)
            .setQuantity(1, 10).setPeakQuantity(1, 20)
            .setQuantity(2, 5).setPeakQuantity(2, 5).setMovable(true).move(seller);


        Ledger ledger1 = new Ledger(economy);

        // set desired max utilization
        seller1.getCommoditiesSold().get(0).getSettings().setUtilizationUpperBound(0.7);
        seller1.getCommoditiesSold().get(1).getSettings().setUtilizationUpperBound(0.7);
        seller1.getCommoditiesSold().get(2).getSettings().setUtilizationUpperBound(0.7);

        revFromComm.clear();
        // get revenues from individual commodities
        commSoldUtil_0 = seller1.getCommoditiesSold().get(0).getQuantity()/seller1.getCommoditiesSold().get(0).getEffectiveCapacity();
        revFromComm.add(pf0.unitPrice(commSoldUtil_0) * commSoldUtil_0);

        commSoldUtil_1 = seller1.getCommoditiesSold().get(1).getQuantity()/seller1.getCommoditiesSold().get(1).getEffectiveCapacity();
        revFromComm.add(pf1.unitPrice(commSoldUtil_1) * commSoldUtil_1);

        commSoldUtil_2 = seller1.getCommoditiesSold().get(2).getQuantity()/seller1.getCommoditiesSold().get(2).getEffectiveCapacity();
        revFromComm.add(pf2.unitPrice(commSoldUtil_2) * commSoldUtil_2);
        Collections.sort(revFromComm, Collections.reverseOrder());

        IncomeStatement seller1IncomeStmt = ledger1.getTraderIncomeStatements().get(seller1.getEconomyIndex());
        // get seller's revenue
        for (Market market : economy.getMarkets()) {
            ledger1.calculateExpAndRevForSellersInMarket(economy, market);
        }

        // assert seller's revenue equals revenues from top 2 commodities
        totalRev = revFromComm.get(0) + revFromComm.get(1);
        assertEquals(seller1IncomeStmt.getRevenues(), totalRev, 0);

    }

} // end LedgerTest class
