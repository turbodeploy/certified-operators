package com.vmturbo.platform.analysis.ledger;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.pricefunction.PriceFunction;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;
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


    // Result type used in tests
    private static enum RESULTYPE {MAX_DESIRED, MIN_DESIRED, NUMBER, PREVIOUS};
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

    @SuppressWarnings("unused")
    private static Collection<Object[]> parametersForTestCalculateExpensesForSeller() {
        // NumberOfBuyers,NumberOfSellers,NumberOfCommdities,ListOfCommosdityUtil,ExceptedResult
        Random rand = new Random();
        List<Object[]> objectList = new ArrayList<>(Arrays.asList(new Object[][] {
                               {1, 1, 1, new double[] {0}, RESULTYPE.NUMBER, 0.0d, 0},
                               {1, 1, 1, new double[] {0.8}, RESULTYPE.MAX_DESIRED, 0.0d, -1},
                               {1, 1, 1, new double[] {0.2}, RESULTYPE.MIN_DESIRED, 0.0d, 1},
                               {1, 2, 2, new double[] {1, 0.99}, RESULTYPE.NUMBER, 1E22, 0},
                               {2, 2, 4, new double[] {0.55, 0.66,0.56,0.69}, RESULTYPE.MAX_DESIRED, 0.0d, 1},
                               {2, 2, 4, new double[] {0.55, 0.66,0.56,0.69}, RESULTYPE.MIN_DESIRED, 0.0d, -1}
        }));
        // let utilization go up random between desire utilization range to check expense also go up
        double util = 0.51d;
        previousExp = 0d;
        while (util < 0.7d) {
            objectList.add(new Object[]{rand.nextInt(5) + 1, rand.nextInt(5) + 1, 1,
                                        new double[] {util}, util == 0.51d ?
                                                        RESULTYPE.NUMBER : RESULTYPE.PREVIOUS, 0.0d, -1});
            // In avg, we go through 7.5 times.
            util += rand.nextDouble() / 20.0f + 1.0E-4;
        }
        // let utilization go down random between desire utilization range to check expense also go down
        // First, reset Utilization to 68% in the desireRange, and lower a random commodity utilization
        util = 0.68;
        double[] utilArray = new double[10];
        Arrays.fill(utilArray, 0.68d);
        while (util > 0.5d) {
            // Pick up random commodity
            int idx = rand.nextInt(10);
            objectList.add(new Object[]{rand.nextInt(10) + 1, rand.nextInt(10) + 1, 10,
                                        Arrays.copyOf(utilArray, utilArray.length), util == 0.68d ? RESULTYPE.NUMBER : RESULTYPE.PREVIOUS
                                        , Double.MAX_VALUE, 1});
            // In avg, we go through 11 times.
            util -= rand.nextDouble() / 30.0f + 1.0E-4;
            // Set new utilization
            utilArray[idx] = util;
        }
        return objectList;
    }


    @SuppressWarnings("unused")
    private static Object[] parametersForTestCalculateExpensesForSellerEdgeCases() {
        return new Object[][] {
            {true, 1, 1, new double[] {0}, RESULTYPE.NUMBER, Double.POSITIVE_INFINITY, 0},
            {false, 2, 1, new double[] {0.5}, RESULTYPE.MIN_DESIRED, 0.0d, -1},
            {false, 2, 1, new double[] {0.5}, RESULTYPE.MAX_DESIRED, 0.0d, 1},
            {true, 2, 2, new double[] {0, 1}, RESULTYPE.NUMBER, Double.POSITIVE_INFINITY, 0},
        };
    }

    static double previousExp = 0d;

    @Test
    @Parameters
    @TestCaseName("Test #{index}: CalculateExpensesForSeller({0},{1},{2},{3},{4},{5},{6})")
    /**
     * Testing the expense for a seller of the in different senarios
     *
     * @param numOfBuyers Number of buyers need to create
     * @param numOfSellers Number of sellers need to create
     * @param numOfCommodities Number of commodities need to create
     * @param util utilization of those commodities
     * @param resultType which result we need to compare
     * @param result the exactly result we need to compare
     * @param compareTo 0 is equals, 1 is greater than, 2 is less than
     *
     */
    public final void testCalculateExpensesForSeller(int numOfBuyers, int numOfSellers,
                                                     int numOfCommodities, double[] util,
                                                     RESULTYPE resultType, double result,
                                                     int compareTo) {
        Economy economy = new Economy();
        Ledger ledger = new Ledger(economy);
        Random rand = new Random();
        // Create Commodities List
        List<CommoditySpecification> commList = createCommodities(numOfCommodities);
        // Add Basket
        Basket basket = new Basket(commList);
        // Create Sellers List
        List<Trader> sellerList = IntStream.range(0, numOfSellers)
                        .mapToObj(idx -> economy.addTrader(1, TraderState.ACTIVE, basket))
                        .collect(Collectors.toCollection(() -> new ArrayList<Trader>()));
        sellerList.forEach(seller -> ledger.addTraderIncomeStatement(seller));
        Basket buyerBasket = new Basket(createCommodities(numOfCommodities).get(0));
        // Create Buyer List
        List<Trader> buyerList = IntStream.range(0, numOfBuyers)
                        .mapToObj(idx -> economy.addTrader(2, TraderState.ACTIVE, buyerBasket))
                        .collect(Collectors.toCollection(() -> new ArrayList<Trader>()));
        for (Trader buyer : buyerList) {
            // Create a shopping list for each seller
            ShoppingList shoppingList = economy.addBasketBought(buyer, basket);
            Trader seller = sellerList.get(rand.nextInt(sellerList.size()));
            // Set max and min DesireUtil
            seller.getSettings().setMaxDesiredUtil(0.7);
            seller.getSettings().setMinDesiredUtil(0.5);
            // Set supplier for each seller
            shoppingList.move(seller);
            // Set Commodity utilization to the percentage that we set
            for (int idx = 0; idx < seller.getCommoditiesSold().size(); idx++) {
                CommoditySold commSold = seller.getCommoditiesSold().get(idx);
                commSold.setCapacity(commList.get(idx).getQualityUpperBound() / 2)
                                .setQuantity(util[idx] * commSold.getCapacity())
                                .setPeakQuantity(commSold.getQuantity());
                shoppingList.setQuantity(idx, commSold.getQuantity()).setPeakQuantity(idx, commSold.getQuantity());
            }
            ledger.addTraderIncomeStatement(buyer);
            ledger.calculateExpRevForTraderAndGetTopRevenue(economy, buyer);
        }
        // Get a random IncomeStatement
        int economyIndex = buyerList.get(rand.nextInt(buyerList.size())).getEconomyIndex();
        IncomeStatement selectedIncomeStatement =
                        ledger.getTraderIncomeStatements().get(economyIndex);
        compareResult(result, selectedIncomeStatement, compareTo, resultType);

    }

    private List<CommoditySpecification> createCommodities(int numOfComm){
        Random rand = new Random();
        return IntStream.range(0, numOfComm)
                        .mapToObj(idx -> new CommoditySpecification(idx, 0, rand.nextInt(100000) + 10000, false))
                        .collect(Collectors.toCollection(() -> new ArrayList<CommoditySpecification>()));
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: CalculateExpensesForSellerEdgeCases({0},{1},{2},{3},{4},{5},{6})")
    public final void testCalculateExpensesForSellerEdgeCases(boolean wrongProvider, int suppliers,
                                                     int numOfCommodities, double[] util,
                                                     RESULTYPE resultType, double result,
                                                     int compareTo) {
        Economy economy = new Economy();
        Ledger ledger = new Ledger(economy);
        Random rand = new Random();
        // Create Commodities List
        List<CommoditySpecification> commList = createCommodities(numOfCommodities);
        // Add Basket
        Basket basket = new Basket(commList);
        // Create Sellers List
        List<Trader> sellerList = IntStream.range(0, rand.nextInt(10) + suppliers)
                        .mapToObj(idx -> economy.addTrader(1, TraderState.ACTIVE, basket))
                        .collect(Collectors.toCollection(() -> new ArrayList<Trader>()));
        sellerList.forEach(seller -> ledger.addTraderIncomeStatement(seller));
        Basket buyerBasket = new Basket(createCommodities(numOfCommodities).get(0));
        Basket wrongBasket = new Basket(createCommodities(numOfCommodities));
        // Create Buyer List
        List<Trader> buyerList = IntStream.range(0, rand.nextInt(10) + 1)
                        .mapToObj(idx -> economy.addTrader(2, TraderState.ACTIVE, buyerBasket))
                        .collect(Collectors.toCollection(() -> new ArrayList<Trader>()));
        for (Trader buyer : buyerList) {
            // Create a shopping list for each seller
            ShoppingList shoppingList = economy.addBasketBought(buyer, wrongProvider ? wrongBasket : basket);
            ShoppingList shoppingList2 = null;
            Trader seller = sellerList.get(0);
            // Set max and min DesireUtil
            seller.getSettings().setMaxDesiredUtil(0.8);
            seller.getSettings().setMinDesiredUtil(0.3);
            // Set supplier for each seller
            if (suppliers != 0) {
                shoppingList.move(seller);
            }
            if (suppliers > 1) {
                shoppingList2 =
                              economy.addBasketBought(buyer, wrongProvider ? wrongBasket : basket);
                shoppingList2.move(sellerList.get(1));
            }
            // Set Commodity utilization to the percentage that we set
            for (int idx = 0; idx < seller.getCommoditiesSold().size(); idx++) {
                CommoditySold commSold = seller.getCommoditiesSold().get(idx);
                commSold.setCapacity(commList.get(idx).getQualityUpperBound() / 2)
                                .setQuantity(util[idx] * commSold.getCapacity())
                                .setPeakQuantity(commSold.getQuantity());
                shoppingList.setQuantity(idx, commSold.getQuantity())
                                .setPeakQuantity(idx, commSold.getQuantity());
                if (suppliers > 1) {
                    shoppingList2.setQuantity(idx, 0).setPeakQuantity(idx, 0);
                }
            }
            ledger.addTraderIncomeStatement(buyer);
            ledger.calculateExpRevForTraderAndGetTopRevenue(economy, buyer);
        }
        int economyIndex = buyerList.get(rand.nextInt(buyerList.size())).getEconomyIndex();
        IncomeStatement selectedIncomeStatement =
                        ledger.getTraderIncomeStatements().get(economyIndex);
        compareResult(result, selectedIncomeStatement, compareTo, resultType);
    }

    public void compareResult(double result, IncomeStatement selectedIncomeStatement, int compareTo, RESULTYPE resultType){
        // Get Expense from that IncomeStatement
        double exp = selectedIncomeStatement.getExpenses();
        if (resultType == RESULTYPE.PREVIOUS) {
            result = previousExp;
        } else if (resultType != RESULTYPE.NUMBER) {
            result = resultType == RESULTYPE.MAX_DESIRED
                            ? selectedIncomeStatement.getMaxDesiredExpenses()
                            : selectedIncomeStatement.getMinDesiredExpenses();
        }
        // The compareTo is a flag that decide how
        // we compare the result
        if (compareTo == 0) {
            assertEquals(result, exp, TestUtils.FLOATING_POINT_DELTA);
        } else if (compareTo < 0) {
            assertTrue(exp > result);
        } else {
            assertTrue(exp < result);
        }
        previousExp = exp;
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
        commSpecSold.add(new CommoditySpecification(0));
        commSpecSold.add(new CommoditySpecification(1));
        commSpecSold.add(new CommoditySpecification(2));

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
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        Ledger ledger = new Ledger(economy);

        List<Double> revFromComm = new ArrayList<>();
        // get revenues from individual commodities
        PriceFunction pf0 = seller.getCommoditiesSold().get(0).getSettings().getPriceFunction();
        CommoditySold commSold0 = seller.getCommoditiesSold().get(0);
        double commSoldUtil_0 = commSold0.getQuantity()/commSold0.getEffectiveCapacity();
        revFromComm.add(pf0.unitPrice(commSoldUtil_0, null, seller, commSold0, economy) * commSoldUtil_0);

        PriceFunction pf1 = seller.getCommoditiesSold().get(1).getSettings().getPriceFunction();
        CommoditySold commSold1 = seller.getCommoditiesSold().get(1);
        double commSoldUtil_1 = seller.getCommoditiesSold().get(1).getQuantity()/seller.getCommoditiesSold().get(1).getEffectiveCapacity();
        revFromComm.add(pf1.unitPrice(commSoldUtil_1, null, seller, commSold1, economy) * commSoldUtil_1);

        PriceFunction pf2 = seller.getCommoditiesSold().get(2).getSettings().getPriceFunction();
        CommoditySold commSold2 = seller.getCommoditiesSold().get(2);
        double commSoldUtil_2 = seller.getCommoditiesSold().get(2).getQuantity()/seller.getCommoditiesSold().get(2).getEffectiveCapacity();
        revFromComm.add(pf2.unitPrice(commSoldUtil_2, null, seller, commSold2, economy) * commSoldUtil_2);
        Collections.sort(revFromComm, Collections.reverseOrder());

        IncomeStatement sellerIncomeStmt = ledger.getTraderIncomeStatements().get(seller.getEconomyIndex());

        // get seller's revenue
        for (Market market : economy.getMarkets()) {
            ledger.calculateExpAndRevForSellersInMarket(economy, market);
        }

        // assert seller's revenue equals revenues from top 2 commodities
        double totalRev = revFromComm.get(0) + revFromComm.get(1);
        assertEquals(sellerIncomeStmt.getRevenues(), totalRev, TestUtils.FLOATING_POINT_DELTA);

        // change max, min desired util
        revFromComm.clear();
        commSold0.getSettings().setUtilizationUpperBound(0.7);
        commSold1.getSettings().setUtilizationUpperBound(0.6);
        commSold2.getSettings().setUtilizationUpperBound(0.5);
        commSoldUtil_0 = seller.getCommoditiesSold().get(0).getQuantity()/seller.getCommoditiesSold().get(0).getEffectiveCapacity();
        revFromComm.add(pf0.unitPrice(commSoldUtil_0, null, seller, commSold0, economy) * commSoldUtil_0);
        commSoldUtil_1 = seller.getCommoditiesSold().get(1).getQuantity()/seller.getCommoditiesSold().get(1).getEffectiveCapacity();
        revFromComm.add(pf1.unitPrice(commSoldUtil_1, null, seller, commSold1, economy) * commSoldUtil_1);
        commSoldUtil_2 = seller.getCommoditiesSold().get(2).getQuantity()/seller.getCommoditiesSold().get(2).getEffectiveCapacity();
        revFromComm.add(pf2.unitPrice(commSoldUtil_2, null, seller, commSold2, economy) * commSoldUtil_2);
        Collections.sort(revFromComm, Collections.reverseOrder());

        // get seller's revenue again
        for (Market market : economy.getMarkets()) {
            ledger.calculateExpAndRevForSellersInMarket(economy, market);
        }

        totalRev = revFromComm.get(0) + revFromComm.get(1);
        assertEquals(sellerIncomeStmt.getRevenues(), totalRev, TestUtils.FLOATING_POINT_DELTA);


        // apply a step price function
        // change back max, min desired util
        revFromComm.clear();
        seller.getSettings().setMaxDesiredUtil(1.0);
        seller.getSettings().setMinDesiredUtil(0.0);
        commSold0.getSettings().setPriceFunction(PriceFunction.Cache.createStepPriceFunction(0.3, 50.0, 20000.0));
        pf0 = commSold0.getSettings().getPriceFunction();
        commSoldUtil_0 = seller.getCommoditiesSold().get(0).getQuantity()/seller.getCommoditiesSold().get(0).getEffectiveCapacity();
        revFromComm.add(pf0.unitPrice(commSoldUtil_0, null, seller, commSold0, economy) * commSoldUtil_0);

        commSold1.getSettings().setPriceFunction(PriceFunction.Cache.createStepPriceFunction(3.0, 50.0, 20000.0));
        pf1 = commSold1.getSettings().getPriceFunction();
        commSoldUtil_1 = seller.getCommoditiesSold().get(1).getQuantity()/seller.getCommoditiesSold().get(1).getEffectiveCapacity();
        revFromComm.add(pf1.unitPrice(commSoldUtil_1, null, seller, commSold1, economy) * commSoldUtil_1);

        commSold2.getSettings().setPriceFunction(PriceFunction.Cache.createStepPriceFunction(2.0, 50.0, 20000.0));
        pf2 = commSold2.getSettings().getPriceFunction();
        commSoldUtil_2 = commSold2.getQuantity()/seller.getCommoditiesSold().get(2).getEffectiveCapacity();
        revFromComm.add(pf2.unitPrice(commSoldUtil_2, null, seller, commSold2, economy) * commSoldUtil_2);
        Collections.sort(revFromComm, Collections.reverseOrder());

        // get seller's revenue again
        for (Market market : economy.getMarkets()) {
            ledger.calculateExpAndRevForSellersInMarket(economy, market);
        }

        totalRev = revFromComm.get(0) + revFromComm.get(1);
        assertEquals(sellerIncomeStmt.getRevenues(), totalRev, TestUtils.FLOATING_POINT_DELTA);

    }

    /**
     * Test some edge cases for Sellers revenues.
     *
     */
    @Test
    public final void testCalculateRevenuesForSellersEdgeCases_Economy_Trader() {
        Economy economy = new Economy();

        List<CommoditySpecification> commSpecSold = new ArrayList<>();
        commSpecSold.add(new CommoditySpecification(0, 0, 10, false));
        commSpecSold.add(new CommoditySpecification(1, 0, 20, false));
        commSpecSold.add(new CommoditySpecification(2, 0, 5, false));

        Basket basketSold = new Basket(commSpecSold);

        //1. Test all commodities having a used of 0
        // create supplier
        Trader seller = economy.addTrader(0, TraderState.ACTIVE, basketSold);
        CommoditySold commSold0 = seller.getCommoditiesSold().get(0).setQuantity(0).setPeakQuantity(10).setCapacity(100);
        CommoditySold commSold1 = seller.getCommoditiesSold().get(1).setQuantity(0).setPeakQuantity(20).setCapacity(20);
        CommoditySold commSold2 = seller.getCommoditiesSold().get(2).setQuantity(0).setPeakQuantity(5).setCapacity(500);

        // create consumer
        Trader buyer = economy.addTrader(1, TraderState.ACTIVE, basketSold);
        economy.addBasketBought(buyer, basketSold)
            .setQuantity(0, 0).setPeakQuantity(0, 10)
            .setQuantity(1, 0).setPeakQuantity(1, 20)
            .setQuantity(2, 0).setPeakQuantity(2, 5).setMovable(true).move(seller);


        Ledger ledger = new Ledger(economy);

        List<Double> revFromComm = new ArrayList<>();
        // get revenues from individual commodities
        PriceFunction pf0 = commSold0.getSettings().getPriceFunction();
        double commSoldUtil_0 = commSold0.getQuantity()/seller.getCommoditiesSold().get(0).getEffectiveCapacity();
        revFromComm.add(pf0.unitPrice(commSoldUtil_0, null, seller, commSold0, economy) * commSoldUtil_0);

        PriceFunction pf1 = commSold1.getSettings().getPriceFunction();
        double commSoldUtil_1 = commSold1.getQuantity()/seller.getCommoditiesSold().get(1).getEffectiveCapacity();
        revFromComm.add(pf1.unitPrice(commSoldUtil_1, null, seller, commSold1, economy) * commSoldUtil_1);

        PriceFunction pf2 = commSold2.getSettings().getPriceFunction();
        double commSoldUtil_2 = commSold2.getQuantity()/seller.getCommoditiesSold().get(2).getEffectiveCapacity();
        revFromComm.add(pf2.unitPrice(commSoldUtil_2, null, seller, commSold2, economy) * commSoldUtil_2);
        Collections.sort(revFromComm, Collections.reverseOrder());

        IncomeStatement sellerIncomeStmt = ledger.getTraderIncomeStatements().get(seller.getEconomyIndex());

        // get seller's revenue
        for (Market market : economy.getMarkets()) {
            ledger.calculateExpAndRevForSellersInMarket(economy, market);
        }

        // assert seller's revenue equals revenues from top 2 commodities
        double totalRev = revFromComm.get(0) + revFromComm.get(1);
        assertEquals(sellerIncomeStmt.getRevenues(), totalRev, TestUtils.FLOATING_POINT_DELTA);

        //2.  Test some commodities over max desired utilization
        commSpecSold.clear();
        commSpecSold.add(new CommoditySpecification(0, 4, 10, false));
        commSpecSold.add(new CommoditySpecification(1, 10, 20, false));
        commSpecSold.add(new CommoditySpecification(2, 5, 5, false));

        basketSold = new Basket(commSpecSold);

        // create supplier
        Trader seller1 = economy.addTrader(0, TraderState.ACTIVE, basketSold);
        commSold0.setQuantity(4).setPeakQuantity(10).setCapacity(10);
        commSold1.setQuantity(20).setPeakQuantity(20).setCapacity(20);
        commSold2.setQuantity(5).setPeakQuantity(5).setCapacity(5);

        // create consumer
        Trader buyer1 = economy.addTrader(1, TraderState.ACTIVE, basketSold);
        economy.addBasketBought(buyer1, basketSold)
            .setQuantity(0, 4).setPeakQuantity(0, 10)
            .setQuantity(1, 10).setPeakQuantity(1, 20)
            .setQuantity(2, 5).setPeakQuantity(2, 5).setMovable(true).move(seller);
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        Ledger ledger1 = new Ledger(economy);

        // set desired max utilization
        commSold0.getSettings().setUtilizationUpperBound(0.7);
        commSold1.getSettings().setUtilizationUpperBound(0.7);
        commSold2.getSettings().setUtilizationUpperBound(0.7);

        revFromComm.clear();
        // get revenues from individual commodities
        commSoldUtil_0 = seller1.getCommoditiesSold().get(0).getQuantity()/seller1.getCommoditiesSold().get(0).getEffectiveCapacity();
        revFromComm.add(pf0.unitPrice(commSoldUtil_0, null, seller, commSold0, economy) * commSoldUtil_0);

        commSoldUtil_1 = seller1.getCommoditiesSold().get(1).getQuantity()/seller1.getCommoditiesSold().get(1).getEffectiveCapacity();
        revFromComm.add(pf1.unitPrice(commSoldUtil_1, null, seller, commSold1, economy) * commSoldUtil_1);

        commSoldUtil_2 = seller1.getCommoditiesSold().get(2).getQuantity()/seller1.getCommoditiesSold().get(2).getEffectiveCapacity();
        revFromComm.add(pf2.unitPrice(commSoldUtil_2, null, seller, commSold2, economy) * commSoldUtil_2);
        Collections.sort(revFromComm, Collections.reverseOrder());

        IncomeStatement seller1IncomeStmt = ledger1.getTraderIncomeStatements().get(seller1.getEconomyIndex());
        // get seller's revenue
        for (Market market : economy.getMarkets()) {
            ledger1.calculateExpAndRevForSellersInMarket(economy, market);
        }

        // assert seller's revenue equals revenues from top 2 commodities
        totalRev = revFromComm.get(0) + revFromComm.get(1);
        assertEquals(seller1IncomeStmt.getRevenues(), totalRev, TestUtils.FLOATING_POINT_DELTA);
    }

    /**
     * This method tests the income statements generated by ledger based on historical
     * quantity contains valid revenues and expenses.
     *
     * @param historicalQuanity the historical quantity of commodity sold by trader.
     */
    @Test
    @Parameters({"0", "1", "3", "4", "7", "8", "9", "10"})
    @TestCaseName("Test #{index}: TestRevExpHistoricalQuanity({0})")
    public final void testCalculateRevenueAndExpensesUsingHistoricalQuanity(
                    double historicalQuanity) {

        Economy economy = new Economy();
        final CommoditySpecification cpuSpec =
                        new CommoditySpecification(0, 1000, 1, Integer.MAX_VALUE);
        final CommoditySpecification vcpuSpec =
                        new CommoditySpecification(1, 1000, 1, Integer.MAX_VALUE);
        Basket basketSoldBySeller = new Basket(Collections.singleton(cpuSpec));
        Basket basketSoldByBuyer = new Basket(Collections.singleton(vcpuSpec));

        // create supplier
        Trader seller = economy.addTrader(0, TraderState.ACTIVE, basketSoldBySeller);
        seller
            .getCommoditiesSold()
            .get(0)
                .setQuantity(5)
                .setPeakQuantity(10)
                .setCapacity(100);

        // create consumer
        Trader buyer = economy.addTrader(1, TraderState.ACTIVE, basketSoldByBuyer);
        economy.addBasketBought(buyer, basketSoldBySeller)
            .setQuantity(0, 6)
            .setPeakQuantity(0, 9)
            .setMovable(true)
            .move(seller);

        buyer
            .getCommoditiesSold()
            .get(0)
                .setQuantity(6)
                .setHistoricalQuantity(historicalQuanity)
                .setPeakQuantity(9)
                .setCapacity(10);

        economy.getModifiableRawCommodityMap().put(vcpuSpec.getBaseType(),
                        Arrays.asList(cpuSpec.getBaseType()));

        buyer.getSettings().setMinDesiredUtil(0.6);
        buyer.getSettings().setMaxDesiredUtil(0.7);

        seller.getSettings().setMinDesiredUtil(0.6);
        seller.getSettings().setMaxDesiredUtil(0.7);

        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        Ledger ledger = new Ledger(economy);

        ledger.calculateCommodityExpensesAndRevenuesForTrader(economy, buyer);

        IncomeStatement incomeStatements = ledger.getCommodityIncomeStatements(buyer).get(0);

        CommoditySold cs = buyer
                        .getCommoditiesSold()
                        .get(0);
        PriceFunction pf = cs
                        .getSettings()
                        .getPriceFunction();

        // Calculating expected revenues and expenses
        double revenues = pf.unitPrice(cs.getHistoricalOrElseCurrentUtilization(), null, buyer, cs,
                        economy) * cs.getHistoricalOrElseCurrentUtilization();
        double expense =
                  (( pf.unitPrice(buyer.getSettings().getMaxDesiredUtil(), null, buyer, cs, economy)
                  + pf.unitPrice(buyer.getSettings().getMinDesiredUtil(), null, buyer, cs, economy))
                  / 2)
                  * 0.65;


        assertEquals(revenues, incomeStatements.getRevenues(), 0.001);
        assertEquals(expense, incomeStatements.getExpenses(), 0.001);
    }

} // end LedgerTest class
