package com.vmturbo.platform.analysis.economy;

import static com.vmturbo.platform.analysis.utility.ListTests.verifyUnmodifiableInvalidOperations;
import static com.vmturbo.platform.analysis.utility.ListTests.verifyUnmodifiableValidOperations;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.vmturbo.platform.analysis.pricefunction.QuoteFunctionFactory;
import com.vmturbo.platform.analysis.protobuf.BalanceAccountDTOs.BalanceAccountDTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommodityBoughtTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldSettingsTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySpecificationTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.Context;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.ShoppingListTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderSettingsTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderStateTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.analysis.protobuf.PriceFunctionDTOs.PriceFunctionTO;
import com.vmturbo.platform.analysis.protobuf.PriceFunctionDTOs.PriceFunctionTO.StandardWeighted;
import com.vmturbo.platform.analysis.protobuf.QuoteFunctionDTOs.QuoteFunctionDTO;
import com.vmturbo.platform.analysis.protobuf.QuoteFunctionDTOs.QuoteFunctionDTO.SumOfCommodity;
import com.vmturbo.platform.analysis.protobuf.UpdatingFunctionDTOs.UpdatingFunctionTO;
import com.vmturbo.platform.analysis.protobuf.UpdatingFunctionDTOs.UpdatingFunctionTO.Delta;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;
import com.vmturbo.platform.analysis.topology.Topology;
import com.vmturbo.platform.analysis.translators.ProtobufToAnalysis;
import com.vmturbo.platform.analysis.utilities.CostFunction;
import com.vmturbo.platform.analysis.utility.MapTests;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

/**
 * A test case for the {@link Market} class.
 */
@RunWith(JUnitParamsRunner.class)
public class MarketTest {

    // Fields
    private static final CommoditySpecification A = new CommoditySpecification(0,1000);
    private static final CommoditySpecification B = new CommoditySpecification(0,1000);
    private static final CommoditySpecification C = new CommoditySpecification(1,1001);

    private static final Basket EMPTY = new Basket();

    private static final TraderWithSettings T0 = new TraderWithSettings(0, 0, TraderState.ACTIVE, EMPTY);
    private static final TraderWithSettings T0A = new TraderWithSettings(0, 0, TraderState.ACTIVE, new Basket(A));
    private static final TraderWithSettings T0AB = new TraderWithSettings(0, 0, TraderState.ACTIVE, new Basket(A,B));
    private static final TraderWithSettings T0AC = new TraderWithSettings(0, 0, TraderState.ACTIVE, new Basket(A,C));
    private static final TraderWithSettings T1A = new TraderWithSettings(1, 0, TraderState.ACTIVE, new Basket(A));
    private static final TraderWithSettings T1B = new TraderWithSettings(1, 0, TraderState.ACTIVE, new Basket(B));
    private static final TraderWithSettings T1C = new TraderWithSettings(1, 0, TraderState.ACTIVE, new Basket(C));
    private static final TraderWithSettings T1AB = new TraderWithSettings(1, 0, TraderState.ACTIVE, new Basket(A,B));
    private static final TraderWithSettings T1ABC = new TraderWithSettings(1, 0, TraderState.ACTIVE, new Basket(A,B,C));
    private static final TraderWithSettings T2 = new TraderWithSettings(2, 0, TraderState.ACTIVE, EMPTY);
    private static final TraderWithSettings T2AC = new TraderWithSettings(2, 0, TraderState.ACTIVE, new Basket(A,C));
    private static final TraderWithSettings T2ABC = new TraderWithSettings(2, 0, TraderState.ACTIVE, new Basket(A,B,C));

    private static final TraderWithSettings IT0 = new TraderWithSettings(0, 0, TraderState.INACTIVE, EMPTY);
    private static final TraderWithSettings IT0A = new TraderWithSettings(0, 0, TraderState.INACTIVE, new Basket(A));
    private static final TraderWithSettings IT1B = new TraderWithSettings(1, 0, TraderState.INACTIVE, new Basket(B));

    private static final ShoppingList PT0_0 = new ShoppingList(T0, EMPTY);
    private static final ShoppingList PT0A_0 = new ShoppingList(T0A, EMPTY);
    private static final ShoppingList PIT0_0 = new ShoppingList(IT0, EMPTY);
    private static final ShoppingList PIT0A_0 = new ShoppingList(IT0A, EMPTY);

    private Market fixture_;

    // Methods

    @Before
    public void setUp() {
        fixture_ = new Market(EMPTY);
    }


    @Test
    @Parameters
    @TestCaseName("Test #{index}: new Market({0}).getBasket() == {0}")
    public final void testMarket_GetBasket(@NonNull Basket basket) {
        assertSame(basket, new Market(basket).getBasket());
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestMarket_GetBasket() {
        return new Basket[] {
            EMPTY,
            new Basket(A),
            new Basket(A,B),
            new Basket(A,B,C)
        };
    }

    @Test
    public final void testGetActiveSellers_ValidOperations() {
        verifyUnmodifiableValidOperations(fixture_.getActiveSellers(),T0);
    }

    @Test
    public final void testGetActiveSellers_InvalidOperations() {
        verifyUnmodifiableInvalidOperations(fixture_.getActiveSellers(),T0);
    }

    @Test
    public final void testGetCliques_ValidOperations() {
        MapTests.verifyUnmodifiableValidOperations(fixture_.getCliques(),42L,Arrays.asList());
    }

    @Test
    public final void testGetCliques_InvalidOperations() {
        MapTests.verifyUnmodifiableInvalidOperations(fixture_.getCliques(),42L,Arrays.asList());
    }

    @Test
    public final void testGetInactiveSellers_ValidOperations() {
        verifyUnmodifiableValidOperations(fixture_.getInactiveSellers(),T0);
    }

    @Test
    public final void testGetInactiveSellers_InvalidOperations() {
        verifyUnmodifiableInvalidOperations(fixture_.getInactiveSellers(),T0);
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: new Market({0}).(add|remove)Seller(...) sequence")
    // TODO (Vaptistis): may need to check cases when a trader is added to more than one market.
    // May also need to check more complex sequences that include arbitrary interleaving.
    public final void testAddRemoveSeller_NormalInput(@NonNull Basket basket, TraderWithSettings[] tradersToAdd,
                                                      TraderWithSettings[] tradersToRemove) {
        final Market market = new Market(basket);

        for (TraderWithSettings trader : tradersToAdd) {
            assertSame(market, market.addSeller(trader));
        }

        assertEquals(tradersToAdd.length, market.getActiveSellers().size() + market.getInactiveSellers().size());
        for (TraderWithSettings trader : tradersToAdd) {
            assertEquals(trader.getState().isActive(), market.getActiveSellers().contains(trader));
            assertNotEquals(trader.getState().isActive(), market.getInactiveSellers().contains(trader));
            assertTrue(trader.getMarketsAsSeller().contains(market));
        }

        for (TraderWithSettings trader : tradersToRemove) {
            assertSame(market, market.removeSeller(trader));
        }

        assertEquals(tradersToAdd.length - tradersToRemove.length,
                     market.getActiveSellers().size() + market.getInactiveSellers().size());

        HashSet<TraderWithSettings> remainingTraders = new HashSet<>(Arrays.asList(tradersToAdd));
        remainingTraders.removeAll(Arrays.asList(tradersToRemove));
        for (TraderWithSettings trader : remainingTraders) {
            assertEquals(trader.getState().isActive(), market.getActiveSellers().contains(trader));
            assertNotEquals(trader.getState().isActive(), market.getInactiveSellers().contains(trader));
            assertTrue(trader.getMarketsAsSeller().contains(market));
        }

        for (TraderWithSettings trader : tradersToRemove) {
            assertTrue(!market.getActiveSellers().contains(trader));
            assertTrue(!market.getInactiveSellers().contains(trader));
            assertTrue(!trader.getMarketsAsSeller().contains(market));
        }
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestAddRemoveSeller_NormalInput() {
        return new Object[][] {
            {EMPTY, new TraderWithSettings[]{T0}, new TraderWithSettings[]{}},
            {EMPTY, new TraderWithSettings[]{IT0}, new TraderWithSettings[]{}},
            {EMPTY, new TraderWithSettings[]{T0}, new TraderWithSettings[]{T0}},
            {EMPTY, new TraderWithSettings[]{IT0}, new TraderWithSettings[]{IT0}},
            {new Basket(A), new TraderWithSettings[]{T0AB}, new TraderWithSettings[]{}},
            {new Basket(A), new TraderWithSettings[]{T0AB}, new TraderWithSettings[]{T0AB}},
            {new Basket(A), new TraderWithSettings[]{IT0A}, new TraderWithSettings[]{}},
            {new Basket(A), new TraderWithSettings[]{IT0A}, new TraderWithSettings[]{IT0A}},
            {EMPTY, new TraderWithSettings[]{T0,T1AB},new TraderWithSettings[]{}},
            {EMPTY, new TraderWithSettings[]{T0,T1AB},new TraderWithSettings[]{T0}},
            {EMPTY, new TraderWithSettings[]{T0,T1AB},new TraderWithSettings[]{T0,T1AB}},
            {EMPTY, new TraderWithSettings[]{T0,T1AB},new TraderWithSettings[]{T1AB}},
            {EMPTY, new TraderWithSettings[]{IT0,T1AB},new TraderWithSettings[]{}},
            {EMPTY, new TraderWithSettings[]{IT0,T1AB},new TraderWithSettings[]{IT0}},
            {EMPTY, new TraderWithSettings[]{IT0,T1AB},new TraderWithSettings[]{IT0,T1AB}},
            {EMPTY, new TraderWithSettings[]{IT0,T1AB},new TraderWithSettings[]{T1AB}},
            {new Basket(A), new TraderWithSettings[]{T0A,T1ABC},new TraderWithSettings[]{}},
            {new Basket(A), new TraderWithSettings[]{T0A,T1ABC},new TraderWithSettings[]{T0A}},
            {new Basket(A), new TraderWithSettings[]{T0A,T1ABC},new TraderWithSettings[]{T0A,T1ABC}},
            {new Basket(A), new TraderWithSettings[]{T0A,T1ABC},new TraderWithSettings[]{T1ABC}},
            {EMPTY, new TraderWithSettings[]{T0AC,T1A,T2},new TraderWithSettings[]{}},
            {EMPTY, new TraderWithSettings[]{T0AC,T1A,T2},new TraderWithSettings[]{T0AC}},
            {EMPTY, new TraderWithSettings[]{T0AC,T1A,T2},new TraderWithSettings[]{T1A}},
            {EMPTY, new TraderWithSettings[]{T0AC,T1A,T2},new TraderWithSettings[]{T2}},
            {EMPTY, new TraderWithSettings[]{T0AC,T1A,T2},new TraderWithSettings[]{T0AC,T1A}},
            {EMPTY, new TraderWithSettings[]{T0AC,T1A,T2},new TraderWithSettings[]{T0AC,T2}},
            {EMPTY, new TraderWithSettings[]{T0AC,T1A,T2},new TraderWithSettings[]{T1A,T2}},
            {EMPTY, new TraderWithSettings[]{T0AC,T1A,T2},new TraderWithSettings[]{T0AC,T1A,T2}},
            {new Basket(A), new TraderWithSettings[]{T0A,T1B,T2AC},new TraderWithSettings[]{}},
            {new Basket(A), new TraderWithSettings[]{T0A,T1B,T2AC},new TraderWithSettings[]{T2AC,T1B}},
            {new Basket(A), new TraderWithSettings[]{T0A,T1B,T2AC},new TraderWithSettings[]{T2AC,T0A}},
            {new Basket(A), new TraderWithSettings[]{T0A,T1B,T2AC},new TraderWithSettings[]{T1B,T0A}},
            {new Basket(A), new TraderWithSettings[]{T0A,T1B,T2AC},new TraderWithSettings[]{T2AC,T1B,T0A}},
            {new Basket(A,B), new TraderWithSettings[]{T0AB,T1AB,T2ABC},new TraderWithSettings[]{}},
        };
    }

    @Test(expected = IllegalArgumentException.class)
    @Parameters
    @TestCaseName("Test #{index}: new Market({0}).(add|remove)Seller(...) sequence")
    public final void testAddRemoveSeller_InvalidInput(@NonNull Basket basket, TraderWithSettings[] tradersToAdd,
                                                       TraderWithSettings[] tradersToRemove) {
        final Market market = new Market(basket);

        for (TraderWithSettings trader : tradersToAdd) {
            market.addSeller(trader);
        }

        for (TraderWithSettings trader : tradersToRemove) {
            market.removeSeller(trader);
        }
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestAddRemoveSeller_InvalidInput() {
        return new Object[][] {
            {new Basket(A), new TraderWithSettings[]{T0}, new TraderWithSettings[]{}},
            {new Basket(A), new TraderWithSettings[]{IT0}, new TraderWithSettings[]{}},
            {new Basket(A), new TraderWithSettings[]{T0,T1ABC}, new TraderWithSettings[]{}},
            {new Basket(A), new TraderWithSettings[]{T0A,T1C}, new TraderWithSettings[]{}},
            {new Basket(A), new TraderWithSettings[]{IT0A,T1C}, new TraderWithSettings[]{}},

            {EMPTY, new TraderWithSettings[]{}, new TraderWithSettings[]{T0}},
            {EMPTY, new TraderWithSettings[]{}, new TraderWithSettings[]{IT0}},
            {new Basket(A), new TraderWithSettings[]{T0AB}, new TraderWithSettings[]{T0}},
            {new Basket(A), new TraderWithSettings[]{T0AB}, new TraderWithSettings[]{IT0}},
            {new Basket(A), new TraderWithSettings[]{T0AB}, new TraderWithSettings[]{T0AB,T0A}},
            {EMPTY, new TraderWithSettings[]{T0,T1AB},new TraderWithSettings[]{T0A}},
            {EMPTY, new TraderWithSettings[]{T0,T1AB},new TraderWithSettings[]{IT0A}},
            {EMPTY, new TraderWithSettings[]{T0,T1AB},new TraderWithSettings[]{T0,T0A}},
            {EMPTY, new TraderWithSettings[]{T0,T1AB},new TraderWithSettings[]{T0,T0A,T1AB}},
        };
    }

    @Test
    public final void testGetBuyers_ValidOperations() {
        verifyUnmodifiableValidOperations(fixture_.getBuyers(), new ShoppingList(T0, EMPTY));
    }

    @Test
    public final void testGetBuyers_InvalidOperations() {
        verifyUnmodifiableInvalidOperations(fixture_.getBuyers(), new ShoppingList(T0, EMPTY));
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: new Market({0}).(add|remove)Buyer(...) sequence")
    // TODO (Vaptistis): may need to check cases when a trader is added to more than one market.
    // May also need to check more complex sequences that include arbitrary interleaving.
    public final void testAddRemoveBuyer_NormalInput(@NonNull Basket basket, TraderWithSettings[] tradersToAdd) {
        final Market market = new Market(basket);
        final ShoppingList[] shoppingLists = new ShoppingList[tradersToAdd.length];

        for (int i = 0 ; i < tradersToAdd.length ; ++i) {
            shoppingLists[i] = market.addBuyer(tradersToAdd[i]);
            assertSame(tradersToAdd[i], shoppingLists[i].getBuyer());
        }

        int nActive = 0;
        for (int i = 0 ; i < tradersToAdd.length ; ++i) {
            if (shoppingLists[i].getBuyer().getState().isActive()) {
                assertSame(shoppingLists[i], market.getBuyers().get(nActive));
                ++nActive;
            }
            assertSame(market, tradersToAdd[i].getMarketsAsBuyer().get(shoppingLists[i]));
        }
        assertEquals(nActive, market.getBuyers().size());

        for (int i = 0 ; i < shoppingLists.length ; ++i) {
            assertSame(market, market.removeShoppingList(shoppingLists[i]));
            if (shoppingLists[i].getBuyer().getState().isActive()) {
                --nActive;
            }
            assertEquals(nActive, market.getBuyers().size());
            assertFalse(market.getBuyers().contains(shoppingLists[i]));
            assertNull(tradersToAdd[i].getMarketsAsBuyer().get(shoppingLists[i]));
        }
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestAddRemoveBuyer_NormalInput() {
        return new Object[][] {
            {EMPTY, new TraderWithSettings[]{T0}},
            {EMPTY, new TraderWithSettings[]{IT0}},
            {EMPTY, new TraderWithSettings[]{T0A}},
            {EMPTY, new TraderWithSettings[]{IT0A}},
            {new Basket(A), new TraderWithSettings[]{T0}},
            {new Basket(A), new TraderWithSettings[]{T0A}},
            {new Basket(A), new TraderWithSettings[]{IT0}},
            {new Basket(A), new TraderWithSettings[]{IT0A}},
            {new Basket(A), new TraderWithSettings[]{T0,T0}},
            {EMPTY, new TraderWithSettings[]{T0A,T0A}},
            {new Basket(A), new TraderWithSettings[]{IT0,IT0}},
            {EMPTY, new TraderWithSettings[]{IT0A,IT0A}},
            {new Basket(A), new TraderWithSettings[]{T0A,T1B,T2AC}},
            {new Basket(A), new TraderWithSettings[]{T0A,T1B,T2AC,T1B}},
            {new Basket(A), new TraderWithSettings[]{T0A,T1B,T2AC,IT1B}},
            {new Basket(A), new TraderWithSettings[]{IT0A,T1B,T2AC}},
            {new Basket(A), new TraderWithSettings[]{IT0A,T1B,T2AC,T1B}},
            {new Basket(A), new TraderWithSettings[]{IT0A,T1B,T2AC,IT1B}},
        };
    }

    @Test(expected = IllegalArgumentException.class)
    @Parameters
    @TestCaseName("Test #{index}: new Market({0}).(add|remove)Buyer(...) sequence")
    public final void testAddRemoveBuyer_InvalidInput(@NonNull Basket basket, TraderWithSettings[] tradersToAdd,
                                                      ShoppingList[] shoppingListsToRemove) {
        final Market market = new Market(basket);

        for (TraderWithSettings trader : tradersToAdd) {
            market.addBuyer(trader);
        }

        for (ShoppingList shoppingList : shoppingListsToRemove) {
            market.removeShoppingList(shoppingList);
        }
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestAddRemoveBuyer_InvalidInput() {
        return new Object[][] {
            {EMPTY, new TraderWithSettings[]{}, new ShoppingList[]{PT0_0}},
            {EMPTY, new TraderWithSettings[]{T0}, new ShoppingList[]{PT0_0}},
            {EMPTY, new TraderWithSettings[]{T0,T0A}, new ShoppingList[]{PT0A_0}},
            {EMPTY, new TraderWithSettings[]{IT0}, new ShoppingList[]{PIT0_0}},
            {EMPTY, new TraderWithSettings[]{IT0,IT0A}, new ShoppingList[]{PIT0A_0}},
        };
    }

    /*
    This test the sorting of virtual machines in a cloud based on the cost.
    The test has 3 VMS all buying storage from the same storage. VM0 is buying 20,
    VM1 is buying 40 and VM2 is buying 30. Since VM1 is buying the most it should
    be the one that is spening the most. So in the buyers list it will be the first
    on the list.
    */
    @Test
    public void testBuyerSortCloud() {
        BalanceAccountDTO ba = BalanceAccountDTO.newBuilder().setBudget(10000).setSpent(100).setId(1).build();
        TraderSettingsTO shoptogetherFalseTO =
                TraderSettingsTO.newBuilder().setIsShopTogether(false)
                        .setCurrentContext(Context.newBuilder().setBalanceAccount(ba).setRegionId(10L).build())
                        .setQuoteFunction(QuoteFunctionDTO.newBuilder()
                                .setSumOfCommodity(SumOfCommodity
                                        .newBuilder().build())
                                .build())
                        .build();


        CommodityBoughtTO storageBoughtTO0 = CommodityBoughtTO.newBuilder().setQuantity(20)
                .setPeakQuantity(50).setSpecification(TestUtils.stAmtTO).build();
        CommodityBoughtTO storageBoughtTO1 = CommodityBoughtTO.newBuilder().setQuantity(40)
                .setPeakQuantity(40).setSpecification(TestUtils.stAmtTO).build();
        CommodityBoughtTO storageBoughtTO2 = CommodityBoughtTO.newBuilder().setQuantity(30)
                .setPeakQuantity(30).setSpecification(TestUtils.stAmtTO).build();

        PriceFunctionTO standardPriceTO = PriceFunctionTO.newBuilder().setStandardWeighted(
                StandardWeighted.newBuilder().setWeight(
                        1).build()).build();
        UpdatingFunctionTO ufTO = UpdatingFunctionTO.newBuilder().setDelta(Delta.newBuilder()
                .build()).build();
        CommoditySoldSettingsTO standardSettingTO = CommoditySoldSettingsTO.newBuilder()
                .setPriceFunction(standardPriceTO).setUpdateFunction(ufTO).build();


        CommoditySoldTO storageSoldByST1 = CommoditySoldTO.newBuilder()
                .setSpecification(TestUtils.stAmtTO).setQuantity(1000)
                .setPeakQuantity(1000).setMaxQuantity(1000).setCapacity(2000)
                .setSettings(standardSettingTO).build();




        TraderTO shopAloneVMTO0 = TraderTO.newBuilder().setOid(23456).setType(55555)
                .setState(TraderStateTO.ACTIVE)
                .setSettings(shoptogetherFalseTO)
                .addShoppingLists(ShoppingListTO.newBuilder()
                        .setOid(11114).setMovable(true).setSupplier(56789)
                        .addCommoditiesBought(storageBoughtTO0))
                .build();
        TraderTO shopAloneVMTO1 = TraderTO.newBuilder().setOid(123456).setType(55555)
                .setState(TraderStateTO.ACTIVE)
                .setSettings(shoptogetherFalseTO)
                .addShoppingLists(ShoppingListTO.newBuilder()
                        .setOid(111114).setMovable(true).setSupplier(56789)
                        .addCommoditiesBought(storageBoughtTO1))
                .build();
        TraderTO shopAloneVMTO2 = TraderTO.newBuilder().setOid(223456).setType(55555)
                .setState(TraderStateTO.ACTIVE)
                .setSettings(shoptogetherFalseTO)
                .addShoppingLists(ShoppingListTO.newBuilder()
                        .setOid(211114).setMovable(true).setSupplier(56789)
                        .addCommoditiesBought(storageBoughtTO2))
                .build();



        TraderTO st2TO = TraderTO.newBuilder().setOid(56789).setType(77777)
                .setState(TraderStateTO.ACTIVE)
                .setSettings(shoptogetherFalseTO)
                .addCommoditiesSold(storageSoldByST1).build();

        Topology topology = new Topology();
        Trader shopAloneVM0 = ProtobufToAnalysis.addTrader(topology, shopAloneVMTO0);
        Trader shopAloneVM1 = ProtobufToAnalysis.addTrader(topology, shopAloneVMTO1);
        Trader shopAloneVM2 = ProtobufToAnalysis.addTrader(topology, shopAloneVMTO2);
        TraderSettings traderSst2 = ProtobufToAnalysis.addTrader(topology, st2TO).getSettings();
        traderSst2.setCanAcceptNewCustomers(true);
        CostFunction gp2CostFunc = TestUtils.setUpGP2CostFunction();
        traderSst2.setQuoteFunction(
                QuoteFunctionFactory.budgetDepletionRiskBasedQuoteFunction());
        traderSst2.setCostFunction(gp2CostFunc);
        topology.populateMarketsWithSellersAndMergeConsumerCoverage();
        Economy economy = (Economy)topology.getEconomy();
        economy.getSettings().setSortShoppingLists(true);
        economy.sortBuyersofMarket();
        for (com.vmturbo.platform.analysis.economy.Market market : economy.getMarkets()) {
            assertTrue(market.getBuyers().get(0).getBuyer() == shopAloneVM1);
        }

    }

    @Test
    public void testBuyerSortOnPrem() {

        TraderSettingsTO shoptogetherFalseTO =
                TraderSettingsTO.newBuilder().setIsShopTogether(false)
                        .setQuoteFunction(QuoteFunctionDTO.newBuilder()
                                .setSumOfCommodity(SumOfCommodity
                                        .newBuilder().build())
                                .build())
                        .build();
        CommoditySpecificationTO cpuSpecTO =
                CommoditySpecificationTO.newBuilder().setBaseType(0).setType(1).build();
        CommoditySpecificationTO storageProvisionSpecTO =
                CommoditySpecificationTO.newBuilder().setBaseType(4).setType(5).build();

        CommodityBoughtTO cpuBoughtTO = CommodityBoughtTO.newBuilder().setQuantity(100)
                .setPeakQuantity(100).setSpecification(cpuSpecTO).build();
        CommodityBoughtTO cpuBoughtTO1 = CommodityBoughtTO.newBuilder().setQuantity(120)
                .setPeakQuantity(120).setSpecification(cpuSpecTO).build();
        CommodityBoughtTO cpuBoughtTO2 = CommodityBoughtTO.newBuilder().setQuantity(110)
                .setPeakQuantity(110).setSpecification(cpuSpecTO).build();
        CommodityBoughtTO storageProvisionBoughtTO = CommodityBoughtTO.newBuilder().setQuantity(20)
                .setPeakQuantity(50).setSpecification(storageProvisionSpecTO).build();
        CommodityBoughtTO storageProvisionBoughtTO1 = CommodityBoughtTO.newBuilder().setQuantity(40)
                .setPeakQuantity(40).setSpecification(storageProvisionSpecTO).build();
        CommodityBoughtTO storageProvisionBoughtTO2 = CommodityBoughtTO.newBuilder().setQuantity(30)
                .setPeakQuantity(30).setSpecification(storageProvisionSpecTO).build();

        PriceFunctionTO standardPriceTO = PriceFunctionTO.newBuilder().setStandardWeighted(
                StandardWeighted.newBuilder().setWeight(
                        1).build()).build();
        UpdatingFunctionTO ufTO = UpdatingFunctionTO.newBuilder().setDelta(Delta.newBuilder()
                .build()).build();
        CommoditySoldSettingsTO standardSettingTO = CommoditySoldSettingsTO.newBuilder()
                .setPriceFunction(standardPriceTO).setUpdateFunction(ufTO).build();

        CommoditySoldTO cpuSoldByPM1 = CommoditySoldTO.newBuilder().setSpecification(cpuSpecTO)
                .setQuantity(1000).setPeakQuantity(1000)
                .setMaxQuantity(1000).setCapacity(2000)
                .setSettings(standardSettingTO).build();
        CommoditySoldTO storageSoldByST1 = CommoditySoldTO.newBuilder()
                .setSpecification(storageProvisionSpecTO).setQuantity(1000)
                .setPeakQuantity(1000).setMaxQuantity(1000).setCapacity(2000)
                .setSettings(standardSettingTO).build();




        TraderTO shopAloneVMTO = TraderTO.newBuilder().setOid(23456).setType(55555)
                .setState(TraderStateTO.ACTIVE)
                .setSettings(shoptogetherFalseTO)
                .addShoppingLists(ShoppingListTO.newBuilder()
                        .setOid(11113).setMovable(true).setSupplier(34567)
                        .addCommoditiesBought(cpuBoughtTO))
                .addShoppingLists(ShoppingListTO.newBuilder()
                        .setOid(11114).setMovable(true).setSupplier(56789)
                        .addCommoditiesBought(storageProvisionBoughtTO))
                .build();
        TraderTO shopAloneVMT1 = TraderTO.newBuilder().setOid(123456).setType(55555)
                .setState(TraderStateTO.ACTIVE)
                .setSettings(shoptogetherFalseTO)
                .addShoppingLists(ShoppingListTO.newBuilder()
                        .setOid(111113).setMovable(true).setSupplier(34567)
                        .addCommoditiesBought(cpuBoughtTO1))
                .addShoppingLists(ShoppingListTO.newBuilder()
                        .setOid(111114).setMovable(true).setSupplier(56789)
                        .addCommoditiesBought(storageProvisionBoughtTO1))
                .build();
        TraderTO shopAloneVMT2 = TraderTO.newBuilder().setOid(223456).setType(55555)
                .setState(TraderStateTO.ACTIVE)
                .setSettings(shoptogetherFalseTO)
                .addShoppingLists(ShoppingListTO.newBuilder()
                        .setOid(211113).setMovable(true).setSupplier(34567)
                        .addCommoditiesBought(cpuBoughtTO2))
                .addShoppingLists(ShoppingListTO.newBuilder()
                        .setOid(211114).setMovable(true).setSupplier(56789)
                        .addCommoditiesBought(storageProvisionBoughtTO2))
                .build();
        TraderTO pm1TO = TraderTO.newBuilder().setOid(34567).setType(66666)
                .setState(TraderStateTO.ACTIVE)
                .setSettings(shoptogetherFalseTO)
                .addCommoditiesSold(cpuSoldByPM1).build();

        TraderTO st1TO = TraderTO.newBuilder().setOid(56789).setType(77777)
                .setState(TraderStateTO.ACTIVE)
                .setSettings(shoptogetherFalseTO)
                .addCommoditiesSold(storageSoldByST1).build();

        Topology topology = new Topology();
        Trader shopAloneVM0 = ProtobufToAnalysis.addTrader(topology, shopAloneVMTO);
        Trader shopAloneVM1 = ProtobufToAnalysis.addTrader(topology, shopAloneVMT1);
        Trader shopAloneVM2 = ProtobufToAnalysis.addTrader(topology, shopAloneVMT2);
        TraderSettings traderSpm1 = ProtobufToAnalysis.addTrader(topology, pm1TO).getSettings();
        traderSpm1.setCanAcceptNewCustomers(true);
        TraderSettings traderSst1 = ProtobufToAnalysis.addTrader(topology, st1TO).getSettings();
        traderSst1.setCanAcceptNewCustomers(true);
        topology.populateMarketsWithSellersAndMergeConsumerCoverage();
        Economy economy = (Economy)topology.getEconomy();
        economy.getSettings().setSortShoppingLists(true);
        economy.sortBuyersofMarket();
        for (com.vmturbo.platform.analysis.economy.Market market : economy.getMarkets()) {
            assertTrue(market.getBuyers().get(0).getBuyer() == shopAloneVM1);
        }

    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: changeTraderState({0},TraderState.(ACTIVE|INACTIVE))")
    public final void testChangeTraderState(@NonNull TraderWithSettings trader,
            @NonNull Market[] buyerMarkets, @NonNull Market[] sellerMarkets) {
        for(int i = 0 ; i < 2 ; ++i) { // the second time should have no effect
            Market.changeTraderState(trader, TraderState.ACTIVE);
            assertSame(TraderState.ACTIVE, trader.getState());

            assertEquals(sellerMarkets.length, trader.getMarketsAsSeller().size());
            int j = 0;
            for (Market market : trader.getMarketsAsSeller()) {
                assertSame(sellerMarkets[j++], market);
                assertTrue(market.getActiveSellers().contains(trader));
                assertFalse(market.getInactiveSellers().contains(trader));
            }

            assertEquals(buyerMarkets.length, trader.getMarketsAsBuyer().size());
            j = 0;
            for (Entry<@NonNull ShoppingList, @NonNull Market> entry : trader.getMarketsAsBuyer().entrySet()) {
                assertSame(buyerMarkets[j++], entry.getValue());
                assertTrue(entry.getValue().getBuyers().contains(entry.getKey()));
            }
        }

        for(int i = 0 ; i < 2 ; ++i) { // the second time should have no effect
            Market.changeTraderState(trader, TraderState.INACTIVE);
            assertSame(TraderState.INACTIVE, trader.getState());

            assertEquals(sellerMarkets.length, trader.getMarketsAsSeller().size());
            int j = 0;
            for (Market market : trader.getMarketsAsSeller()) {
                assertSame(sellerMarkets[j++], market);
                assertFalse(market.getActiveSellers().contains(trader));
                assertTrue(market.getInactiveSellers().contains(trader));
            }

            assertEquals(buyerMarkets.length, trader.getMarketsAsBuyer().size());
            j = 0;
            for (Entry<@NonNull ShoppingList, @NonNull Market> entry : trader.getMarketsAsBuyer().entrySet()) {
                assertSame(buyerMarkets[j++], entry.getValue());
                assertFalse(entry.getValue().getBuyers().contains(entry.getKey()));
            }
        }
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestChangeTraderState() {
        List<Object[]> output = new ArrayList<>();

        for (int nMarketsAsSeller = 0 ; nMarketsAsSeller < 3 ; ++nMarketsAsSeller) {
            for (int nMarketsAsBuyer = 0 ; nMarketsAsBuyer < 3 ; ++nMarketsAsBuyer) {
                for (int nParticipationsPerMarket = 1 ; nParticipationsPerMarket < 2 ; ++nParticipationsPerMarket) {
                    TraderWithSettings trader = new TraderWithSettings(0, 0, TraderState.INACTIVE, EMPTY);

                    Market[] sellerMarkets = new Market[nMarketsAsSeller];
                    for (int i = 0 ; i < nMarketsAsSeller ; ++i) {
                        (sellerMarkets[i] = new Market(EMPTY)).addSeller(trader);
                    }

                    Market[] buyerMarkets = new Market[nMarketsAsBuyer*nParticipationsPerMarket];
                    for (int i = 0 ; i < nMarketsAsBuyer ; ++i) {
                        for (int j = 0 ; j < nParticipationsPerMarket ; ++j) {
                            (buyerMarkets[i*nParticipationsPerMarket+j] = new Market(EMPTY)).addBuyer(trader);
                        }
                    }

                    output.add(new Object[]{trader,buyerMarkets,sellerMarkets});
                }
            }
        }

        return output.toArray();
    }

} // end MarketTest class
