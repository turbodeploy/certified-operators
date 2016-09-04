package com.vmturbo.platform.analysis.economy;

import static org.junit.Assert.*;
import static com.vmturbo.platform.analysis.utility.ListTests.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

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
    private static final CommoditySpecification A = new CommoditySpecification(0,1000,0,0);
    private static final CommoditySpecification B = new CommoditySpecification(0,1000,0,100);
    private static final CommoditySpecification C = new CommoditySpecification(1,1001,0,100);

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
        MapTests.verifyUnmodifiableValidOperations(fixture_.getCliques(),42,Arrays.asList());
    }

    @Test
    public final void testGetCliques_InvalidOperations() {
        MapTests.verifyUnmodifiableInvalidOperations(fixture_.getCliques(),42,Arrays.asList());
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
