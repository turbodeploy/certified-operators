package com.vmturbo.platform.analysis.economy;

import static org.junit.Assert.*;
import static com.vmturbo.platform.analysis.utility.ListTests.*;

import java.util.Arrays;
import java.util.HashSet;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;


import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

/**
 * A test case for the {@link Market} class.
 */
@RunWith(JUnitParamsRunner.class)
public class MarketTest {

    // Fields

    private static final CommoditySpecification A = new CommoditySpecification(0,0,0);
    private static final CommoditySpecification B = new CommoditySpecification(0,0,100);
    private static final CommoditySpecification C = new CommoditySpecification(1,0,100);

    private static final TraderWithSettings T0 = new TraderWithSettings(0, 0, TraderState.ACTIVE, new Basket());
    private static final TraderWithSettings T0A = new TraderWithSettings(0, 0, TraderState.ACTIVE, new Basket(A));
    private static final TraderWithSettings T0AB = new TraderWithSettings(0, 0, TraderState.ACTIVE, new Basket(A,B));
    private static final TraderWithSettings T0AC = new TraderWithSettings(0, 0, TraderState.ACTIVE, new Basket(A,C));
    private static final TraderWithSettings T1A = new TraderWithSettings(1, 0, TraderState.ACTIVE, new Basket(A));
    private static final TraderWithSettings T1B = new TraderWithSettings(1, 0, TraderState.ACTIVE, new Basket(B));
    private static final TraderWithSettings T1C = new TraderWithSettings(1, 0, TraderState.ACTIVE, new Basket(C));
    private static final TraderWithSettings T1AB = new TraderWithSettings(1, 0, TraderState.ACTIVE, new Basket(A,B));
    private static final TraderWithSettings T1ABC = new TraderWithSettings(1, 0, TraderState.ACTIVE, new Basket(A,B,C));
    private static final TraderWithSettings T2 = new TraderWithSettings(2, 0, TraderState.ACTIVE, new Basket());
    private static final TraderWithSettings T2AC = new TraderWithSettings(2, 0, TraderState.ACTIVE, new Basket(A,C));
    private static final TraderWithSettings T2ABC = new TraderWithSettings(2, 0, TraderState.ACTIVE, new Basket(A,B,C));

    private static final TraderWithSettings IT0 = new TraderWithSettings(0, 0, TraderState.INACTIVE, new Basket());
    private static final TraderWithSettings IT0A = new TraderWithSettings(0, 0, TraderState.INACTIVE, new Basket(A));
    private static final TraderWithSettings IT1B = new TraderWithSettings(1, 0, TraderState.INACTIVE, new Basket(B));

    private static final BuyerParticipation PT0_0 = new BuyerParticipation(T0, 0);
    private static final BuyerParticipation PT0A_0 = new BuyerParticipation(T0A, 0);

    private Market fixture_;

    // Methods

    @Before
    public void setUp() {
        fixture_ = new Market(new Basket());
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
            new Basket(),
            new Basket(A),
            new Basket(A,B),
            new Basket(A,B,C)
        };
    }

    @Test
    public final void testGetSellers_ValidOperations() {
        verifyUnmodifiableValidOperations(fixture_.getSellers(),T0);
    }

    @Test
    public final void testGetSellers_InvalidOperations() {
        verifyUnmodifiableInvalidOperations(fixture_.getSellers(),T0);
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
            market.addSeller(trader);
        }

        assertEquals(tradersToAdd.length, market.getSellers().size());
        for (TraderWithSettings trader : tradersToAdd) {
            assertTrue(market.getSellers().contains(trader));
            assertTrue(trader.getMarketsAsSeller().contains(market));
        }

        for (TraderWithSettings trader : tradersToRemove) {
            market.removeSeller(trader);
        }

        assertEquals(tradersToAdd.length - tradersToRemove.length, market.getSellers().size());

        HashSet<TraderWithSettings> remainingTraders = new HashSet<>(Arrays.asList(tradersToAdd));
        remainingTraders.removeAll(Arrays.asList(tradersToRemove));
        for (TraderWithSettings trader : remainingTraders) {
            assertTrue(market.getSellers().contains(trader));
            assertTrue(trader.getMarketsAsSeller().contains(market));
        }

        for (TraderWithSettings trader : tradersToRemove) {
            assertTrue(!market.getSellers().contains(trader));
            assertTrue(!trader.getMarketsAsSeller().contains(market));
        }
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestAddRemoveSeller_NormalInput() {
        return new Object[][] {
            {new Basket(), new TraderWithSettings[]{T0}, new TraderWithSettings[]{}},
            {new Basket(), new TraderWithSettings[]{T0}, new TraderWithSettings[]{T0}},
            {new Basket(A), new TraderWithSettings[]{T0AB}, new TraderWithSettings[]{}},
            {new Basket(A), new TraderWithSettings[]{T0AB}, new TraderWithSettings[]{T0AB}},
            {new Basket(), new TraderWithSettings[]{T0,T1AB},new TraderWithSettings[]{}},
            {new Basket(), new TraderWithSettings[]{T0,T1AB},new TraderWithSettings[]{T0}},
            {new Basket(), new TraderWithSettings[]{T0,T1AB},new TraderWithSettings[]{T0,T1AB}},
            {new Basket(), new TraderWithSettings[]{T0,T1AB},new TraderWithSettings[]{T1AB}},
            {new Basket(A), new TraderWithSettings[]{T0A,T1ABC},new TraderWithSettings[]{}},
            {new Basket(A), new TraderWithSettings[]{T0A,T1ABC},new TraderWithSettings[]{T0A}},
            {new Basket(A), new TraderWithSettings[]{T0A,T1ABC},new TraderWithSettings[]{T0A,T1ABC}},
            {new Basket(A), new TraderWithSettings[]{T0A,T1ABC},new TraderWithSettings[]{T1ABC}},
            {new Basket(), new TraderWithSettings[]{T0AC,T1A,T2},new TraderWithSettings[]{}},
            {new Basket(), new TraderWithSettings[]{T0AC,T1A,T2},new TraderWithSettings[]{T0AC}},
            {new Basket(), new TraderWithSettings[]{T0AC,T1A,T2},new TraderWithSettings[]{T1A}},
            {new Basket(), new TraderWithSettings[]{T0AC,T1A,T2},new TraderWithSettings[]{T2}},
            {new Basket(), new TraderWithSettings[]{T0AC,T1A,T2},new TraderWithSettings[]{T0AC,T1A}},
            {new Basket(), new TraderWithSettings[]{T0AC,T1A,T2},new TraderWithSettings[]{T0AC,T2}},
            {new Basket(), new TraderWithSettings[]{T0AC,T1A,T2},new TraderWithSettings[]{T1A,T2}},
            {new Basket(), new TraderWithSettings[]{T0AC,T1A,T2},new TraderWithSettings[]{T0AC,T1A,T2}},
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
            {new Basket(), new TraderWithSettings[]{IT0}, new TraderWithSettings[]{}},
            {new Basket(A), new TraderWithSettings[]{T0}, new TraderWithSettings[]{}},
            {new Basket(A), new TraderWithSettings[]{IT0A}, new TraderWithSettings[]{}},
            {new Basket(A), new TraderWithSettings[]{IT0}, new TraderWithSettings[]{}},
            {new Basket(), new TraderWithSettings[]{T0,IT1B}, new TraderWithSettings[]{}},
            {new Basket(), new TraderWithSettings[]{IT0,T1B}, new TraderWithSettings[]{}},
            {new Basket(A), new TraderWithSettings[]{T0,T1ABC}, new TraderWithSettings[]{}},
            {new Basket(A), new TraderWithSettings[]{T0A,T1C}, new TraderWithSettings[]{}},

            {new Basket(), new TraderWithSettings[]{}, new TraderWithSettings[]{T0}},
            {new Basket(), new TraderWithSettings[]{}, new TraderWithSettings[]{IT0}},
            {new Basket(A), new TraderWithSettings[]{T0AB}, new TraderWithSettings[]{T0}},
            {new Basket(A), new TraderWithSettings[]{T0AB}, new TraderWithSettings[]{T0AB,T0A}},
            {new Basket(), new TraderWithSettings[]{T0,T1AB},new TraderWithSettings[]{T0A}},
            {new Basket(), new TraderWithSettings[]{T0,T1AB},new TraderWithSettings[]{T0,T0A}},
            {new Basket(), new TraderWithSettings[]{T0,T1AB},new TraderWithSettings[]{T0,T0A,T1AB}},
        };
    }

    @Test
    public final void testGetBuyers_ValidOperations() {
        verifyUnmodifiableValidOperations(fixture_.getBuyers(), new BuyerParticipation(T0, 0));
    }

    @Test
    public final void testGetBuyers_InvalidOperations() {
        verifyUnmodifiableInvalidOperations(fixture_.getBuyers(), new BuyerParticipation(T0, 0));
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: new Market({0}).(add|remove)Buyer(...) sequence")
    // TODO (Vaptistis): may need to check cases when a trader is added to more than one market.
    // May also need to check more complex sequences that include arbitrary interleaving.
    public final void testAddRemoveBuyer_NormalInput(@NonNull Basket basket, TraderWithSettings[] tradersToAdd) {
        final Market market = new Market(basket);
        final BuyerParticipation[] participations = new BuyerParticipation[tradersToAdd.length];

        for (int i = 0 ; i < tradersToAdd.length ; ++i) {
            participations[i] = market.addBuyer(tradersToAdd[i]);
        }

        assertEquals(tradersToAdd.length, market.getBuyers().size());
        for (int i = 0 ; i < tradersToAdd.length ; ++i) {
            assertTrue(market.getBuyers().contains(participations[i]));
            assertTrue(tradersToAdd[i].getMarketsAsBuyer().containsEntry(market, participations[i]));
        }

        for (int i = 0 ; i < participations.length ; ++i) {
            market.removeBuyerParticipation(participations[i]);
            assertEquals(tradersToAdd.length-i-1, market.getBuyers().size());
            assertFalse(market.getBuyers().contains(participations[i]));
            assertFalse(tradersToAdd[i].getMarketsAsBuyer().containsEntry(market, participations[i]));
        }
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestAddRemoveBuyer_NormalInput() {
        return new Object[][] {
            {new Basket(), new TraderWithSettings[]{T0}},
            {new Basket(), new TraderWithSettings[]{T0A}},
            {new Basket(A), new TraderWithSettings[]{T0}},
            {new Basket(A), new TraderWithSettings[]{T0A}},
            {new Basket(A), new TraderWithSettings[]{T0,T0}},
            {new Basket(), new TraderWithSettings[]{T0A,T0A}},
            {new Basket(A), new TraderWithSettings[]{T0A,T1B,T2AC}},
            {new Basket(A), new TraderWithSettings[]{T0A,T1B,T2AC,T1B}},
        };
    }

    @Test(expected = IllegalArgumentException.class)
    @Parameters
    @TestCaseName("Test #{index}: new Market({0}).(add|remove)Buyer(...) sequence")
    public final void testAddRemoveBuyer_InvalidInput(@NonNull Basket basket, TraderWithSettings[] tradersToAdd,
                                                      BuyerParticipation[] participationsToRemove) {
        final Market market = new Market(basket);

        for (TraderWithSettings trader : tradersToAdd) {
            market.addBuyer(trader);
        }

        for (BuyerParticipation participation : participationsToRemove) {
            market.removeBuyerParticipation(participation);
        }
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestAddRemoveBuyer_InvalidInput() {
        return new Object[][] {
            {new Basket(), new TraderWithSettings[]{IT0}, new BuyerParticipation[]{}},
            {new Basket(), new TraderWithSettings[]{IT0A}, new BuyerParticipation[]{}},
            {new Basket(), new TraderWithSettings[]{T0}, new BuyerParticipation[]{PT0_0}},
            {new Basket(), new TraderWithSettings[]{T0,T0A}, new BuyerParticipation[]{PT0A_0}},
        };
    }

} // end MarketTest class
