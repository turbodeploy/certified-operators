package com.vmturbo.platform.analysis.actions;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.BuyerParticipation;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;

import junitparams.naming.TestCaseName;

public class CollapseTest {

    private static final Basket EMPTY = new Basket();
    private static final Basket BASKET = new Basket(new CommoditySpecification(0));
    private static final Economy EC = new Economy();
    private static final int TYPE_PM = 0;
    private static final int TYPE_VM = 1;

    @Test
    @TestCaseName("Test collapse Move")
    public final void testCollapseMove() {
        // Sellers
        Trader s1 = EC.addTrader(TYPE_PM, TraderState.ACTIVE, BASKET);
        Trader s2 = EC.addTrader(TYPE_PM, TraderState.ACTIVE, BASKET);
        Trader s3 = EC.addTrader(TYPE_PM, TraderState.ACTIVE, BASKET);
        // Buyers
        Trader b1 = EC.addTrader(TYPE_VM, TraderState.ACTIVE, EMPTY);
        // Buyer participations
        BuyerParticipation p1 = EC.addBasketBought(b1, BASKET);
        BuyerParticipation p2 = EC.addBasketBought(b1, BASKET);

        List<Action> actions = new ArrayList<>();
        // An empty list is collapsed to an empty list
        List<Action> collapsed = Action.collapse(actions);
        assertTrue(collapsed.isEmpty());

        // List with one Move is collapsed to the same list
        actions.add(new Move(EC, p1, s1, s2));
        collapsed = Action.collapse(actions);
        assertEquals(collapsed, actions);

        // Move from S2 to S1 cancels Move from S1 to S2
        actions.add(new Move(EC, p1, s2, s1));
        collapsed = Action.collapse(actions);
        assertTrue(collapsed.isEmpty());

        // Move from s1 to s2 to s3 collapsed to move from s1 to s3
        actions = new ArrayList<>();
        actions.add(new Move(EC, p1, s1, s2));
        actions.add(new Move(EC, p1, s2, s3));
        collapsed = Action.collapse(actions);
        // TODO: Add equals to Move
        //List<Action> expectedCollapsed = Lists.newArrayList(new Move(EC, p1, s1, s3));
        //assertEquals(expectedCollapsed, collapsed);
        Move move = (Move) collapsed.get(0);
        assertEquals(1, collapsed.size());
        assertSame(s1, move.getSource());
        assertSame(s3, move.getDestination());
        assertSame(p1, move.getTarget());

        // Move from S1 to S2 to S3 to S1 collapsed to no action
        actions = new ArrayList<>();
        actions.add(new Move(EC, p1, s1, s2));
        actions.add(new Move(EC, p1, s2, s3));
        actions.add(new Move(EC, p1, s3, s1));
        collapsed = Action.collapse(actions);
        assertTrue(collapsed.isEmpty());

        // More buyer participations
        actions = new ArrayList<>();
        actions.add(new Move(EC, p1, s1, s2));
        actions.add(new Move(EC, p2, s1, s3));

        // Collapsing two moves of different buyer participations returns the same list
        collapsed = Action.collapse(actions);
        assertEquals(collapsed, actions);

        // Move one buyer participation back. It should cancel the other move for the same participation.
        actions.add(new Move(EC, p1, s2, s1));
        collapsed = Action.collapse(actions);
        assertSame(collapsed.get(0), actions.get(1));

        // Move the other buyer participations back. Collapsed list should be empty.
        actions.add(new Move(EC, p2, s3, s1));
        collapsed = Action.collapse(actions);
        assertTrue(collapsed.isEmpty());
    }

    @Test // Create 10 non-combinable moves, collapse and verify we get the same list in the same order
    @TestCaseName("Test collapse maintains order")
    public final void testCollapseMaintainsOrder() {
        List<Action> actions = Lists.newArrayList();
        Trader b = EC.addTrader(TYPE_VM, TraderState.ACTIVE, EMPTY);
        for (int i = 0; i < 10; i++) {
            Trader s1 = EC.addTrader(TYPE_PM, TraderState.ACTIVE, BASKET);
            Trader s2 = EC.addTrader(TYPE_PM, TraderState.ACTIVE, BASKET);
            BuyerParticipation p = EC.addBasketBought(b, BASKET);
            actions.add(new Move(EC, p, s1, s2));
        }
        List<Action> collapsed = Action.collapse(actions);
        assertEquals(collapsed, actions);
    }
}
