package com.vmturbo.platform.analysis.actions;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;

/**
 * A test case for the {@link Action} interface.
 */
public class ActionTest {

    private static final Basket EMPTY = new Basket();
    private static final Basket BASKET = new Basket(new CommoditySpecification(0), new CommoditySpecification(1));
    private static final Basket BASKET2 = new Basket(new CommoditySpecification(2));
    private static final Economy EC = new Economy();
    private static final int TYPE_PM = 0;
    private static final int TYPE_VM = 1;
    private static final int TYPE_DC = 2;

    @Test
    public final void testCollapsed_MovesOnly() {
        // Sellers
        Trader s1 = EC.addTrader(TYPE_PM, TraderState.ACTIVE, BASKET);
        Trader s2 = EC.addTrader(TYPE_PM, TraderState.ACTIVE, BASKET);
        Trader s3 = EC.addTrader(TYPE_PM, TraderState.ACTIVE, BASKET);
        // Buyers
        Trader b1 = EC.addTrader(TYPE_VM, TraderState.ACTIVE, EMPTY);
        // Shopping lists
        ShoppingList p1 = EC.addBasketBought(b1, BASKET);
        ShoppingList p2 = EC.addBasketBought(b1, BASKET);

        List<Action> actions = new ArrayList<>();
        // An empty list is collapsed to an empty list
        List<Action> collapsed = Action.collapsed(actions);
        assertTrue(collapsed.isEmpty());

        // List with one Move is collapsed to the same list
        actions.add(new Move(EC, p1, s1, s2));
        collapsed = Action.collapsed(actions);
        assertEquals(collapsed, actions);

        // Move from S2 to S1 cancels Move from S1 to S2
        actions.add(new Move(EC, p1, s2, s1));
        // First verify the 'combine keys' of Move actions with the same shopping list are the same
        assertEquals(actions.get(0).getCombineKey(), actions.get(1).getCombineKey());
        // Now verify the actions are collapsed properly
        collapsed = Action.collapsed(actions);
        assertTrue(collapsed.isEmpty());
        // Move from s1 to s2 to s3 collapsed to move from s1 to s3
        actions = new ArrayList<>();
        actions.add(new Move(EC, p1, s1, s2));
        actions.add(new Move(EC, p1, s2, s3));
        collapsed = Action.collapsed(actions);
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
        collapsed = Action.collapsed(actions);
        assertTrue(collapsed.isEmpty());

        // More shopping lists
        actions = new ArrayList<>();
        actions.add(new Move(EC, p1, s1, s2));
        actions.add(new Move(EC, p2, s1, s3));

        // First verify the 'combine keys' are different
        assertNotEquals(actions.get(0).getCombineKey(), actions.get(1).getCombineKey());

        // Collapsing two moves of different shopping lists returns the same list
        collapsed = Action.collapsed(actions);
        assertEquals(collapsed, actions);

        // Move one shopping list back. It should cancel the other move for the same shopping list.
        actions.add(new Move(EC, p1, s2, s1));
        collapsed = Action.collapsed(actions);
        assertSame(collapsed.get(0), actions.get(1));

        // Move the other shopping lists back. Collapsed list should be empty.
        actions.add(new Move(EC, p2, s3, s1));
        collapsed = Action.collapsed(actions);
        assertTrue(collapsed.isEmpty());
    }

    @Test // Create 10 non-combinable moves, collapse and verify we get the same list in the same order
    public final void testCollapsed_MaintainsOrderOfMoves() {
        List<Action> actions = Lists.newArrayList();
        Trader b = EC.addTrader(TYPE_VM, TraderState.ACTIVE, EMPTY);
        for (int i = 0; i < 10; i++) {
            Trader s1 = EC.addTrader(TYPE_PM, TraderState.ACTIVE, BASKET);
            Trader s2 = EC.addTrader(TYPE_PM, TraderState.ACTIVE, BASKET);
            ShoppingList p = EC.addBasketBought(b, BASKET);
            actions.add(new Move(EC, p, s1, s2));
        }
        // This also tests that the argument list is not modified.
        // An attempt to modify it (in collapsed) will throw UnsupportedOperationException
        actions = Collections.unmodifiableList(actions);
        List<Action> collapsed = Action.collapsed(actions);
        assertEquals(collapsed, actions);
    }

    @Test // Test combining Resize actions of the same trader and same commodity specification
    public final void testCollapsed_Resize1() {
        double cap0 = 20;
        double cap1= 10;
        double cap2 = 50;
        double cap3 = 40;
        Trader t = EC.addTrader(TYPE_VM,  TraderState.ACTIVE, BASKET);
        t.getCommoditiesSold().get(0).setCapacity(cap0);
        Resize r1 = new Resize(EC, t, new CommoditySpecification(0), cap1);
        Resize r2 = new Resize(EC, t, new CommoditySpecification(0), cap2);
        Resize r3 = new Resize(EC, t, new CommoditySpecification(0), cap3);
        List<Action> actions = Lists.newArrayList(r1, r2, r3);
        List<Action> collapsed = Action.collapsed(actions);
        assertEquals(1, collapsed.size());
        Resize collapsedResize = (Resize)collapsed.get(0);
        assertEquals(cap3, collapsedResize.getNewCapacity(), 1e-5);
        assertEquals(cap0, collapsedResize.getOldCapacity(), 1e-5);
        // Add another Resize so that the actions get cancelled
        Resize r4 = new Resize(EC, t, new CommoditySpecification(0), cap0);
        actions.add(r4);
        collapsed = Action.collapsed(actions);
        assertTrue(collapsed.isEmpty());
    }

    @Test // Test combining Resize actions of different trader/commodity specification combinations
    public final void testCollapsed_Resize2() {
        Trader t1 = EC.addTrader(TYPE_VM,  TraderState.ACTIVE, BASKET);
        Trader t2 = EC.addTrader(TYPE_VM,  TraderState.ACTIVE, BASKET);
        // Resize the commodity of a target
        Resize r1_0 = new Resize(EC, t1, new CommoditySpecification(0), 10);
        // Resize a different commodity on the same target
        Resize r1_1 = new Resize(EC, t1, new CommoditySpecification(1), 10);
        // Resize a different target
        Resize r2 = new Resize(EC, t2, new CommoditySpecification(1), 10);
        List<Action> actions = Lists.newArrayList(r1_0, r1_1, r2);
        List<Action> collapsedActions = Action.collapsed(actions);
        assertEquals(actions, collapsedActions);
    }

    @Test // Collapse a few Resize actions on two targets and two commodity types
    public final void testCollapsed_Resize3() {
        Trader t1 = EC.addTrader(TYPE_VM,  TraderState.ACTIVE, BASKET);
        Trader t2 = EC.addTrader(TYPE_VM,  TraderState.ACTIVE, BASKET);
        CommoditySpecification C0 = new CommoditySpecification(0);
        CommoditySpecification C1 = new CommoditySpecification(1);
        double t2c1InitialCapacity = 100;
        t2.getCommoditySold(C1).setCapacity(t2c1InitialCapacity);
        Resize r1c0_1 = new Resize(EC, t1, C0, 10);
        Resize r1c0_2 = new Resize(EC, t1, C0, 20);
        Resize r1c1_1 = new Resize(EC, t1, C1, 30);
        Resize r1c1_2 = new Resize(EC, t1, C1, 40);
        Resize r2c0_1 = new Resize(EC, t2, C0, 50);
        Resize r2c0_2 = new Resize(EC, t2, C0, 60);
        Resize r2c1_1 = new Resize(EC, t2, C1, 50);
        Resize r2c1_2 = new Resize(EC, t2, C1, t2c1InitialCapacity);
        List<Action> actions = Lists.newArrayList(
                r1c0_1,
                r2c1_1,
                r2c0_1,
                r1c1_1,
                r1c1_2, // will merge with r1c1_1
                r2c0_1,
                r1c0_2, // will merge with r1c0_1
                r2c0_2, // will merge with r2c0_1
                r2c1_2 // will cancel r2c1_1
                );
        List<Action> collapsedActions = Action.collapsed(actions);
        // The expected actions are:
        // a Resize on t1 of commodity 0 to 20
        // a Resize on t2 of commodity 0 to 60
        // a Resize on t1 of commodity 1 to 40.
        // The resizes on t2 commodity 1 should cancel each other.
        assertEquals(3, collapsedActions.size());
        Resize collapsed1 = (Resize) collapsedActions.get(0);
        assertEquals(t1, collapsed1.getSellingTrader());
        assertEquals(C0, collapsed1.getResizedCommoditySpec());
        assertEquals(20, collapsed1.getNewCapacity(), 1e-5);

        Resize collapsed2 = (Resize) collapsedActions.get(1);
        assertEquals(t2, collapsed2.getSellingTrader());
        assertEquals(C0, collapsed2.getResizedCommoditySpec());
        assertEquals(60, collapsed2.getNewCapacity(), 1e-5);

        Resize collapsed3 = (Resize) collapsedActions.get(2);
        assertEquals(t1, collapsed3.getSellingTrader());
        assertEquals(C1, collapsed3.getResizedCommoditySpec());
        assertEquals(40, collapsed3.getNewCapacity(), 1e-5);
    }

    @Test // Collapse actions with same target which end with Deactivate action
    public final void testCollapsed_EndWithDeactivate() {
        Trader v1 = EC.addTrader(TYPE_VM, TraderState.ACTIVE, EMPTY, BASKET);
        Trader p1 = EC.addTrader(TYPE_PM, TraderState.ACTIVE, BASKET, BASKET2);
        Trader p2 = EC.addTrader(TYPE_PM, TraderState.INACTIVE, BASKET, BASKET2);
        Trader dc1 = EC.addTrader(TYPE_DC, TraderState.ACTIVE, BASKET2);
        ShoppingList s1 = EC.addBasketBought(v1, BASKET);

        // suppose the sequence for actions are : ProvisionByDemand -> Deactivate
        ProvisionByDemand provD1 = (ProvisionByDemand)new ProvisionByDemand(EC, s1, p1).take();
        Deactivate d2 = new Deactivate(EC, provD1.getProvisionedSeller(), EC.getMarket(BASKET));
        List<Action> sequence1 = new ArrayList<Action>(Arrays.asList(provD1, d2));
        assertTrue(Action.collapsed(sequence1).isEmpty());
        // suppose the sequence for actions are : ProvisionBySupply -> Move -> Deactivate
        ProvisionBySupply provS1 = (ProvisionBySupply)new ProvisionBySupply(EC, p1).take();
        Deactivate d3 = new Deactivate(EC, provS1.getProvisionedSeller(), EC.getMarket(BASKET));
        ShoppingList sl3 = EC.addBasketBought(provS1.getProvisionedSeller(), BASKET2);
        Move m3 = new Move(EC, sl3, dc1);
        List<Action> sequence2 = new ArrayList<Action>(Arrays.asList(provS1, m3, d3));
        assertTrue(Action.collapsed(sequence2).isEmpty());
        // suppose the sequence for actions are: ProvisionBySupply1 -> ProvisionBySupply2 -> Deactivate1
        ProvisionBySupply provS5 = (ProvisionBySupply)new ProvisionBySupply(EC, p2).take();
        ProvisionBySupply provS6 = (ProvisionBySupply)new ProvisionBySupply(EC, p2).take();
        Deactivate d5 = new Deactivate(EC, provS5.getProvisionedSeller(), EC.getMarket(BASKET));
        List<Action> sequence3 = new ArrayList<Action>(Arrays.asList(provS5, provS6, d5));
        List<Action> results3 = Action.collapsed(sequence3);
        assertEquals(1, results3.size());
        assertEquals(provS6, results3.get(0));
        // suppose the sequence for actions are : ProvisionBySupply1 -> ProvisionByDemand2 -> Deactivate2
        ProvisionBySupply provS7 = (ProvisionBySupply)new ProvisionBySupply(EC, p2).take();
        ProvisionByDemand provD8 = (ProvisionByDemand)new ProvisionByDemand(EC, s1, p1).take();
        Deactivate d6 = new Deactivate(EC, provD8.getProvisionedSeller(), EC.getMarket(BASKET));
        List<Action> sequence4 = new ArrayList<Action>(Arrays.asList(provS7, provD8, d6));
        List<Action> results4 = Action.collapsed(sequence4);
        assertEquals(1, results4.size());
        assertEquals(provS7, results4.get(0));
        // suppose the sequence for actions with different traders
        List<Action> sequence5 = new ArrayList<Action>(Arrays.asList(provS1, m3, d5));
        List<Action> results5 = Action.collapsed(sequence5);
        assertEquals(3, results5.size());
    }
}
