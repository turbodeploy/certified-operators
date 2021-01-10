package com.vmturbo.platform.analysis.actions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;

import org.junit.Test;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;

/**
 * A test case for the {@link Action} interface.
 */
public class ActionCollapseTest {

    private static final Basket EMPTY = new Basket();
    private static final Basket CPU_MEM_BASKET = new Basket(TestUtils.CPU, TestUtils.MEM);
    private static final Basket ST_BASKET = new Basket(TestUtils.ST_AMT);
    private static final Basket STORAGE_BASKET = new Basket(TestUtils.STORAGE);
    private static final int TYPE_PM = 0;
    private static final int TYPE_VM = 1;
    private static final int TYPE_DC = 2;
    private static final int TYPE_ST = 3;

    @Test
    public final void testCollapsed_movesOnly() {
        Economy economy = new Economy();
        // Sellers
        Trader s1 = economy.addTrader(TYPE_PM, TraderState.ACTIVE, CPU_MEM_BASKET);
        Trader s2 = economy.addTrader(TYPE_PM, TraderState.ACTIVE, CPU_MEM_BASKET);
        Trader s3 = economy.addTrader(TYPE_PM, TraderState.ACTIVE, CPU_MEM_BASKET);
        // Buyers
        Trader b1 = economy.addTrader(TYPE_VM, TraderState.ACTIVE, EMPTY);
        // Shopping lists
        ShoppingList p1 = economy.addBasketBought(b1, CPU_MEM_BASKET);
        ShoppingList p2 = economy.addBasketBought(b1, CPU_MEM_BASKET);

        List<Action> actions = new ArrayList<>();
        // An empty list is collapsed to an empty list
        List<Action> collapsed = ActionCollapse.collapsed(actions);
        assertTrue(collapsed.isEmpty());

        // List with one Move is collapsed to the same list
        actions.add(new Move(economy, p1, s1, s2));
        collapsed = ActionCollapse.collapsed(actions);
        assertEquals(collapsed, actions);

        // Move from S2 to S1 cancels Move from S1 to S2
        actions.add(new Move(economy, p1, s2, s1));
        // First verify the 'combine keys' of Move actions with the same shopping list are the same
        assertEquals(actions.get(0).getCombineKey(), actions.get(1).getCombineKey());
        // Now verify the actions are collapsed properly
        collapsed = ActionCollapse.collapsed(actions);
        assertTrue(collapsed.isEmpty());
        // Move from s1 to s2 to s3 collapsed to move from s1 to s3
        actions = new ArrayList<>();
        actions.add(new Move(economy, p1, s1, s2));
        actions.add(new Move(economy, p1, s2, s3));
        collapsed = ActionCollapse.collapsed(actions);
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
        actions.add(new Move(economy, p1, s1, s2));
        actions.add(new Move(economy, p1, s2, s3));
        actions.add(new Move(economy, p1, s3, s1));
        collapsed = ActionCollapse.collapsed(actions);
        assertTrue(collapsed.isEmpty());

        // Move from S1 to S1 and S1 to S1 collapsed to a single move of S1 to S1
        actions = new ArrayList<>();
        actions.add(new Move(economy, p1, s1, s1));
        actions.add(new Move(economy, p1, s1, s1));
        collapsed = ActionCollapse.collapsed(actions);
        assertEquals(1, collapsed.size());
        move = (Move) collapsed.get(0);
        assertSame(s1, move.getSource());
        assertSame(s1, move.getDestination());
        assertSame(p1, move.getTarget());

        // More shopping lists
        actions = new ArrayList<>();
        actions.add(new Move(economy, p1, s1, s2));
        actions.add(new Move(economy, p2, s1, s3));

        // First verify the 'combine keys' are different
        assertNotEquals(actions.get(0).getCombineKey(), actions.get(1).getCombineKey());

        // Collapsing two moves of different shopping lists returns the same list
        collapsed = ActionCollapse.collapsed(actions);
        assertEquals(collapsed, actions);

        // Move one shopping list back. It should cancel the other move for the same shopping list.
        actions.add(new Move(economy, p1, s2, s1));
        collapsed = ActionCollapse.collapsed(actions);
        assertSame(collapsed.get(0), actions.get(1));

        // Move the other shopping lists back. Collapsed list should be empty.
        actions.add(new Move(economy, p2, s3, s1));
        collapsed = ActionCollapse.collapsed(actions);
        assertTrue(collapsed.isEmpty());
    }

    /*
     * Create 10 non-combinable moves, collapse and verify we get the same list in the same order.
     */
    @Test
    public final void testCollapsed_maintainsOrderOfMoves() {
        Economy economy = new Economy();
        List<Action> actions = Lists.newArrayList();
        Trader b = economy.addTrader(TYPE_VM, TraderState.ACTIVE, EMPTY);
        for (int i = 0; i < 10; i++) {
            Trader s1 = economy.addTrader(TYPE_PM, TraderState.ACTIVE, CPU_MEM_BASKET);
            Trader s2 = economy.addTrader(TYPE_PM, TraderState.ACTIVE, CPU_MEM_BASKET);
            ShoppingList p = economy.addBasketBought(b, CPU_MEM_BASKET);
            actions.add(new Move(economy, p, s1, s2));
        }
        // This also tests that the argument list is not modified.
        // An attempt to modify it (in collapsed) will throw UnsupportedOperationException
        actions = Collections.unmodifiableList(actions);
        List<Action> collapsed = ActionCollapse.collapsed(actions);
        assertEquals(collapsed, actions);
    }

    @Test
    public final void testCollapsed_movesAndCompoundMove() {
        Economy economy = new Economy();
        // Sellers
        Trader PMSource = economy.addTrader(TYPE_PM, TraderState.ACTIVE, CPU_MEM_BASKET);
        Trader STSource = economy.addTrader(TYPE_ST, TraderState.ACTIVE, CPU_MEM_BASKET);
        Trader PMDestination1 = economy.addTrader(TYPE_PM, TraderState.ACTIVE, STORAGE_BASKET);
        Trader STDestination1 = economy.addTrader(TYPE_ST, TraderState.ACTIVE, STORAGE_BASKET);
        Trader PMDestination2 = economy.addTrader(TYPE_PM, TraderState.ACTIVE, STORAGE_BASKET);
        Trader STDestination2 = economy.addTrader(TYPE_ST, TraderState.ACTIVE, STORAGE_BASKET);
        // Buyer
        Trader buyer = economy.addTrader(TYPE_VM, TraderState.ACTIVE, EMPTY);
        // Shopping lists
        ShoppingList sl1 = economy.addBasketBought(buyer, CPU_MEM_BASKET);
        sl1.move(PMSource);
        ShoppingList sl2 = economy.addBasketBought(buyer, STORAGE_BASKET);
        sl2.move(STSource);

        // Expected Action.
        // Since the order of Moves in CompoundMove is uncertain, we need to check both of them.
        Action expectedAction1 = CompoundMove.createAndCheckCompoundMoveWithExplicitSources(
            economy, Lists.newArrayList(sl1, sl2),
            Lists.newArrayList(PMSource, STSource),
            Lists.newArrayList(PMDestination2, STDestination2));
        Action expectedAction2 = CompoundMove.createAndCheckCompoundMoveWithExplicitSources(
            economy, Lists.newArrayList(sl2, sl1),
            Lists.newArrayList(STSource, PMSource),
            Lists.newArrayList(STDestination2, PMDestination2));

        // Individual moves are generated during the first placement,
        // and a compoundMove is generated during the second placement.
        // Merge to a compoundMove.
        List<Action> actions = new ArrayList<>();
        actions.add(new Move(economy, sl1, PMSource, PMDestination1));
        actions.add(new Move(economy, sl2, STSource, STDestination1));
        actions.add(CompoundMove.createAndCheckCompoundMoveWithExplicitSources(
            economy, Lists.newArrayList(sl1, sl2),
            Lists.newArrayList(PMDestination1, STDestination1),
            Lists.newArrayList(PMDestination2, STDestination2)));

        List<Action> collapsed = ActionCollapse.collapsed(actions);
        assertEquals(1, collapsed.size());
        assertTrue(expectedAction1.equals(collapsed.get(0)) ||
            expectedAction2.equals(collapsed.get(0)));

        // A compoundMove is generated during the first placement,
        // and individual moves are generated during the second placement.
        // Merge to a compoundMove.
        actions = new ArrayList<>();
        actions.add(CompoundMove.createAndCheckCompoundMoveWithExplicitSources(
            economy, Lists.newArrayList(sl1, sl2),
            Lists.newArrayList(PMSource, STSource),
            Lists.newArrayList(PMDestination1, STDestination1)));
        actions.add(new Move(economy, sl1, PMDestination1, PMDestination2));
        actions.add(new Move(economy, sl2, STDestination1, STDestination2));

        collapsed = ActionCollapse.collapsed(actions);
        assertEquals(1, collapsed.size());
        assertTrue(expectedAction1.equals(collapsed.get(0)) ||
            expectedAction2.equals(collapsed.get(0)));

        // A compoundMove is generated during the first placement,
        // and a compoundMove is generated during the second placement.
        // Merge to a compoundMove.
        actions = new ArrayList<>();
        actions.add(CompoundMove.createAndCheckCompoundMoveWithExplicitSources(
            economy, Lists.newArrayList(sl1, sl2),
            Lists.newArrayList(PMSource, STSource),
            Lists.newArrayList(PMDestination1, STDestination1)));
        actions.add(CompoundMove.createAndCheckCompoundMoveWithExplicitSources(
            economy, Lists.newArrayList(sl1, sl2),
            Lists.newArrayList(PMDestination1, STDestination1),
            Lists.newArrayList(PMDestination2, STDestination2)));

        collapsed = ActionCollapse.collapsed(actions);
        assertEquals(1, collapsed.size());
        assertTrue(expectedAction1.equals(collapsed.get(0)) ||
            expectedAction2.equals(collapsed.get(0)));
    }

    /*
     * Test combining Resize actions of the same trader and same commodity specification.
     */
    @Test
    public final void testCollapsed_resize1() {
        Economy economy = new Economy();
        double cap0 = 20;
        double cap1= 10;
        double cap2 = 50;
        double cap3 = 40;
        Trader t = economy.addTrader(TYPE_VM,  TraderState.ACTIVE, CPU_MEM_BASKET);
        t.getCommoditiesSold().get(0).setCapacity(cap0);
        Resize r1 = new Resize(economy, t, new CommoditySpecification(0), cap1);
        Resize r2 = new Resize(economy, t, new CommoditySpecification(0), cap2);
        Resize r3 = new Resize(economy, t, new CommoditySpecification(0), cap3);
        List<Action> actions = Lists.newArrayList(r1, r2, r3);
        List<Action> collapsed = ActionCollapse.collapsed(actions);
        assertEquals(1, collapsed.size());
        Resize collapsedResize = (Resize)collapsed.get(0);
        assertEquals(cap3, collapsedResize.getNewCapacity(), TestUtils.FLOATING_POINT_DELTA);
        assertEquals(cap0, collapsedResize.getOldCapacity(), TestUtils.FLOATING_POINT_DELTA);
        // Add another Resize so that the actions get cancelled
        Resize r4 = new Resize(economy, t, new CommoditySpecification(0), cap0);
        actions.add(r4);
        collapsed = ActionCollapse.collapsed(actions);
        assertTrue(collapsed.isEmpty());
    }

    /*
     * Test combining Resize actions of different trader/commodity specification combinations.
     */
    @Test
    public final void testCollapsed_resize2() {
        Economy economy = new Economy();
        Trader t1 = economy.addTrader(TYPE_VM,  TraderState.ACTIVE, CPU_MEM_BASKET);
        Trader t2 = economy.addTrader(TYPE_VM,  TraderState.ACTIVE, CPU_MEM_BASKET);
        // Resize the commodity of a target
        Resize r1_0 = new Resize(economy, t1, new CommoditySpecification(0), 10);
        // Resize a different commodity on the same target
        Resize r1_1 = new Resize(economy, t1, new CommoditySpecification(1), 10);
        // Resize a different target
        Resize r2 = new Resize(economy, t2, new CommoditySpecification(1), 10);
        List<Action> actions = Lists.newArrayList(r1_0, r1_1, r2);
        List<Action> collapsedActions = ActionCollapse.collapsed(actions);
        assertEquals(actions, collapsedActions);
    }

    /*
     * Collapse a few Resize actions on two targets and two commodity types.
     */
    @Test
    public final void testCollapsed_resize3() {
        Economy economy = new Economy();
        Trader t1 = economy.addTrader(TYPE_VM,  TraderState.ACTIVE, CPU_MEM_BASKET);
        Trader t2 = economy.addTrader(TYPE_VM,  TraderState.ACTIVE, CPU_MEM_BASKET);
        CommoditySpecification C0 = new CommoditySpecification(0);
        CommoditySpecification C1 = new CommoditySpecification(1);
        double t2c1InitialCapacity = 100;
        t2.getCommoditySold(C1).setCapacity(t2c1InitialCapacity);
        Resize r1c0_1 = new Resize(economy, t1, C0, 10);
        Resize r1c0_2 = new Resize(economy, t1, C0, 20);
        Resize r1c1_1 = new Resize(economy, t1, C1, 30);
        Resize r1c1_2 = new Resize(economy, t1, C1, 40);
        Resize r2c0_1 = new Resize(economy, t2, C0, 50);
        Resize r2c0_2 = new Resize(economy, t2, C0, 60);
        Resize r2c1_1 = new Resize(economy, t2, C1, 50);
        Resize r2c1_2 = new Resize(economy, t2, C1, t2c1InitialCapacity);
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
        List<Action> collapsedActions = ActionCollapse.collapsed(actions);
        // The expected actions are:
        // a Resize on t1 of commodity 0 to 20
        // a Resize on t2 of commodity 0 to 60
        // a Resize on t1 of commodity 1 to 40.
        // The resizes on t2 commodity 1 should cancel each other.
        assertEquals(3, collapsedActions.size());
        Resize collapsed1 = (Resize) collapsedActions.get(0);
        assertEquals(t1, collapsed1.getSellingTrader());
        assertEquals(C0, collapsed1.getResizedCommoditySpec());
        assertEquals(20, collapsed1.getNewCapacity(), TestUtils.FLOATING_POINT_DELTA);

        Resize collapsed2 = (Resize) collapsedActions.get(1);
        assertEquals(t2, collapsed2.getSellingTrader());
        assertEquals(C0, collapsed2.getResizedCommoditySpec());
        assertEquals(60, collapsed2.getNewCapacity(), TestUtils.FLOATING_POINT_DELTA);

        Resize collapsed3 = (Resize) collapsedActions.get(2);
        assertEquals(t1, collapsed3.getSellingTrader());
        assertEquals(C1, collapsed3.getResizedCommoditySpec());
        assertEquals(40, collapsed3.getNewCapacity(), TestUtils.FLOATING_POINT_DELTA);
    }

    /*
     * Collapse actions with same target which end with Deactivate action.
     */
    @Test
    public final void testCollapsed_endWithDeactivate() {
        Economy economy = new Economy();
        Trader v1 = economy.addTrader(TYPE_VM, TraderState.ACTIVE, EMPTY, CPU_MEM_BASKET);
        Trader p1 = economy.addTrader(TYPE_PM, TraderState.ACTIVE, CPU_MEM_BASKET, ST_BASKET);
        Trader p2 = economy.addTrader(TYPE_PM, TraderState.INACTIVE, CPU_MEM_BASKET, ST_BASKET);
        Trader dc1 = economy.addTrader(TYPE_DC, TraderState.ACTIVE, ST_BASKET);
        ShoppingList s1 = economy.addBasketBought(v1, CPU_MEM_BASKET);

        // suppose the sequence for actions are : ProvisionByDemand -> Deactivate
        ProvisionByDemand provD1 = (ProvisionByDemand)new ProvisionByDemand(economy, s1, p1).take();
        Deactivate d2 = new Deactivate(economy, provD1.getProvisionedSeller(), CPU_MEM_BASKET);
        List<Action> sequence1 = new ArrayList<Action>(Arrays.asList(provD1, d2));
        assertTrue(ActionCollapse.collapsed(sequence1).isEmpty());
        // suppose the sequence for actions are : ProvisionBySupply -> Move -> Deactivate
        ProvisionBySupply provS1 = (ProvisionBySupply)new ProvisionBySupply(economy, p1, TestUtils.CPU).take();
        Deactivate d3 = new Deactivate(economy, provS1.getProvisionedSeller(), CPU_MEM_BASKET);
        ShoppingList sl3 = economy.addBasketBought(provS1.getProvisionedSeller(), ST_BASKET);
        Move m3 = new Move(economy, sl3, dc1);
        List<Action> sequence2 = new ArrayList<Action>(Arrays.asList(provS1, m3, d3));
        assertTrue(ActionCollapse.collapsed(sequence2).isEmpty());
        // suppose the sequence for actions are: ProvisionBySupply1 -> ProvisionBySupply2 -> Deactivate1
        ProvisionBySupply provS5 = (ProvisionBySupply)new ProvisionBySupply(economy, p2, TestUtils.CPU).take();
        ProvisionBySupply provS6 = (ProvisionBySupply)new ProvisionBySupply(economy, p2, TestUtils.CPU).take();
        Deactivate d5 = new Deactivate(economy, provS5.getProvisionedSeller(), CPU_MEM_BASKET);
        List<Action> sequence3 = new ArrayList<Action>(Arrays.asList(provS5, provS6, d5));
        List<Action> results3 = ActionCollapse.collapsed(sequence3);
        assertEquals(1, results3.size());
        assertEquals(provS6, results3.get(0));
        // suppose the sequence for actions are : ProvisionBySupply1 -> ProvisionByDemand2 -> Deactivate2
        ProvisionBySupply provS7 = (ProvisionBySupply)new ProvisionBySupply(economy, p2, TestUtils.CPU).take();
        ProvisionByDemand provD8 = (ProvisionByDemand)new ProvisionByDemand(economy, s1, p1).take();
        Deactivate d6 = new Deactivate(economy, provD8.getProvisionedSeller(), CPU_MEM_BASKET);
        List<Action> sequence4 = new ArrayList<Action>(Arrays.asList(provS7, provD8, d6));
        List<Action> results4 = ActionCollapse.collapsed(sequence4);
        assertEquals(1, results4.size());
        assertEquals(provS7, results4.get(0));
        // suppose the sequence for actions with different traders
        List<Action> sequence5 = new ArrayList<Action>(Arrays.asList(provS1, m3, d5));
        List<Action> results5 = ActionCollapse.collapsed(sequence5);
        assertEquals(3, results5.size());
    }

    /*
     * The action order should be provision-> move-> resize-> suspension -> reconfigure.
     */
    @Test
    public void testGroupActionsByTypeAndReorderBeforeSending_correctOrder() {
        Economy economy = new Economy();
        // Create traders and shopping lists.
        Trader p1 = economy.addTrader(TYPE_PM, TraderState.ACTIVE, CPU_MEM_BASKET, ST_BASKET);
        Trader p2 = economy.addTrader(TYPE_PM, TraderState.ACTIVE, CPU_MEM_BASKET, ST_BASKET);
        Trader p3 = economy.addTrader(TYPE_PM, TraderState.INACTIVE, CPU_MEM_BASKET, ST_BASKET);
        Trader v1 = economy.addTrader(TYPE_VM, TraderState.ACTIVE, EMPTY, CPU_MEM_BASKET);
        Trader v2 = economy.addTrader(TYPE_VM, TraderState.ACTIVE, EMPTY, CPU_MEM_BASKET);
        ShoppingList s1 = economy.addBasketBought(v1, CPU_MEM_BASKET);

        // Create actions.
        ProvisionBySupply provisionBySupply = new ProvisionBySupply(economy, p1, TestUtils.CPU);
        ProvisionByDemand provisionByDemand = new ProvisionByDemand(economy, s1, p1);
        Move move = new Move(economy, s1, v2);
        Activate activate = new Activate(economy, p3, CPU_MEM_BASKET, p2, TestUtils.CPU);
        Resize resize = new Resize(economy, p2, new CommoditySpecification(0),10, 20);
        Deactivate deactivate =
            new Deactivate(economy, provisionBySupply.getProvisionedSeller(), CPU_MEM_BASKET);
        ReconfigureConsumer reconfigure = new ReconfigureConsumer(economy, s1);

        // Act : Random insertion order.
        List<Action> actionsAfterReorder = ActionCollapse.groupActionsByTypeAndReorderBeforeSending
                        (Arrays.asList(deactivate, move, provisionByDemand, reconfigure, activate,
                                        resize , provisionBySupply));

        // Assert
        assertEquals(provisionBySupply, actionsAfterReorder.get(0));
        assertEquals(provisionByDemand ,actionsAfterReorder.get(1));
        assertEquals(activate, actionsAfterReorder.get(2));
        assertEquals(move, actionsAfterReorder.get(3));
        assertEquals(resize, actionsAfterReorder.get(4));
        assertEquals(deactivate, actionsAfterReorder.get(5));
        assertEquals(reconfigure, actionsAfterReorder.get(6));
    }
}
