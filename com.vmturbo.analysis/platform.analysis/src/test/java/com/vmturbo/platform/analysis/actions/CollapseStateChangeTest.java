package com.vmturbo.platform.analysis.actions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

import org.junit.Test;
import org.junit.runner.RunWith;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;

@RunWith(JUnitParamsRunner.class)
public class CollapseStateChangeTest {

    private static final Basket EMPTY = new Basket();
    private static final Basket BASKET = new Basket(new CommoditySpecification(0));
    private static final Economy EC = new Economy();
    private static final int TYPE_PM = 0;
    private static final int TYPE_VM = 1;

    private static final Trader vm1 = EC.addTrader(TYPE_VM, TraderState.ACTIVE, EMPTY);
    private static final Trader vm2 = EC.addTrader(TYPE_VM, TraderState.ACTIVE, EMPTY);
    private static final ShoppingList bp0 = EC.addBasketBought(vm1, BASKET);
    private static final Trader pm1 = EC.addTrader(TYPE_PM, TraderState.ACTIVE, BASKET, EMPTY);
    private static final Trader pm2 = EC.addTrader(TYPE_PM, TraderState.ACTIVE, BASKET, EMPTY);

    private static Trader lastMoveTo;

    /**
     * Use a String representation of a list of {@link #Action}s where A stands for {@link Activate},
     * D for {@link Deactivate} and M for {@link Move}.
     * All actions operate on {@link #vm} and the Moves are for different shopping lists,
     * so they don't get collapsed.
     * @param actionsString a String representation of the list of actions to collapse
     * @param expectedCollapsedString a String representation of the expected collapsed list of
     *            actions
     * @see #actionsToString
     * @see #stringToActionsDifferentBPs
     */
    @Parameters
    @TestCaseName("Test #1.{index}: {0} -> {1}")
    @Test
    public void testCollapseActionsDifferentBPs(String actionsString, String expectedCollapsedString) {
        List<Action> actions = stringToActionsDifferentBPs(actionsString);
        List<Action> collapsed = ActionCollapse.collapsed(actions);
        String collapsedString = actionsToString(collapsed);
        assertEquals(expectedCollapsedString, collapsedString);
    }

    @SuppressWarnings("unused")
    private static String[][] parametersForTestCollapseActionsDifferentBPs() {
        return new String[][]{
            // Alternating A and D start with A
            {"A", "A"},
            {"AD", ""},
            {"ADA", "A"},
            {"ADAD", ""},
            {"ADADA", "A"},

            // Alternating D and A start with D
            {"D", "D"},
            {"DA", ""},
            {"DAD", "D"},
            {"DADA", ""},
            {"DADAD", "D"},

            // Multiple Ms between A and D
            // Start with AMM and add D then A then MM - repeat
            {"AMM", "AMM"},
            {"AMMD", ""},
            {"AMMDA", "AMM"},
            {"AMMDAMM", "AMMMM"},
            {"AMMDAMMD", ""},
            {"AMMDAMMDA", "AMMMM"},

            // Start with DA and add M then D then A - repeat
            {"DA", ""},
            {"DAM", "M"},
            {"DAMD", "D"},
            {"DAMDA", "M"},
            {"DAMDAM", "MM"},
            {"DAMDAMD", "D"},
            {"DAMDAMDA", "MM"},
            {"DAMDAMDAM", "MMM"},
            {"DAMDAMDAMD", "D"},

            // Start with AM and add D then A then M - repeat
            {"AM", "AM"},
            {"AMD", ""},
            {"AMDA", "AM"},
            {"AMDAM", "AMM"},
            {"AMDAMD", ""},
            {"AMDAMDA", "AMM"},
            {"AMDAMDAM", "AMMM"},
            {"AMDAMDAMD", ""},

            // Start with MD and add A then M then D - repeat
            {"MD", "D"},
            {"MDA", "M"},
            {"MDAM", "MM"},
            {"MDAMD", "D"},
            {"MDAMDA", "MM"},
            {"MDAMDAM", "MMM"},

            // Other cases
            {"MMD", "D"},
            {"MMDA", "MM"}
        };
    }

    /**
     * Use a String representation of a list of {@link #Action}s where A stands for {@link Activate},
     * D for {@link Deactivate} and M for {@link Move}.
     * All actions operate on {@link #vm}. The {@link Move}s are for the same shopping list,
     * so they do get collapsed. It is expected to have one {@link Move} in the collapsed list, from {@link #pm1}
     * to the destination of the last move in the original list of actions.
     * @param actionsString a String representation of the list of actions to collapse
     * @param expectedCollapsedString a String representation of the expected collapsed list of
     *            actions
     * @see #actionsToString
     * @see #stringToActionsSameBP
     */
    @Parameters
    @TestCaseName("Test #2.{index}: {0} -> {1}")
    @Test
    public void testCollapseActionsSameBP(String actionsString, String expectedCollapsedString) {
        lastMoveTo = null;
        List<Action> actions = stringToActionsSameBP(actionsString);
        List<Action> collapsed = ActionCollapse.collapsed(actions);
        String collapsedString = actionsToString(collapsed);
        assertEquals(expectedCollapsedString, collapsedString);
        // Verify that if there is a Move then it is from the source of the first Move in actions
        // (which is pm1) to the destination of the last Move in actions.
        int collapsedMoveIndex = expectedCollapsedString.indexOf("M");
        if (collapsedMoveIndex >= 0) {
            Move collapsedMove = (Move) collapsed.get(collapsedMoveIndex);
            assertSame(vm1, collapsedMove.getActionTarget());
            assertSame(bp0, collapsedMove.getTarget());
            assertSame(pm1, collapsedMove.getSource());
            Move lastMove = (Move) actions.get(actionsString.lastIndexOf("M"));
            assertSame(lastMove.getDestination(), collapsedMove.getDestination());
        }
    }

    @SuppressWarnings("unused")
    private static String[][] parametersForTestCollapseActionsSameBP() {
        return new String[][]{
            // Only care about cases that have more than one Move
            // Multiple Ms between A and D
            // Start with AMM and add D then A then MM - repeat
            {"AMM", "AM"},
            {"AMMD", ""},
            {"AMMDA", "AM"},
            {"AMMDAMM", "AM"},
            {"AMMDAMMD", ""},
            {"AMMDAMMDA", "AM"},

            // Start with DA and add M then D then A - repeat
            {"DAMDAM", "M"},
            {"DAMDAMD", "D"},
            {"DAMDAMDA", "M"},
            {"DAMDAMDAM", "M"},
            {"DAMDAMDAMD", "D"},

            // Start with AM and add D then A then M - repeat
            {"AMDAM", "AM"},
            {"AMDAMD", ""},
            {"AMDAMDA", "AM"},
            {"AMDAMDAM", "AM"},
            {"AMDAMDAMD", ""},

            // Start with MD and add A then M then D - repeat
            {"MDAM", "M"},
            {"MDAMD", "D"},
            {"MDAMDA", "M"},
            {"MDAMDAM", "M"},

            // Other
            {"MMD", "D"},
            {"MMDA", "M"},
            {"MMDAMM", "M"}
        };
    }

    /**
     * Convert a list of {@link Action}s to a string. For example, the list [Activate, Move,
     * Deactivate] is converted to "AMD".
     * @param actions a list of {@link Action}s
     * @return a String which length is the same as the length of the argument list, where each
     * character is the first letter of the action type (class) from the list
     */
    private static String actionsToString(List<Action> actions) {
        StringBuilder name = new StringBuilder();
        for (Action action : actions) {
            name.append(action.getClass().getSimpleName().charAt(0));
        }
        return name.toString();
    }

    /**
     * Convert a String representation of a list of actions into a list of {@link Action}s.
     * {@link Move} actions in the resulted list operate on different instances of {@link ShoppingList},
     * therefore they are not collapsed.
     * For example, "AMD" will be converted to the list [Activate, Move, Deactivate]
     * @param actionsString a String representation of a list of actions
     * @return a list of actions which contains one {@link Action} for every letter in the input String.
     */
    private List<Action> stringToActionsDifferentBPs(String actionsString) {
        validate(actionsString);
        return actionsString.chars().mapToObj(c -> actionDifferentBP(c)).collect(Collectors.toList());
    }

    /**
     * @param c the first letter of the {@link Action} to return.
     * @return an Action
     * @throws IllegalArgumentException if the argument is not A, M or D.
     */
    private Action actionDifferentBP(int c) {
        switch (c) {
        case 'A':
            return new Activate(EC, vm1, EMPTY, vm2, TestUtils.CPU);
        case 'M':
            ShoppingList bp = EC.addBasketBought(vm1, BASKET);
            return new Move(EC, bp, pm1, pm2);
        case 'D':
            return new Deactivate(EC, vm1, EMPTY);
        default:
            throw new IllegalArgumentException("Action " + c);
        }
    }

    /**
     * Validate the sequence of actions. A valid sequence is one that obeys the following rules:
     * <p>After A can come either M or D
     * <p>After M can come either another M or D
     * <p>After D can come only A
     * @param actionsString a String representation of a list of actions
     * @throws IllegalArgumentException if the argument is an invalid string
     */
    private void validate(String actionsString) {
        if (actionsString.matches(".*AA.*|.*MA.*|.*D[MD].*")) {
            throw new IllegalArgumentException("Illegal sequence: " + actionsString);
        }
    }

    private List<Action> stringToActionsSameBP(String actionsString) {
        validate(actionsString);
        return actionsString.chars().mapToObj(c -> actionSameBP(c)).collect(Collectors.toList());
    }

    /**
     * Convert a String representation of a list of actions into a list of {@link Action}s.
     * {@link Move} actions in the resulted list operate on the same instance of {@link ShoppingList},
     * therefore they get collapsed.
     * For example, "AMD" will be converted to the list [Activate, Move, Deactivate]
     * @param actionsString a String representation of a list of actions
     * @return a list of actions which contains one {@link Action} for every letter in the input String.
     */
    private Action actionSameBP(int c) {
        switch (c) {
        case 'A':
            return new Activate(EC, vm1, EMPTY, vm2, TestUtils.CPU);
        case 'M':
            Trader moveFrom = lastMoveTo == null ? pm1 : lastMoveTo;
            Trader newMoveTo = EC.addTrader(TYPE_PM, TraderState.ACTIVE, BASKET);
            Move move = new Move(EC, bp0, moveFrom, newMoveTo);
            lastMoveTo = newMoveTo;
            return move;
        case 'D':
            return new Deactivate(EC, vm1, EMPTY);
        default:
            throw new IllegalArgumentException("Action " + c);
        }
    }

    /**
     * Test collapsing {@link Action}s on 3 different traders. For each trader we test a different
     * sequence of actions.
     */
    @Test
    public final void testCollapseMultipleTraders() {
        Trader vm1 = EC.addTrader(TYPE_VM, TraderState.ACTIVE, EMPTY);
        ShoppingList bp1_1 = EC.addBasketBought(vm1, BASKET);
        ShoppingList bp1_2 = EC.addBasketBought(vm1, BASKET);
        Trader vm2 = EC.addTrader(TYPE_VM, TraderState.ACTIVE, EMPTY);
        ShoppingList bp2_1 = EC.addBasketBought(vm2, BASKET);
        Trader vm3 = EC.addTrader(TYPE_VM, TraderState.ACTIVE, EMPTY);
        ShoppingList bp3_1 = EC.addBasketBought(vm3, BASKET);
        ShoppingList bp3_2 = EC.addBasketBought(vm3, BASKET);
        Trader pm1 = EC.addTrader(TYPE_PM, TraderState.ACTIVE, BASKET);
        Trader pm2 = EC.addTrader(TYPE_PM, TraderState.ACTIVE, BASKET);
        Trader pm3 = EC.addTrader(TYPE_PM, TraderState.ACTIVE, BASKET);

        // Trader 1 : AMDAM -> AMM (two BPs)
        Action A1_1 = new Activate(EC, vm1, EMPTY, vm2, TestUtils.CPU);
        Action A1_2 = new Move(EC, bp1_1, pm1, pm2);
        Action A1_3 = new Deactivate(EC, vm1, EMPTY);
        Action A1_4 = new Activate(EC, vm1, EMPTY, vm2, TestUtils.CPU);
        Action A1_5 = new Move(EC, bp1_2, pm1, pm2);

        // Trader 2 : MMDA -> [] (one BP moves from pm1 to pm2 and back to to pm1)
        Action A2_1 = new Move(EC, bp2_1, pm1, pm2);
        Action A2_2 = new Move(EC, bp2_1, pm2, pm1);
        Action A2_3 = new Deactivate(EC, vm2, EMPTY);
        Action A2_4 = new Activate(EC, vm2, EMPTY, vm1, TestUtils.CPU);

        // Trader 3 : MDAMD -> D (two BPs)
        Action A3_1 = new Move(EC, bp3_1, pm1, pm2);
        Action A3_2 = new Deactivate(EC, vm3, EMPTY);
        Action A3_3 = new Activate(EC, vm3, EMPTY, vm1, TestUtils.CPU);
        Action A3_4 = new Move(EC, bp3_2, pm2, pm3);
        Action A3_5 = new Deactivate(EC, vm3, EMPTY);

        // Test 1 : Sequential concatenation
        List<Action> actionsSequential = Lists.newArrayList(
                A1_1, A1_2, A1_3, A1_4, A1_5,
                A2_1, A2_2, A2_3, A2_4,
                A3_1, A3_2, A3_3, A3_4, A3_5
        );
        List<Action> expectedCollapsed = Lists.newArrayList(
                A1_1, A1_2, A1_5, A3_5
        );
        List<Action> collapsedSequential = ActionCollapse.collapsed(actionsSequential);
        assertEquals(expectedCollapsed, collapsedSequential);

        // Test 2 : Interleaved
        List<Action> actionsInterleaved = Lists.newArrayList(
                A1_1, A2_1, A3_1,
                A1_2, A2_2, A3_2,
                A1_3, A2_3, A3_3,
                A1_4, A2_4, A3_4,
                A1_5, A3_5);
        List<Action> collapsedInterleaved = ActionCollapse.collapsed(actionsInterleaved);
        assertEquals(expectedCollapsed, collapsedInterleaved);
    }

    /**
     * Test a Move from A to B then from B to A with an Activate/Deactivate in between
     */
    @Test
    public final void testCollapse() {
        List<Action> actions = Lists.newArrayList();
        actions.add(new Move(EC, bp0, pm1, pm2));
        actions.add(new Deactivate(EC, vm1, EMPTY));
        actions.add(new Activate(EC, vm1, EMPTY, vm2, TestUtils.CPU));
        actions.add(new Move(EC, bp0, pm2, pm1));
        List<Action> collapsed = ActionCollapse.collapsed(actions);
        assertTrue(collapsed.isEmpty());
    }

}
