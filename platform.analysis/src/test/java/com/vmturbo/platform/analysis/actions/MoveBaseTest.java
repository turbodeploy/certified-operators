package com.vmturbo.platform.analysis.actions;

import static org.junit.Assert.*;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

/**
 * A test case for the {@link MoveBase} class.
 */
@RunWith(JUnitParamsRunner.class)
public class MoveBaseTest {
    // Fields
    private static final Basket EMPTY = new Basket();

    // Methods

    @Test
    @Parameters
    @TestCaseName("Test #{index}: new MoveBase({0},{1})")
    public final void testMoveBase(@NonNull Economy economy, @NonNull ShoppingList shoppingList,
                                   @Nullable Trader source) {
        MoveBase mb = new MoveBase(economy,shoppingList,source);

        assertSame(economy, mb.getEconomy());
        assertSame(shoppingList, mb.getTarget());
        assertSame(source, mb.getSource());
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestMoveBase() {
        Economy e1 = new Economy();
        ShoppingList p1 = e1.addBasketBought(e1.addTrader(0, TraderState.ACTIVE, EMPTY), EMPTY);

        Economy e2 = new Economy();
        ShoppingList p2 = e2.addBasketBought(e2.addTrader(0, TraderState.ACTIVE, EMPTY), EMPTY);
        Trader s2 = e2.addTrader(1, TraderState.ACTIVE, EMPTY);
        p2.move(s2);

        return new Object[][]{{e1,p1,null},{e2,p2,s2},{e2,p2,null}};
    }

} // end MoveBaseTest class
