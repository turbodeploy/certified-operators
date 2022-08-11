package com.vmturbo.platform.analysis.actions;

import static org.junit.Assert.*;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

/**
 * A test case for the {@link StateChangeBase} class.
 */
@RunWith(JUnitParamsRunner.class)
public class StateChangeBaseTest {
    // Fields
    private static final Basket EMPTY = new Basket();

    // Methods

    @Test
    @Parameters
    @TestCaseName("Test #{index}: new MoveBase({0},{1})")
    public final void testStateChangeBase(@NonNull Trader target, @NonNull Market sourceMarket) {
        @NonNull StateChangeBase scb = new StateChangeBase(target, sourceMarket);

        assertSame(target, scb.getTarget());
        assertSame(sourceMarket, scb.getSourceMarket());
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestStateChangeBase() {
        Economy e1 = new Economy();
        Trader t1 = e1.addTrader(0, TraderState.ACTIVE, EMPTY, EMPTY);
        Trader t2 = e1.addTrader(0, TraderState.INACTIVE, EMPTY, EMPTY);

        return new Object[][]{{t1,e1.getMarket(EMPTY)},{t2,e1.getMarket(EMPTY)}};
    }

} // end StateChangeBaseTest class
