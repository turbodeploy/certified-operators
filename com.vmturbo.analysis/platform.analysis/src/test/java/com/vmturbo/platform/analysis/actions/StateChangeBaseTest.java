package com.vmturbo.platform.analysis.actions;

import junitparams.JUnitParamsRunner;

import org.junit.runner.RunWith;

import com.vmturbo.platform.analysis.economy.Basket;

/**
 * A test case for the {@link StateChangeBase} class.
 */
@RunWith(JUnitParamsRunner.class)
public class StateChangeBaseTest {
    // Fields
    private static final Basket EMPTY = new Basket();

    // Methods

//    @Test
//    @Parameters
//    @TestCaseName("Test #{index}: new MoveBase({0},{1},{2})")
//    public final void testStateChangeBase(@NonNull Economy economy, @NonNull Trader target,
//                                          @NonNull Market sourceMarket) {
//        @NonNull StateChangeBase scb = new StateChangeBase(economy, target, sourceMarket);
//
//        assertSame(target, scb.getTarget());
//        assertSame(sourceMarket, scb.getSourceMarket());
//    }
//
//    @SuppressWarnings("unused") // it is used reflectively
//    private static Object[] parametersForTestStateChangeBase() {
//        Economy e1 = new Economy();
//        Trader t1 = e1.addTrader(0, TraderState.ACTIVE, EMPTY, EMPTY);
//        Trader t2 = e1.addTrader(0, TraderState.INACTIVE, EMPTY, EMPTY);
//
//        return new Object[][]{{e1, t1, e1.getMarket(EMPTY)}, {e1, t2, e1.getMarket(EMPTY)}};
//    }

} // end StateChangeBaseTest class
