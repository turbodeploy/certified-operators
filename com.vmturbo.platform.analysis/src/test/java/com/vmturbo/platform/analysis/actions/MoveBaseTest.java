package com.vmturbo.platform.analysis.actions;

import static org.junit.Assert.*;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.BuyerParticipation;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.TraderState;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

/**
 * A test case for the {@link MoveBase} class.
 */
@RunWith(JUnitParamsRunner.class)
public class MoveBaseTest {
    // Methods

    @Test
    @Parameters
    @TestCaseName("Test #{index}: new MoveBase({0},{1})")
    public final void testMoveBase(@NonNull Economy economy, @NonNull BuyerParticipation participation) {
        MoveBase mb = new MoveBase(economy,participation);

        assertSame(economy, mb.getEconomy());
        assertSame(participation, mb.getTarget());
        assertSame(participation.getSupplier(), mb.getSource());
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestMoveBase() {
        Economy e1 = new Economy();
        BuyerParticipation p1 = e1.addBasketBought(e1.addTrader(0, TraderState.ACTIVE, new Basket()), new Basket());

        Economy e2 = new Economy();
        BuyerParticipation p2 = e2.addBasketBought(e2.addTrader(0, TraderState.ACTIVE, new Basket()), new Basket());
        p2.move(e2.addTrader(1, TraderState.ACTIVE, new Basket()));

        return new Object[][]{{e1,p1},{e2,p2}};
    }

    @Test
    @Ignore
    public final void testAppendTrader() {
        fail("Not yet implemented"); // TODO
    }

} // end MoveBaseTest class
