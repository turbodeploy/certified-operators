package com.vmturbo.platform.analysis.ede;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.vmturbo.platform.analysis.economy.Economy;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;

/**
 * A test case for the {@link QuoteSummer} class.
 */
@RunWith(JUnitParamsRunner.class)
public class QuoteSummerTest {
    // Fields

    // Methods

    @Test
    @Parameters
    @TestCaseName("Test #{index}: QuoteSummer({0},{1})")
    public final void testQuoteSummer_And_Getters(@NonNull Economy economy, long clique) {
        QuoteSummer summer = new QuoteSummer(economy, clique);

        assertSame(economy, summer.getEconomy());
        assertSame(clique, summer.getClique());
        assertEquals(0.0, summer.getTotalQuote(), TestUtils.FLOATING_POINT_DELTA);
        assertTrue(summer.getBestSellers().isEmpty());
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestQuoteSummer_And_Getters() {
        return new Object[][] {
            {new Economy(),-3},
            {new Economy(),0},
            {new Economy(),42},
        };
    }

    @Test
    @Ignore
    public final void testAccept() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    @Ignore
    public final void testCombine() {
        fail("Not yet implemented"); // TODO
    }

} // end QuoteSummerTest class
