package com.vmturbo.platform.analysis.economy;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

/**
 * A test case for the combined TraderWithSettings class.
 *
 * <p>
 *  It tests both Trader and TraderSettings interfaces without casting to the corresponding interface.
 * </p>
 */
@RunWith(JUnitParamsRunner.class)
public final class TraderWithSettingsTest {

    // Fields
    private TraderWithSettings fixture;


    // Methods

    @Before
    public void setUp() {
        fixture = new TraderWithSettings(null, null, new Basket());
    }

    @After
    public void tearDown() {}

    @Test
    public final void testTraderWithSettings() {
        fail("Not yet implemented");// TODO
    }

    @Test
    @Parameters({"true","false"})
    @TestCaseName("(set|is)Suspendable({0})")
    public final void testIsSetSuspendable(boolean suspendable) {
        fixture.setSuspendable(suspendable);
        assertEquals(suspendable, fixture.isSuspendable());
    }

    @Test
    @Parameters({"true","false"})
    @TestCaseName("(set|is)Cloneable({0})")
    public final void testIsCloneable(boolean cloneable) {
        fixture.setCloneable(cloneable);
        assertEquals(cloneable, fixture.isCloneable());
    }

    @Test
    @Parameters({"0.0,0.0","0.0,0.5","0.0,1.0",
                 "0.5,0.5","0.5,1.0",
                 "1.0,1.0"})
    @TestCaseName("(set|get)MaxDesiredUtil({1}) while minDesiredUtilization == {0}")
    public final void testGetSetMaxDesiredUtil_NormalInput(double minDesiredUtilization, double maxDesiredUtilization) {
        fixture.setMinDesiredUtil(minDesiredUtilization);
        fixture.setMaxDesiredUtil(maxDesiredUtilization);
        assertEquals(maxDesiredUtilization, fixture.getMaxDesiredUtil(), 0.0);
    }

    @Test(expected = IllegalArgumentException.class)
    @Parameters({"0.0,-1.0","0.0,1.5",
                 "0.5,0.0","0.5,1.5",
                 "1.0,0.5","1.0,1.5"})
    @TestCaseName("(set|get)MaxDesiredUtil({1}) while minDesiredUtilization == {0}")
    public final void testGetSetMaxDesiredUtil_InvalidInput(double minDesiredUtilization, double maxDesiredUtilization) {
        fixture.setMinDesiredUtil(minDesiredUtilization);
        fixture.setMaxDesiredUtil(maxDesiredUtilization);
    }

    @Test
    @Parameters({"0.0,1.0","0.5,1.0","1.0,1.0",
                 "0.0,0.5","0.5,0.5",
                 "0.0,0.0"})
    @TestCaseName("(set|get)MinDesiredUtil({0}) while maxDesiredUtilization == {1}")
    public final void testGetSetMinDesiredUtil_NormalInput(double minDesiredUtilization, double maxDesiredUtilization) {
        fixture.setMaxDesiredUtil(maxDesiredUtilization);
        fixture.setMinDesiredUtil(minDesiredUtilization);
        assertEquals(minDesiredUtilization, fixture.getMinDesiredUtil(), 0.0);
    }

    @Test(expected = IllegalArgumentException.class)
    @Parameters({"-1.0,1.0","1.5,1.0",
                 "-1.0,0.5","1.0,0.5",
                 "-1.0,0.0","0.5,0.0"})
    @TestCaseName("(set|get)MinDesiredUtil({0}) while maxDesiredUtilization == {1}")
    public final void testGetSetMinDesiredUtil_InvalidInput(double minDesiredUtilization, double maxDesiredUtilization) {
        fixture.setMaxDesiredUtil(maxDesiredUtilization);
        fixture.setMinDesiredUtil(minDesiredUtilization);
    }

    @Test
    public final void testGetBasketsBought() {
        fail("Not yet implemented");// TODO
    }

    @Test
    public final void testGetCommoditiesSold() {
        fail("Not yet implemented");// TODO
    }

    @Test
    public final void testGetSettings() {
        assertSame(fixture, fixture.getSettings());
    }

    @Test
    public final void testGetType() {
        fail("Not yet implemented");// TODO
    }

    @Test
    @Parameters(source = TraderState.class)
    @TestCaseName("(set|get)State({0})")
    public final void testGetSetState(TraderState state) {
        fixture.setState(state);
        assertSame(state, fixture.getState());
    }

} // end class TraderWithSettingsTest
