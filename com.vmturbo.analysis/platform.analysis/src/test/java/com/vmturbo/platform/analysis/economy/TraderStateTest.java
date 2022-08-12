package com.vmturbo.platform.analysis.economy;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.runner.RunWith;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

/**
 * A test case for the {@link TraderState} class.
 */
@RunWith(JUnitParamsRunner.class)
public class TraderStateTest {

    // Methods
    @Test
    @Parameters({"ACTIVE,true","INACTIVE,false"})
    @TestCaseName("Test #{index}: {0}.isActive() == {1}")
    public final void testIsActive(TraderState state, boolean output) {
        assertEquals(output, state.isActive());
    }

    @Test
    @Parameters(source = TraderState.class)
    @TestCaseName("Test #{index}: TraderState.valueOf({0}.toString()) == {0}")
    public final void testValueOf_and_toString(TraderState state) {
        assertSame(state, TraderState.valueOf(state.toString()));
    }

} // end TraderStateTest class
