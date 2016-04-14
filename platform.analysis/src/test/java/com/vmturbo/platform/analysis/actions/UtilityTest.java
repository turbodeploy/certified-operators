package com.vmturbo.platform.analysis.actions;

import static org.junit.Assert.*;

import java.util.Arrays;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.topology.LegacyTopology;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

/**
 * A test case for the {@link Utility} class.
 */
@RunWith(JUnitParamsRunner.class)
public class UtilityTest {
    // Methods

    @Test
    @Parameters
    @TestCaseName("Test #{index}: Utility.appendTrader(\"{0}\",{1},[{2}]) == \"{3}\"")
    public final void testAppendTrader(@NonNull StringBuilder builder, @NonNull Trader trader,
            @NonNull LegacyTopology topology, @NonNull String result) {
        Utility.appendTrader(builder, trader, topology.getUuids()::get, topology.getNames()::get);
        assertEquals(result, builder.toString());
    }

    // TODO: test cases of invalid arguments too if this method is used outside of debug-only code!
    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestAppendTrader() {
        LegacyTopology topology1 = new LegacyTopology();
        Trader trader1 = topology1.addTrader("uuid-1-1", "name-1-1", "type1", TraderState.ACTIVE, Arrays.asList());
        Trader trader2 = topology1.addTrader("uuid-1-2", "name-1-2", "type1", TraderState.INACTIVE, Arrays.asList("a"));
        Trader trader3 = topology1.addTrader("uuid-1-3", "name-1-3", "type2", TraderState.ACTIVE, Arrays.asList("a","b"));
        LegacyTopology topology2 = new LegacyTopology();
        Trader trader4 = topology2.addTrader("uuid-2-1", "name-2-1", "type1", TraderState.ACTIVE, Arrays.asList("c"));
        Trader trader5 = topology2.addTrader("uuid-2-2", "name-2-2", "type2", TraderState.INACTIVE, Arrays.asList("c"));

        return new Object[][]{
            {new StringBuilder(),trader1,topology1,"name-1-1 [uuid-1-1] (#0)"},
            {new StringBuilder(),trader2,topology1,"name-1-2 [uuid-1-2] (#1)"},
            {new StringBuilder(),trader3,topology1,"name-1-3 [uuid-1-3] (#2)"},
            {new StringBuilder(),trader4,topology2,"name-2-1 [uuid-2-1] (#0)"},
            {new StringBuilder(),trader5,topology2,"name-2-2 [uuid-2-2] (#1)"},
            {new StringBuilder().append("Check "),trader1,topology1,"Check name-1-1 [uuid-1-1] (#0)"},
            {new StringBuilder().append("Check "),trader2,topology1,"Check name-1-2 [uuid-1-2] (#1)"},
            {new StringBuilder().append("Check "),trader3,topology1,"Check name-1-3 [uuid-1-3] (#2)"},
            {new StringBuilder().append("Check "),trader4,topology2,"Check name-2-1 [uuid-2-1] (#0)"},
            {new StringBuilder().append("Check "),trader5,topology2,"Check name-2-2 [uuid-2-2] (#1)"},
        };
    }
} // end UtilityTest class
