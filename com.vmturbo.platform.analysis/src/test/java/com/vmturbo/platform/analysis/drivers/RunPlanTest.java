package com.vmturbo.platform.analysis.drivers;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.vmturbo.platform.analysis.drivers.RunPlan;

import junitparams.JUnitParamsRunner;

/**
 * Test cases for {@link RunPlan} class.
 */
@RunWith(JUnitParamsRunner.class)
public final class RunPlanTest {

    @Test
    public void testConstructor() {
        new RunPlan();
    }

    @Test
    @Ignore // TODO: decide what there is to test about main method
    public void testMain() {
        RunPlan.main(new String[0]);
    }

} // end RunPlanTest class
