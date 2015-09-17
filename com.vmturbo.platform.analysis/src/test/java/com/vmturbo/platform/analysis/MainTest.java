package com.vmturbo.platform.analysis;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.runner.RunWith;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

/**
 * Test cases for Main class.
 *
 * <p>
 *  We probably wont need to test the Main class, but this class was added so that:
 *  <ol>
 *   <li>We know that Maven is configured correctly to run unit tests.</li>
 *   <li>We know that all dependencies are resolved correctly.</li>
 *   <li>We have a sample of what a parameterized test using JUnitParams, looks like.</li>
 *  </ol>
 * </p>
 */
@RunWith(JUnitParamsRunner.class)
public final class MainTest {

    @Test
    @Parameters({"0,1","1,1","2,2","3,6","4,24","5,120"})
    @TestCaseName("factorial({0}) = {1}")
    public void testFactorialNormalInput(int input, int output) {
        assertEquals(output, Main.factorial(input));
    }

    @Test(expected = IllegalArgumentException.class)
    @Parameters({"-1","-2","-100"})
    @TestCaseName("factorial({0})")
    public void testFactorialInvalidInput(int input) {
        Main.factorial(input);
    }

}
