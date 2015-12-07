package com.vmturbo.platform.analysis;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import junitparams.JUnitParamsRunner;

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
    public void testConstructor() {
        new Main();
    }

    @Test
    @Ignore // TODO: decide what there is to test about main method
    public void testMain() {
        Main.main(new String[0]);
    }

} // end MainTest class
