package com.vmturbo.platform.analysis.economy;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

/**
 * A test case for the combined CommoditySoldWithSettings class.
 *
 * <p>
 *  It tests both CommoditySold and CommoditySoldSettings interfaces without casting to the
 *  corresponding interface.
 * </p>
 */
@RunWith(JUnitParamsRunner.class)
public final class CommoditySoldWithSettingsTest {

    // Fields
    CommoditySoldWithSettings fixture;

    // Methods

    @Before
    public void setUp() {
        fixture = new CommoditySoldWithSettings();
    }

    @After
    public void tearDown() {}

    @Test
    public final void testCommoditySoldWithSettings() {
        CommoditySoldWithSettings csws = new CommoditySoldWithSettings();
        // Sanity check: make sure initial values are valid.
        csws.setCapacity(csws.getCapacity());
        csws.setCapacityIncrement(csws.getCapacityIncrement());
        csws.setCapacityLowerBound(csws.getCapacityLowerBound());
        csws.setCapacityUpperBound(csws.getCapacityUpperBound());
        csws.setResizable(csws.isResizable());
        csws.setThin(csws.isThin());
        csws.setUtilizationUpperBound(csws.getUtilizationUpperBound());
    }

    @Test
    @Ignore
    public final void testGetConsumers() {
        fail("Not yet implemented");// TODO
    }

    @Test
    public final void testGetSettings() {
        assertSame(fixture, fixture.getSettings());
    }

    @Test
    @Ignore
    public final void testGetType() {
        fail("Not yet implemented");// TODO
    }

    @Test
    @Ignore
    public final void testGetKey() {
        fail("Not yet implemented");// TODO
    }

    @Test
    @Parameters({"0.0","1.0","1000"})
    @TestCaseName("Test #{index}: (set|get)Capacity({0})")
    public final void testGetSetCapacity_NormalInput(double capacity) {
        fixture.setCapacity(capacity);
        assertEquals(capacity, fixture.getCapacity(), 0.0);
    }

    @Test(expected = IllegalArgumentException.class)
    @Parameters({"-0.001","-1.0","-1000"})
    @TestCaseName("Test #{index}: (set|get)Capacity({0})")
    public final void testGetSetCapacity_InvalidInput(double capacity) {
        fixture.setCapacity(capacity);
    }

    @Test
    @Parameters({"true","false"})
    @TestCaseName("Test #{index}: (set|is)Thin({0})")
    public final void testIsSetThin(boolean thin) {
        fixture.setThin(thin);
        assertEquals(thin, fixture.isThin());
    }

    @Test
    @Parameters({"true","false"})
    @TestCaseName("Test #{index}: (set|is)Resizable({0})")
    public final void testIsSetResizable(boolean resizable) {
        fixture.setResizable(resizable);
        assertEquals(resizable, fixture.isResizable());
    }

    @Test
    @Parameters({"0.0,0.0","0.0,1.0","0.0,1000",
                 "1.0,1.0","1.0,1000",
                 "1000,1000"})
    @TestCaseName("Test #{index}: (set|get)CapacityUpperBound({1}) with capacityLowerBound == {0}")
    public final void testGetSetCapacityUpperBound_NomralInput(double capacityLowerBound, double capacityUpperBound) {
        fixture.setCapacityLowerBound(capacityLowerBound);
        fixture.setCapacityUpperBound(capacityUpperBound);
        assertEquals(capacityUpperBound, fixture.getCapacityUpperBound(), 0.0);
    }

    @Test(expected = IllegalArgumentException.class)
    @Parameters({"0.0,-0.1","0.0,-1.0",
                 "1.0,0.0","1.0,-1000",
                 "1000,2","1000,-500"})
    @TestCaseName("Test #{index}: (set|get)CapacityUpperBound({1}) with capacityLowerBound == {0}")
    public final void testGetSetCapacityUpperBound_InvalidInput(double capacityLowerBound, double capacityUpperBound) {
        fixture.setCapacityLowerBound(capacityLowerBound);
        fixture.setCapacityUpperBound(capacityUpperBound);
    }

    @Test
    @Parameters({"0.0,0.0","0.0,1.0","0.0,1000",
                 "1.0,1.0","1.0,1000",
                 "1000,1000"})
    @TestCaseName("Test #{index}: (set|get)CapacityLowerBound({0}) with capacityUpperBound == {1}")
    public final void testGetSetCapacityLowerBound_NormalInput(double capacityLowerBound, double capacityUpperBound) {
        fixture.setCapacityUpperBound(capacityUpperBound);
        fixture.setCapacityLowerBound(capacityLowerBound);
        assertEquals(capacityLowerBound, fixture.getCapacityLowerBound(), 0.0);
    }

    @Test(expected = IllegalArgumentException.class)
    @Parameters({"-0.01,0.0","1000,0.0",
                 "-50,1.0","2.0,1.0",
                 "-1.0,1000","2000,1000"})
    @TestCaseName("Test #{index}: (set|get)CapacityLowerBound({0}) with capacityUpperBound == {1}")
    public final void testGetSetCapacityLowerBound_InvalidInput(double capacityLowerBound, double capacityUpperBound) {
        fixture.setCapacityUpperBound(capacityUpperBound);
        fixture.setCapacityLowerBound(capacityLowerBound);
        assertEquals(capacityLowerBound, fixture.getCapacityLowerBound(), 0.0);
    }

    @Test
    @Parameters({"0.0","0.4","0.999","1.0","100"})
    @TestCaseName("Test #{index}: (set|get)CapacityIncrement({0})")
    public final void testGetSetCapacityIncrement_NormalInput(double capacityIncrement) {
        fixture.setCapacityIncrement(capacityIncrement);
        assertEquals(capacityIncrement, fixture.getCapacityIncrement(), 0.0);
    }

    @Test(expected = IllegalArgumentException.class)
    @Parameters({"-0.01","-2","-100"})
    @TestCaseName("Test #{index}: (set|get)CapacityIncrement({0})")
    public final void testGetSetCapacityIncrement_InvalidInput(double capacityIncrement) {
        fixture.setCapacityIncrement(capacityIncrement);
    }

    @Test
    @Parameters({"0.0","0.4","0.999","1.0"})
    @TestCaseName("Test #{index}: (set|get)UtilizationUpperBound({0})")
    public final void testGetSetUtilizationUpperBound_NormalInput(double utilizationUpperBound) {
        fixture.setUtilizationUpperBound(utilizationUpperBound);
        assertEquals(utilizationUpperBound, fixture.getUtilizationUpperBound(), 0.0);
    }

    @Test(expected = IllegalArgumentException.class)
    @Parameters({"-0.01","-5","1.001","100"})
    @TestCaseName("Test #{index}: (set|get)UtilizationUpperBound({0})")
    public final void testGetSetUtilizationUpperBound_InvalidInput(double utilizationUpperBound) {
        fixture.setUtilizationUpperBound(utilizationUpperBound);
    }

} // end class CommoditySoldWithSettingsTest
