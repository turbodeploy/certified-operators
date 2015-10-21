package com.vmturbo.platform.analysis.economy;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;
import java.util.function.UnaryOperator;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Before;
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
    private CommoditySoldWithSettings fixture;

    // Methods

    @Before
    public void setUp() {
        fixture = new CommoditySoldWithSettings();
    }

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
        csws.setPriceFunction(csws.getPriceFunction());
    }

    @Test // That the returned list is indeed unmodifiable (part 1)
    public final void testGetBuyers_ValidOperations() {
        @NonNull @ReadOnly List<@NonNull @ReadOnly BuyerParticipation> buyers = new CommoditySoldWithSettings().getBuyers();
        assertFalse(buyers.contains(null));
        assertFalse(buyers.containsAll(Arrays.asList(null,null)));
        assertTrue(buyers.equals(buyers));
        assertEquals(-1, buyers.indexOf(null));
        assertTrue(buyers.isEmpty());
        assertNotNull(buyers.iterator());
        assertEquals(-1, buyers.lastIndexOf(null));
        assertNotNull(buyers.listIterator());
        assertEquals(0, buyers.size());
        assertNotNull(buyers.toArray());
    }

    @Test // That the returned list is indeed unmodifiable (part 2)
    public final void testGetBuyers_InvalidOperations() {
        @NonNull @ReadOnly List<@NonNull @ReadOnly BuyerParticipation> buyers = new CommoditySoldWithSettings().getBuyers();
        // TODO: should change nulls to constructor calls once we finalize Trader.
        // TODO: may also need to test these on a non-empty buyers list because the API does not
        // guarantee that this exception will be thrown in some cases.
        try{
            buyers.add(null);
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try{
            buyers.add(0,null);
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try{
            buyers.addAll(Arrays.asList(null,null));
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try{
            buyers.addAll(0,Arrays.asList(null,null));
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try{
            buyers.clear();
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try{
            buyers.remove(0);
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try{
            buyers.remove(null);
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try{
            buyers.removeAll(buyers);
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try{
            buyers.retainAll(buyers);
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try{
            buyers.set(0, null);
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
    }

    @Test // That the returned list is indeed modifiable.
    public final void testGetModifiableBuyersList() {
        @NonNull List<@NonNull @ReadOnly BuyerParticipation> buyers = new CommoditySoldWithSettings().getModifiableBuyersList();
        assertFalse(buyers.contains(null));
        assertFalse(buyers.containsAll(Arrays.asList(null,null)));
        assertTrue(buyers.equals(buyers));
        assertEquals(-1, buyers.indexOf(null));
        assertTrue(buyers.isEmpty());
        assertNotNull(buyers.iterator());
        assertEquals(-1, buyers.lastIndexOf(null));
        assertNotNull(buyers.listIterator());
        assertEquals(0, buyers.size());
        assertNotNull(buyers.toArray());
        assertTrue(buyers.add(null));
        buyers.add(0,null);
        assertTrue(buyers.addAll(Arrays.asList(null,null)));
        assertTrue(buyers.addAll(0,Arrays.asList(null,null)));
        //buyers.remove(0);
        assertTrue(buyers.remove(null));
        assertTrue(buyers.removeAll(buyers));
        assertFalse(buyers.retainAll(buyers));
        //buyers.set(0, null);
        buyers.clear();
    }


    @Test
    public final void testGetSettings() {
        assertSame(fixture, fixture.getSettings());
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

    @Test
    @Parameters
    @TestCaseName("Test #{index}: (set|get)PriceFunction({0})")
    public final void testGetSetPriceFunction(UnaryOperator<Double> priceFunction) {
        fixture.setPriceFunction(priceFunction);
        assertSame(priceFunction, fixture.getPriceFunction());
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestGetSetPriceFunction() {
        return new Object[]{
            (UnaryOperator<Double>)x -> x*x,
            (UnaryOperator<Double>)x -> 1 / ((1-x)*(1-x)),
            (UnaryOperator<Double>)x -> 1/x
        };
    }

    @Test
    @Parameters({"0,1","0.1,1.234567","0.5,4","0.9,100"})
    @TestCaseName("Test #{index}: getPriceFunction.apply({0}) == {1}")
    public final void testDefaultPriceFunction(double input, double output) {
        assertEquals(output, fixture.getPriceFunction().apply(input), 0.000001f); // TODO: improve delta
    }

} // end class CommoditySoldWithSettingsTest
