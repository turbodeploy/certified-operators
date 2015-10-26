package com.vmturbo.platform.analysis.pricefunction;

import static org.junit.Assert.*;

import java.util.function.UnaryOperator;

import org.junit.Test;

public class TestPriceFunction {

    final static double CONST = 40;

    @Test
    /**
     * Test that the factory reuses instances of PriceFunction
     */
    public void testMap() {
        PriceFunction pf1 = PFUtility.createConstantPriceFunction(CONST);
        PriceFunction pf2 = PFUtility.createConstantPriceFunction(CONST);
        assertSame(pf1, pf2);

        int h1 = System.identityHashCode(pf1);
        pf1 = null;
        pf2 = null;
        // Now the price function has no references, so in the next GC it is
        // supposed to be removed from the map.
        System.gc();
        // Asserting that another request for a constant price function
        // will give a new instance (one with a different identity hash code).
        pf1 = PFUtility.createConstantPriceFunction(CONST);
        assertNotEquals(h1, System.identityHashCode(pf1));

        PriceFunction pf3 = PFUtility.createStandardWeightedPriceFunction(CONST + 1);
        assertNotSame(pf1, pf3);

        UnaryOperator<Double> uod = (u) -> u * u;
        PriceFunction pf4 = PFUtility.createPriceFunction(uod);
        PriceFunction pf5 = PFUtility.createPriceFunction(uod);
        assertSame(pf4, pf5);

        PriceFunction pf6 = PFUtility.createPriceFunction((u) -> u * u);
        // although the function is the same, these are two distinct instances
        assertNotSame(pf5, pf6);
    }

    final static double delta = 1e-10; // used in assertEquals(double, double, delta)

    @Test
    /**
     * Test that the values returned by the constant price function are as expected
     */
    public void testValuesConst() {
        PriceFunction pfConst = PFUtility.createConstantPriceFunction(CONST);
        assertEquals(pfConst.unitPrice(0.0), CONST, delta);
        assertEquals(pfConst.unitPrice(0.5), CONST, delta);
        assertEquals(pfConst.unitPrice(1.0 - delta), CONST, delta);
        assertEquals(pfConst.unitPrice(1.0), PFUtility.MAX_UNIT_PRICE, delta);
    }

    final static double BELOW = 5;
    final static double ABOVE = 100;
    final static double STEP_AT = 0.7;

    @Test
    /**
     * Test that the values returned by the step price function are as expected
     */
    public void testValuesStep() {
        PriceFunction pfStep = PFUtility.createStepPriceFunction(STEP_AT, BELOW, ABOVE);
        assertEquals(pfStep.unitPrice(0.0), BELOW, delta);
        assertEquals(pfStep.unitPrice(STEP_AT - delta), BELOW, delta);
        assertEquals(pfStep.unitPrice(STEP_AT + delta), ABOVE, delta);
        assertEquals(pfStep.unitPrice(1.0 - delta), ABOVE, delta);
        assertEquals(pfStep.unitPrice(1.0), PFUtility.MAX_UNIT_PRICE, delta);
    }

    final static double WEIGHT = 27.0;

    @Test
    /**
     * Test that the values returned by the standard weighted price function are as expected
     */
    public void testValuesStandardWeighted() {
        PriceFunction pfStd = PFUtility.createStandardWeightedPriceFunction(WEIGHT);
        assertEquals(pfStd.unitPrice(0.0), WEIGHT, delta);
        assertEquals(pfStd.unitPrice(0.5), 4 * WEIGHT, delta);
        assertEquals(pfStd.unitPrice(0.75), 16 * WEIGHT, delta);
        assertEquals(pfStd.unitPrice(1.0), PFUtility.MAX_UNIT_PRICE, delta);
    }

    final static UnaryOperator<Double> uod = (u) -> 0.7 + u + u * u;

    @Test
    /**
     * Test that a custom price function returns the expected values
     */
    public void testCustom() {
        PriceFunction pfCustom = PFUtility.createPriceFunction(uod);
        for (double d : new Double[]{0.0, 0.3, 0.7, 1.0 - delta}) {
            assertEquals(pfCustom.unitPrice(d), uod.apply(d), delta);
        }
        assertEquals(pfCustom.unitPrice(1.0), PFUtility.MAX_UNIT_PRICE, delta);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBadValue() {
        PriceFunction pfCustom = PFUtility.createPriceFunction(uod);
        double unitPrice = pfCustom.unitPrice(-0.5);
        fail("Not supposed to get here : " + unitPrice);
    }

    // TODO(Shai): add unit test for unit peak price function
}
