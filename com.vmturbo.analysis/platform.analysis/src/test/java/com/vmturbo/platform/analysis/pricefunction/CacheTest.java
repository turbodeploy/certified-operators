package com.vmturbo.platform.analysis.pricefunction;

import static org.junit.Assert.*;

import java.lang.ref.WeakReference;
import org.junit.Test;
import org.junit.runner.RunWith;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;

/**
 * A test case for the {@link Cache} class.
 */
@RunWith(JUnitParamsRunner.class)
public class CacheTest {

    @Test
    /**
     * Test that the factory reuses instances of PriceFunction
     */
    public void testMap() {
        final double CONST2 = 400;
        // Test that price functions are reused

        PriceFunction pf1a = Cache.createConstantPriceFunction(CONST2);
        PriceFunction pf1b = Cache.createConstantPriceFunction(CONST2);
        assertSame(pf1a, pf1b);

        WeakReference<PriceFunction> weakPf1a = new WeakReference<>(pf1a);
        pf1a = null;
        pf1b = null;
        // Now the price function has only weak references (weakPf1 and a link from the map),
        // so in the next GC it is supposed to be removed from the map.
        System.gc();
        assertNull(weakPf1a.get());

        // Different arguments give different price functions
        PriceFunction pf3a = Cache.createStandardWeightedPriceFunction(CONST2 + 1);
        PriceFunction pf3b = Cache.createStandardWeightedPriceFunction(CONST2);
        assertNotSame(pf3b, pf3a);

        PriceFunction uod = (u, sl, seller, commSold, economy) -> u * u;
        PriceFunction pf4a = Cache.createPriceFunction(uod);
        PriceFunction pf4b = Cache.createPriceFunction(uod);
        assertSame(pf4a, pf4b);

        PriceFunction pf4c = Cache.createPriceFunction((u, sl, seller, commSold, economy) -> u * u);
        // although the function is the same, these are two distinct instances
        assertNotSame(pf4b, pf4c);
    }

    private static final double ONE = 1.0;
    private static final double delta = 1e-10;

    private static final double CONST = 40;
    private static final PriceFunction pfConst = Cache.createConstantPriceFunction(CONST);

    @Test
    @Parameters
    @TestCaseName("Test #{index}: constant price function with u={0} and ut={1}")
    /**
     * Test that the values returned by the constant price function are as expected
     */
    public void testValuesConst(double u, double ut, double p) {
        assertEquals(CONST, pfConst.unitPrice(u/ut, null, null, null, null), TestUtils.FLOATING_POINT_DELTA);
    }

    @SuppressWarnings("unused")
    private static Double[][] parametersForTestValuesConst() {
        return new Double[][]{
            {0.0, ONE, CONST},
            {0.5, ONE, CONST},
            {ONE - delta, ONE, CONST}
        };
    }

    private static final double BELOW = 5;
    private static final double ABOVE = 100;
    private static final double STEP_AT = 0.7;
    private static final PriceFunction pfStep = Cache.createStepPriceFunction(STEP_AT, BELOW, ABOVE);

    @Test
    @Parameters
    @TestCaseName("Test #{index}: step price function with u={0} and ut={1}")
    /**
     * Test that the values returned by the step price function are as expected
     */
    public void testValuesStep(double u, double ut, double p) {
        assertEquals(p, pfStep.unitPrice(u/ut, null, null, null, null), TestUtils.FLOATING_POINT_DELTA);
    }

    @SuppressWarnings("unused")
    private static Double[][] parametersForTestValuesStep() {
        return new Double[][]{
            {0.0, ONE, BELOW},
            {STEP_AT - delta, ONE, BELOW},
            {STEP_AT + delta, ONE, ABOVE},
            {ONE - delta, ONE, ABOVE}
        };
    }

    private static final double WEIGHT = 27.0;
    private static final PriceFunction pfStd = Cache.createStandardWeightedPriceFunction(WEIGHT);

    @Test
    @Parameters
    @TestCaseName("Test #{index}: standard weighted price function with u={0}")
    /**
     * Test that the values returned by the standard weighted price function are as expected
     */
    public void testValuesStandardWeighted(double u, double factor) {
        assertEquals(factor * WEIGHT, pfStd.unitPrice(u, null, null, null, null), TestUtils.FLOATING_POINT_DELTA);
    }

    @SuppressWarnings("unused")
    private static Double[][] parametersForTestValuesStandardWeighted() {
        return new Double[][]{
            {0.0, 1.0},
            {0.5, 4.0},
            {0.75, 16.0}
        };
    }

    private static final PriceFunction uod = (u, sl, seller, commSold, economy) -> 0.7 + u + u * u;
    private static final PriceFunction pfCustom = Cache.createPriceFunction(uod);

    /**
     * Test that a custom price function returns the expected values
     */
    @Test
    @Parameters
    @TestCaseName("Test #{index}: Custom price function with value {0}")
    public void testCustom(double d) {
        assertEquals(pfCustom.unitPrice(d, null, null, null, null), uod.unitPrice(d, null, null, null, null), TestUtils.FLOATING_POINT_DELTA);
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Double[] parametersForTestCustom() {
        return new Double[]{0.0, 0.3, 0.7, ONE - delta};
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: {1} function")
    public void testMaxPrice(PriceFunction pf, String name) {
        assertTrue(pf.unitPrice(ONE, null, null, null, null) <= Cache.MAX_UNIT_PRICE);
    }

    @SuppressWarnings("unused")
    private static Object[][] parametersForTestMaxPrice() {
        return new Object[][]{
            {pfConst, "Constant"},
            {pfStep, "Step"},
            {pfStd, "Starndard Weighted"},
            {pfCustom, "Custom"}
        };
    }
}
