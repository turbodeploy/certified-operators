package com.vmturbo.platform.analysis.pricefunction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.lang.ref.WeakReference;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

import org.junit.Test;
import org.junit.runner.RunWith;

import com.vmturbo.platform.analysis.testUtilities.TestUtils;

/**
 * A test case for the {@link PriceFunctionFactory} class.
 */
@RunWith(JUnitParamsRunner.class)
public class PriceFunctionFactoryTest {

    /**
     * Test that the factory reuses instances of PriceFunction.
     */
    @Test
    public void testMap() {
        final double const2 = 400;
        // Test that price functions are reused

        PriceFunction pf1a = PriceFunctionFactory.createConstantPriceFunction(const2);
        PriceFunction pf1b = PriceFunctionFactory.createConstantPriceFunction(const2);
        assertSame(pf1a, pf1b);

        // Now the price function has only weak references (weakPf1 and a link from the map),
        // so in the next GC it is supposed to be removed from the map.
        WeakReference<PriceFunction> weakPf1a = new WeakReference<>(pf1a);
        weakPf1a.get();
        pf1a = null;
        pf1b = null;
        System.gc();
        assertNull(weakPf1a.get());

        // Different arguments give different price functions
        PriceFunction pf3a = PriceFunctionFactory.createStandardWeightedPriceFunction(const2 + 1);
        PriceFunction pf3b = PriceFunctionFactory.createStandardWeightedPriceFunction(const2);
        assertNotSame(pf3b, pf3a);

        PriceFunction uod = PriceFunctionFactory.createConstantPriceFunction(0.1);
        PriceFunction pf4a = PriceFunctionFactory.createPriceFunction(uod);
        PriceFunction pf4b = PriceFunctionFactory.createPriceFunction(uod);
        assertSame(pf4a, pf4b);

        PriceFunction pf4c = new ConstantPriceFunction(0.1);
        // although the function is the same, these are two distinct instances
        assertNotSame(pf4b, pf4c);
    }

    /**
     * Test to make sure that the existing price functions return with expected params. If there
     * a new parameter is provided to any price function in the constructor, the
     * PriceFunction.getParams() and AnalysisToProtobuf.priceFunction() methods should be updated.
     */
    @Test
    public void testGetParams() {
        PriceFunction constant = PriceFunctionFactory.createConstantPriceFunction(0.1);
        assertTrue(constant.getParams().length == 1);
        assertEquals(0.1, constant.getParams()[0], 0.00001);

        PriceFunction externalPriceFunction = PriceFunctionFactory.createExternalPriceFunction();
        assertTrue(externalPriceFunction.getParams().length == 0);

        PriceFunction finiteStandardWeighted = PriceFunctionFactory
                .createFiniteStandardWeightedPriceFunction(0.7);
        assertTrue(finiteStandardWeighted.getParams().length == 1);
        assertEquals(0.7, finiteStandardWeighted.getParams()[0], 0.00001);

        PriceFunction ignoreUtil = PriceFunctionFactory.createIgnoreUtilizationPriceFunction();
        assertTrue(ignoreUtil.getParams().length == 0);

        PriceFunction scaledCapacity = PriceFunctionFactory
                .createScaledCapacityStandardWeightedPriceFunction(0.7, 1.2);
        assertTrue(scaledCapacity.getParams().length == 2);
        assertEquals(0.7, scaledCapacity.getParams()[0], 0.00001);
        assertEquals(1.2, scaledCapacity.getParams()[1], 0.00001);

        PriceFunction squaredReciprocal = PriceFunctionFactory
                .createSquaredReciprocalBoughtUtilizationPriceFunction(0.5);
        assertTrue(squaredReciprocal.getParams().length == 1);
        assertEquals(0.5, squaredReciprocal.getParams()[0], 0.00001);

        PriceFunction standardWeighted = PriceFunctionFactory
                .createStandardWeightedPriceFunction(0.6);
        assertTrue(standardWeighted.getParams().length == 1);
        assertEquals(0.6, standardWeighted.getParams()[0], 0.00001);

        PriceFunction step = PriceFunctionFactory
                        .createStepPriceFunction(0.5, 0, 1);
        assertTrue(step.getParams().length == 3);
        assertEquals(0.5, step.getParams()[0], 0.00001);
        assertEquals(0, step.getParams()[1], 0.00001);
        assertEquals(1, step.getParams()[2], 0.00001);

        PriceFunction stepForCloud = PriceFunctionFactory.createStepPriceFunctionForCloud();
        assertEquals(11.11, stepForCloud.getParams()[0], 0.00001);

        PriceFunction overProvision =
            PriceFunctionFactory.createOverProvisionedPriceFunction(1.0, 1.0, 1.0, 2.0);
        assertTrue(overProvision.getParams().length == 4);
        assertEquals(1.0, overProvision.getParams()[0], 0.00001);
        assertEquals(1.0, overProvision.getParams()[1], 0.00001);
        assertEquals(1.0, overProvision.getParams()[2], 0.00001);
        assertEquals(2.0, overProvision.getParams()[3], 0.00001);
    }

    /**
     * Test that the factory reuses instances of PriceFunction during weight updation.
     */
    @Test
    public void testUpdatePriceFunctionWithWeight() {
        final double const3 = 400;

        PriceFunction swpf = PriceFunctionFactory.createStandardWeightedPriceFunction(const3);
        assertNotEquals(swpf.updatePriceFunctionWithWeight(const3 + 1), swpf);

        PriceFunction cpf = PriceFunctionFactory.createConstantPriceFunction(const3);
        assertEquals(cpf.updatePriceFunctionWithWeight(const3 + 1), cpf);
    }

    private static final double ONE = 1.0;
    private static final double delta = 1e-10;

    private static final double CONST = 40;
    private static final PriceFunction pfConst = PriceFunctionFactory.createConstantPriceFunction(CONST);

    /**
     * Test that the values returned by the constant price function are as expected.
     *
     * @param u - u
     * @param ut - ut
     * @param p - p
     */
    @Test
    @Parameters
    @TestCaseName("Test #{index}: constant price function with u={0} and ut={1}")
    public void testValuesConst(double u, double ut, double p) {
        assertEquals(CONST, pfConst.unitPrice(u / ut, null, null, null, null), TestUtils.FLOATING_POINT_DELTA);
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
    private static final PriceFunction pfStep = PriceFunctionFactory.createStepPriceFunction(STEP_AT, BELOW, ABOVE);

    /**
     * Test that the values returned by the step price function are as expected.
     *
     * @param u - u
     * @param ut - ut
     * @param p - p
     */
    @Test
    @Parameters
    @TestCaseName("Test #{index}: step price function with u={0} and ut={1}")
    public void testValuesStep(double u, double ut, double p) {
        assertEquals(p, pfStep.unitPrice(u / ut, null, null, null, null), TestUtils.FLOATING_POINT_DELTA);
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
    private static final PriceFunction pfStd = PriceFunctionFactory.createStandardWeightedPriceFunction(WEIGHT);

    /**
     * Test that the values returned by the standard weighted price function are as expected.
     *
     * @param u - u
     * @param factor - factor
     */
    @Test
    @Parameters
    @TestCaseName("Test #{index}: standard weighted price function with u={0}")
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

    private static final PriceFunction uod = PriceFunctionFactory.createConstantPriceFunction(1);
    private static final PriceFunction pfCustom = PriceFunctionFactory.createPriceFunction(uod);

    /**
     * Test that a custom price function returns the expected values.
     *
     * @param d - d
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

    /**
     * Test max price.
     *
     * @param pf - pf
     * @param name - name
     */
    @Test
    @Parameters
    @TestCaseName("Test #{index}: {1} function")
    public void testMaxPrice(PriceFunction pf, String name) {
        assertTrue(pf.unitPrice(ONE, null, null, null, null)
                <= PriceFunctionFactory.MAX_UNIT_PRICE);
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
