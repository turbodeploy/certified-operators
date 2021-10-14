package com.vmturbo.components.common.utils;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Test class for {@link FuzzyDoubleUtils}.
 */
public class FuzzyDoubleUtilsTest {

    /**
     * Tests {@link FuzzyDoubleUtils#isPositive(double)}.
     */
    @Test
    public void testIsPositive() {

        assertTrue(FuzzyDoubleUtils.isPositive(0.0001));
        assertFalse(FuzzyDoubleUtils.isPositive(0.0));
        assertFalse(FuzzyDoubleUtils.isPositive(-0.001));
        assertFalse(FuzzyDoubleUtils.isPositive(Double.NaN));
        assertTrue(FuzzyDoubleUtils.isPositive(Double.POSITIVE_INFINITY));
        assertFalse(FuzzyDoubleUtils.isPositive(Double.NEGATIVE_INFINITY));
    }

    /**
     * Tests {@link FuzzyDoubleUtils#isNegative(double)} (double)}.
     */
    @Test
    public void testIsNegative() {

        assertFalse(FuzzyDoubleUtils.isNegative(0.0001));
        assertFalse(FuzzyDoubleUtils.isNegative(0.0));
        assertTrue(FuzzyDoubleUtils.isNegative(-0.001));
        assertFalse(FuzzyDoubleUtils.isNegative(Double.NaN));
        assertFalse(FuzzyDoubleUtils.isNegative(Double.POSITIVE_INFINITY));
        assertTrue(FuzzyDoubleUtils.isNegative(Double.NEGATIVE_INFINITY));
    }
}
