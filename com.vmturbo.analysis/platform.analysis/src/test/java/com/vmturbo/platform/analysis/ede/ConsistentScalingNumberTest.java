package com.vmturbo.platform.analysis.ede;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Tests for {@link ConsistentScalingNumber}.
 */
public class ConsistentScalingNumberTest {

    /**
     * testInConsistentUnits.
     */
    @Test
    public void testInConsistentUnits() {
        final ConsistentScalingNumber csq = ConsistentScalingNumber.fromConsistentNumber(1.0);
        assertEquals(1.0, csq.inConsistentUnits(), 0);
    }

    /**
     * testInNormalizedUnits.
     */
    @Test
    public void testInNormalizedUnits() {
        final ConsistentScalingNumber csq = ConsistentScalingNumber.fromConsistentNumber(1.0);
        assertEquals(0.5, csq.inNormalizedUnits(2.0), 0);
    }

    /**
     * testToString.
     */
    @Test
    public void testToString() {
        final ConsistentScalingNumber csq = ConsistentScalingNumber.fromConsistentNumber(1.0);
        assertEquals("CSQ=1.0", csq.toString());
    }

    /**
     * testPlus.
     */
    @Test
    public void testPlus() {
        final ConsistentScalingNumber a = ConsistentScalingNumber.fromConsistentNumber(1.0);
        final ConsistentScalingNumber b = ConsistentScalingNumber.fromConsistentNumber(2.0);
        assertEquals(3.0, a.plus(b).inConsistentUnits(), 0);
        assertEquals(3.0, b.plus(a).inConsistentUnits(), 0);
    }

    /**
     * testMinus.
     */
    @Test
    public void testMinus() {
        final ConsistentScalingNumber a = ConsistentScalingNumber.fromConsistentNumber(1.0);
        final ConsistentScalingNumber b = ConsistentScalingNumber.fromConsistentNumber(2.0);
        assertEquals(-1.0, a.minus(b).inConsistentUnits(), 0);
        assertEquals(1.0, b.minus(a).inConsistentUnits(), 0);
    }

    /**
     * testSubtractFrom.
     */
    @Test
    public void testSubtractFrom() {
        final ConsistentScalingNumber a = ConsistentScalingNumber.fromConsistentNumber(1.0);
        final ConsistentScalingNumber b = ConsistentScalingNumber.fromConsistentNumber(2.0);
        assertEquals(1.0, a.subtractFrom(b).inConsistentUnits(), 0);
        assertEquals(-1.0, b.subtractFrom(a).inConsistentUnits(), 0);
    }

    /**
     * testTimes.
     */
    @Test
    public void testTimes() {
        final ConsistentScalingNumber a = ConsistentScalingNumber.fromConsistentNumber(1.0);
        final ConsistentScalingNumber b = ConsistentScalingNumber.fromConsistentNumber(2.0);
        assertEquals(2.0, a.times(b).inConsistentUnits(), 0);
        assertEquals(2.0, b.times(a).inConsistentUnits(), 0);
    }

    /**
     * testTimesFactor.
     */
    @Test
    public void testTimesFactor() {
        final ConsistentScalingNumber a = ConsistentScalingNumber.fromConsistentNumber(1.0);
        assertEquals(2.0, a.timesFactor(2.0).inConsistentUnits(), 0);
    }

    /**
     * testTimesFactorInteger.
     */
    @Test
    public void testTimesFactorInteger() {
        final ConsistentScalingNumber a = ConsistentScalingNumber.fromConsistentNumber(1.0);
        assertEquals(2.0, a.timesFactor(2).inConsistentUnits(), 0);
    }

    /**
     * testDividedBy.
     */
    @Test
    public void testDividedBy() {
        final ConsistentScalingNumber a = ConsistentScalingNumber.fromConsistentNumber(1.0);
        final ConsistentScalingNumber b = ConsistentScalingNumber.fromConsistentNumber(2.0);
        assertEquals(0.5, a.dividedBy(b).inConsistentUnits(), 0);
        assertEquals(2.0, b.dividedBy(a).inConsistentUnits(), 0);
    }

    /**
     * testDividedByFactor.
     */
    @Test
    public void testDividedByFactor() {
        final ConsistentScalingNumber a = ConsistentScalingNumber.fromConsistentNumber(1.0);
        assertEquals(0.5, a.dividedByFactor(2.0).inConsistentUnits(), 0);
    }

    /**
     * testDividedByFactorInteger.
     */
    @Test
    public void testDividedByFactorInteger() {
        final ConsistentScalingNumber a = ConsistentScalingNumber.fromConsistentNumber(1.0);
        assertEquals(0.5, a.dividedByFactor(2).inConsistentUnits(), 0);
    }

    /**
     * testCeiling.
     */
    @Test
    public void testCeiling() {
        final ConsistentScalingNumber a = ConsistentScalingNumber.fromConsistentNumber(1.1);
        assertEquals(2.0, a.ceiling().inConsistentUnits(), 0);
    }

    /**
     * testFloor.
     */
    @Test
    public void testFloor() {
        final ConsistentScalingNumber a = ConsistentScalingNumber.fromConsistentNumber(1.1);
        assertEquals(1.0, a.floor().inConsistentUnits(), 0);
    }

    /**
     * testAbs.
     */
    @Test
    public void testAbs() {
        final ConsistentScalingNumber a = ConsistentScalingNumber.fromConsistentNumber(1.0);
        final ConsistentScalingNumber b = ConsistentScalingNumber.fromConsistentNumber(-1.0);
        assertEquals(1.0, a.abs().inConsistentUnits(), 0);
        assertEquals(1.0, b.abs().inConsistentUnits(), 0);
    }

    /**
     * testFromNormalizedQuantity.
     */
    @Test
    public void testFromNormalizedQuantity() {
        final ConsistentScalingNumber a = ConsistentScalingNumber.fromNormalizedNumber(2.0, 2.0);
        assertEquals(4.0, a.inConsistentUnits(), 0);
    }

    /**
     * testEquals.
     */
    @Test
    public void testEquals() {
        final ConsistentScalingNumber a = ConsistentScalingNumber.fromConsistentNumber(1.0);
        final ConsistentScalingNumber b = ConsistentScalingNumber.fromConsistentNumber(1.0);
        final ConsistentScalingNumber c = ConsistentScalingNumber.fromConsistentNumber(1.0
            + ConsistentScalingNumber.COMPARISON_EPSILON * 0.5);
        final ConsistentScalingNumber d = ConsistentScalingNumber.fromConsistentNumber(1.0
            + ConsistentScalingNumber.COMPARISON_EPSILON * 2);
        assertEquals(a, a);
        assertEquals(a, b);
        assertEquals(b, a);
        assertNotEquals(a, c);
        assertNotEquals(c, a);
        assertNotEquals(a, d);
        assertNotEquals(d, a);
    }

    /**
     * testMax.
     */
    @Test
    public void testMax() {
        final ConsistentScalingNumber a = ConsistentScalingNumber.fromConsistentNumber(1.0);
        final ConsistentScalingNumber b = ConsistentScalingNumber.fromConsistentNumber(1.0);
        final ConsistentScalingNumber c = ConsistentScalingNumber.fromConsistentNumber(1.0
            + ConsistentScalingNumber.COMPARISON_EPSILON * 0.5);
        final ConsistentScalingNumber d = ConsistentScalingNumber.fromConsistentNumber(1.0
            + ConsistentScalingNumber.COMPARISON_EPSILON * 2);

        assertEquals(a, ConsistentScalingNumber.max(a, a));
        assertEquals(a, ConsistentScalingNumber.max(a, b));
        assertEquals(c, ConsistentScalingNumber.max(a, c));
        assertEquals(c, ConsistentScalingNumber.max(c, a));
        assertEquals(d, ConsistentScalingNumber.max(a, d));
        assertEquals(d, ConsistentScalingNumber.max(d, a));
    }

    /**
     * testMin.
     */
    @Test
    public void testMin() {
        final ConsistentScalingNumber a = ConsistentScalingNumber.fromConsistentNumber(1.0);
        final ConsistentScalingNumber b = ConsistentScalingNumber.fromConsistentNumber(1.0);
        final ConsistentScalingNumber c = ConsistentScalingNumber.fromConsistentNumber(1.0
            + ConsistentScalingNumber.COMPARISON_EPSILON * 0.5);
        final ConsistentScalingNumber d = ConsistentScalingNumber.fromConsistentNumber(1.0
            + ConsistentScalingNumber.COMPARISON_EPSILON * 2);

        assertEquals(a, ConsistentScalingNumber.min(a, a));
        assertEquals(a, ConsistentScalingNumber.min(a, b));
        assertEquals(a, ConsistentScalingNumber.min(a, c));
        assertEquals(a, ConsistentScalingNumber.min(c, a));
        assertEquals(a, ConsistentScalingNumber.min(a, d));
        assertEquals(a, ConsistentScalingNumber.min(d, a));
    }

    /**
     * testApproxEquals.
     */
    @Test
    public void testApproxEquals() {
        final ConsistentScalingNumber a = ConsistentScalingNumber.fromConsistentNumber(1.0);
        final ConsistentScalingNumber b = ConsistentScalingNumber.fromConsistentNumber(1.0);
        final ConsistentScalingNumber c = ConsistentScalingNumber.fromConsistentNumber(1.0
            + ConsistentScalingNumber.COMPARISON_EPSILON * 0.5);
        final ConsistentScalingNumber d = ConsistentScalingNumber.fromConsistentNumber(1.0
            + ConsistentScalingNumber.COMPARISON_EPSILON * 2);

        assertTrue(a.approxEquals(b));
        assertTrue(b.approxEquals(a));
        assertTrue(a.approxEquals(c));
        assertTrue(c.approxEquals(a));
        assertFalse(a.approxEquals(d));
        assertFalse(d.approxEquals(a));
    }

    /**
     * testIsGreaterThan.
     */
    @Test
    public void testIsGreaterThan() {
        final ConsistentScalingNumber a = ConsistentScalingNumber.fromConsistentNumber(1.0);
        final ConsistentScalingNumber b = ConsistentScalingNumber.fromConsistentNumber(1.0);
        final ConsistentScalingNumber c = ConsistentScalingNumber.fromConsistentNumber(1.0
            + ConsistentScalingNumber.COMPARISON_EPSILON * 0.5);
        final ConsistentScalingNumber d = ConsistentScalingNumber.fromConsistentNumber(1.0
            + ConsistentScalingNumber.COMPARISON_EPSILON * 2);

        assertFalse(a.isGreaterThan(b));
        assertFalse(b.isGreaterThan(a));
        assertFalse(a.isGreaterThan(c));
        assertFalse(c.isGreaterThan(a));
        assertFalse(a.isGreaterThan(d));
        assertTrue(d.isGreaterThan(a));
    }

    /**
     * testIsLessThan.
     */
    @Test
    public void testIsLessThan() {
        final ConsistentScalingNumber a = ConsistentScalingNumber.fromConsistentNumber(1.0);
        final ConsistentScalingNumber b = ConsistentScalingNumber.fromConsistentNumber(1.0);
        final ConsistentScalingNumber c = ConsistentScalingNumber.fromConsistentNumber(1.0
            + ConsistentScalingNumber.COMPARISON_EPSILON * 0.5);
        final ConsistentScalingNumber d = ConsistentScalingNumber.fromConsistentNumber(1.0
            + ConsistentScalingNumber.COMPARISON_EPSILON * 2);

        assertFalse(a.isLessThan(b));
        assertFalse(b.isLessThan(a));
        assertFalse(a.isLessThan(c));
        assertFalse(c.isLessThan(a));
        assertTrue(a.isLessThan(d));
        assertFalse(d.isLessThan(a));
    }

    /**
     * testIsGreaterThanOrApproxEqual.
     */
    @Test
    public void testIsGreaterThanOrApproxEqual() {
        final ConsistentScalingNumber a = ConsistentScalingNumber.fromConsistentNumber(1.0);
        final ConsistentScalingNumber b = ConsistentScalingNumber.fromConsistentNumber(1.0);
        final ConsistentScalingNumber c = ConsistentScalingNumber.fromConsistentNumber(1.0
            + ConsistentScalingNumber.COMPARISON_EPSILON * 0.5);
        final ConsistentScalingNumber d = ConsistentScalingNumber.fromConsistentNumber(1.0
            + ConsistentScalingNumber.COMPARISON_EPSILON * 2);

        assertTrue(a.isGreaterThanOrApproxEqual(b));
        assertTrue(b.isGreaterThanOrApproxEqual(a));
        assertTrue(a.isGreaterThanOrApproxEqual(c));
        assertTrue(c.isGreaterThanOrApproxEqual(a));
        assertFalse(a.isGreaterThanOrApproxEqual(d));
        assertTrue(d.isGreaterThanOrApproxEqual(a));
    }

    /**
     * testIsLessThanOrApproxEqual.
     */
    @Test
    public void testIsLessThanOrApproxEqual() {
        final ConsistentScalingNumber a = ConsistentScalingNumber.fromConsistentNumber(1.0);
        final ConsistentScalingNumber b = ConsistentScalingNumber.fromConsistentNumber(1.0);
        final ConsistentScalingNumber c = ConsistentScalingNumber.fromConsistentNumber(1.0
            + ConsistentScalingNumber.COMPARISON_EPSILON * 0.5);
        final ConsistentScalingNumber d = ConsistentScalingNumber.fromConsistentNumber(1.0
            + ConsistentScalingNumber.COMPARISON_EPSILON * 2);

        assertTrue(a.isLessThanOrApproxEqual(b));
        assertTrue(b.isLessThanOrApproxEqual(a));
        assertTrue(a.isLessThanOrApproxEqual(c));
        assertTrue(c.isLessThanOrApproxEqual(a));
        assertTrue(a.isLessThanOrApproxEqual(d));
        assertFalse(d.isLessThanOrApproxEqual(a));
    }
}