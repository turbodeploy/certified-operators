package com.vmturbo.trax;

import static com.vmturbo.trax.Trax.trax;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Tests for {@link TraxNumber}.
 */
public class TraxNumberTest {

    /**
     * Delta for double equality tests.
     */
    public static final double EQUALITY_DELTA = 1e-8;

    /**
     * testPlusNumber.
     */
    @Test
    public void testPlusNumber() {
        assertEquals(3.0, trax(1.0).plus(2.0).getValue(), EQUALITY_DELTA);
    }

    /**
     * testPlusNumberName.
     */
    @Test
    public void testPlusNumberName() {
        assertEquals(3.0, trax(1.0).plus(2.0, "foo").getValue(), EQUALITY_DELTA);
    }

    /**
     * testPlusNumberNameDetails.
     */
    @Test
    public void testPlusNumberNameDetails() {
        assertEquals(3.0, trax(1.0).plus(2.0, "foo", "details").getValue(), EQUALITY_DELTA);
    }

    /**
     * testPlusTrax.
     */
    @Test
    public void testPlusTrax() {
        assertEquals(3.0, trax(1.0).plus(trax(2.0)).getValue(), EQUALITY_DELTA);
    }

    /**
     * testMinusNumber.
     */
    @Test
    public void testMinusNumber() {
        assertEquals(-1.0, trax(1.0).minus(2.0).getValue(), EQUALITY_DELTA);
    }

    /**
     * testMinusNumberName.
     */
    @Test
    public void testMinusNumberName() {
        assertEquals(-1.0, trax(1.0).minus(2.0, "foo").getValue(), EQUALITY_DELTA);
    }

    /**
     * testMinusNumberNameDetails.
     */
    @Test
    public void testMinusNumberNameDetails() {
        assertEquals(-1.0, trax(1.0).minus(2.0, "foo", "details").getValue(), EQUALITY_DELTA);
    }

    /**
     * testMinusTrax.
     */
    @Test
    public void testMinusTrax() {
        assertEquals(-1.0, trax(1.0).minus(trax(2.0)).getValue(), EQUALITY_DELTA);
    }

    /**
     * testSubtractFromNumber.
     */
    @Test
    public void testSubtractFromNumber() {
        assertEquals(1.0, trax(1.0).subtractFrom(2.0).getValue(), EQUALITY_DELTA);
    }

    /**
     * testSubtractFromNumberName.
     */
    @Test
    public void testSubtractFromNumberName() {
        assertEquals(1.0, trax(1.0).subtractFrom(2.0, "foo").getValue(), EQUALITY_DELTA);
    }

    /**
     * testSubtractFromNumberNameDetails.
     */
    @Test
    public void testSubtractFromNumberNameDetails() {
        assertEquals(1.0, trax(1.0).subtractFrom(2.0, "foo", "details").getValue(), EQUALITY_DELTA);
    }

    /**
     * testSubtractFromTrax.
     */
    @Test
    public void testSubtractFromTrax() {
        assertEquals(1.0, trax(1.0).subtractFrom(trax(2.0)).getValue(), EQUALITY_DELTA);
    }

    /**
     * testTimesNumber.
     */
    @Test
    public void testTimesNumber() {
        assertEquals(2.0, trax(1.0).times(2.0).getValue(), EQUALITY_DELTA);
    }

    /**
     * testTimesNumberName.
     */
    @Test
    public void testTimesNumberName() {
        assertEquals(2.0, trax(1.0).times(2.0, "foo").getValue(), EQUALITY_DELTA);
    }

    /**
     * testTimesNumberNameDetails.
     */
    @Test
    public void testTimesNumberNameDetails() {
        assertEquals(2.0, trax(1.0).times(2.0, "foo", "details").getValue(), EQUALITY_DELTA);
    }

    /**
     * testTimesTrax.
     */
    @Test
    public void testTimesTrax() {
        assertEquals(2.0, trax(1.0).times(trax(2.0)).getValue(), EQUALITY_DELTA);
    }

    /**
     * testDividedByNumber.
     */
    @Test
    public void testDividedByNumber() {
        assertEquals(0.5, trax(1.0).dividedBy(2.0).getValue(), EQUALITY_DELTA);
    }

    /**
     * testDividedByNumberName.
     */
    @Test
    public void testDividedByNumberName() {
        assertEquals(0.5, trax(1.0).dividedBy(2.0, "foo").getValue(), EQUALITY_DELTA);
    }

    /**
     * testDividedByNumberNameDetails.
     */
    @Test
    public void testDividedByNumberNameDetails() {
        assertEquals(0.5, trax(1.0).dividedBy(2.0, "foo", "details").getValue(), EQUALITY_DELTA);
    }

    /**
     * testDividedByTrax.
     */
    @Test
    public void testDividedByTrax() {
        assertEquals(0.5, trax(1.0).dividedBy(trax(2.0)).getValue(), EQUALITY_DELTA);
    }

    /**
     * testMinOne.
     */
    @Test
    public void testMinOne() {
        assertEquals(0.5, Trax.min(trax(0.5)).compute().getValue(), EQUALITY_DELTA);
    }

    /**
     * testMinLeft.
     */
    @Test
    public void testMinLeft() {
        assertEquals(0.5, Trax.min(trax(1.0), trax(0.5)).compute().getValue(), EQUALITY_DELTA);
    }

    /**
     * testMinRight.
     */
    @Test
    public void testMinRight() {
        assertEquals(-1.0, Trax.min(trax(-1.0), trax(0.5)).compute().getValue(), EQUALITY_DELTA);
    }

    /**
     * testMinThree.
     */
    @Test
    public void testMinThree() {
        assertEquals(-2.0, Trax.min(trax(-1.0), trax(0.5), trax(-2.0)).compute().getValue(), EQUALITY_DELTA);
    }

    /**
     * testMaxOne.
     */
    @Test
    public void testMaxOne() {
        assertEquals(1.0, Trax.max(trax(1.0)).compute().getValue(), EQUALITY_DELTA);
    }

    /**
     * testMaxRight.
     */
    @Test
    public void testMaxRight() {
        assertEquals(1.0, Trax.max(trax(1.0), trax(0.5)).compute().getValue(), EQUALITY_DELTA);
    }

    /**
     * testMaxLeft.
     */
    @Test
    public void testMaxLeft() {
        assertEquals(0.5, Trax.max(trax(-1.0), trax(0.5)).compute().getValue(), EQUALITY_DELTA);
    }

    /**
     * testMaxThree.
     */
    @Test
    public void testMaxThree() {
        assertEquals(2.0, Trax.max(trax(-1.0), trax(0.5), trax(2.0)).compute().getValue(), EQUALITY_DELTA);
    }

    /**
     * testTraxNumberValueEquals.
     */
    @Test
    public void testTraxNumberValueEquals() {
        assertTrue(trax(-1.0).valueEquals(trax(-1.0, "foo")));
        assertFalse(trax(-1.0).valueEquals(trax(-1.1, "foo")));
    }

    /**
     * testDoubleValueEquals.
     */
    @Test
    public void testDoubleValueEquals() {
        assertTrue(trax(-1.0).valueEquals(-1.0));
        assertFalse(trax(-1.0).valueEquals(1.0));
    }

    /**
     * testCompareTo.
     */
    @Test
    public void testCompareTo() {
        final TraxNumber a = trax(1.0);
        final TraxNumber b = trax(2.0);

        assertEquals(-1, a.compareTo(b));
        assertEquals(1, b.compareTo(a));
        assertEquals(0, b.compareTo(b));
        assertEquals(0, a.compareTo(a));
    }

    /**
     * testNameValueEquivalence.
     */
    @Test
    public void testNameValueEquivalence() {
        final TraxNumber a = trax(1.0);
        assertTrue(a.valueEquals(a.named("b")));
    }
}
