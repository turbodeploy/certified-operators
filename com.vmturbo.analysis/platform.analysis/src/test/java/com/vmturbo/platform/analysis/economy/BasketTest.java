package com.vmturbo.platform.analysis.economy;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Iterator;

import org.junit.Test;
import org.junit.runner.RunWith;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

/**
 * A test case for the {@link Basket} class.
 */
@RunWith(JUnitParamsRunner.class)
public class BasketTest {
    // Fields
    private static final CommoditySpecification A = new CommoditySpecification(0);
    private static final CommoditySpecification B = new CommoditySpecification(0,1000);
    private static final CommoditySpecification C = new CommoditySpecification(1,1001);
    private static final CommoditySpecification D = new CommoditySpecification(2,1002);

    // Methods

    @Test
    @Parameters
    @TestCaseName("Test #{index}: Basket({0}).iterator() == {1}") // Java doesn't know how to print arrays. may need a workaround...
    public final void testIterator(CommoditySpecification[] input, CommoditySpecification[] output) {
        final Basket basket = new Basket(input);
        assertEquals(output.length, basket.size());

        int i = 0;
        for (CommoditySpecification specification : basket) {
            assertEquals(output[i++], specification);
        }
        assertEquals(basket.size(), i);
    }

    // CommSpecs will be sorted when creating a new basket.
    // CommSpecs are sorted by type, then by qualityLowerBound, then by qualityUpperBound.
    // So this test passes (even if output is shown as different order for test case {{B,A}, {A,B}}).
    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestIterator() {
        return new CommoditySpecification[][][]{
            {{}, {}},
            {{A}, {A}},
            {{B}, {B}},
            {{A,A}, {A}},
            // since it is only type that we care about, A, B is just treated as A
            {{A,B}, {A}},
            {{B,A}, {B}},
            {{B,B}, {B}},
            {{A,B,B}, {A}},
            {{A,B,C}, {A, C}}
        };
    }

    @Test
    @Parameters(method = "parametersForTestIterator") // reuse test inputs
    @TestCaseName("Test #{index}: Basket(Arrays.asList({0})).compareTo(Basket({1})) == 0") // Java doesn't know how to print arrays. may need a workaround...
    public final void testConstructors(CommoditySpecification[] input, CommoditySpecification[] output) {
        assertEquals(0,new Basket(Arrays.asList(input)).compareTo(new Basket(input)));
        assertEquals(0,new Basket(Arrays.asList(input)).compareTo(new Basket(output)));
    }

    @Test
    @Parameters()
    @TestCaseName("Test #{index}: Basket({0}).reverseIterator() == {1}")
    public final void testReverseIterator(CommoditySpecification[] input, CommoditySpecification[] output) {
        final Basket basket = new Basket(input);
        assertEquals(output.length, basket.size());

        int i = 0;
        Iterator<CommoditySpecification> reverseIterator = basket.reverseIterator();
        while (reverseIterator.hasNext()) {
            CommoditySpecification specification = reverseIterator.next();
            assertEquals(output[i++], specification);
        }
        assertEquals(basket.size(), i);
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestReverseIterator() {
        return new CommoditySpecification[][][]{
            {{}, {}},
            {{A}, {A}},
            {{B}, {B}},
            {{A,A}, {A}},
            {{A,B}, {A}},
            {{B,A}, {B}},
            {{B,B}, {B}},
            {{A,B,B}, {B}},
            // doesnt matter if we replace B by A
            {{A,B,B,C}, {C,A}},
            {{A,B,B,C,D}, {D,C,A}},
        };
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: Basket({0}).size() == {1}") // Java doesn't know how to print arrays. may need a workaround...
    public final void testSize(CommoditySpecification[] input, int output) {
        assertEquals(output, new Basket(input).size());
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestSize() {
        return new Object[][]{
            {new CommoditySpecification[]{}, 0},
            {new CommoditySpecification[]{A}, 1},
            {new CommoditySpecification[]{B}, 1},
            {new CommoditySpecification[]{A,A}, 1},
            {new CommoditySpecification[]{A,B}, 1},
            {new CommoditySpecification[]{B,A}, 1},
            {new CommoditySpecification[]{B,B}, 1},
            {new CommoditySpecification[]{A,B,B}, 1},
            {new CommoditySpecification[]{A,B,B,B}, 1},
            {new CommoditySpecification[]{A,B,B,C}, 2}
        };
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: Basket({0}).size() == {1}") // Java doesn't know how to print arrays. may need a workaround...
    public final void testIsEmpty(CommoditySpecification[] input, boolean output) {
        assertEquals(output, new Basket(input).isEmpty());
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestIsEmpty() {
        return new Object[][]{
            {new CommoditySpecification[]{}, true},
            {new CommoditySpecification[]{A}, false},
            {new CommoditySpecification[]{B}, false},
            {new CommoditySpecification[]{A,A}, false},
            {new CommoditySpecification[]{A,B}, false},
            {new CommoditySpecification[]{B,A}, false},
            {new CommoditySpecification[]{B,B}, false},
            {new CommoditySpecification[]{A,B,B}, false},
            {new CommoditySpecification[]{A,B,B,B}, false}
        };
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: {0}.get({1}) == {2}")
    public final void testGet_NormalInput(Basket basket, int index, CommoditySpecification output) {
        assertEquals(output, basket.get(index));
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestGet_NormalInput() {
        return new Object[][]{
            {new Basket(A), 0, A},
            {new Basket(B), 0, B},
            {new Basket(A,B), 0, A},
            {new Basket(A,B,C), 0, A},
            {new Basket(A,B,C), 1, C}
        };
    }

    @Test(expected = IndexOutOfBoundsException.class)
    @Parameters
    @TestCaseName("Test #{index}: {0}.get({1})")
    public final void testGet_InvalidInput(Basket basket, int index) {
        basket.get(index);
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestGet_InvalidInput() {
        return new Object[][]{
            {new Basket(), -1},
            {new Basket(), 0},
            {new Basket(), 1},
            {new Basket(A), -1},
            {new Basket(B), 1},
            {new Basket(A,B), -1},
            {new Basket(A,B), 1},
            {new Basket(A,B), 2},
            {new Basket(A,B,B), -1},
            {new Basket(A,B,B), -100},
            {new Basket(A,B,B), 1},
            {new Basket(A,B,B), 3},
            {new Basket(A,B,B), 100}
        };
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: {0}.indexOf({1}) == {2}")
    public final void testIndexOf(Basket basket, CommoditySpecification specification, int output) {
        assertEquals(output, basket.indexOf(specification));
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestIndexOf() {
        return new Object[][]{
            {new Basket(), A, -1},
            {new Basket(), B, -1},
            {new Basket(A), A, 0},
            {new Basket(A), B, 0},
            {new Basket(B), A, 0},
            {new Basket(B), B, 0},
            {new Basket(A,B), B, 0},
            {new Basket(A,B), A, 0},
            {new Basket(A,B,B), D, -1},
            {new Basket(A,B,B), A, 0},
            {new Basket(A,B,B), B, 0},
            {new Basket(A,B,C), C, 1},
        };
    }

    @Test
    @Parameters(method = "parametersForTestIndexOf")
    @TestCaseName("Test #{index}: {0}.lastIndexOf({1}) == {2}")
    public final void testLastIndexOf(Basket basket, CommoditySpecification specification, int output) {
        assertEquals(output, basket.lastIndexOf(specification));
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: {0}.contains({1}) == {2}")
    public final void testContains(Basket basket, CommoditySpecification specification, boolean output) {
        assertEquals(output, basket.contains(specification));
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestContains() {
        return new Object[][]{
            {new Basket(), A, false},
            {new Basket(), B, false},
            {new Basket(A), A, true},
            // the next 2 conditions are true. Even though B has a different baseType, A and B match because of same type
            {new Basket(A), B, true},
            {new Basket(B), A, true},
            {new Basket(B), B, true},
            {new Basket(A,B), B, true},
            {new Basket(A,B), A, true},
            {new Basket(A,B,B), D, false},
            {new Basket(A,B,B), A, true},
            {new Basket(A,B,B), B, true}
        };
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: {0}.add({1}) == {2}")
    public final void testAdd(Basket basket, CommoditySpecification specification, Basket output) {
        assertEquals(0, basket.add(specification).compareTo(output));
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestAdd() {
        return new Object[][]{
            {new Basket(), A, new Basket(A)},
            {new Basket(), B, new Basket(B)},
            {new Basket(A), A, new Basket(A)},
            {new Basket(A), B, new Basket(A,B)},
            {new Basket(A,B), B, new Basket(A,B,B)},
            {new Basket(A,B), A, new Basket(A,B)},
            {new Basket(A,B), B, new Basket(A,B)},
        };
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: {0}.remove({1}) == {2}")
    public final void testRemove(Basket basket, CommoditySpecification specification, Basket output) {
        assertEquals(0, basket.remove(specification).compareTo(output));
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestRemove() {
        return new Object[][]{
            {new Basket(), A, new Basket()},
            {new Basket(), B, new Basket()},
            {new Basket(A), A, new Basket()},
            {new Basket(A), B, new Basket()},
            {new Basket(A,B), B, new Basket()},
            {new Basket(A,B), A, new Basket()},
            {new Basket(A,B), C, new Basket(A)},
            {new Basket(A,B,C), C, new Basket(A)}
        };
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: {0}.isSatisfiedBy({1}) == {2}")
    public final void testIsSatisfiedBy(Basket bought, Basket sold, boolean output) {
        assertEquals(output, bought.isSatisfiedBy(sold));
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestIsSatisfiedBy() {
        return new Object[][]{
            {new Basket(A), new Basket(A), true},
            {new Basket(B), new Basket(B), true},
            {new Basket(A), new Basket(B), true},
            {new Basket(B), new Basket(A), true},
            {new Basket(A,B,C), new Basket(A,B,C), true},
            {new Basket(A,B,C), new Basket(A,B,C,C), true},
            {new Basket(A,C), new Basket(A,B,C,C), true},
            {new Basket(B,C), new Basket(A,B,C,C), true},
            {new Basket(C,C), new Basket(A,B,C,C), true},
            {new Basket(A,C), new Basket(B,C,C), true},
            {new Basket(C,A), new Basket(B,C,C), true},
            {new Basket(A,C), new Basket(C,C), false},
            {new Basket(C,A), new Basket(C,C), false},
        };
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: {0}.compareTo({1}) == {2}")
    public final void testCompareTo(Basket left, Basket right, int output) {
        assertEquals(output, (int)Math.signum(left.compareTo(right)));
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestCompareTo() {
        return new Object[][]{
            // length 0 common prefix
            {new Basket(), new Basket(), 0},
            {new Basket(), new Basket(C), -1},
            {new Basket(C), new Basket(), 1},
            {new Basket(C), new Basket(C), 0},
            {new Basket(C), new Basket(C,D), -1},
            {new Basket(C,D), new Basket(C), 1},
            {new Basket(C,D), new Basket(C,D), 0},
            // length 1 common prefix
            {new Basket(B), new Basket(B), 0},
            {new Basket(B), new Basket(B,C), -1},
            {new Basket(B,C), new Basket(B), 1},
            {new Basket(B,C), new Basket(B,C), 0},
            {new Basket(B,C), new Basket(B,C,D), -1},
            {new Basket(B,C,D), new Basket(B,C), 1},
            {new Basket(B,C,D), new Basket(B,C,D), 0},

            // length 2 common prefix
            {new Basket(B,B), new Basket(B,B), 0},
            {new Basket(B,B), new Basket(B,B,C), -1},
            {new Basket(B,B,C), new Basket(B,B), 1},

            {new Basket(B,B,C), new Basket(B,B,C,D), -1},
            {new Basket(B,B,C,D), new Basket(B,B,C), 1},
            {new Basket(B,B,C,D), new Basket(B,B,C,D), 0},

            {new Basket(B,B,C), new Basket(B,B,C), 0},
            {new Basket(B,B,C), new Basket(B,B,C,D), -1},
            {new Basket(B,B,C,D), new Basket(B,B,C), 1},
            {new Basket(B,B,C,D), new Basket(B,B,C,D), 0}
        };
    }

    @Test
    public void testEqualsNullDoesNotThrowException() {
        Basket b = new Basket();
        assertFalse(b.equals(null));
    }

    @Test
    public void testEqualsDifferentSizeBaskets() {
        Basket b1 = new Basket();
        Basket b2 = new Basket(A);

        assertNotEquals(b1, b2);
        assertNotEquals(b2, b1);
    }

    @Test
    public void testEqualBasketsAreEqual() {
        Basket b1 = new Basket(A);
        Basket b2 = new Basket(A);

        assertEquals(b1, b2);
        assertEquals(b2, b1);
    }

    @Test
    public void testEqualTypeAndUnequalBaseTypeAreEqual() {
        Basket b1 = new Basket(A);
        Basket b2 = new Basket(B);

        assertEquals(b1, b2);
        assertEquals(b2, b1);
    }

    @Test
    public void testUnequalBasketCommoditiesAreUnequal() {
        Basket b1 = new Basket(A);
        Basket b2 = new Basket(C);

        assertNotEquals(b1, b2);
        assertNotEquals(b2, b1);
    }

    @Test
    public void testUnequalTypeAreUnequal() {
        Basket b = new Basket(A);

        assertNotEquals(b, A);
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: ({0}.hashCode() == {1}.hashCode()) == {2}")
    public void testHashCode(Basket left, Basket right, boolean output) {
        assertEquals(output, left.equals(right));
        assertEquals(output, right.equals(left));
        assertEquals(output, left.hashCode() == right.hashCode());
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestHashCode() {
        return new Object[][] {
            {new Basket(A), new Basket(A), true},
            {new Basket(A), new Basket(B), true},
            {new Basket(A, B), new Basket(B), true},
            {new Basket(A), new Basket(D), false}
        };
    }

} // end class BasketTest
