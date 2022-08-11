package com.vmturbo.platform.analysis.economy;

import static org.junit.Assert.*;

import java.util.Arrays;

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
    private static final CommoditySpecification B = new CommoditySpecification(0,1000,4,8);
    private static final CommoditySpecification C1 = new CommoditySpecification(0,1000,5,10);
    private static final CommoditySpecification C2 = new CommoditySpecification(0,1000,5,10);
    private static final CommoditySpecification D = new CommoditySpecification(1,1001,2,5);
    private static final CommoditySpecification E = new CommoditySpecification(1,1001,9,11);
    private static final CommoditySpecification F = new CommoditySpecification(2,1002,2,5);
    private static final CommoditySpecification G = new CommoditySpecification(2,1002,9,11);

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

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestIterator() {
        return new CommoditySpecification[][][]{
            {{}, {}},
            {{A}, {A}},
            {{B}, {B}},
            {{A,A}, {A}},
            {{A,B}, {A,B}},
            {{B,A}, {A,B}},
            {{C1,C2}, {C1}},
            {{C2,C1}, {C2}},
            {{A,B,C1}, {A,B,C1}},
            {{A,B,C1,C2}, {A,B,C1}}
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
            {new CommoditySpecification[]{A,B}, 2},
            {new CommoditySpecification[]{B,A}, 2},
            {new CommoditySpecification[]{C1,C2}, 1},
            {new CommoditySpecification[]{C2,C1}, 1},
            {new CommoditySpecification[]{A,B,C1}, 3},
            {new CommoditySpecification[]{A,B,C1,C2}, 3}
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
            {new CommoditySpecification[]{C1,C2}, false},
            {new CommoditySpecification[]{C2,C1}, false},
            {new CommoditySpecification[]{A,B,C1}, false},
            {new CommoditySpecification[]{A,B,C1,C2}, false}
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
            {new Basket(A,B), 1, B},
            {new Basket(A,B,C1), 0, A},
            {new Basket(A,B,C1), 1, B},
            {new Basket(A,B,C1), 2, C1},
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
            {new Basket(A,B), 2},
            {new Basket(A,B,C1), -1},
            {new Basket(A,B,C1), -100},
            {new Basket(A,B,C1), 3},
            {new Basket(A,B,C1), 100},
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
            {new Basket(A), B, -1},
            {new Basket(B), A, -1},
            {new Basket(B), B, 0},
            {new Basket(A,B), C1, -1},
            {new Basket(A,B), A, 0},
            {new Basket(A,B), B, 1},
            {new Basket(A,C1,B), F, -1},
            {new Basket(A,C1,B), A, 0},
            {new Basket(A,C1,B), B, 1},
            {new Basket(A,C1,B), C1, 2},
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
            {new Basket(A), B, false},
            {new Basket(B), A, false},
            {new Basket(B), B, true},
            {new Basket(A,B), C1, false},
            {new Basket(A,B), A, true},
            {new Basket(A,B), B, true},
            {new Basket(A,C1,B), F, false},
            {new Basket(A,C1,B), A, true},
            {new Basket(A,C1,B), B, true},
            {new Basket(A,C1,B), C1, true},
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
            {new Basket(A,B), C1, new Basket(A,B,C1)},
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
            {new Basket(A), B, new Basket(A)},
            {new Basket(A,B), C1, new Basket(A,B)},
            {new Basket(A,B), A, new Basket(B)},
            {new Basket(A,B), B, new Basket(A)},
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
            {new Basket(), new Basket(), true},
            {new Basket(), new Basket(A), true},
            {new Basket(A), new Basket(), false},
            {new Basket(A), new Basket(A), true},
            {new Basket(B), new Basket(B), true},
            {new Basket(A), new Basket(B), true},
            {new Basket(B), new Basket(A), true},
            {new Basket(E), new Basket(D), false},
            {new Basket(D), new Basket(E), false},
            {new Basket(A), new Basket(D), false},
            {new Basket(D), new Basket(A), false},
            {new Basket(A,B,D), new Basket(A,B,D), true},
            {new Basket(A,B,D), new Basket(A,B,D,E), true},
            {new Basket(A,D), new Basket(A,B,D,E), true},
            {new Basket(A,E), new Basket(A,B,D,E), true},
            {new Basket(B,E), new Basket(A,B,D,E), true},
            {new Basket(D,E), new Basket(A,B,D,E), true},
            {new Basket(A,E), new Basket(B,D,E), true},
            {new Basket(E,A), new Basket(B,D,E), true},
            {new Basket(A,E), new Basket(D,E), false},
            {new Basket(E,A), new Basket(D,E), false},
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
            {new Basket(), new Basket(D), -1},
            {new Basket(D), new Basket(), 1},

            {new Basket(D), new Basket(E), -1},
            {new Basket(D), new Basket(E,F), -1},
            {new Basket(D,F), new Basket(E), -1},
            {new Basket(D,F), new Basket(E,F), -1},
            {new Basket(D,F), new Basket(E,G), -1},
            {new Basket(D,G), new Basket(E,F), -1},
            {new Basket(D,G), new Basket(E,G), -1},

            {new Basket(E), new Basket(D), 1},
            {new Basket(E), new Basket(D,F), 1},
            {new Basket(E,F), new Basket(D), 1},
            {new Basket(E,F), new Basket(D,F), 1},
            {new Basket(E,F), new Basket(D,G), 1},
            {new Basket(E,G), new Basket(D,F), 1},
            {new Basket(E,G), new Basket(D,G), 1},

            // length 1 common prefix
            {new Basket(C1), new Basket(C1), 0},
            {new Basket(C1), new Basket(C1,D), -1},
            {new Basket(C1,D), new Basket(C1), 1},

            {new Basket(C1,D), new Basket(C1,E), -1},
            {new Basket(C1,D), new Basket(C1,E,F), -1},
            {new Basket(C1,D,F), new Basket(C1,E), -1},
            {new Basket(C1,D,F), new Basket(C1,E,F), -1},
            {new Basket(C1,D,F), new Basket(C1,E,G), -1},
            {new Basket(C1,D,G), new Basket(C1,E,F), -1},
            {new Basket(C1,D,G), new Basket(C1,E,G), -1},

            {new Basket(C1,E), new Basket(C1,D), 1},
            {new Basket(C1,E), new Basket(C1,D,F), 1},
            {new Basket(C1,E,F), new Basket(C1,D), 1},
            {new Basket(C1,E,F), new Basket(C1,D,F), 1},
            {new Basket(C1,E,F), new Basket(C1,D,G), 1},
            {new Basket(C1,E,G), new Basket(C1,D,F), 1},
            {new Basket(C1,E,G), new Basket(C1,D,G), 1},

            // length 2 common prefix
            {new Basket(B,C1), new Basket(B,C1), 0},
            {new Basket(B,C1), new Basket(B,C1,D), -1},
            {new Basket(B,C1,D), new Basket(B,C1), 1},

            {new Basket(B,C1,D), new Basket(B,C1,E), -1},
            {new Basket(B,C1,D), new Basket(B,C1,E,F), -1},
            {new Basket(B,C1,D,F), new Basket(B,C1,E), -1},
            {new Basket(B,C1,D,F), new Basket(B,C1,E,F), -1},
            {new Basket(B,C1,D,F), new Basket(B,C1,E,G), -1},
            {new Basket(B,C1,D,G), new Basket(B,C1,E,F), -1},
            {new Basket(B,C1,D,G), new Basket(B,C1,E,G), -1},

            {new Basket(B,C1,E), new Basket(B,C1,D), 1},
            {new Basket(B,C1,E), new Basket(B,C1,D,F), 1},
            {new Basket(B,C1,E,F), new Basket(B,C1,D), 1},
            {new Basket(B,C1,E,F), new Basket(B,C1,D,F), 1},
            {new Basket(B,C1,E,F), new Basket(B,C1,D,G), 1},
            {new Basket(B,C1,E,G), new Basket(B,C1,D,F), 1},
            {new Basket(B,C1,E,G), new Basket(B,C1,D,G), 1},
        };
    }

} // end class BasketTest
