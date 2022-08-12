package com.vmturbo.platform.analysis.economy;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.runner.RunWith;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

/**
 * A test case for the {@link CommoditySpecification} class.
 */
@RunWith(JUnitParamsRunner.class)
public class CommoditySpecificationTest {
    // Fields
    private static final Integer[] validTypes = {0,1,10,100,Integer.MAX_VALUE};
    private static final Integer[] invalidTypes = {(int)((long)Integer.MAX_VALUE+1),-1,-10,-100,Integer.MIN_VALUE};

    // Methods

    @Test
    @Parameters
    @TestCaseName("Test #{index}: CommoditySpecification({0}) and getters")
    public final void testCommoditySpecification_Int_NormalInput(int type) {
        CommoditySpecification cs = new CommoditySpecification(type);
        assertEquals(type, cs.getType());
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestCommoditySpecification_Int_NormalInput() {
        return validTypes;
    }

    @Test(expected = IllegalArgumentException.class)
    @Parameters
    @TestCaseName("Test #{index}: CommoditySpecification({0})")
    public final void testCommoditySpecification_Int_InvalidInput(int type) {
        new CommoditySpecification(type);
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestCommoditySpecification_Int_InvalidInput() {
        return invalidTypes;
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: compareTo, equals and hashCode for ({0},{1})")
    public final void testCompareTo_Equals_and_HashCode(CommoditySpecification left, CommoditySpecification right, int result) {
        assertEquals((int)Math.signum(result), (int)Math.signum(left.compareTo(right)));
        assertEquals(left.compareTo(right) == 0, left.equals(right));
        if(left.compareTo(right) == 0)
            assertEquals(left.hashCode(), right.hashCode());
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestCompareTo_Equals_and_HashCode() {
        Object[] output = new Object[2 * 2];
        int c = 0;

        // create a small test set because performance drops dramatically after a point for some reason.
        for(int i1 = 0 ; i1 < 2 ; ++i1) {
            for(int i2 = 0 ; i2 < 2 ; ++i2) {
                output[c++] = new Object[]{new CommoditySpecification(i1, 1000 + i1),
                                           new CommoditySpecification(i2, 1000 + i2),
                                           i1 - i2};
            }
        }
        return output;
    }

    @Test
    @Parameters({"0, 0, true",
                 "0, 1, false",
                 "1, 0, false"})
    @TestCaseName("Test #{index}: ({0}).isEquals({1}) == {2}")
    public final void testEquals_CommoditySpecification(int type1, int type2, boolean result) {
        assertEquals(result, new CommoditySpecification(type1, 1000+type1)
              .equals(new CommoditySpecification(type2, 1000+type2)));
    }

    @Test
    @Parameters({"null","some string"})
    @TestCaseName("Test #{index}: equals({0})")
    public final void testEquals_Object(Object o) {
        assertFalse(new CommoditySpecification(0).equals(o));
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: CommoditySpecification({0}).toString() == \"{3}\"")
    public final void testToString(int type, String result) {
        assertEquals(result, new CommoditySpecification(type, 1000+type).toString());
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestToString() {
        return new Object[][]{
            {0, "<0>"},
            {0, "<0>"},
            {0, "<0>"},
            {1, "<1>"},
            {1, "<1>"},
            {1, "<1>"},
            {10, "<10>"}
        };
    }

} // end class CommoditySpecificationTest
