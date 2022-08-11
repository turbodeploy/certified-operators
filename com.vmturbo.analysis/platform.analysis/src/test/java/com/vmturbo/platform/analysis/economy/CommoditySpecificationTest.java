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
    private static final Integer[] validBounds = {0,1,10,100,32767,Integer.MAX_VALUE};
    private static final Integer[] invalidBounds = {-1,-10,-100,-32767,Integer.MIN_VALUE};

    // Methods

    @Test
    @Parameters
    @TestCaseName("Test #{index}: CommoditySpecification({0}) and getters")
    public final void testCommoditySpecification_Int_NormalInput(int type) {
        CommoditySpecification cs = new CommoditySpecification(type);
        assertEquals(type, cs.getType());
        assertEquals(Integer.MAX_VALUE, cs.getQualityUpperBound());
        assertEquals(0, cs.getQualityLowerBound());
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
    @TestCaseName("Test #{index}: CommoditySpecification({0},{1},{2}) and getters")
    public final void testCommoditySpecification_Int_Int_Int_NormalInput(int type, int lowerBound, int upperBound) {
        CommoditySpecification cs = new CommoditySpecification(type,1000+type,lowerBound,upperBound);
        assertEquals(type, cs.getType());
        assertEquals(upperBound, cs.getQualityUpperBound());
        assertEquals(lowerBound, cs.getQualityLowerBound());
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestCommoditySpecification_Int_Int_Int_NormalInput() {
        Object[] output = new Object[validTypes.length*validBounds.length*(validBounds.length+1)/2];

        for(int c = 0, i = 0 ; i < validTypes.length ; ++i) {
            for(int j = 0 ; j < validBounds.length ; ++j) {
                for(int k = j ; k < validBounds.length ; ++k) {
                    output[c++] = new Object[]{validTypes[i],validBounds[j],validBounds[k]};
                }
            }
        }

        return output;
    }

    @Test(expected = IllegalArgumentException.class)
    @Parameters
    @TestCaseName("Test #{index}: CommoditySpecification({0},{1},{2})")
    public final void testCommoditySpecification_Int_Int_Int_InvalidInput(int type, int lowerBound, int upperBound) {
        new CommoditySpecification(type,1000+type,lowerBound,upperBound);
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestCommoditySpecification_Int_Int_Int_InvalidInput() {
        Object[] output = new Object[validTypes.length*validBounds.length*(validBounds.length-1)/2
                                     + invalidTypes.length + invalidBounds.length*2];

        int c = 0;

        // invalid combinations of valid bounds.
        for(int i = 0 ; i < validTypes.length ; ++i) {
            for(int j = 0 ; j < validBounds.length ; ++j) {
                for(int k = 0 ; k < j ; ++k) {
                    output[c++] = new Object[]{validTypes[i],validBounds[j],validBounds[k]};
                }
            }
        }

        // just test each invalid parameter individually to reduce number of tests (not very safe)...
        for(int i = 0 ; i < invalidTypes.length ; ++i) {
            output[c++] = new Object[]{invalidTypes[i],1,10};
        }
        for(int i = 0 ; i < invalidBounds.length ; ++i) {
            output[c++] = new Object[]{0,invalidBounds[i],10};
            output[c++] = new Object[]{0,1,invalidBounds[i]};
        }

        return output;
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
        Object[] output = new Object[6 * 6];
        int c = 0;

        // create a small test set because performance drops dramatically after a point for some reason.
        int n1 = 0;
        for(int i1 = 0 ; i1 < 2 ; ++i1) {
            for(int j1 = 0 ; j1 < 2 ; ++j1) {
                for(int k1 = j1 ; k1 < 2 ; ++k1) {
                    int n2 = 0;
                    for(int i2 = 0 ; i2 < 2 ; ++i2) {
                        for(int j2 = 0 ; j2 < 2 ; ++j2) {
                            for(int k2 = j2 ; k2 < 2 ; ++k2) {
                                output[c++] = new Object[]{new CommoditySpecification(i1, 1000+i1, j1, k1),
                                                           new CommoditySpecification(i2, 1000+i2, j2, k2),
                                                           n1 - n2};
                                ++n2;
                            }
                        }
                    }
                    ++n1;
                }
            }
        }
        return output;
    }

    @Test
    @Parameters({"0,0,3, 0,4,6, false",
                 "0,0,4, 0,4,6, true",
                 "0,0,5, 0,4,6, true",
                 "0,4,5, 0,4,6, true",
                 "0,0,8, 0,4,6, true",
                 "0,4,8, 0,4,6, true",
                 "0,6,8, 0,4,6, true",
                 "0,7,8, 0,4,6, false",

                 "1,0,3, 0,4,6, false",
                 "1,0,4, 0,4,6, false",
                 "1,0,5, 0,4,6, false",
                 "1,4,5, 0,4,6, false",
                 "1,0,8, 0,4,6, false",
                 "1,4,8, 0,4,6, false",
                 "1,6,8, 0,4,6, false",
                 "1,7,8, 0,4,6, false",})
    @TestCaseName("Test #{index}: ({0},{1},{2}).isSatisfiedBy(({3},{4},{5})) == {6}")
    public final void testIsSatisfiedBy(int type1, int lowerBound1, int upperBound1,
                                        int type2, int lowerBound2, int upperBound2, boolean result) {
        assertEquals(result, new CommoditySpecification(type1, 1000+type1, lowerBound1, upperBound1)
              .isSatisfiedBy(new CommoditySpecification(type2, 1000+type2, lowerBound2, upperBound2)));
    }

    @Test
    @Parameters({"null","some string"})
    @TestCaseName("Test #{index}: equals({0})")
    public final void testEquals_Object(Object o) {
        assertFalse(new CommoditySpecification(0).equals(o));
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: CommoditySpecification({0},{1},{2}).toString() == \"{3}\"")
    public final void testToString(int type, int lowerBound, int upperBound, String result) {
        assertEquals(result, new CommoditySpecification(type, 1000+type, lowerBound, upperBound).toString());
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestToString() {
        return new Object[][]{
            {0, 0, 0, "<0, 0, 0>"},
            {0, 0, 1, "<0, 0, 1>"},
            {0, 1, 1, "<0, 1, 1>"},
            {1, 0, 0, "<1, 0, 0>"},
            {1, 0, 1, "<1, 0, 1>"},
            {1, 1, 1, "<1, 1, 1>"},
            {10,0,Integer.MAX_VALUE,"<10, 0, MAX_VALUE>"}
        };
    }

} // end class CommoditySpecificationTest
