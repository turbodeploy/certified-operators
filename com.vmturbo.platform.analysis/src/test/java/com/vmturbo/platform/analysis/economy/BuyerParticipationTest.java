package com.vmturbo.platform.analysis.economy;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

/**
 * A test case for the {@link BuyerParticipation} class.
 */
@RunWith(JUnitParamsRunner.class)
public class BuyerParticipationTest {
    // Fields
    private static final Integer[] validBuyerIndices = {0,1,100,Integer.MAX_VALUE};
    private static final Integer[] invalidBuyerIndices = {-1,-10,Integer.MIN_VALUE};
    private static final Integer[] validSupplierIndices = {BuyerParticipation.NO_SUPPLIER,0,1,100,Integer.MAX_VALUE};
    private static final Integer[] invalidSupplierIndices = {-2,-3,Integer.MIN_VALUE};
    private static final Integer[] validSizes = {0,1,100};
    private static final Integer[] invalidSizes = {-1,Integer.MIN_VALUE};
    private static final Double[] validQuantities = {0.0,1.0,100.0};
    private static final Double[] invalidQuantities = {-0.1,-1.0,-100.0};
    private static final Integer[] validIndices = {0,1,9}; // with respect to fixture
    private static final Integer[] invalidIndices = {-1,Integer.MIN_VALUE,10,Integer.MAX_VALUE}; // with respect to fixture


    private BuyerParticipation fixture;

    // Methods
    @Before
    public void setUp() {
        fixture = new BuyerParticipation(0, BuyerParticipation.NO_SUPPLIER, 10);
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: new BuyerParticipation({0},{1},{2})")
    public final void testBuyerParticipation_NormalInput(int buyerIndex, int supplierIndex, int nCommodities) {
        BuyerParticipation participation = new BuyerParticipation(buyerIndex, supplierIndex, nCommodities);
        assertEquals(buyerIndex, participation.getBuyerIndex());
        assertEquals(supplierIndex, participation.getSupplierIndex());
        assertNotSame(participation.getQuantities(), participation.getPeakQuantities());
        assertEquals(nCommodities, participation.getQuantities().length);
        assertEquals(nCommodities, participation.getPeakQuantities().length);
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestBuyerParticipation_NormalInput() {
        Integer[][] output = new Integer[validBuyerIndices.length*validSupplierIndices.length*validSizes.length][];

        int c = 0;
        for(int buyerIndex : validBuyerIndices) {
            for(int supplierIndex : validSupplierIndices) {
                for(int size : validSizes) {
                    output[c++] = new Integer[]{buyerIndex,supplierIndex,size};
                }
            }
        }

        return output;
    }

    @Test(expected = IllegalArgumentException.class)
    @Parameters
    @TestCaseName("Test #{index}: new BuyerParticipation({0},{1},{2})")
    public final void testBuyerParticipation_InvalidIndices(int buyerIndex, int supplierIndex, int nCommodities) {
        new BuyerParticipation(buyerIndex, supplierIndex, nCommodities);
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestBuyerParticipation_InvalidIndices() {
        Integer[][] output = new Integer[invalidBuyerIndices.length + invalidSupplierIndices.length][];

        int c = 0;
        for(int buyerIndex : invalidBuyerIndices) {
            output[c++] = new Integer[]{buyerIndex,0,10}; // should cover more combinations...
        }
        for(int supplierIndex : invalidSupplierIndices) {
            output[c++] = new Integer[]{0,supplierIndex,10}; // should cover more combinations...
        }

        return output;
    }

    @Test(expected = NegativeArraySizeException.class)
    @Parameters
    @TestCaseName("Test #{index}: new BuyerParticipation({0},{1},{2})")
    public final void testBuyerParticipation_InvalidSizes(int buyerIndex, int supplierIndex, int nCommodities) {
        new BuyerParticipation(buyerIndex, supplierIndex, nCommodities);
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestBuyerParticipation_InvalidSizes() {
        Integer[][] output = new Integer[invalidSizes.length][];

        int c = 0;
        for(int size : invalidSizes) {
            output[c++] = new Integer[]{0,0,size}; // should cover more combinations...
        }

        return output;
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: (set|get)BuyerIndex({0})")
    public final void testSetGetBuyerIndex_NormalInput(int buyerIndex) {
        assertSame(fixture, fixture.setBuyerIndex(buyerIndex));
        assertEquals(buyerIndex, fixture.getBuyerIndex());
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestSetGetBuyerIndex_NormalInput() {
        return validBuyerIndices;
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: (set|get)SupplierIndex({0})")
    public final void testSetGetSupplierIndex_NormalInput(int supplierIndex) {
        assertSame(fixture, fixture.setSupplierIndex(supplierIndex));
        assertEquals(supplierIndex, fixture.getSupplierIndex());
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestSetGetSupplierIndex_NormalInput() {
        return validSupplierIndices;
    }

    @Test
    public final void testGetQuantities() {
        double increased = ++fixture.getQuantities()[2]; // ideally this should be a compile time error
        assertEquals(increased-1, fixture.getQuantities()[2], 0.0);
    }

    @Test
    public final void testGetPeakQuantities() {
        double increased = ++fixture.getPeakQuantities()[2]; // ideally this should be a compile time error
        assertEquals(increased-1, fixture.getPeakQuantities()[2], 0.0);
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: (set|get)Quantity({0},{1})")
    public final void testSetGetQuantity_NormalInput(int index, double quantity) {
        assertSame(fixture, fixture.setQuantity(index, quantity));
        assertEquals(quantity, fixture.getQuantity(index), 0.0);
        assertEquals(quantity, fixture.getQuantities()[index], 0.0);
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestSetGetQuantity_NormalInput() {
        Number[][] output = new Number[validQuantities.length*validIndices.length][];

        int c = 0;
        for(Double quantity : validQuantities) {
            for(Integer index : validIndices) {
                output[c++] = new Number[]{index,quantity};
            }
        }

        return output;
    }

    @Test
    @Parameters(method = "parametersForTestSetGetQuantity_NormalInput") // reuse inputs
    @TestCaseName("Test #{index}: (set|get)PeakQuantity({0},{1})")
    public final void testSetGetPeakQuantity_NormalInput(int index, double peakQuantity) {
        assertSame(fixture, fixture.setPeakQuantity(index, peakQuantity));
        assertEquals(peakQuantity, fixture.getPeakQuantity(index), 0.0);
        assertEquals(peakQuantity, fixture.getPeakQuantities()[index], 0.0);
    }

    @Test(expected = IllegalArgumentException.class)
    @Parameters
    @TestCaseName("Test #{index}: setBuyerIndex({0})")
    public final void testSetBuyerIndex_InvalidInput(int buyerIndex) {
        fixture.setBuyerIndex(buyerIndex);
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestSetBuyerIndex_InvalidInput() {
        return invalidBuyerIndices;
    }

    @Test(expected = IllegalArgumentException.class)
    @Parameters
    @TestCaseName("Test #{index}: setSupplierIndex({0})")
    public final void testSetSupplierIndex_InvalidInput(int supplierIndex) {
        fixture.setSupplierIndex(supplierIndex);
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestSetSupplierIndex_InvalidInput() {
        return invalidSupplierIndices;
    }

    @Test(expected = IndexOutOfBoundsException.class)
    @Parameters
    @TestCaseName("Test #{index}: setQuantity({0},{1})")
    public final void testSetQuantity_InvalidIndex(int index, double quantity) {
        fixture.setQuantity(index, quantity);
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestSetQuantity_InvalidIndex() {
        Number[][] output = new Number[validQuantities.length*invalidIndices.length][];

        int c = 0;
        for(Double quantity : validQuantities) {
            for(Integer index : invalidIndices) {
                output[c++] = new Number[]{index,quantity};
            }
        }

        return output;
    }

    @Test(expected = IllegalArgumentException.class)
    @Parameters
    @TestCaseName("Test #{index}: setQuantity({0},{1})")
    public final void testSetQuantity_InvalidQuantity(int index, double quantity) {
        fixture.setQuantity(index, quantity);
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestSetQuantity_InvalidQuantity() {
        Number[][] output = new Number[invalidQuantities.length*validIndices.length][];

        int c = 0;
        for(Double quantity : invalidQuantities) {
            for(Integer index : validIndices) {
                output[c++] = new Number[]{index,quantity};
            }
        }

        return output;
    }

    @Test(expected = IndexOutOfBoundsException.class)
    @Parameters(method = "parametersForTestSetQuantity_InvalidIndex") // reuse inputs
    @TestCaseName("Test #{index}: setPeakQuantity({0},{1})")
    public final void testSetPeakQuantity_InvalidIndex(int index, double peakQuantity) {
        fixture.setPeakQuantity(index, peakQuantity);
    }

    @Test(expected = IllegalArgumentException.class)
    @Parameters(method = "parametersForTestSetQuantity_InvalidQuantity") // reuse inputs
    @TestCaseName("Test #{index}: setPeakQuantity({0},{1})")
    public final void testSetPeakQuantity_InvalidQuantity(int index, double peakQuantity) {
        fixture.setPeakQuantity(index, peakQuantity);
    }


} // end BuyerParticipationTest class
