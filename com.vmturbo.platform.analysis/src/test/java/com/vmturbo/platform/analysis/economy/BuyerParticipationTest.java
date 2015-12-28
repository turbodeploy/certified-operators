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
    private static final Trader trader1 = new TraderWithSettings(0, 0, TraderState.ACTIVE, new Basket());
    private static final Trader trader2 = new TraderWithSettings(0, 0, TraderState.INACTIVE, new Basket());
    private static final Trader trader3 = new TraderWithSettings(0, 0, TraderState.ACTIVE, new Basket(new CommoditySpecification(0, 0, 0)));
    private static final Trader trader4 = new TraderWithSettings(0, 0, TraderState.INACTIVE, new Basket(new CommoditySpecification(0, 0, 0)));

    private static final Trader[] validBuyers = {trader1, trader2, trader3, trader4};
    private static final Trader[] validSuppliers = {null,trader1, trader2, trader3, trader4};
    private static final Integer[] validSizes = {0,1,100};
    private static final Integer[] invalidSizes = {-1,Integer.MIN_VALUE};
    private static final Double[] validQuantities = {0.0,1.0,100.0};
    private static final Double[] invalidQuantities = {-0.1,-1.0,-100.0};
    private static final Integer[] validIndices = {0,1,9}; // with respect to fixture
    private static final Integer[] invalidIndices = {-1,Integer.MIN_VALUE,10,Integer.MAX_VALUE}; // with respect to fixture


    private BuyerParticipation fixture_;

    // Methods
    @Before
    public void setUp() {
        fixture_ = new BuyerParticipation(trader1, null, 10);
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: new BuyerParticipation({0},{1},{2})")
    public final void testBuyerParticipation_NormalInput(Trader buyer, Trader supplier, int nCommodities) {
        BuyerParticipation participation = new BuyerParticipation(buyer, supplier, nCommodities);
        assertSame(buyer, participation.getBuyer());
        assertSame(supplier, participation.getSupplier());
        assertNotSame(participation.getQuantities(), participation.getPeakQuantities());
        assertEquals(nCommodities, participation.getQuantities().length);
        assertEquals(nCommodities, participation.getPeakQuantities().length);
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestBuyerParticipation_NormalInput() {
        Object[][] output = new Object[validBuyers.length*validSuppliers.length*validSizes.length][];

        int c = 0;
        for(Trader buyer : validBuyers) {
            for(Trader supplier : validSuppliers) {
                for(int size : validSizes) {
                    output[c++] = new Object[]{buyer,supplier,size};
                }
            }
        }

        return output;
    }

    @Test(expected = NegativeArraySizeException.class)
    @Parameters
    @TestCaseName("Test #{index}: new BuyerParticipation({0},{1},{2})")
    public final void testBuyerParticipation_InvalidSizes(Trader buyer, Trader supplier, int nCommodities) {
        new BuyerParticipation(buyer, supplier, nCommodities);
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestBuyerParticipation_InvalidSizes() {
        Object[][] output = new Object[validBuyers.length*validSuppliers.length*invalidSizes.length][];

        int c = 0;
        for(Trader buyer : validBuyers) {
            for(Trader supplier : validSuppliers) {
                for(int size : invalidSizes) {
                    output[c++] = new Object[]{buyer,supplier,size};
                }
            }
        }

        return output;
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: (set|get)Supplier({0})")
    public final void testSetGetSupplier_NormalInput(Trader supplier) {
        assertSame(fixture_, fixture_.setSupplier(supplier));
        assertSame(supplier, fixture_.getSupplier());
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestSetGetSupplier_NormalInput() {
        return validSuppliers;
    }

    @Test
    public final void testGetQuantities() {
        double increased = ++fixture_.getQuantities()[2];
        assertEquals(increased, fixture_.getQuantities()[2], 0.0);
    }

    @Test
    public final void testGetPeakQuantities() {
        double increased = ++fixture_.getPeakQuantities()[2];
        assertEquals(increased, fixture_.getPeakQuantities()[2], 0.0);
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: (set|get)Quantity({0},{1})")
    public final void testSetGetQuantity_NormalInput(int index, double quantity) {
        assertSame(fixture_, fixture_.setQuantity(index, quantity));
        assertEquals(quantity, fixture_.getQuantity(index), 0.0);
        assertEquals(quantity, fixture_.getQuantities()[index], 0.0);
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
        assertSame(fixture_, fixture_.setPeakQuantity(index, peakQuantity));
        assertEquals(peakQuantity, fixture_.getPeakQuantity(index), 0.0);
        assertEquals(peakQuantity, fixture_.getPeakQuantities()[index], 0.0);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    @Parameters
    @TestCaseName("Test #{index}: setQuantity({0},{1})")
    public final void testSetQuantity_InvalidIndex(int index, double quantity) {
        fixture_.setQuantity(index, quantity);
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
        fixture_.setQuantity(index, quantity);
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
        fixture_.setPeakQuantity(index, peakQuantity);
    }

    @Test(expected = IllegalArgumentException.class)
    @Parameters(method = "parametersForTestSetQuantity_InvalidQuantity") // reuse inputs
    @TestCaseName("Test #{index}: setPeakQuantity({0},{1})")
    public final void testSetPeakQuantity_InvalidQuantity(int index, double peakQuantity) {
        fixture_.setPeakQuantity(index, peakQuantity);
    }

} // end BuyerParticipationTest class
