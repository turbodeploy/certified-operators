package com.vmturbo.platform.analysis.economy;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

/**
 * A test case for the CommodityBought class.
 */
@RunWith(JUnitParamsRunner.class)
public class CommodityBoughtTest {
    // Fields
    private CommodityBought fixture_;

    // Methods

    @Before
    public void setUp() throws Exception {
        fixture_ = new CommodityBought(new BuyerParticipation(0, BuyerParticipation.NO_SUPPLIER, 1), 0);
    }

    @Test
    @Parameters({"1,0",
                 "2,0","2,1",
                 "100,0","100,50","100,99"})
    @TestCaseName("Test #{index}: new CommodityBought(new BuyerParticipation(?,?,{0}),{1}")
    public final void testCommodityBought_NormalInput(int size, int index) {
        CommodityBought commodity = new CommodityBought(new BuyerParticipation(0, BuyerParticipation.NO_SUPPLIER, size), index);
        // Sanity check: make sure initial values are valid and that no exceptions are thrown.
        commodity.setQuantity(commodity.getQuantity());
        commodity.setPeakQuantity(commodity.getPeakQuantity());
    }

    @Test(expected = IllegalArgumentException.class)
    @Parameters({"1,-1","1,1",
                 "2,-1","2,-100","2,2","2,100",
                 "100,-1","100,-100","100,100","100,1000"})
    @TestCaseName("Test #{index}: new CommodityBought(new BuyerParticipation(?,?,{0}),{1}")
    public final void testCommodityBought_InvalidInput(int size, int index) {
        new CommodityBought(new BuyerParticipation(0, BuyerParticipation.NO_SUPPLIER, size), index);
    }

    @Test
    @Parameters({"0.0","1.0","1000"})
    @TestCaseName("Test #{index}: (set|get)Quantity({0})")
    public final void testGetSetQuantity_NormalInput(double quantity) {
        fixture_.setQuantity(quantity);
        assertEquals(quantity, fixture_.getQuantity(), 0.0);
    }

    @Test(expected = IllegalArgumentException.class)
    @Parameters({"-0.001","-1.0","-1000"})
    @TestCaseName("Test #{index}: setQuantity({0})")
    public final void testSetQuantity_InvalidInput(double qauantity) {
        fixture_.setQuantity(qauantity);
    }

    @Test
    @Parameters({"0.0","1.0","1000"})
    @TestCaseName("Test #{index}: (set|get)PeakQuantity({0})")
    public final void testGetSetPeakQuantity_NormalInput(double peakQuantity) {
        fixture_.setPeakQuantity(peakQuantity);
        assertEquals(peakQuantity, fixture_.getPeakQuantity(), 0.0);
    }

    @Test(expected = IllegalArgumentException.class)
    @Parameters({"-0.001","-1.0","-1000"})
    @TestCaseName("Test #{index}: setPeakQuantity({0})")
    public final void testSetPeakQuantity_InvalidInput(double peakQauantity) {
        fixture_.setPeakQuantity(peakQauantity);
    }

} // end CommodityBoughtTest class
