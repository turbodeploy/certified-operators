package com.vmturbo.platform.analysis.economy;

import static org.junit.Assert.*;

import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;

/**
 * A test case for the CommodityBought class.
 */
@RunWith(JUnitParamsRunner.class)
public class CommodityBoughtTest {
    // Fields
    private CommodityBought fixture_;

    // Methods

    @Before
    public void setUp() {
        Basket basketBought = new Basket(new CommoditySpecification(0));
        Basket basketSold = new Basket(new CommoditySpecification(1));
        fixture_ = new CommodityBought(new ShoppingList(
                      new TraderWithSettings(0, 0, TraderState.ACTIVE, basketSold), basketBought), 0);
    }

    @Test
    @Parameters({"1,0",
                 "2,0","2,1",
                 "100,0","100,50","100,99"})
    @TestCaseName("Test #{index}: new CommodityBought(new ShoppingList(?,?,{0}),{1}")
    public final void testCommodityBought_NormalInput(int size, int index) {
        CommoditySpecification[] commodities = IntStream.range(0, size).mapToObj(CommoditySpecification::new)
                                                        .toArray(CommoditySpecification[]::new);
        Basket basket = new Basket(commodities);
        CommodityBought commodity = new CommodityBought(new ShoppingList(
            new TraderWithSettings(0, 0, TraderState.ACTIVE, new Basket()), basket), index);
        // Sanity check: make sure initial values are valid and that no exceptions are thrown.
        commodity.setQuantity(commodity.getQuantity());
        commodity.setPeakQuantity(commodity.getPeakQuantity());
    }

    @Test(expected = IllegalArgumentException.class)
    @Parameters({"1,-1","1,1",
                 "2,-1","2,-100","2,2","2,100",
                 "100,-1","100,-100","100,100","100,1000"})
    @TestCaseName("Test #{index}: new CommodityBought(new ShoppingList(?,?,{0}),{1}")
    public final void testCommodityBought_InvalidInput(int size, int index) {
        CommoditySpecification[] commodities = IntStream.range(0, size).mapToObj(CommoditySpecification::new)
                                                        .toArray(CommoditySpecification[]::new);
        Basket basket = new Basket(commodities);
        new CommodityBought(new ShoppingList(
               new TraderWithSettings(0, 0, TraderState.ACTIVE, new Basket()), basket), index);
    }

    @Test
    @Parameters({"0.0","1.0","1000"})
    @TestCaseName("Test #{index}: (set|get)Quantity({0})")
    public final void testGetSetQuantity_NormalInput(double quantity) {
        fixture_.setQuantity(quantity);
        assertEquals(quantity, fixture_.getQuantity(), TestUtils.FLOATING_POINT_DELTA);
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
        assertEquals(peakQuantity, fixture_.getPeakQuantity(), TestUtils.FLOATING_POINT_DELTA);
    }

    @Test(expected = IllegalArgumentException.class)
    @Parameters({"-0.001","-1.0","-1000"})
    @TestCaseName("Test #{index}: setPeakQuantity({0})")
    public final void testSetPeakQuantity_InvalidInput(double peakQauantity) {
        fixture_.setPeakQuantity(peakQauantity);
    }

} // end CommodityBoughtTest class
