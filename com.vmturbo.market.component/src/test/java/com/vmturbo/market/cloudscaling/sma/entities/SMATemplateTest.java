package com.vmturbo.market.cloudscaling.sma.entities;

import org.junit.Test;

import com.vmturbo.market.cloudscaling.sma.analysis.SMATestConstants;

/**
 * test the SMATemplate constructor.
 */
public class SMATemplateTest {

    private final long templateOid = SMATestConstants.TEMPLATE_BASE + 111L;
    private final String templateName = "templateName";
    private final String family = "family";
    //    final private SMAContext context = new SMAContext();
    // on-demand cost
    private SMACost onDemandCost = new SMACost(0, 0);
    // discounted (or RI) cost
    private SMACost discountedCost = new SMACost(0, 0);
    // number of coupons
    private int coupons = 0;

    /**
     * Test for non null constructor arguments.
     */
    @Test(expected = NullPointerException.class)
    public void testNullNameInTemplate() {
        new SMATemplate(templateOid, null, family, onDemandCost, discountedCost, coupons);
    }
}
