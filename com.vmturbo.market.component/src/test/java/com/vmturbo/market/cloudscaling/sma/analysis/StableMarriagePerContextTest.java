package com.vmturbo.market.cloudscaling.sma.analysis;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.vmturbo.market.cloudscaling.sma.entities.SMACost;
import com.vmturbo.market.cloudscaling.sma.entities.SMATemplate;

/**
 * Class to test the methods in StableMarriagePerContext.
 */
public class StableMarriagePerContextTest {
    /**
     * test computeFamilyToTemplateMap.
     */
    @Test
    public void testComputeFamilyToTemplateMap() {
        SMACost cost = new SMACost(2.0f, 1.0f);
        long templateOid = SMATestConstants.TEMPLATE_BASE + 1;
        SMATemplate f1t1 = new SMATemplate(templateOid, "f1." + templateOid, "f1", 2, SMAUtils.BOGUS_CONTEXT, null);
        f1t1.setOnDemandCost(SMATestConstants.BUSINESS_ACCOUNT_BASE, cost);
        f1t1.setDiscountedCost(SMATestConstants.BUSINESS_ACCOUNT_BASE, cost);
        templateOid++;
        SMATemplate f1t2 = new SMATemplate(templateOid, "f1." + templateOid, "f1",4, SMAUtils.BOGUS_CONTEXT, null);
        f1t2.setOnDemandCost(SMATestConstants.BUSINESS_ACCOUNT_BASE, cost);
        f1t2.setDiscountedCost(SMATestConstants.BUSINESS_ACCOUNT_BASE, cost);
        templateOid++;
        SMATemplate f1t3 = new SMATemplate(templateOid, "f1." + templateOid, "f1",8, SMAUtils.BOGUS_CONTEXT, null);
        f1t3.setOnDemandCost(SMATestConstants.BUSINESS_ACCOUNT_BASE, cost);
        f1t3.setDiscountedCost(SMATestConstants.BUSINESS_ACCOUNT_BASE, cost);
        templateOid = SMATestConstants.TEMPLATE_BASE + 11;
        SMATemplate f2t1 = new SMATemplate(templateOid, "f1." + templateOid, "f2",2, SMAUtils.BOGUS_CONTEXT, null);
        f2t1.setOnDemandCost(SMATestConstants.BUSINESS_ACCOUNT_BASE, cost);
        f2t1.setDiscountedCost(SMATestConstants.BUSINESS_ACCOUNT_BASE, cost);
        templateOid = SMATestConstants.TEMPLATE_BASE + 14;
        SMATemplate f2t4 = new SMATemplate(templateOid, "f1." + templateOid, "f2",16, SMAUtils.BOGUS_CONTEXT, null);
        f2t4.setOnDemandCost(SMATestConstants.BUSINESS_ACCOUNT_BASE, cost);
        f2t4.setDiscountedCost(SMATestConstants.BUSINESS_ACCOUNT_BASE, cost);
        templateOid = SMATestConstants.TEMPLATE_BASE + 14;
        List<SMATemplate> templates = new ArrayList<>();
        templates.add(f1t1);
        templates.add(f1t2);
        templates.add(f1t3);
        templates.add(f2t1);
        templates.add(f2t4);
        Map<String, List<SMATemplate>> map = StableMarriagePerContext.computeFamilyToTemplateMap(templates);
        assertTrue(map != null);
        assertTrue(map.size() == 2);
        assertTrue(map.get("f1").size() == 3);
        List<SMATemplate> list = map.get("f1");
        assertTrue(list.contains(f1t1));
        assertTrue(list.contains(f1t2));
        assertTrue(list.contains(f1t3));
        assertFalse(list.contains(f2t1));
        assertFalse(list.contains(f2t4));
        assertTrue(map.get("f2").size() == 2);
        list = map.get("f2");
        assertFalse(list.contains(f1t1));
        assertFalse(list.contains(f1t2));
        assertFalse(list.contains(f1t3));
        assertTrue(list.contains(f2t1));
        assertTrue(list.contains(f2t4));
    }

}
