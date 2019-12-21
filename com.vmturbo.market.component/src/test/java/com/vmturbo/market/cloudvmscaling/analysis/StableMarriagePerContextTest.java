package com.vmturbo.market.cloudvmscaling.analysis;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.vmturbo.market.cloudvmscaling.entities.SMACost;
import com.vmturbo.market.cloudvmscaling.entities.SMATemplate;

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
        SMATemplate f1t1 = new SMATemplate(templateOid, "f1." + templateOid, "f1", cost, cost, 2);
        templateOid++;
        SMATemplate f1t2 = new SMATemplate(templateOid, "f1." + templateOid, "f1", cost, cost, 4);
        templateOid++;
        SMATemplate f1t3 = new SMATemplate(templateOid, "f1." + templateOid, "f1", cost, cost, 8);
        templateOid = SMATestConstants.TEMPLATE_BASE + 11;
        SMATemplate f2t1 = new SMATemplate(templateOid, "f1." + templateOid, "f2", cost, cost, 2);
        templateOid = SMATestConstants.TEMPLATE_BASE + 14;
        SMATemplate f2t4 = new SMATemplate(templateOid, "f1." + templateOid, "f2", cost, cost, 16);
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
