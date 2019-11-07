package com.vmturbo.cost.component.reserved.instance.filter;

import java.util.Collections;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.jooq.Condition;
import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Test class to test the ReservedInstanceBoughtFilter.
 */
public class ReservedInstanceBoughtFilterTest {

    private static final Long REGION_ID = 1L;
    private static final Long ZONE_ID = 2L;
    private static final String REGION_CLAUSE = "\"region_id\" in (" + REGION_ID + ")";
    private static final String OR = "or";
    private static final String ZONE_CLAUSE = "\"availability_zone_id\" in (" + ZONE_ID + ")";

    /**
     * Tests if the conditions generated in the filter class
     * is an or condition when both region and zone is present.
     */
    @Test
    public void testRegionAndAZConditions() {

        ReservedInstanceBoughtFilter testFilter =
                ReservedInstanceBoughtFilter.newBuilder()
                        .addAllScopeId(Collections.emptyList())
                        .setCloudScopesTuple(
                                ImmutableMap.of(EntityType.REGION, ImmutableSet.of(REGION_ID),
                                        EntityType.AVAILABILITY_ZONE, ImmutableSet.of(ZONE_ID)))
                        .setJoinWithSpecTable(true)
                        .build();
        Condition[] conditions = testFilter.getConditions();
        Assert.assertEquals(1, conditions.length);
        Condition combinedCondition = conditions[0];
        Assert.assertNotNull(combinedCondition);
        Assert.assertTrue(combinedCondition.toString().contains(REGION_CLAUSE));
        Assert.assertTrue(combinedCondition.toString().contains(OR));
        Assert.assertTrue(combinedCondition.toString().contains(ZONE_CLAUSE));
    }
}
