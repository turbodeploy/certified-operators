package com.vmturbo.cost.component.reserved.instance.filter;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.jooq.Condition;
import org.junit.Test;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Test class to test the ReservedInstanceBoughtFilter.
 */
public class ReservedInstanceBoughtFilterTest {

    private static final Long REGION_ID = 1L;
    private static final Long ZONE_ID = 2L;
    private static final String REGION_CLAUSE = "\"cost\".\"reserved_instance_spec\".\"region_id\" in (" + REGION_ID + ")";
    private static final String OR = "or";
    private static final String ZONE_CLAUSE = "\"cost\".\"reserved_instance_bought\".\"availability_zone_id\" in (" + ZONE_ID + ")";

    /**
     * Tests if the conditions generated in the filter class. They should be treated as separated
     * conditions that will be AND together
     */
    @Test
    public void testRegionAndAZConditions() {

        ReservedInstanceBoughtFilter testFilter =
                ReservedInstanceBoughtFilter.newBuilder()
                        .cloudScopeTuples(
                                ImmutableMap.of(EntityType.REGION, ImmutableSet.of(REGION_ID),
                                        EntityType.AVAILABILITY_ZONE, ImmutableSet.of(ZONE_ID)))
                        .build();
        final List<Condition> conditions = ImmutableList.copyOf(testFilter.generateConditions());
        final List<String> conditionStrings = conditions.stream()
                .map(Condition::toString)
                .collect(ImmutableList.toImmutableList());

        assertThat(conditions, hasSize(1));
        assertThat(conditionStrings.get(0), containsString(REGION_CLAUSE));
        assertThat(conditionStrings.get(0), containsString(OR));
        assertThat(conditionStrings.get(0), containsString(ZONE_CLAUSE));
    }
}
