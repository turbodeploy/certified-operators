package com.vmturbo.cost.component.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.Optional;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.junit.Test;

import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.CostCategoryFilter;
import com.vmturbo.common.protobuf.cost.Cost.CostSource;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost.ComponentCost;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost.ComponentCost.CostSourceLinkDTO;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.cost.component.util.EntityCostFilter.EntityCostFilterBuilder;

/**
 * Tests the {@link EntityCostFilter} class.
 */
public class EntityCostFilterTest {

    private static final long RT_TOPO_CONTEXT_ID = 777777L;

    /**
     * This method implements methods {@code equals}, {@code hashCode}, {@code toString} that
     * has been overridden in this class.
     */
    @Test
    public void testObjectOverrideMethods() {
        EntityCostFilter filter =
            EntityCostFilterBuilder.newBuilder(TimeFrame.LATEST, RT_TOPO_CONTEXT_ID)
                    .duration(1L, 2L)
                    .entityIds(Collections.singleton(5L))
                    .costCategoryFilter(CostCategoryFilter.newBuilder()
                            .setExclusionFilter(false)
                            .addCostCategory(CostCategory.IP)
                            .addCostCategory(CostCategory.ON_DEMAND_COMPUTE)
                            .build())
            .costSources(true, Collections.singleton(CostSource.ON_DEMAND_RATE))
            .accountIds(ImmutableSet.of(20L, 21L))
            .regionIds(ImmutableSet.of(30L, 31L))
            .availabilityZoneIds(ImmutableSet.of(40L, 41L))
            .build();
        filter.toString();

        assertThat(filter.getStartDateMillis(), is(Optional.of(1L)));
        assertThat(filter.getEndDateMillis(), is(Optional.of(2L)));

        final EntityCostFilterBuilder builder =
            EntityCostFilterBuilder.newBuilder(TimeFrame.LATEST, RT_TOPO_CONTEXT_ID)
                .duration(1L, 2L)
                .entityIds(Collections.singleton(5L))
                    .costCategoryFilter(CostCategoryFilter.newBuilder()
                            .setExclusionFilter(false)
                            .addCostCategory(CostCategory.IP)
                            .addCostCategory(CostCategory.ON_DEMAND_COMPUTE)
                            .build())
                .costSources(true, Collections.singleton(CostSource.ON_DEMAND_RATE))
                .accountIds(ImmutableSet.of(20L, 21L))
                .regionIds(ImmutableSet.of(30L, 31L))
                .availabilityZoneIds(ImmutableSet.of(40L, 41L));

        assertTrue(filter.equals(builder.build()));
        assertFalse(filter.equals(null));
        assertThat(filter.hashCode(), is(builder.build().hashCode()));

        builder.costCategoryFilter(CostCategoryFilter.newBuilder()
                .setExclusionFilter(false)
                .addCostCategory(CostCategory.ON_DEMAND_LICENSE)
                .build());
        assertFalse(filter.equals(builder.build()));
        assertThat(filter.hashCode(),  not(builder.build()));
        builder.costCategoryFilter(CostCategoryFilter.newBuilder()
                .setExclusionFilter(false)
                .addCostCategory(CostCategory.IP)
                .addCostCategory(CostCategory.ON_DEMAND_COMPUTE)
                .build());
        builder.costSources(false, Collections.singleton(CostSource.ON_DEMAND_RATE));
        assertFalse(filter.equals(builder.build()));

        assertThat(filter.getAccountIds(), is(Optional.of(ImmutableSet.of(20L, 21L))));
        assertThat(filter.getRegionIds(), is(Optional.of(ImmutableSet.of(30L, 31L))));
        assertThat(filter.getAvailabilityZoneIds(), is(Optional.of(ImmutableSet.of(40L, 41L))));
    }

    @Test
    public void filterComponentCostByCategoryInclusion() {

        final ComponentCost componentCost = ComponentCost.newBuilder()
                .setCategory(CostCategory.IP)
                .build();

        /*
        SUT
         */
        EntityCostFilter filter =
                EntityCostFilterBuilder.newBuilder(TimeFrame.LATEST, RT_TOPO_CONTEXT_ID)
                        .costCategoryFilter(CostCategoryFilter.newBuilder()
                                .setExclusionFilter(false)
                                .addCostCategory(CostCategory.IP)
                                .addCostCategory(CostCategory.ON_DEMAND_COMPUTE)
                                .build())
                        .build();

        assertTrue(filter.filterComponentCost(componentCost));
    }

    @Test
    public void filterComponentCostByCategoryExclusion() {

        final ComponentCost componentCost = ComponentCost.newBuilder()
                .setCategory(CostCategory.IP)
                .build();

        /*
        SUT
         */
        EntityCostFilter filter =
                EntityCostFilterBuilder.newBuilder(TimeFrame.LATEST, RT_TOPO_CONTEXT_ID)
                        .costCategoryFilter(CostCategoryFilter.newBuilder()
                                .setExclusionFilter(true)
                                .addCostCategory(CostCategory.IP)
                                .addCostCategory(CostCategory.ON_DEMAND_COMPUTE)
                                .build())
                        .build();

        assertFalse(filter.filterComponentCost(componentCost));
    }

    @Test
    public void filterComponentCostBySourceInclusion() {

        final ComponentCost componentCost = ComponentCost.newBuilder()
                .setCostSourceLink(CostSourceLinkDTO.newBuilder()
                        .setCostSource(CostSource.ENTITY_UPTIME_DISCOUNT)
                        .setDiscountCostSourceLink(CostSourceLinkDTO.newBuilder()
                                .setCostSource(CostSource.BUY_RI_DISCOUNT)))
                .build();

        /*
        SUT
         */
        final EntityCostFilter filterWithBothCostSources =
                EntityCostFilterBuilder.newBuilder(TimeFrame.LATEST, RT_TOPO_CONTEXT_ID)
                        .costSources(false, ImmutableSet.of(
                                CostSource.BUY_RI_DISCOUNT,
                                CostSource.ENTITY_UPTIME_DISCOUNT))
                        .build();

        final EntityCostFilter filterWithEntityUptime =
                EntityCostFilterBuilder.newBuilder(TimeFrame.LATEST, RT_TOPO_CONTEXT_ID)
                        .costSources(false, Collections.singleton(
                                CostSource.ENTITY_UPTIME_DISCOUNT))
                        .build();

        assertTrue(filterWithBothCostSources.filterComponentCost(componentCost));
        assertFalse(filterWithEntityUptime.filterComponentCost(componentCost));
    }

    @Test
    public void filterComponentCostBySourceExclusion() {

        final ComponentCost componentCost = ComponentCost.newBuilder()
                .setCostSourceLink(CostSourceLinkDTO.newBuilder()
                        .setCostSource(CostSource.ENTITY_UPTIME_DISCOUNT)
                        .setDiscountCostSourceLink(CostSourceLinkDTO.newBuilder()
                                .setCostSource(CostSource.BUY_RI_DISCOUNT)))
                .build();

        /*
        SUT
         */
        EntityCostFilter filter =
                EntityCostFilterBuilder.newBuilder(TimeFrame.LATEST, RT_TOPO_CONTEXT_ID)
                        .costSources(true, Collections.singleton(
                                CostSource.BUY_RI_DISCOUNT))
                        .build();

        assertFalse(filter.filterComponentCost(componentCost));
    }

    @Test
    public void testCreateGroupBy() {
        EntityCostFilterBuilder builder =
                EntityCostFilterBuilder.newBuilder(TimeFrame.LATEST, RT_TOPO_CONTEXT_ID)
                        .groupByFields(ImmutableSet.of("unknown_field", "plan_id", CostGroupBy.ENTITY));

        // The unknown_field field should be ignored. The plan_id field is only valid when a topology ID
        // is provided.  The created_time field is automatically added here.
        EntityCostFilter filter = builder.build();
        assertEquals(2, filter.getCostGroupBy().getGroupByFields().size());
        // Include the plan ID, which makes plan_id valid
        filter = builder.topologyContextId(2116L).build();
        assertEquals(3, filter.getCostGroupBy().getGroupByFields().size());
    }
}
