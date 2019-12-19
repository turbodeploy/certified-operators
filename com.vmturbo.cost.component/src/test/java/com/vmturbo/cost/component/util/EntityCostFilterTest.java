package com.vmturbo.cost.component.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.Optional;

import com.google.common.collect.ImmutableSet;

import org.junit.Test;

import com.vmturbo.components.common.utils.TimeFrameCalculator;
import com.vmturbo.cost.component.util.EntityCostFilter.EntityCostFilterBuilder;

/**
 * Tests the {@link EntityCostFilter} class.
 */
public class EntityCostFilterTest {

    /**
     * This method implements methods {@code equals}, {@code hashCode}, {@code toString} that
     * has been overridden in this class.
     */
    @Test
    public void testObjectOverrideMethods() {
        EntityCostFilter filter =
            EntityCostFilterBuilder.newBuilder(TimeFrameCalculator.TimeFrame.LATEST)
            .duration(1L, 2L)
            .entityIds(Collections.singleton(5L))
            .costCategories(ImmutableSet.of(10, 11))
            .costSources(true, Collections.singleton(8))
            .accountIds(ImmutableSet.of(20L, 21L))
            .regionIds(ImmutableSet.of(30L, 31L))
            .availabilityZoneIds(ImmutableSet.of(40L, 41L))
            .build();
        filter.toString();

        assertThat(filter.getStartDateMillis(), is(Optional.of(1L)));
        assertThat(filter.getEndDateMillis(), is(Optional.of(2L)));

        final EntityCostFilterBuilder builder =
            EntityCostFilterBuilder.newBuilder(TimeFrameCalculator.TimeFrame.LATEST)
                .duration(1L, 2L)
                .entityIds(Collections.singleton(5L))
                .costCategories(ImmutableSet.of(10, 11))
                .costSources(true, Collections.singleton(8))
                .accountIds(ImmutableSet.of(20L, 21L))
                .regionIds(ImmutableSet.of(30L, 31L))
                .availabilityZoneIds(ImmutableSet.of(40L, 41L));

        assertTrue(filter.equals(builder.build()));
        assertFalse(filter.equals(null));
        assertThat(filter.hashCode(), is(builder.build().hashCode()));

        builder.costCategories(Collections.singleton(9));
        assertFalse(filter.equals(builder.build()));
        assertThat(filter.hashCode(),  not(builder.build()));
        builder.costCategories(ImmutableSet.of(10, 11));
        builder.costSources(false, Collections.singleton(8));
        assertFalse(filter.equals(builder.build()));

        assertThat(filter.getAccountIds(), is(Optional.of(ImmutableSet.of(20L, 21L))));
        assertThat(filter.getRegionIds(), is(Optional.of(ImmutableSet.of(30L, 31L))));
        assertThat(filter.getAvailabilityZoneIds(), is(Optional.of(ImmutableSet.of(40L, 41L))));
    }
}
