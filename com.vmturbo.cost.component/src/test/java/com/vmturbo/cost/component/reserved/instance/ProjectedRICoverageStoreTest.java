package com.vmturbo.cost.component.reserved.instance;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;

/**
 * Unit tests for {@link ProjectedRICoverageStore}.
 */
public class ProjectedRICoverageStoreTest {

    private static final EntityReservedInstanceCoverage ENTITY_RI_COVERAGE =
        EntityReservedInstanceCoverage.newBuilder()
            .setEntityId(1L)
            .putCouponsCoveredByRi(10L, 100.0)
            .build();

    @Test
    public void testUpdateAndGet() {
        final ProjectedRICoverageStore store = new ProjectedRICoverageStore();
        store.updateProjectedRICoverage(Stream.of(ENTITY_RI_COVERAGE));
        final Map<Long, Map<Long, Double>> retCostMap =
                store.getAllProjectedEntitiesRICoverages();
        assertThat(retCostMap, is(ImmutableMap.of(
                        ENTITY_RI_COVERAGE.getEntityId(),
                        ENTITY_RI_COVERAGE.getCouponsCoveredByRiMap())));
    }

    @Test
    public void testGetEmpty() {
        final ProjectedRICoverageStore store = new ProjectedRICoverageStore();
        assertThat(store.getAllProjectedEntitiesRICoverages(),
                is(Collections.emptyMap()));
    }

}
