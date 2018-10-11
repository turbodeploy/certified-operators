package com.vmturbo.cost.component.entity.cost;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost.ComponentCost;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;

/**
 * Unit tests for {@link ProjectedEntityCostStore}.
 */
public class ProjectedEntityCostStoreTest {

    private static final EntityCost COST = EntityCost.newBuilder()
            .setAssociatedEntityId(7L)
            .addComponentCost(ComponentCost.newBuilder()
                    .setCategory(CostCategory.COMPUTE)
                    .setAmount(CurrencyAmount.newBuilder()
                            .setAmount(100)))
            .build();

    @Test
    public void testUpdateAndGet() {
        final ProjectedEntityCostStore store = new ProjectedEntityCostStore();
        store.updateProjectedEntityCosts(Stream.of(COST));
        final Map<Long, EntityCost> retCostMap =
                store.getProjectedEntityCosts(Collections.singleton(COST.getAssociatedEntityId()));
        assertThat(retCostMap, is(ImmutableMap.of(COST.getAssociatedEntityId(), COST)));
    }

    @Test
    public void testGetEmpty() {
        final ProjectedEntityCostStore store = new ProjectedEntityCostStore();
        store.updateProjectedEntityCosts(Stream.of(COST));
        assertThat(store.getProjectedEntityCosts(Collections.emptySet()), is(Collections.emptyMap()));
    }

    @Test
    public void testGetMissing() {
        final ProjectedEntityCostStore store = new ProjectedEntityCostStore();
        store.updateProjectedEntityCosts(Stream.of(COST));
        assertThat(store.getProjectedEntityCosts(Collections.singleton(1 + COST.getAssociatedEntityId())),
                is(Collections.emptyMap()));
    }
}
