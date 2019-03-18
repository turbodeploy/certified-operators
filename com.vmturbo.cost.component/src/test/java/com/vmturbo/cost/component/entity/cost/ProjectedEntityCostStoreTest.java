package com.vmturbo.cost.component.entity.cost;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost.ComponentCost;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

/**
 * Unit tests for {@link ProjectedEntityCostStore}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
        classes = {TestSQLDatabaseConfig.class}
)
@TestPropertySource(properties = {"originalSchemaName=cost"})
public class ProjectedEntityCostStoreTest {

    private ProjectedEntityCostStore store;

    private static final EntityCost COST = EntityCost.newBuilder()
                    .setAssociatedEntityId(7L)
                    .setAssociatedEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .addComponentCost(ComponentCost.newBuilder()
                    .setCategory(CostCategory.ON_DEMAND_COMPUTE)
                    .setAmount(CurrencyAmount.newBuilder()
                            .setAmount(100).setCurrency(840)))
            .build();

    @Before
    public void setup() throws Exception {
        store = new ProjectedEntityCostStore();
    }

    @Test
    public void testUpdateAndGet() {
        store.updateProjectedEntityCosts(Arrays.asList(COST));
        final Map<Long, EntityCost> retCostMap =
                store.getProjectedEntityCosts(Collections.singleton(COST.getAssociatedEntityId()));
        assertThat(retCostMap, is(ImmutableMap.of(COST.getAssociatedEntityId(), COST)));
    }

    @Test
    public void testGetEmpty() {
        store.updateProjectedEntityCosts(Arrays.asList(COST));
        assertThat(store.getProjectedEntityCosts(Collections.emptySet()), is(Collections.emptyMap()));
    }

    @Test
    public void testGetMissing() {
        store.updateProjectedEntityCosts(Arrays.asList(COST));
        assertThat(store.getProjectedEntityCosts(Collections.singleton(1 + COST.getAssociatedEntityId())),
                is(Collections.emptyMap()));
    }

}
