package com.vmturbo.cost.component.entity.cost;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import io.grpc.Channel;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.CostCategoryFilter;
import com.vmturbo.common.protobuf.cost.Cost.CostSource;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost.ComponentCost;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.components.common.utils.TimeFrameCalculator;
import com.vmturbo.components.common.utils.TimeFrameCalculator.TimeFrame;
import com.vmturbo.cost.component.util.EntityCostFilter;
import com.vmturbo.cost.component.util.EntityCostFilter.EntityCostFilterBuilder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.repository.api.RepositoryClient;
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

    private static final long VM1_OID = 7L;
    private static final long VM2_OID = 8L;
    private static final long DB1_OID = 9L;
    private static final long realTimeContextId = 777777L;

    private static final ComponentCost VM1_ON_DEM_COST = ComponentCost.newBuilder()
            .setCategory(CostCategory.ON_DEMAND_COMPUTE)
                    .setCostSource(CostSource.ON_DEMAND_RATE)
                    .setAmount(CurrencyAmount.newBuilder()
                            .setAmount(100).setCurrency(840))
                    .build();

    private static final EntityCost VM1_COST = EntityCost.newBuilder()
                    .setAssociatedEntityId(VM1_OID)
                    .setAssociatedEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .addComponentCost(VM1_ON_DEM_COST)
            .build();

    private static final ComponentCost VM2_ON_DEM_COST = ComponentCost.newBuilder()
            .setCategory(CostCategory.ON_DEMAND_COMPUTE)
            .setCostSource(Cost.CostSource.ON_DEMAND_RATE)
            .setAmount(CurrencyAmount.newBuilder()
                    .setAmount(35).setCurrency(840))
            .build();

    private static final ComponentCost VM2_STORAGE_COST = ComponentCost.newBuilder()
            .setCategory(CostCategory.STORAGE)
            .setAmount(CurrencyAmount.newBuilder()
                    .setAmount(33).setCurrency(840))
            .build();

    private static final ComponentCost VM2_BUY_RI_DIS = ComponentCost.newBuilder()
            .setCategory(CostCategory.STORAGE)
            .setCategory(CostCategory.ON_DEMAND_COMPUTE)
            .setCostSource(CostSource.BUY_RI_DISCOUNT)
            .setAmount(CurrencyAmount.newBuilder()
                    .setAmount(-10).setCurrency(840))
            .build();

    private static final EntityCost VM2_COST = EntityCost.newBuilder()
            .setAssociatedEntityId(VM2_OID)
            .setAssociatedEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .addComponentCost(VM2_ON_DEM_COST)
            .addComponentCost(VM2_STORAGE_COST)
            .addComponentCost(VM2_BUY_RI_DIS)
            .build();

    static final ComponentCost DB1_ON_DEM_COST = ComponentCost.newBuilder()
            .setCategory(CostCategory.ON_DEMAND_COMPUTE)
            .setAmount(CurrencyAmount.newBuilder()
                    .setAmount(77).setCurrency(840))
            .build();

    private static final EntityCost DB1_COST = EntityCost.newBuilder()
            .setAssociatedEntityId(DB1_OID)
            .setAssociatedEntityType(EntityType.DATABASE_VALUE)
            .addComponentCost(DB1_ON_DEM_COST)
            .build();

    private RepositoryClient repositoryClient;
    private SupplyChainServiceBlockingStub serviceBlockingStub;

    @Before
    public void setup() {
        repositoryClient = mock(RepositoryClient.class);
        serviceBlockingStub = SupplyChainServiceGrpc.newBlockingStub(mock(Channel.class));
        store = new ProjectedEntityCostStore(repositoryClient, serviceBlockingStub,
                realTimeContextId);
    }

    @Test
    public void testUpdateAndGet() {
        store.updateProjectedEntityCosts(Arrays.asList(VM1_COST));
        final Map<Long, EntityCost> retCostMap =
                store.getProjectedEntityCosts(Collections.singleton(VM1_COST.getAssociatedEntityId()));
        assertThat(retCostMap, is(ImmutableMap.of(VM1_COST.getAssociatedEntityId(), VM1_COST)));
    }

    @Test
    public void testGetEmpty() {
        store.updateProjectedEntityCosts(Arrays.asList(VM1_COST));
        assertThat(store.getProjectedEntityCosts(Collections.emptySet()), is(Collections.emptyMap()));
    }

    @Test
    public void testGetMissing() {
        store.updateProjectedEntityCosts(Arrays.asList(VM1_COST));
        assertThat(store.getProjectedEntityCosts(Collections.singleton(1 + VM1_COST.getAssociatedEntityId())),
                is(Collections.emptyMap()));
    }

    /**
     * Test the projected cost store with an empty cost filter.
     */
    @Test
    public void testGetProjectedEntityCostsWithCostFilter() {
        store.updateProjectedEntityCosts(Arrays.asList(VM1_COST, VM2_COST, DB1_COST));
        Map<Long, EntityCost> costs = store.getProjectedEntityCosts(EntityCostFilterBuilder
            .newBuilder(TimeFrameCalculator.TimeFrame.LATEST).build());
        assertThat(costs.keySet(), containsInAnyOrder(VM1_OID, VM2_OID, DB1_OID));
        assertThat(costs.get(VM1_OID), is(VM1_COST));
        assertThat(costs.get(VM2_OID), is(VM2_COST));
        assertThat(costs.get(DB1_OID), is(DB1_COST));
    }

    /**
     * Test the projected cost store with cost filter based on entities.
     */
    @Test
    public void testGetProjectedEntityCostsWithCostFilterOnEntities() {
        store.updateProjectedEntityCosts(Arrays.asList(VM1_COST, VM2_COST, DB1_COST));
        Map<Long, EntityCost> costs = store.getProjectedEntityCosts(EntityCostFilterBuilder
            .newBuilder(TimeFrameCalculator.TimeFrame.LATEST)
            .entityIds(ImmutableSet.of(VM1_OID, DB1_OID))
            .build());
        assertThat(costs.keySet(), containsInAnyOrder(VM1_OID, DB1_OID));
        assertThat(costs.get(VM1_OID), is(VM1_COST));
        assertThat(costs.get(DB1_OID), is(DB1_COST));
    }

    /**
     * Test the projected cost store with cost filter based on entities types.
     */
    @Test
    public void testGetProjectedEntityCostsWithCostFilterOnEntitiesTypes() {
        store.updateProjectedEntityCosts(Arrays.asList(VM1_COST, VM2_COST, DB1_COST));
        Map<Long, EntityCost> costs = store.getProjectedEntityCosts(EntityCostFilterBuilder
            .newBuilder(TimeFrameCalculator.TimeFrame.LATEST)
            .entityTypes(ImmutableSet.of(EntityType.VIRTUAL_MACHINE_VALUE))
            .build());
        assertThat(costs.keySet(), containsInAnyOrder(VM1_OID, VM2_OID));
        assertThat(costs.get(VM1_OID), is(VM1_COST));
        assertThat(costs.get(VM2_OID), is(VM2_COST));
    }

    /**
     * Test the projected cost store with cost filter based on cost categories.
     */
    @Test
    public void testGetProjectedEntityCostsWithCostFilterOnCategory() {
        store.updateProjectedEntityCosts(Arrays.asList(VM1_COST, VM2_COST, DB1_COST));
        Map<Long, EntityCost> costs = store.getProjectedEntityCosts(EntityCostFilterBuilder
            .newBuilder(TimeFrame.LATEST)
            .costCategoryFilter(CostCategoryFilter.newBuilder()
                    .setExclusionFilter(false)
                    .addCostCategory(CostCategory.STORAGE)
                    .build())
            .build());
        assertThat(costs.keySet(), containsInAnyOrder(VM2_OID));
        assertThat(costs.get(VM2_OID).getComponentCostCount(), is(1));
        assertThat(costs.get(VM2_OID).getComponentCost(0), is(VM2_STORAGE_COST));
    }

    /**
     * Test the projected cost store with cost filter based on cost sources when specific cost
     * sources are queried.
     */
    @Test
    public void testGetProjectedEntityCostsWithCostFilterOnCostSourceInclusion() {
        store.updateProjectedEntityCosts(Arrays.asList(VM1_COST, VM2_COST, DB1_COST));
        Map<Long, EntityCost> costs = store.getProjectedEntityCosts(EntityCostFilterBuilder
            .newBuilder(TimeFrameCalculator.TimeFrame.LATEST)
            .costSources(false, ImmutableSet.of(CostSource.ON_DEMAND_RATE_VALUE))
            .build());
        assertThat(costs.keySet(), containsInAnyOrder(VM1_OID, VM2_OID));
        assertThat(costs.get(VM1_OID).getComponentCostCount(), is(1));
        assertThat(costs.get(VM1_OID).getComponentCost(0), is(VM1_ON_DEM_COST));
        assertThat(costs.get(VM2_OID).getComponentCostCount(), is(1));
        assertThat(costs.get(VM2_OID).getComponentCost(0), is(VM2_ON_DEM_COST));
    }

    /**
     * Test the projected cost store with cost filter based on cost sources when some cost
     * sources are being excluded.
     */
    @Test
    public void testGetProjectedEntityCostsWithCostFilterOnCostSourceExclusion() {
        store.updateProjectedEntityCosts(Arrays.asList(VM1_COST, VM2_COST, DB1_COST));
        Map<Long, EntityCost> costs = store.getProjectedEntityCosts(EntityCostFilterBuilder
            .newBuilder(TimeFrameCalculator.TimeFrame.LATEST)
            .costSources(true, ImmutableSet.of(CostSource.BUY_RI_DISCOUNT_VALUE))
            .build());
        assertThat(costs.keySet(), containsInAnyOrder(VM1_OID, VM2_OID, DB1_OID));
        assertThat(costs.get(VM1_OID), is(VM1_COST));
        assertThat(costs.get(VM2_OID).getComponentCostCount(), is(2));
        assertThat(costs.get(VM2_OID).getComponentCostList(), containsInAnyOrder(VM2_ON_DEM_COST,
            VM2_STORAGE_COST));
        assertThat(costs.get(DB1_OID), is(DB1_COST));
    }

    /**
     * Test that with region AND account filter, the returned projected costs contains costs for
     * entities present in *both* the region scope *and* account scope but not the entities that
     * are present either only in region scope or only in account scope (intersection set).
     */
    @Test
    public void testGetProjectedEntityCostsRegionalAndAccountFilter() {
        final long regionId = 11111L;
        final long accountId = 22222L;
        store.updateProjectedEntityCosts(Arrays.asList(VM1_COST, VM2_COST));
        final EntityCostFilter costFilterWithRegion = EntityCostFilterBuilder
                .newBuilder(TimeFrame.LATEST)
                .regionIds(Collections.singleton(regionId))
                .accountIds(Collections.singleton(accountId))
                .build();
        when(repositoryClient.getEntityOidsByType(Collections.singletonList(regionId),
                realTimeContextId, serviceBlockingStub)).thenReturn(Collections.singletonMap(
                EntityType.VIRTUAL_MACHINE, new HashSet<>(Arrays.asList(VM1_OID, VM2_OID))));
        when(repositoryClient.getEntityOidsByType(Collections.singletonList(accountId),
                realTimeContextId, serviceBlockingStub)).thenReturn(Collections.singletonMap(
                        EntityType.VIRTUAL_MACHINE, Collections.singleton(VM2_OID)));
        final Map<Long, EntityCost> costMap = store.getProjectedEntityCosts(costFilterWithRegion);
        Assert.assertEquals(1, costMap.size());
        Assert.assertEquals(VM2_COST, costMap.get(VM2_OID));
    }

    /**
     * Test that empty account filter scope and non-empty region filter scope results in empty
     * projected costs being returned.
     */
    @Test
    public void testGetProjectedEntityCostsEmptyAccountFilter() {
        final long regionId = 11111L;
        final long accountId = 22222L;
        store.updateProjectedEntityCosts(Arrays.asList(VM1_COST, VM2_COST));
        final EntityCostFilter costFilterWithRegion = EntityCostFilterBuilder
                .newBuilder(TimeFrame.LATEST)
                .regionIds(Collections.singleton(regionId))
                .accountIds(Collections.singleton(accountId))
                .build();
        when(repositoryClient.getEntityOidsByType(Collections.singletonList(regionId),
                realTimeContextId, serviceBlockingStub)).thenReturn(Collections.singletonMap(
                EntityType.VIRTUAL_MACHINE, new HashSet<>(Arrays.asList(VM1_OID, VM2_OID))));
        when(repositoryClient.getEntityOidsByType(Collections.singletonList(accountId),
                realTimeContextId, serviceBlockingStub)).thenReturn(Collections.emptyMap());
        final Map<Long, EntityCost> costMap = store.getProjectedEntityCosts(costFilterWithRegion);
        Assert.assertTrue(costMap.isEmpty());
    }

    /**
     * Test that with region filter only, correct projected costs are returned.
     */
    @Test
    public void testGetProjectedEntityCostsRegionalFilter() {
        final long regionId = 11111L;
        store.updateProjectedEntityCosts(Arrays.asList(VM1_COST, VM2_COST));
        final EntityCostFilter costFilterWithRegion = EntityCostFilterBuilder
                .newBuilder(TimeFrame.LATEST)
                .regionIds(Collections.singleton(regionId))
                .build();
        when(repositoryClient.getEntityOidsByType(Collections.singletonList(regionId),
                realTimeContextId, serviceBlockingStub)).thenReturn(Collections.singletonMap(
                EntityType.VIRTUAL_MACHINE, Collections.singleton(VM1_OID)));
        final Map<Long, EntityCost> costMap = store.getProjectedEntityCosts(costFilterWithRegion);
        Assert.assertFalse(costMap.isEmpty());
        Assert.assertEquals(VM1_COST, costMap.get(VM1_OID));
    }
}
