package com.vmturbo.cost.component.entity.cost;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.CostCategoryFilter;
import com.vmturbo.common.protobuf.cost.Cost.CostSource;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost.ComponentCost;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetMultiSupplyChainsResponse;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.repository.SupplyChainProtoMoles.SupplyChainServiceMole;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.cost.component.util.EntityCostFilter;
import com.vmturbo.cost.component.util.EntityCostFilter.EntityCostFilterBuilder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.repository.api.RepositoryClient;

/**
 * Unit tests for {@link ProjectedEntityCostStore}.
 */
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

    private static final ComponentCost DB1_ON_DEM_COST = ComponentCost.newBuilder()
            .setCategory(CostCategory.ON_DEMAND_COMPUTE)
            .setAmount(CurrencyAmount.newBuilder()
                    .setAmount(77).setCurrency(840))
            .build();

    private static final EntityCost DB1_COST = EntityCost.newBuilder()
            .setAssociatedEntityId(DB1_OID)
            .setAssociatedEntityType(EntityType.DATABASE_VALUE)
            .addComponentCost(DB1_ON_DEM_COST)
            .build();

    private static final int vmEntityType = ApiEntityType.VIRTUAL_MACHINE.typeNumber();
    private static final int dbEntityType = ApiEntityType.DATABASE.typeNumber();

    private RepositoryClient repositoryClient;
    private SupplyChainServiceBlockingStub serviceBlockingStub;

    private SupplyChainServiceMole supplyChainServiceMole = spy(new SupplyChainServiceMole());
    private GrpcTestServer testServer = GrpcTestServer.newServer(supplyChainServiceMole);

    @Before
    public void setup() throws IOException {
        testServer.start();
        serviceBlockingStub = SupplyChainServiceGrpc.newBlockingStub(testServer.getChannel());
        repositoryClient = mock(RepositoryClient.class);
        when(repositoryClient.getEntitiesByTypePerScope(any(), any())).thenCallRealMethod();
        when(repositoryClient.parseSupplyChainResponseToEntityOidsMap(any())).thenCallRealMethod();
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
                .newBuilder(TimeFrame.LATEST, realTimeContextId).build());
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
        final List<Long> vmOids = Collections.singletonList(VM1_OID);
        final List<Long> dbOids = Collections.singletonList(DB1_OID);
        final List<SupplyChainNode> supplyChainNodes = Arrays.asList(
                createSupplyChainNode(vmEntityType, vmOids),
                createSupplyChainNode(dbEntityType, dbOids));
        final GetMultiSupplyChainsResponse response = createResponse(supplyChainNodes);
        when(supplyChainServiceMole.getMultiSupplyChains(any()))
                .thenReturn(Collections.singletonList(response));
        Map<Long, EntityCost> costs = store.getProjectedEntityCosts(EntityCostFilterBuilder
                .newBuilder(TimeFrame.LATEST, realTimeContextId)
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
                .newBuilder(TimeFrame.LATEST, realTimeContextId)
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
            .newBuilder(TimeFrame.LATEST, realTimeContextId)
            .costCategoryFilter(CostCategoryFilter.newBuilder()
                    .setExclusionFilter(false)
                    .addCostCategory(CostCategory.STORAGE)
                    .build())
            .build());

        assertThat(costs.keySet(), containsInAnyOrder(VM2_OID));
        assertThat(costs.get(VM2_OID).getComponentCostCount(), is(1));
        assertThat(costs.get(VM2_OID).getComponentCost(0), is(VM2_STORAGE_COST));
    }

    private GetMultiSupplyChainsResponse createResponse(
            final List<SupplyChainNode> supplyChainNodes) {
        return  GetMultiSupplyChainsResponse.newBuilder()
                .setSupplyChain(SupplyChain.newBuilder()
                        .addAllSupplyChainNodes(supplyChainNodes)
                        .build())
                .build();
    }

    private SupplyChainNode createSupplyChainNode(final int entityType,
                                                  final List<Long> memberOids) {
        return SupplyChainNode.newBuilder()
                .setEntityType(entityType)
                .putMembersByState(0, MemberList.newBuilder()
                        .addAllMemberOids(memberOids)
                        .build())
                .build();
    }

    /**
     * Test the projected cost store with cost filter based on cost sources when specific cost
     * sources are queried.
     */
    @Test
    public void testGetProjectedEntityCostsWithCostFilterOnCostSourceInclusion() {
        store.updateProjectedEntityCosts(Arrays.asList(VM1_COST, VM2_COST, DB1_COST));
        Map<Long, EntityCost> costs = store.getProjectedEntityCosts(EntityCostFilterBuilder
                .newBuilder(TimeFrame.LATEST, realTimeContextId)
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
                .newBuilder(TimeFrame.LATEST, realTimeContextId)
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
                .newBuilder(TimeFrame.LATEST, realTimeContextId)
                .regionIds(Collections.singleton(regionId))
                .accountIds(Collections.singleton(accountId))
                .build();
        final List<Long> vmOidsRegion = Arrays.asList(VM1_OID, VM2_OID);
        final List<SupplyChainNode> supplyChainNodesRegion = Collections.singletonList(
                createSupplyChainNode(vmEntityType, vmOidsRegion));
        final GetMultiSupplyChainsResponse regionResponse = createResponse(supplyChainNodesRegion);

        final List<Long> vmOidsAccount = Collections.singletonList(VM2_OID);
        final List<SupplyChainNode> supplyChainNodesAccount =
                Collections.singletonList(createSupplyChainNode(vmEntityType, vmOidsAccount));
        final GetMultiSupplyChainsResponse accountResponse = createResponse(
                supplyChainNodesAccount);
        when(supplyChainServiceMole.getMultiSupplyChains(any()))
                .thenReturn(Arrays.asList(regionResponse, accountResponse));
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
                .newBuilder(TimeFrame.LATEST, realTimeContextId)
                .regionIds(Collections.singleton(regionId))
                .accountIds(Collections.singleton(accountId))
                .build();
        final List<Long> vmOidsRegion = Arrays.asList(VM1_OID, VM2_OID);
        final List<SupplyChainNode> supplyChainNodesRegion =
                Collections.singletonList(createSupplyChainNode(vmEntityType, vmOidsRegion));
        final GetMultiSupplyChainsResponse regionResponse = createResponse(supplyChainNodesRegion);
        final GetMultiSupplyChainsResponse accountResponse =
                createResponse(Collections.emptyList());
        when(supplyChainServiceMole.getMultiSupplyChains(any()))
                .thenReturn(Arrays.asList(regionResponse, accountResponse));
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
                .newBuilder(TimeFrame.LATEST, realTimeContextId)
                .regionIds(Collections.singleton(regionId))
                .build();
        final List<Long> vmOids = Collections.singletonList(VM1_OID);
        final List<SupplyChainNode> supplyChainNodesRegion =
                Collections.singletonList(createSupplyChainNode(vmEntityType, vmOids));
        final GetMultiSupplyChainsResponse regionResponse = createResponse(supplyChainNodesRegion);
        when(supplyChainServiceMole.getMultiSupplyChains(any()))
                .thenReturn(Collections.singletonList(regionResponse));
        final Map<Long, EntityCost> costMap = store.getProjectedEntityCosts(costFilterWithRegion);
        Assert.assertEquals(1, costMap.size());
        Assert.assertEquals(VM1_COST, costMap.get(VM1_OID));
    }

    /**
     * Tests if storeReady flag gets set after updateProjectedEntityCosts is called.
     */
    @Test
    public void testReadinessOfStore() {
        assertThat(store.isStoreReady(), is(false));
        store.updateProjectedEntityCosts(Arrays.asList(VM1_COST, VM2_COST));
        assertThat(store.isStoreReady(), is(true));
    }

    /**
     * Clean up test resources.
     */
    @After
    public void cleanUp() {
        testServer.close();
    }
}
