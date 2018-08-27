package com.vmturbo.topology.processor.topology;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.PlanChanges;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.PlanChanges.HistoricalBaseline;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.StatsMoles.StatsHistoryServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;

/**
 * Test editing of commodities.
 */
public class CommoditiesEditorTest {

    private StatsHistoryServiceMole statsHistoryService = Mockito.spy(new StatsHistoryServiceMole());

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(statsHistoryService);

    private StatsHistoryServiceBlockingStub historyClient;

    @Before
    public void setup() {
        historyClient = StatsHistoryServiceGrpc.newBlockingStub(grpcServer.getChannel());
    }

    private List<ScenarioChange> getScenarioChangeWithBaselineDate(long baselineDate) {
        List<ScenarioChange> changes = new ArrayList<>();
        ScenarioChange baselineChange = ScenarioChange.newBuilder()
                        .setPlanChanges(PlanChanges.newBuilder()
                            .setHistoricalBaseline(HistoricalBaseline.newBuilder()
                                .setBaselineDate(baselineDate))).build();
        changes.add(baselineChange);
        return changes;
    }

    private TopologyDTO.TopologyInfo getTopologyInfoWithGivenScope(List<Long> oids) {
        return TopologyDTO.TopologyInfo.newBuilder().addAllScopeSeedOids(oids).build();
    }

    private TopologyGraph createGraph(CommodityDTO.CommodityType commType) {
        final Map<Long, TopologyEntity.Builder> topology = new HashMap<>();

        // Set physical machine with commodities sold.
        topology.put(1L, buildTopologyEntityWithCommSold(1L, commType.getNumber(),
                        EntityType.PHYSICAL_MACHINE_VALUE));

        // Set virtual machine with commodities bought.
        topology.put(2L, buildTopologyEntityWithCommBought(2L, commType.getNumber(),
                        EntityType.VIRTUAL_MACHINE_VALUE, 1L));


        final TopologyGraph graph = TopologyGraph.newGraph(topology);
        return graph;
    }

    private TopologyEntity.Builder buildTopologyEntityWithCommBought(long oid, int commType, int entityType, long provider) {

        CommoditiesBoughtFromProvider.Builder commFromProvider = CommoditiesBoughtFromProvider.newBuilder().addCommodityBought(
            CommodityBoughtDTO.newBuilder().setCommodityType(
                CommodityType.newBuilder().setType(commType).setKey("").build())
            .setActive(true)
            .setUsed(10)
            .setPeak(20))
        .setProviderId(provider);

        return TopologyEntityUtils.topologyEntityBuilder(
                    TopologyEntityDTO.newBuilder().setOid(oid).addCommoditiesBoughtFromProviders(commFromProvider)
                    .setEntityType(entityType));
    }

    private TopologyEntity.Builder buildTopologyEntityWithCommSold(long oid, int commType, int entityType) {
        final ImmutableList.Builder<CommoditySoldDTO> commSoldList =
                        ImmutableList.builder();
        commSoldList.add(
            CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(commType).setKey("").build())
                .setActive(true)
                .setUsed(70)
                .setPeak(80)
                .build());
        return TopologyEntityUtils.topologyEntityBuilder(
                        TopologyEntityDTO.newBuilder().setOid(oid)
                        .addAllCommoditySoldList(commSoldList.build())
                        .setEntityType(entityType));
    }

    private EntityStats getEntityStats(long entityOid, String providerOid, float peak, float used, String commodityName) {
        return EntityStats.newBuilder().setOid(entityOid).addStatSnapshots(
            StatSnapshot.newBuilder().addStatRecords(
                StatRecord.newBuilder()
                .setName(commodityName)
                .setProviderUuid(providerOid)
                .setPeak(StatValue.newBuilder().setAvg(peak))
                .setUsed(StatValue.newBuilder().setAvg(used)))).build();
    }


    @Test
    public void testEditCommoditiesForBaselineChanges() throws IOException {
        // Get graph
        // Sets value for VM(Used :10 , Peak : 20)
        // Sets value for PM(Used : 70, Peak : 80)
        TopologyGraph g = createGraph(CommodityDTO.CommodityType.MEM);

        TopologyEntity pm = g.getEntity(1L).get();
        TopologyEntity vm = g.getEntity(2L).get();

        // Check values before calling CommoditiesEditor.
        // Compare used
        Assert.assertEquals(70, pm.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getUsed(), 0.0001);
        Assert.assertEquals(10, vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                        .get(0).getCommodityBoughtList().get(0).getUsed(), 0.0001);

        // Compare peak
        Assert.assertEquals(80, pm.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getPeak(), 0.0001);
        Assert.assertEquals(20, vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                        .get(0).getCommodityBoughtList().get(0).getPeak(), 0.0001);

        // Get scenario change
        Long baselineDate = 123456789L;
        List<ScenarioChange> changes = getScenarioChangeWithBaselineDate(baselineDate);
        // Get TopologyInfo : Scope to PMs OID
        List<Long> scopeOids = Lists.newArrayList(1L);
        TopologyDTO.TopologyInfo topoInfo = getTopologyInfoWithGivenScope(scopeOids);

        // Mocks for return value from database for VM(Used :50 , Peak : 100)
        // Provider id is same as current provider.
        GetEntityStatsResponse response = GetEntityStatsResponse.newBuilder()
                        .addEntityStats(getEntityStats(2, "1", 100f, 50f, "Mem")).build();
        Mockito.when(statsHistoryService.getEntityStats(Mockito.any())).thenReturn(response);

        CommoditiesEditor commEditor = new CommoditiesEditor(historyClient);
        commEditor.editCommoditiesForBaselineChanges(g, changes, topoInfo);

        // Check values after calling CommoditiesEditor.
        // Compare used
        // Expected value used for PM : used - currUsedForVM + usedFromDB : 70 - 10 + 50
        // Expected value used for VM : as fetched from database : 50
        Assert.assertEquals(70 - 10 + 50, pm.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getUsed(), 0.0001);
        Assert.assertEquals(50, vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                        .get(0).getCommodityBoughtList().get(0).getUsed(), 0.0001);

        // Compare peak
        // Expected value peak for PM : peak - currPeakForVM + peakFromDB : 80 - 20 + 100
        // Expected value peak for VM : as fetched from database : 100
        Assert.assertEquals(80 - 20 + 100, pm.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getPeak(), 0.0001);
        Assert.assertEquals(100, vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                        .get(0).getCommodityBoughtList().get(0).getPeak(), 0.0001);
    }

    @Test
    public void testEditCommoditiesForDifferntProviderInDatabase() throws IOException {
        // Get graph
        // Sets value for VM(Used :10 , Peak : 20)
        // Sets value for PM(Used : 70, Peak : 80)
        TopologyGraph g = createGraph(CommodityDTO.CommodityType.MEM);

        TopologyEntity pm = g.getEntity(1L).get();
        TopologyEntity vm = g.getEntity(2L).get();

        // Check values before calling CommoditiesEditor.
        // Compare used
        Assert.assertEquals(70, pm.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getUsed(), 0.0001);
        Assert.assertEquals(10, vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                        .get(0).getCommodityBoughtList().get(0).getUsed(), 0.0001);

        // Compare peak
        Assert.assertEquals(80, pm.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getPeak(), 0.0001);
        Assert.assertEquals(20, vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                        .get(0).getCommodityBoughtList().get(0).getPeak(), 0.0001);

        // Get scenario change
        Long baselineDate = 123456789L;
        List<ScenarioChange> changes = getScenarioChangeWithBaselineDate(baselineDate);
        // Get TopologyInfo : Scope to PMs OID
        List<Long> scopeOids = Lists.newArrayList(1L);
        TopologyDTO.TopologyInfo topoInfo = getTopologyInfoWithGivenScope(scopeOids);

        // Mocks for return value from database for VM(Used :50 , Peak : 100)
        // Provider Id different than current provider.
        GetEntityStatsResponse response = GetEntityStatsResponse.newBuilder()
                        .addEntityStats(getEntityStats(2, "3", 100f, 50f, "Mem")).build();
        Mockito.when(statsHistoryService.getEntityStats(Mockito.any())).thenReturn(response);

        CommoditiesEditor commEditor = new CommoditiesEditor(historyClient);
        commEditor.editCommoditiesForBaselineChanges(g, changes, topoInfo);

        // Check values after calling CommoditiesEditor.
        // Compare used
        // Expected value used for PM : used - currUsedForVM + usedFromDB : 70 - 10 + 50
        // Expected value used for VM : as fetched from database : 50
        Assert.assertEquals(70 - 10 + 50, pm.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getUsed(), 0.0001);
        Assert.assertEquals(50, vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                        .get(0).getCommodityBoughtList().get(0).getUsed(), 0.0001);

        // Compare peak
        // Expected value peak for PM : peak - currPeakForVM + peakFromDB : 80 - 20 + 100
        // Expected value peak for VM : as fetched from database : 100
        Assert.assertEquals(80 - 20 + 100, pm.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getPeak(), 0.0001);
        Assert.assertEquals(100, vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                        .get(0).getCommodityBoughtList().get(0).getPeak(), 0.0001);
    }

    @Test
    public void testEditCommoditiesForMultipleProviders() throws IOException {
        // Set virtual machine with commodities bought from both storages.
        TopologyEntity.Builder vmBuilder = TopologyEntityUtils.topologyEntityBuilder(
                        TopologyEntityDTO.newBuilder().setOid(3).addCommoditiesBoughtFromProviders(
                        CommoditiesBoughtFromProvider.newBuilder().addCommodityBought(
                                        CommodityBoughtDTO.newBuilder().setCommodityType(
                                            CommodityType.newBuilder().setType(CommodityDTO.CommodityType.STORAGE_AMOUNT.getNumber()).setKey("").build())
                                        .setActive(true)
                                        .setUsed(10)
                                        .setPeak(20))
                                    .setProviderId(1L))
        .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder().addCommodityBought(
                                        CommodityBoughtDTO.newBuilder().setCommodityType(
                                            CommodityType.newBuilder().setType(CommodityDTO.CommodityType.STORAGE_AMOUNT.getNumber()).setKey("").build())
                                        .setActive(true)
                                        .setUsed(10)
                                        .setPeak(20))
                                    .setProviderId(2L))
        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE));
        final Map<Long, TopologyEntity.Builder> topology = new HashMap<>();

        // Set two storages with commodities sold.
        topology.put(1L, buildTopologyEntityWithCommSold(1L, CommodityDTO.CommodityType.STORAGE_AMOUNT.getNumber(),
                        EntityType.STORAGE_VALUE));
        topology.put(2L, buildTopologyEntityWithCommSold(2L, CommodityDTO.CommodityType.STORAGE_AMOUNT.getNumber(),
                        EntityType.STORAGE_VALUE));
        topology.put(3L, vmBuilder);

        final TopologyGraph g = TopologyGraph.newGraph(topology);

        TopologyEntity st1 = g.getEntity(1L).get();
        TopologyEntity st2 = g.getEntity(2L).get();
        TopologyEntity vm = g.getEntity(3L).get();

        // Check values before calling CommoditiesEditor.
        // Compare used
        Assert.assertEquals(70, st1.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getUsed(), 0.0001);
        Assert.assertEquals(70, st2.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getUsed(), 0.0001);
        Assert.assertEquals(10, vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                        .get(0).getCommodityBoughtList().get(0).getUsed(), 0.0001);

        // Compare peak
        Assert.assertEquals(80, st1.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getPeak(), 0.0001);
        Assert.assertEquals(80, st2.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getPeak(), 0.0001);
        Assert.assertEquals(20, vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                        .get(0).getCommodityBoughtList().get(0).getPeak(), 0.0001);

        // Get scenario change
        Long baselineDate = 123456789L;
        List<ScenarioChange> changes = getScenarioChangeWithBaselineDate(baselineDate);
        // Get TopologyInfo : Scope to PMs OID
        List<Long> scopeOids = Lists.newArrayList(1L);
        TopologyDTO.TopologyInfo topoInfo = getTopologyInfoWithGivenScope(scopeOids);

        // Mocks for return value from database for VM(Used :50 , Peak : 100)
        // Provider Id different than current provider.
        GetEntityStatsResponse response = GetEntityStatsResponse.newBuilder()
                        .addEntityStats(getEntityStats(3, "5", 100f, 50f, "StorageAmount"))
                        .addEntityStats(getEntityStats(3, "6", 40f, 30f, "StorageAmount"))
                        .build();
        Mockito.when(statsHistoryService.getEntityStats(Mockito.any())).thenReturn(response);

        CommoditiesEditor commEditor = new CommoditiesEditor(historyClient);
        commEditor.editCommoditiesForBaselineChanges(g, changes, topoInfo);

        // Check values after calling CommoditiesEditor.
        // Compare used
        // Expected value used for Storage1 : used - currUsedForVM + usedFromDB : 70 - 10 + 50
        // Expected value used for Storage2 : used - currUsedForVM + usedFromDB : 70 - 10 + 30
        // Expected value used for VM form first commodity: as fetched from database : 50
        // Expected value used for VM form second commodity: as fetched from database : 30
        Assert.assertEquals(70 - 10 + 50, st1.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getUsed(), 0.0001);
        Assert.assertEquals(70 - 10 + 30, st2.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getUsed(), 0.0001);
        Assert.assertEquals(50, vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                        .get(0).getCommodityBoughtList().get(0).getUsed(), 0.0001);
        Assert.assertEquals(30, vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                        .get(1).getCommodityBoughtList().get(0).getUsed(), 0.0001);

        // Compare peak
        // Expected value peak for Storage1 : peak - currPeakForVM + peakFromDB : 80 - 20 + 100
        // Expected value peak for Storage2 : used - currUsedForVM + usedFromDB : 80 - 20 + 40
        // Expected value peak for VM : as fetched from database : 100
        // Expected value peak for VM form second commodity: as fetched from database : 30
        Assert.assertEquals(80 - 20 + 100, st1.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getPeak(), 0.0001);
        Assert.assertEquals(80 - 20 + 40, st2.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getPeak(), 0.0001);
        Assert.assertEquals(100, vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                        .get(0).getCommodityBoughtList().get(0).getPeak(), 0.0001);
        Assert.assertEquals(40, vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                        .get(1).getCommodityBoughtList().get(0).getPeak(), 0.0001);
    }

    @Test
    public void testEditCommoditiesForAccessCommodties() throws IOException {
        // Get graph
        // Sets value for VM(Used :10 , Peak : 20)
        // Sets value for PM(Used : 70, Peak : 80)
        TopologyGraph g = createGraph(CommodityDTO.CommodityType.STORAGE_ACCESS);

        TopologyEntity pm = g.getEntity(1L).get();
        TopologyEntity vm = g.getEntity(2L).get();

        // Check values before calling CommoditiesEditor.
        // Compare used
        Assert.assertEquals(70, pm.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getUsed(), 0.0001);
        Assert.assertEquals(10, vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                        .get(0).getCommodityBoughtList().get(0).getUsed(), 0.0001);

        // Compare peak
        Assert.assertEquals(80, pm.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getPeak(), 0.0001);
        Assert.assertEquals(20, vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                        .get(0).getCommodityBoughtList().get(0).getPeak(), 0.0001);

        // Get scenario change
        Long baselineDate = 123456789L;
        List<ScenarioChange> changes = getScenarioChangeWithBaselineDate(baselineDate);
        // Get TopologyInfo : Scope to PMs OID
        List<Long> scopeOids = Lists.newArrayList(1L);
        TopologyDTO.TopologyInfo topoInfo = getTopologyInfoWithGivenScope(scopeOids);

        // Mocks for return value from database for VM(Used :50 , Peak : 100)
        // Provider Id different than current provider.
        GetEntityStatsResponse response = GetEntityStatsResponse.newBuilder()
                        .addEntityStats(getEntityStats(2, "3", 100f, 50f, "Mem")).build();
        Mockito.when(statsHistoryService.getEntityStats(Mockito.any())).thenReturn(response);

        CommoditiesEditor commEditor = new CommoditiesEditor(historyClient);
        commEditor.editCommoditiesForBaselineChanges(g, changes, topoInfo);

        // Check values after calling CommoditiesEditor.
        // Expected : Before and after values should be same because commodity is an access commodity.
        // Compare used
        Assert.assertEquals(70, pm.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getUsed(), 0.0001);
        Assert.assertEquals(10, vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                        .get(0).getCommodityBoughtList().get(0).getUsed(), 0.0001);

        // Compare peak
        Assert.assertEquals(80, pm.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getPeak(), 0.0001);
        Assert.assertEquals(20, vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                        .get(0).getCommodityBoughtList().get(0).getPeak(), 0.0001);
    }

    @Test
    public void testEditCommoditiesForVMValuesGreaterThanPM() throws IOException {
        // Get graph
        // Sets value for VM(Used :10 , Peak : 20)
        // Sets value for PM(Used : 70, Peak : 80)
        TopologyGraph g = createGraph(CommodityDTO.CommodityType.MEM);

        TopologyEntity pm = g.getEntity(1L).get();
        TopologyEntity vm = g.getEntity(2L).get();

        // For VM : Set current peak and used greater than PM(Used : 70, Peak : 80) : VM(Used : 90, Peak : 200)
        vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersBuilderList().get(0)
                        .getCommodityBoughtBuilderList().get(0).setUsed(90);
        vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersBuilderList().get(0)
                        .getCommodityBoughtBuilderList().get(0).setPeak(200);

        // Check values before calling CommoditiesEditor.
        // Compare used
        Assert.assertEquals(70, pm.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getUsed(), 0.0001);
        Assert.assertEquals(90, vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                        .get(0).getCommodityBoughtList().get(0).getUsed(), 0.0001);

        // Compare peak
        Assert.assertEquals(80, pm.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getPeak(), 0.0001);
        Assert.assertEquals(200, vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                        .get(0).getCommodityBoughtList().get(0).getPeak(), 0.0001);

        // Get scenario change
        Long baselineDate = 123456789L;
        List<ScenarioChange> changes = getScenarioChangeWithBaselineDate(baselineDate);
        // Get TopologyInfo : Scope to PMs OID
        List<Long> scopeOids = Lists.newArrayList(1L);
        TopologyDTO.TopologyInfo topoInfo = getTopologyInfoWithGivenScope(scopeOids);

        // Mocks for return value from database for VM(Used :50 , Peak : 100)
        // Provider id is same as current provider.
        GetEntityStatsResponse response = GetEntityStatsResponse.newBuilder()
                        .addEntityStats(getEntityStats(2, "1", 100f, 50f, "Mem")).build();
        Mockito.when(statsHistoryService.getEntityStats(Mockito.any())).thenReturn(response);

        CommoditiesEditor commEditor = new CommoditiesEditor(historyClient);
        commEditor.editCommoditiesForBaselineChanges(g, changes, topoInfo);

        // Check values after calling CommoditiesEditor.
        // Compare used
        // Expected value used for PM : used - currUsedForVM < 0 , hence used = 50 (asfetchedFromDb)
        // Expected value used for VM : as fetched from database : 50
        Assert.assertEquals(50, pm.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getUsed(), 0.0001);
        Assert.assertEquals(50, vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                        .get(0).getCommodityBoughtList().get(0).getUsed(), 0.0001);

        // Compare peak
        // Expected value peak for PM : peak - currPeakForVM < 0, hence peak = 100 (asfetchedFromDb)
        // Expected value peak for VM : as fetched from database : 100
        Assert.assertEquals(100, pm.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getPeak(), 0.0001);
        Assert.assertEquals(100, vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                        .get(0).getCommodityBoughtList().get(0).getPeak(), 0.0001);
    }
}
