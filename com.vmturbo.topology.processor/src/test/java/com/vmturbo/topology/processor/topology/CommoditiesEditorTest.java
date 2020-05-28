package com.vmturbo.topology.processor.topology;

import static com.vmturbo.topology.processor.topology.TopologyEntityUtils.buildTopologyEntityWithCommSold;
import static com.vmturbo.topology.processor.topology.TopologyEntityUtils.createGraph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScope;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScopeEntry;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges.HistoricalBaseline;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;
import com.vmturbo.common.protobuf.stats.Stats.SystemLoadInfoResponse;
import com.vmturbo.common.protobuf.stats.Stats.SystemLoadRecord;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.StatsMoles.StatsHistoryServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PlanTopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * Test editing of commodities.
 */
public class CommoditiesEditorTest {
    private static final double DELTA = 0.0001D;

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
        TopologyGraph<TopologyEntity> g = createGraph(CommodityDTO.CommodityType.MEM, 70, 80, 10, 20);

        TopologyEntity pm = g.getEntity(1L).get();
        TopologyEntity vm = g.getEntity(2L).get();

        // Check values before calling CommoditiesEditor.
        // Compare used
        Assert.assertEquals(70, pm.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getUsed(), DELTA);
        Assert.assertEquals(10, vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                        .get(0).getCommodityBoughtList().get(0).getUsed(), DELTA);

        // Compare peak
        Assert.assertEquals(80, pm.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getPeak(), DELTA);
        Assert.assertEquals(20, vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                        .get(0).getCommodityBoughtList().get(0).getPeak(), DELTA);

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
        commEditor.applyCommodityEdits(g, changes, topoInfo, PlanScope.getDefaultInstance(), null);

        // Check values after calling CommoditiesEditor.
        // Compare used
        // Expected value used for PM : used - currUsedForVM + usedFromDB : 70 - 10 + 50
        // Expected value used for VM : as fetched from database : 50
        Assert.assertEquals(70 - 10 + 50, pm.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getUsed(), DELTA);
        Assert.assertEquals(50, vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                        .get(0).getCommodityBoughtList().get(0).getUsed(), DELTA);

        // Compare peak
        // Expected value peak for PM : peak - currPeakForVM + peakFromDB : 80 - 20 + 100
        // Expected value peak for VM : as fetched from database : 100
        Assert.assertEquals(80 - 20 + 100, pm.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getPeak(), DELTA);
        Assert.assertEquals(100, vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                        .get(0).getCommodityBoughtList().get(0).getPeak(), DELTA);
    }

    @Test
    public void testEditCommoditiesForDifferntProviderInDatabase() throws IOException {
        // Get graph
        // Sets value for VM(Used :10 , Peak : 20)
        // Sets value for PM(Used : 70, Peak : 80)
        TopologyGraph<TopologyEntity> g = createGraph(CommodityDTO.CommodityType.MEM, 70, 80, 10, 20);

        TopologyEntity pm = g.getEntity(1L).get();
        TopologyEntity vm = g.getEntity(2L).get();

        // Check values before calling CommoditiesEditor.
        // Compare used
        Assert.assertEquals(70, pm.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getUsed(), DELTA);
        Assert.assertEquals(10, vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                        .get(0).getCommodityBoughtList().get(0).getUsed(), DELTA);

        // Compare peak
        Assert.assertEquals(80, pm.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getPeak(), DELTA);
        Assert.assertEquals(20, vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                        .get(0).getCommodityBoughtList().get(0).getPeak(), DELTA);

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
        commEditor.applyCommodityEdits(g, changes, topoInfo, PlanScope.getDefaultInstance(), null);

        // Check values after calling CommoditiesEditor.
        // Compare used
        // Expected value used for PM : used - currUsedForVM + usedFromDB : 70 - 10 + 50
        // Expected value used for VM : as fetched from database : 50
        Assert.assertEquals(70 - 10 + 50, pm.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getUsed(), DELTA);
        Assert.assertEquals(50, vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                        .get(0).getCommodityBoughtList().get(0).getUsed(), DELTA);

        // Compare peak
        // Expected value peak for PM : peak - currPeakForVM + peakFromDB : 80 - 20 + 100
        // Expected value peak for VM : as fetched from database : 100
        Assert.assertEquals(80 - 20 + 100, pm.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getPeak(), DELTA);
        Assert.assertEquals(100, vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                        .get(0).getCommodityBoughtList().get(0).getPeak(), DELTA);
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
                        EntityType.STORAGE_VALUE, 70, 80));
        topology.put(2L, buildTopologyEntityWithCommSold(2L, CommodityDTO.CommodityType.STORAGE_AMOUNT.getNumber(),
                        EntityType.STORAGE_VALUE, 70, 80));
        topology.put(3L, vmBuilder);

        final TopologyGraph<TopologyEntity> g = TopologyEntityTopologyGraphCreator.newGraph(topology);

        TopologyEntity st1 = g.getEntity(1L).get();
        TopologyEntity st2 = g.getEntity(2L).get();
        TopologyEntity vm = g.getEntity(3L).get();

        // Check values before calling CommoditiesEditor.
        // Compare used
        Assert.assertEquals(70, st1.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getUsed(), DELTA);
        Assert.assertEquals(70, st2.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getUsed(), DELTA);
        Assert.assertEquals(10, vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                        .get(0).getCommodityBoughtList().get(0).getUsed(), DELTA);

        // Compare peak
        Assert.assertEquals(80, st1.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getPeak(), DELTA);
        Assert.assertEquals(80, st2.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getPeak(), DELTA);
        Assert.assertEquals(20, vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                        .get(0).getCommodityBoughtList().get(0).getPeak(), DELTA);

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
        commEditor.applyCommodityEdits(g, changes, topoInfo, PlanScope.getDefaultInstance(), null);

        // Check values after calling CommoditiesEditor.
        // Compare used
        // Expected value used for Storage1 : used - currUsedForVM + usedFromDB : 70 - 10 + 50
        // Expected value used for Storage2 : used - currUsedForVM + usedFromDB : 70 - 10 + 30
        // Expected value used for VM form first commodity: as fetched from database : 50
        // Expected value used for VM form second commodity: as fetched from database : 30
        Assert.assertEquals(70 - 10 + 50, st1.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getUsed(), DELTA);
        Assert.assertEquals(70 - 10 + 30, st2.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getUsed(), DELTA);
        Assert.assertEquals(50, vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                        .get(0).getCommodityBoughtList().get(0).getUsed(), DELTA);
        Assert.assertEquals(30, vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                        .get(1).getCommodityBoughtList().get(0).getUsed(), DELTA);

        // Compare peak
        // Expected value peak for Storage1 : peak - currPeakForVM + peakFromDB : 80 - 20 + 100
        // Expected value peak for Storage2 : used - currUsedForVM + usedFromDB : 80 - 20 + 40
        // Expected value peak for VM : as fetched from database : 100
        // Expected value peak for VM form second commodity: as fetched from database : 30
        Assert.assertEquals(80 - 20 + 100, st1.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getPeak(), DELTA);
        Assert.assertEquals(80 - 20 + 40, st2.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getPeak(), DELTA);
        Assert.assertEquals(100, vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                        .get(0).getCommodityBoughtList().get(0).getPeak(), DELTA);
        Assert.assertEquals(40, vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                        .get(1).getCommodityBoughtList().get(0).getPeak(), DELTA);
    }

    @Test
    public void testEditCommoditiesForAccessCommodties() throws IOException {
        // Get graph
        // Sets value for VM(Used :10 , Peak : 20)
        // Sets value for PM(Used : 70, Peak : 80)
        TopologyGraph<TopologyEntity> g = createGraph(CommodityDTO.CommodityType.STORAGE_ACCESS, 70, 80, 10, 20);

        TopologyEntity pm = g.getEntity(1L).get();
        TopologyEntity vm = g.getEntity(2L).get();

        // Check values before calling CommoditiesEditor.
        // Compare used
        Assert.assertEquals(70, pm.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getUsed(), DELTA);
        Assert.assertEquals(10, vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                        .get(0).getCommodityBoughtList().get(0).getUsed(), DELTA);

        // Compare peak
        Assert.assertEquals(80, pm.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getPeak(), DELTA);
        Assert.assertEquals(20, vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                        .get(0).getCommodityBoughtList().get(0).getPeak(), DELTA);

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
        commEditor.applyCommodityEdits(g, changes, topoInfo, PlanScope.getDefaultInstance(), null);

        // Check values after calling CommoditiesEditor.
        // Expected : Before and after values should be same because commodity is an access commodity.
        // Compare used
        Assert.assertEquals(70, pm.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getUsed(), DELTA);
        Assert.assertEquals(10, vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                        .get(0).getCommodityBoughtList().get(0).getUsed(), DELTA);

        // Compare peak
        Assert.assertEquals(80, pm.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getPeak(), DELTA);
        Assert.assertEquals(20, vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                        .get(0).getCommodityBoughtList().get(0).getPeak(), DELTA);
    }

    @Test
    public void testEditCommoditiesForVMValuesGreaterThanPM() throws IOException {
        // Get graph
        // Sets value for VM(Used :10 , Peak : 20)
        // Sets value for PM(Used : 70, Peak : 80)
        TopologyGraph<TopologyEntity> g = createGraph(CommodityDTO.CommodityType.MEM, 70, 80, 10, 20);

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
                        .get(0).getUsed(), DELTA);
        Assert.assertEquals(90, vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                        .get(0).getCommodityBoughtList().get(0).getUsed(), DELTA);

        // Compare peak
        Assert.assertEquals(80, pm.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getPeak(), DELTA);
        Assert.assertEquals(200, vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                        .get(0).getCommodityBoughtList().get(0).getPeak(), DELTA);

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
        commEditor.applyCommodityEdits(g, changes, topoInfo, PlanScope.getDefaultInstance(), null);

        // Check values after calling CommoditiesEditor.
        // Compare used
        // Expected value used for PM : used - currUsedForVM < 0 , hence used = 50 (asfetchedFromDb)
        // Expected value used for VM : as fetched from database : 50
        Assert.assertEquals(50, pm.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getUsed(), DELTA);
        Assert.assertEquals(50, vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                        .get(0).getCommodityBoughtList().get(0).getUsed(), DELTA);

        // Compare peak
        // Expected value peak for PM : peak - currPeakForVM < 0, hence peak = 100 (asfetchedFromDb)
        // Expected value peak for VM : as fetched from database : 100
        Assert.assertEquals(100, pm.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getPeak(), DELTA);
        Assert.assertEquals(100, vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                        .get(0).getCommodityBoughtList().get(0).getPeak(), DELTA);
    }

    @Test
    public void testEditCommoditiesForClusterHeadroom() {
        // Get graph
        // Sets value for VM(Used :10 , Peak : 20)
        // Sets value for PM(Used : 70, Peak : 80)
        TopologyGraph<TopologyEntity> g = createGraph(CommodityDTO.CommodityType.MEM, 70, 80, 10, 20);

        TopologyEntity pm = g.getEntity(1L).get();
        TopologyEntity vm = g.getEntity(2L).get();

        // Check values before calling CommoditiesEditor.
        // Compare used
        Assert.assertEquals(70, pm.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getUsed(), DELTA);
        Assert.assertEquals(10, vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                        .get(0).getCommodityBoughtList().get(0).getUsed(), DELTA);

        // Compare peak
        Assert.assertEquals(80, pm.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getPeak(), DELTA);
        Assert.assertEquals(20, vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                        .get(0).getCommodityBoughtList().get(0).getPeak(), DELTA);

        List<SystemLoadInfoResponse> response = Collections.singletonList(
            SystemLoadInfoResponse.newBuilder()
                .addRecord(SystemLoadRecord.newBuilder()
                    .setPropertyType(CommodityDTO.CommodityType.MEM.name())
                    .setAvgValue(50)
                    .setMaxValue(100)
                    .setUuid(2L) // uuid of VM
                    .build())
                .build());

        Mockito.when(statsHistoryService.getSystemLoadInfo(Mockito.any())).thenReturn(response);

        PlanScope scope = PlanScope.newBuilder()
            .addScopeEntries(PlanScopeEntry.newBuilder()
                .setScopeObjectOid(10L)
                .build())
            .build();

        TopologyInfo topologyInfo = TopologyInfo.newBuilder().setPlanInfo(PlanTopologyInfo.newBuilder()
                .setPlanProjectType(com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType.CLUSTER_HEADROOM)
                .build())
            .build();

        CommoditiesEditor commEditor = new CommoditiesEditor(historyClient);
        commEditor.applyCommodityEdits(g, new ArrayList<ScenarioChange>(), topologyInfo, scope, null);

        // Check values after calling CommoditiesEditor.
        // Compare used
        // Expected value used for PM : used - currUsedForVM + usedFromSystemLoad : 70 - 10 + 50
        // Expected value used for VM : as fetched from SystemLoad : 50
        Assert.assertEquals(70 - 10 + 50, pm.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getUsed(), DELTA);
        Assert.assertEquals(50, vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                        .get(0).getCommodityBoughtList().get(0).getUsed(), DELTA);

        // Compare peak
        // Expected value peak for PM : peak - currPeakForVM + peakFromSystemLoad : 80 - 20 + 100
        // Expected value peak for VM : as fetched from SystemLoad : 100
        Assert.assertEquals(80 - 20 + 100, pm.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getPeak(), DELTA);
        Assert.assertEquals(100, vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                        .get(0).getCommodityBoughtList().get(0).getPeak(), DELTA);
    }

    @Test
    public void testEditCommoditiesForCommSoldByVM() {
        final Map<Long, TopologyEntity.Builder> topology = new HashMap<>();
        // Set virtual machine with commodities sold.
        // Sets commodity value as - Used : 70, Peak : 80
        topology.put(1L, buildTopologyEntityWithCommSold(1L, CommodityDTO.CommodityType.VMEM.getNumber(),
                        EntityType.VIRTUAL_MACHINE_VALUE, 70, 80));
        final TopologyGraph<TopologyEntity> g = TopologyEntityTopologyGraphCreator.newGraph(topology);
        // Check values before calling CommoditiesEditor.
        // Compare used
        TopologyEntity vm = g.getEntity(1L).get();
        Assert.assertEquals(70, vm.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getUsed(), DELTA);
        Assert.assertEquals(80, vm.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getPeak(), DELTA);

        List<SystemLoadInfoResponse> response = Collections.singletonList(
            SystemLoadInfoResponse.newBuilder()
                .addRecord(SystemLoadRecord.newBuilder()
                    .setPropertyType(CommodityDTO.CommodityType.VMEM.name())
                    .setAvgValue(50)
                    .setMaxValue(100)
                    .setUuid(1L) // uuid of VM
                    .build())
                .build());

        Mockito.when(statsHistoryService.getSystemLoadInfo(Mockito.any())).thenReturn(response);

        PlanScope scope = PlanScope.newBuilder()
            .addScopeEntries(PlanScopeEntry.newBuilder()
                .setScopeObjectOid(10L)
                .build())
            .build();

        TopologyInfo topologyInfo = TopologyInfo.newBuilder().setPlanInfo(PlanTopologyInfo.newBuilder()
                .setPlanProjectType(com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType.CLUSTER_HEADROOM)
                .build())
            .build();

        CommoditiesEditor commEditor = new CommoditiesEditor(historyClient);
        commEditor.applyCommodityEdits(g, new ArrayList<ScenarioChange>(), topologyInfo, scope, null);

        // Check values after calling CommoditiesEditor.
        // Compare used and peak
        // Expected value used for VM : as fetched from SystemLoad : 50
        Assert.assertEquals(50, vm.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getUsed(), DELTA);
        Assert.assertEquals(100, vm.getTopologyEntityDtoBuilder().getCommoditySoldListList()
                        .get(0).getPeak(), DELTA);
    }

}
