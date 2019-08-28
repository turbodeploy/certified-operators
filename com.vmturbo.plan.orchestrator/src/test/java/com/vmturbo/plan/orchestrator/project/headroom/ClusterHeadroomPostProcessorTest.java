package com.vmturbo.plan.orchestrator.project.headroom;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory;
import com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory.ResourcesCategoryName;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateField;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateResource;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetMultiSupplyChainsResponse;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.repository.SupplyChainProtoMoles.SupplyChainServiceMole;
import com.vmturbo.common.protobuf.stats.Stats.CommodityHeadroom;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.SaveClusterHeadroomRequest;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.StatsMoles.StatsHistoryServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;
import com.vmturbo.plan.orchestrator.plan.PlanDao;
import com.vmturbo.plan.orchestrator.project.ProjectPlanPostProcessor;
import com.vmturbo.plan.orchestrator.templates.TemplatesDao;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class ClusterHeadroomPostProcessorTest {

    private static final long PLAN_ID = 7;
    private static final long CLUSTER_ID = 10;
    private static final int MORE_THAN_A_YEAR = 3650;
    private static final long ONE_HOUR = 3600000;
    // Milliseconds in a day
    private static final long DAY_MILLI_SECS = TimeUnit.DAYS.toMillis(1);

    private static final Group CLUSTER = Group.newBuilder()
            .setType(Group.Type.CLUSTER)
            .setId(CLUSTER_ID)
            .setCluster(ClusterInfo.newBuilder()
                    .setName("foo"))
            .build();

    private RepositoryServiceMole repositoryServiceMole = spy(new RepositoryServiceMole());

    private StatsHistoryServiceMole historyServiceMole = spy(new StatsHistoryServiceMole());

    private SupplyChainServiceMole supplyChainServiceMole = spy(new SupplyChainServiceMole());

    private GroupServiceMole groupRpcServiceMole = spy(new GroupServiceMole());

    private PlanDao planDao = mock(PlanDao.class);

    private TemplatesDao templatesDao = mock(TemplatesDao.class);

    @Rule
    public GrpcTestServer grpcTestServer =
            GrpcTestServer.newServer(repositoryServiceMole, historyServiceMole,
                    supplyChainServiceMole, groupRpcServiceMole);

    @Test
    public void testProjectedTopologyWithHeadroomValues() {
        prepareForHeadroomCalculation(true, true);

        // VmGrowth = 0 (because we don't have data in the past)
        verify(historyServiceMole).saveClusterHeadroom(SaveClusterHeadroomRequest.newBuilder()
                .setClusterId(CLUSTER_ID)
                // Template Value CPU_SPEED = 10, consumedFactor = 0.5, effectiveUsed = 5
                // PM CPU value : used = 50, capacity = 100
                // CPU headroom calculation :
                // headroomCapacity = capacity / effectiveUsed = 20, headroomAvailable = (capacity - used) / effectiveUsed = 10
                // daysToExhaust = MORE_THAN_A_YEAR because VmGrowth = 0
                .setCpuHeadroomInfo(CommodityHeadroom.newBuilder()
                    .setHeadroom(10)
                    .setCapacity(20)
                    .setDaysToExhaustion(MORE_THAN_A_YEAR))
                // Template Value MEMORY_SIZE = 100, consumedFactor = 0.4, effectiveUsed = 40
                // PM MEM value : used = 40, capacity = 200
                // MEM headroom calculation :
                // headroomCapacity = capacity / effectiveUsed = 5, headroomAvailable = (capacity - used) / effectiveUsed = 4
                // daysToExhaust = MORE_THAN_A_YEAR because VmGrowth = 0
                .setMemHeadroomInfo(CommodityHeadroom.newBuilder()
                    .setCapacity(5)
                    .setHeadroom(4)
                    .setDaysToExhaustion(MORE_THAN_A_YEAR))
                // Template Value DISK_SIZE = 200, consumedFactor = 1, effectiveUsed = 200
                // Storage value : used = 100, capacity = 600
                // Storage headroom calculation :
                // headroomCapacity = capacity / effectiveUsed = 3, headroomAvailable = (capacity - used) / effectiveUsed = 2
                // daysToExhaust = MORE_THAN_A_YEAR because VmGrowth = 0
                .setStorageHeadroomInfo(CommodityHeadroom.newBuilder()
                    .setCapacity(3)
                    .setHeadroom(2)
                    .setDaysToExhaustion(MORE_THAN_A_YEAR))
                .setNumHosts(1)
                .setNumVMs(2)
                .setNumStorages(1)
                .setMonthlyVMGrowth(0) // (vmGrowth * daysInMonth) / PeakLookBack days = (0 * 30)/7 = 0
                .setHeadroom(2) // minimum of mem, cpu and storage headroom values : min(10, 4, 2)
                .build());
    }

    @Test
    public void testProjectedTopologyWithInvalidValuesInTemplate() {
        prepareForHeadroomCalculation(false, true);

        verify(historyServiceMole).saveClusterHeadroom(SaveClusterHeadroomRequest.newBuilder()
                        .setClusterId(CLUSTER_ID)
                        .setCpuHeadroomInfo(CommodityHeadroom.newBuilder()
                            .setHeadroom(10)
                            .setCapacity(20)
                            .setDaysToExhaustion(MORE_THAN_A_YEAR))
                        .setMemHeadroomInfo(CommodityHeadroom.newBuilder()
                            .setCapacity(5)
                            .setHeadroom(4)
                            .setDaysToExhaustion(MORE_THAN_A_YEAR))
                         // Every thing same except for Storage headroom because storage commodities had zero usage.
                        .setStorageHeadroomInfo(CommodityHeadroom.getDefaultInstance())
                        .setNumHosts(1)
                        .setNumVMs(2)
                        .setNumStorages(1)
                        .setMonthlyVMGrowth(0) // (vmGrowth * daysInMonth) / PeakLookback days = (0 * 30)/7 = 0
                        .setHeadroom(0) // minimum of mem, cpu and storage headroom values : min(10, 4, 0)
                        .build());
    }

    /**
     * Test headroom with projected topology containing inactive host.
     */
    @Test
    public void testProjectedTopologyWithInActiveHost() {
        prepareForHeadroomCalculation(true, false);

        // VmGrowth = 0 (because we don't have data in the past)
        verify(historyServiceMole).saveClusterHeadroom(SaveClusterHeadroomRequest.newBuilder()
            .setClusterId(CLUSTER_ID)
            .setNumVMs(0L)
            // No active host
            .setCpuHeadroomInfo(CommodityHeadroom.newBuilder()
                .setHeadroom(0)
                .setCapacity(0)
                .setDaysToExhaustion(0))
            // No active host
            .setMemHeadroomInfo(CommodityHeadroom.newBuilder()
                .setCapacity(0)
                .setHeadroom(0)
                .setDaysToExhaustion(0))
            // Template Value DISK_SIZE = 200, consumedFactor = 1, effectiveUsed = 200
            // Storage value : used = 100, capacity = 600
            // Storage headroom calculation :
            // headroomCapacity = capacity / effectiveUsed = 3, headroomAvailable = (capacity - used) / effectiveUsed = 2
            // daysToExhaust = MORE_THAN_A_YEAR because VmGrowth = 0
            .setStorageHeadroomInfo(CommodityHeadroom.newBuilder()
                .setCapacity(3)
                .setHeadroom(2)
                .setDaysToExhaustion(MORE_THAN_A_YEAR))
            .setNumHosts(1)
            .setNumVMs(2)
            .setNumStorages(1)
            .setMonthlyVMGrowth(0) // (vmGrowth * daysInMonth) / PeakLookback days = (0 * 30)/7 = 0
            .setHeadroom(0) // minimum of mem, cpu and storage headroom values : min(0, 0, 2)
            .build());
    }

    /**
     * Prepare for headroom calculation.
     *
     * @param setValidValues whether to set valid values or not
     * @param setHostActive whether to set host active or not
     */
    private void prepareForHeadroomCalculation(boolean setValidValues, boolean setHostActive) {
        when(groupRpcServiceMole.getGroups(any()))
            .thenReturn(Collections.singletonList(Group.newBuilder().setId(CLUSTER_ID).build()));

        final ClusterHeadroomPlanPostProcessor processor =
            spy(new ClusterHeadroomPlanPostProcessor(PLAN_ID, Collections.singleton(CLUSTER.getId()),
                grpcTestServer.getChannel(), grpcTestServer.getChannel(),
                planDao, grpcTestServer.getChannel(), templatesDao));
        final long projectedTopologyId = 100;

        when(repositoryServiceMole.retrieveTopology(RetrieveTopologyRequest.newBuilder()
            .setTopologyId(projectedTopologyId)
            .build())).thenReturn(getProjectedTopology(setHostActive));

        when(templatesDao.getTemplate(Mockito.anyLong()))
            .thenReturn(Optional.of(getTemplateForHeadroom(setValidValues)));

        final List<GetMultiSupplyChainsResponse> supplyChainResponses = ImmutableList.of(
            GetMultiSupplyChainsResponse.newBuilder().setSeedOid(CLUSTER.getId())
                .setSupplyChain(SupplyChain.newBuilder()
                    .addSupplyChainNodes(SupplyChainNode.newBuilder()
                        .setEntityType(StringConstants.VIRTUAL_MACHINE)
                        .putMembersByState(com.vmturbo.api.enums.EntityState.ACTIVE.ordinal(),
                            MemberList.newBuilder().addMemberOids(7).addMemberOids(99).build())
                        .build())
                    .addSupplyChainNodes(SupplyChainNode.newBuilder()
                        .setEntityType(StringConstants.PHYSICAL_MACHINE)
                        .putMembersByState(com.vmturbo.api.enums.EntityState.ACTIVE.ordinal(),
                            MemberList.newBuilder().addMemberOids(8).build())
                        .build())
                    .addSupplyChainNodes(SupplyChainNode.newBuilder()
                        .setEntityType(StringConstants.STORAGE)
                        .putMembersByState(com.vmturbo.api.enums.EntityState.ACTIVE.ordinal(),
                            MemberList.newBuilder().addMemberOids(9).build())
                        .build()))
                .build());
        when(supplyChainServiceMole.getMultiSupplyChains(any())).thenReturn(supplyChainResponses);

        processor.onPlanStatusChanged(PlanInstance.newBuilder()
            .setPlanId(PLAN_ID)
            .setStatus(PlanStatus.WAITING_FOR_RESULT)
            .setProjectedTopologyId(projectedTopologyId)
            .build());
    }

    /**
     * Get the projected topology.
     *
     * @param setHostActive whether to set host active or not
     * @return a list of {@link RetrieveTopologyResponse}
     */
    private List<RetrieveTopologyResponse> getProjectedTopology(boolean setHostActive) {
        return Collections.singletonList(RetrieveTopologyResponse.newBuilder()
            // add 2 VMs
            .addEntities(TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .addAllCommoditiesBoughtFromProviders(
                    ImmutableList.of(CommoditiesBoughtFromProvider.getDefaultInstance()))
                .setOid(7))
            .addEntities(TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .addAllCommoditiesBoughtFromProviders(
                    ImmutableList.of(CommoditiesBoughtFromProvider.getDefaultInstance()))
                .setOid(99))
            // add PM
            .addEntities(TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                .setEntityState(setHostActive ? EntityState.POWERED_ON : EntityState.MAINTENANCE)
                .setOid(8)
                .addAllCommoditySoldList(
                    ImmutableList.of(CommoditySoldDTO.newBuilder()
                            .setActive(true)
                            .setCommodityType(com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType.newBuilder()
                                .setType(CommodityType.CPU_VALUE))
                            .setCapacity(100)
                            .setUsed(50)
                            .build(),
                        CommoditySoldDTO.newBuilder()
                            .setActive(true)
                            .setCommodityType(com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType.newBuilder()
                                .setType(CommodityType.MEM_VALUE))
                            .setCapacity(200)
                            .setUsed(40)
                            .build())))
            // add Storage
            .addEntities(TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.STORAGE_VALUE)
                .setEntityState(EntityState.POWERED_ON)
                .setOid(9)
                .addAllCommoditySoldList(
                    ImmutableList.of(CommoditySoldDTO.newBuilder()
                        .setActive(true)
                        .setCommodityType(com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType.newBuilder()
                            .setType(CommodityType.STORAGE_AMOUNT_VALUE))
                        .setCapacity(600)
                        .setUsed(100)
                        .build())))
            .build());
    }

    @Test
    public void testVMGrowth() {
        final ClusterHeadroomPlanPostProcessor processor =
                new ClusterHeadroomPlanPostProcessor(PLAN_ID, Collections.singleton(CLUSTER.getId()),
                        grpcTestServer.getChannel(), grpcTestServer.getChannel(),
                        planDao, grpcTestServer.getChannel(), templatesDao);

        Long endDate = System.currentTimeMillis();
        // put middle date before the middle of the interval to avoid rounding issues
        Long middleDate = endDate - DAY_MILLI_SECS - ONE_HOUR;
        Long startDate = middleDate - DAY_MILLI_SECS;

        long vmId1 = 20;
        long vmId2 = 21;
        long vmId3 = 22;

        EntityStats entityStats1 = EntityStats.newBuilder()
                    .addStatSnapshots(StatSnapshot.newBuilder()
                        .setSnapshotDate(startDate))
                    .setOid(vmId1)
                    .build();

        EntityStats entityStats2 = EntityStats.newBuilder()
                .addStatSnapshots(StatSnapshot.newBuilder()
                        .setSnapshotDate(middleDate))
                .setOid(vmId2)
                .build();

        EntityStats entityStats3 = EntityStats.newBuilder()
                .addStatSnapshots(StatSnapshot.newBuilder()
                        .setSnapshotDate(endDate))
                .setOid(vmId3)
                .build();

        when(historyServiceMole.getEntityStats(any()))
                .thenReturn(GetEntityStatsResponse.newBuilder()
                    .addEntityStats(entityStats1)
                    .addEntityStats(entityStats2)
                    .addEntityStats(entityStats3)
                    .build());

        List<TopologyEntityDTO> vmsInCluster = ImmutableList.of(
            TopologyEntityDTO.newBuilder().setOid(vmId1).setEntityType(1).build(),
            TopologyEntityDTO.newBuilder().setOid(vmId2).setEntityType(1).build(),
            TopologyEntityDTO.newBuilder().setOid(vmId3).setEntityType(1).build());

        final Map<Long, Long> clusterIdToVMDailyGrowth =
            processor.getVMDailyGrowth(ImmutableMap.of(CLUSTER.getId(),
                    ImmutableMap.of(EntityType.VIRTUAL_MACHINE_VALUE, vmsInCluster)));
        assertEquals(1, clusterIdToVMDailyGrowth.get(CLUSTER.getId()).longValue());
    }

    @Test
    public void testVMGrowthBigSize() {
        final long N = 1000;
        final ClusterHeadroomPlanPostProcessor processor =
                new ClusterHeadroomPlanPostProcessor(PLAN_ID, Collections.singleton(CLUSTER.getId()),
                        grpcTestServer.getChannel(), grpcTestServer.getChannel(),
                        planDao, grpcTestServer.getChannel(), templatesDao);

        EntityStatsScope scope = EntityStatsScope.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE).build();

        long endDate = System.currentTimeMillis();
        // put middle date before the middle of the interval to avoid rounding issues
        long middleDate = endDate - DAY_MILLI_SECS - ONE_HOUR;
        long startDate = middleDate - DAY_MILLI_SECS;

        List<TopologyEntityDTO> vmsInCluster = new ArrayList<>();

        GetEntityStatsResponse.Builder getEntityStatsResponseBuilder = GetEntityStatsResponse.newBuilder();
        for (long i = 0; i < N; i++) {
            getEntityStatsResponseBuilder.addEntityStats(EntityStats.newBuilder()
                    .addStatSnapshots(StatSnapshot.newBuilder()
                            .setSnapshotDate(startDate))
                    .setOid(i)
                    .build());
            getEntityStatsResponseBuilder.addEntityStats(EntityStats.newBuilder()
                    .addStatSnapshots(StatSnapshot.newBuilder()
                            .setSnapshotDate(middleDate))
                    .setOid(N + i)
                    .build());
            getEntityStatsResponseBuilder.addEntityStats(EntityStats.newBuilder()
                    .addStatSnapshots(StatSnapshot.newBuilder()
                            .setSnapshotDate(endDate))
                    .setOid(2 * N + i)
                    .build());
            vmsInCluster.add(TopologyEntityDTO.newBuilder().setOid(2 * N + i).setEntityType(1).build());
        }
        GetEntityStatsResponse getEntityStatsResponse = getEntityStatsResponseBuilder.build();

        when(historyServiceMole.getEntityStats(any()))
                .thenReturn(getEntityStatsResponse);

        final Map<Long, Long> clusterIdToVMDailyGrowth =
            processor.getVMDailyGrowth(ImmutableMap.of(CLUSTER.getId(),
                ImmutableMap.of(EntityType.VIRTUAL_MACHINE_VALUE, vmsInCluster)));
        assertEquals(N, clusterIdToVMDailyGrowth.get(CLUSTER.getId()).longValue());
    }

    @Test
    public void testPlanSucceeded() throws NoSuchObjectException {
        final ClusterHeadroomPlanPostProcessor processor =
                spy(new ClusterHeadroomPlanPostProcessor(PLAN_ID, Collections.singleton(CLUSTER.getId()),
                        grpcTestServer.getChannel(), grpcTestServer.getChannel(),
                        planDao, grpcTestServer.getChannel(), templatesDao));
        Consumer<ProjectPlanPostProcessor> onCompleteHandler = mock(Consumer.class);
        processor.registerOnCompleteHandler(onCompleteHandler);

        processor.onPlanStatusChanged(PlanInstance.newBuilder()
                .setPlanId(PLAN_ID)
                .setStatus(PlanStatus.SUCCEEDED)
                // Don't set the projected topology ID so that we don't try to store headroom
                .build());

        verify(planDao).deletePlan(PLAN_ID);
        verify(onCompleteHandler).accept(processor);
    }

    @Test
    public void testPlanFailed() throws NoSuchObjectException {
        final ClusterHeadroomPlanPostProcessor processor =
                spy(new ClusterHeadroomPlanPostProcessor(PLAN_ID, Collections.singleton(CLUSTER.getId()),
                        grpcTestServer.getChannel(), grpcTestServer.getChannel(),
                        planDao, grpcTestServer.getChannel(), templatesDao));
        Consumer<ProjectPlanPostProcessor> onCompleteHandler = mock(Consumer.class);
        processor.registerOnCompleteHandler(onCompleteHandler);

        processor.onPlanStatusChanged(PlanInstance.newBuilder()
                .setPlanId(PLAN_ID)
                .setStatus(PlanStatus.FAILED)
                // Don't set the projected topology ID so that we don't try to store headroom
                .build());

        verify(planDao).deletePlan(PLAN_ID);
        verify(onCompleteHandler).accept(processor);
    }

    private Template getTemplateForHeadroom(boolean setValidValues) {
        return Template.newBuilder().setTemplateInfo(
                        TemplateInfo.newBuilder()
                        .setName("AVG:Cluster")
                        .setTemplateSpecId(100)
                        .addAllResources(ImmutableList.of(
                            TemplateResource.newBuilder()
                                .setCategory(ResourcesCategory.newBuilder().setName(ResourcesCategoryName.Compute))
                                .addFields(TemplateField.newBuilder().setName(SystemLoadCalculatedProfile.CPU_SPEED).setValue("10"))
                                .addFields(TemplateField.newBuilder().setName(SystemLoadCalculatedProfile.CPU_CONSUMED_FACTOR).setValue("0.5"))
                                .addFields(TemplateField.newBuilder().setName(SystemLoadCalculatedProfile.NUM_OF_CPU).setValue("1"))
                                .addFields(TemplateField.newBuilder().setName(SystemLoadCalculatedProfile.MEMORY_SIZE).setValue("100"))
                                .addFields(TemplateField.newBuilder().setName(SystemLoadCalculatedProfile.MEMORY_CONSUMED_FACTOR).setValue("0.4"))
                                .build(),
                           TemplateResource.newBuilder()
                                .setCategory(ResourcesCategory.newBuilder().setName(ResourcesCategoryName.Storage))
                                .addFields(TemplateField.newBuilder().setName(SystemLoadCalculatedProfile.DISK_SIZE).setValue(setValidValues ? "200" : "0"))
                                .addFields(TemplateField.newBuilder().setName(SystemLoadCalculatedProfile.DISK_CONSUMED_FACTOR).setValue("1"))
                                .build())))
                        .setId(1234)
                        .build();
    }
}
