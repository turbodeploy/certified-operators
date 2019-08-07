package com.vmturbo.plan.orchestrator.project.headroom;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
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
import com.vmturbo.common.protobuf.repository.SupplyChainProtoMoles.SupplyChainServiceMole;
import com.vmturbo.common.protobuf.stats.Stats.CommodityHeadroom;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.SaveClusterHeadroomRequest;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.StatsMoles.StatsHistoryServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.components.api.test.GrpcTestServer;
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
    public void testProjectedTopologyWithHeadroomvalues() {

        when(groupRpcServiceMole.getGroup(GroupID.newBuilder().setId(CLUSTER_ID).build()))
        .thenReturn(GetGroupResponse.newBuilder().setGroup(Group.newBuilder().setId(CLUSTER_ID)).build());

        final ClusterHeadroomPlanPostProcessor processor =
                spy(new ClusterHeadroomPlanPostProcessor(PLAN_ID, CLUSTER.getId(),
                        grpcTestServer.getChannel(), grpcTestServer.getChannel(),
                        planDao, grpcTestServer.getChannel(), templatesDao));
        final long projectedTopologyId = 100;

        when(repositoryServiceMole.retrieveTopology(RetrieveTopologyRequest.newBuilder()
                .setTopologyId(projectedTopologyId)
                .build())).thenReturn(getProjectedTopology());

        when(templatesDao.getTemplate(Mockito.anyLong()))
        .thenReturn(Optional.of(getTemplateForHeadroom(true)));

        processor.onPlanStatusChanged(PlanInstance.newBuilder()
                .setPlanId(PLAN_ID)
                .setStatus(PlanStatus.WAITING_FOR_RESULT)
                .setProjectedTopologyId(projectedTopologyId)
                .build());

        // VmGrowth = 0 (because we don't have data in the past)
        verify(historyServiceMole).saveClusterHeadroom(SaveClusterHeadroomRequest.newBuilder()
                .setClusterId(CLUSTER_ID)
                .setNumVMs(0L)
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
                .setNumHosts(0)
                .setNumVMs(0)
                .setNumStorages(0)
                .setMonthlyVMGrowth(0) // (vmGrowth * daysInMonth) / PeakLookback days = (0 * 30)/7 = 0
                .setHeadroom(2) // minimum of mem, cpu and storage headroom values : min(10, 4, 2)
                .build());
    }

    private List<RetrieveTopologyResponse> getProjectedTopology() {
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
                    .setEntityState(EntityState.POWERED_ON)
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
    public void testProjectedTopologyWithInvalidValuesInTemplate() {
        when(groupRpcServiceMole.getGroup(GroupID.newBuilder().setId(CLUSTER_ID).build()))
        .thenReturn(GetGroupResponse.newBuilder().setGroup(Group.newBuilder().setId(CLUSTER_ID)).build());

        final ClusterHeadroomPlanPostProcessor processor =
                spy(new ClusterHeadroomPlanPostProcessor(PLAN_ID, CLUSTER.getId(),
                        grpcTestServer.getChannel(), grpcTestServer.getChannel(),
                        planDao, grpcTestServer.getChannel(), templatesDao));
        final long projectedTopologyId = 100;

        when(repositoryServiceMole.retrieveTopology(RetrieveTopologyRequest.newBuilder()
                .setTopologyId(projectedTopologyId)
                .build())).thenReturn(getProjectedTopology());

        // Return template with zero Values for Storage
        when(templatesDao.getTemplate(Mockito.anyLong()))
        .thenReturn(Optional.of(getTemplateForHeadroom(false)));

        processor.onPlanStatusChanged(PlanInstance.newBuilder()
                .setPlanId(PLAN_ID)
                .setStatus(PlanStatus.WAITING_FOR_RESULT)
                .setProjectedTopologyId(projectedTopologyId)
                .build());
        verify(historyServiceMole).saveClusterHeadroom(SaveClusterHeadroomRequest.newBuilder()
                        .setClusterId(CLUSTER_ID)
                        .setNumVMs(0L)
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
                        .setNumHosts(0)
                        .setNumVMs(0)
                        .setNumStorages(0)
                        .setMonthlyVMGrowth(0) // (vmGrowth * daysInMonth) / PeakLookback days = (0 * 30)/7 = 0
                        .setHeadroom(0) // minimum of mem, cpu and storage headroom values : min(10, 4, 0)
                        .build());
    }

    @Test
    public void testVMGrowth() throws Exception {
        final ClusterHeadroomPlanPostProcessor processor =
                new ClusterHeadroomPlanPostProcessor(PLAN_ID, CLUSTER.getId(),
                        grpcTestServer.getChannel(), grpcTestServer.getChannel(),
                        planDao, grpcTestServer.getChannel(), templatesDao);

        EntityStatsScope scope = EntityStatsScope.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE).build();

        Long endDate = System.currentTimeMillis();
        // put middle date before the middle of the interval to avoid rounding issues
        Long middleDate = endDate - ClusterHeadroomPlanPostProcessor.DAY_MILLI_SECS - ONE_HOUR;
        Long startDate = middleDate - ClusterHeadroomPlanPostProcessor.DAY_MILLI_SECS;

        long vmId1 = 20;
        long vmId2 = 21;
        long vmId3 = 22;

        Set<Long> vmsInCluster = ImmutableSet.of(vmId1, vmId2, vmId3);

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

        Assert.assertEquals(1, processor.getVMGrowth(vmsInCluster));
    }

    @Test
    public void testVMGrowthBigSize() throws Exception {
        final long N = 1000;
        final ClusterHeadroomPlanPostProcessor processor =
                new ClusterHeadroomPlanPostProcessor(PLAN_ID, CLUSTER.getId(),
                        grpcTestServer.getChannel(), grpcTestServer.getChannel(),
                        planDao, grpcTestServer.getChannel(), templatesDao);

        EntityStatsScope scope = EntityStatsScope.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE).build();

        Long endDate = System.currentTimeMillis();
        // put middle date before the middle of the interval to avoid rounding issues
        Long middleDate = endDate - ClusterHeadroomPlanPostProcessor.DAY_MILLI_SECS - ONE_HOUR;
        Long startDate = middleDate - ClusterHeadroomPlanPostProcessor.DAY_MILLI_SECS;

        Set<Long> vmsInCluster = new HashSet<>();

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
            vmsInCluster.add(2 * N + i);
        }
        GetEntityStatsResponse getEntityStatsResponse = getEntityStatsResponseBuilder.build();

        when(historyServiceMole.getEntityStats(any()))
                .thenReturn(getEntityStatsResponse);

        Assert.assertEquals(N, processor.getVMGrowth(vmsInCluster));
    }

    @Test
    public void testPlanSucceeded() throws NoSuchObjectException {
        final ClusterHeadroomPlanPostProcessor processor =
                spy(new ClusterHeadroomPlanPostProcessor(PLAN_ID, CLUSTER.getId(),
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
                spy(new ClusterHeadroomPlanPostProcessor(PLAN_ID, CLUSTER.getId(),
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
