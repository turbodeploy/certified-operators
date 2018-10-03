package com.vmturbo.plan.orchestrator.project.headroom;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;
import java.util.function.Consumer;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;

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
import com.vmturbo.common.protobuf.repository.SupplyChainMoles.SupplyChainServiceMole;
import com.vmturbo.common.protobuf.stats.Stats.CommodityHeadroom;
import com.vmturbo.common.protobuf.stats.Stats.SaveClusterHeadroomRequest;
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
    private static final long ADDED_CLONES = 50;

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
                        grpcTestServer.getChannel(), grpcTestServer.getChannel(), ADDED_CLONES,
                        planDao, grpcTestServer.getChannel(), templatesDao));
        final long projectedTopologyId = 100;

        when(repositoryServiceMole.retrieveTopology(RetrieveTopologyRequest.newBuilder()
                .setTopologyId(projectedTopologyId)
                .build())).thenReturn(
                        Collections.singletonList(RetrieveTopologyResponse.newBuilder()
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
                                                            .setType(CommodityType.STORAGE_VALUE))
                                            .setCapacity(600)
                                            .setUsed(100)
                                            .build())))
                                .build()));

        when(templatesDao.getTemplate(Mockito.anyLong()))
        .thenReturn(Optional.of(getTemplateForHeadroom()));

        processor.onPlanStatusChanged(PlanInstance.newBuilder()
                .setPlanId(PLAN_ID)
                .setStatus(PlanStatus.WAITING_FOR_RESULT)
                .setProjectedTopologyId(projectedTopologyId)
                .build());

        // VmGrowth = 2 (because history doesn't contain any VMs)
        verify(historyServiceMole).saveClusterHeadroom(SaveClusterHeadroomRequest.newBuilder()
                .setClusterId(CLUSTER_ID)
                .setHeadroom(48L)
                .setNumVMs(0L)
                // Template Value CPU_SPEED = 10, consumedFactor = 0.5, effectiveUsed = 5
                // PM CPU value : used = 50, capacity = 100
                // CPU headroom calculation :
                // headroomCapacity = capacity / effectiveUsed = 20, headroomAvailable = (capacity - used) / effectiveUsed = 10
                // daysToExhaust = (headroomAvailable / vmGrowth) * peakLookbackDays = 35
                .setCpuHeadroomInfo(CommodityHeadroom.newBuilder()
                    .setCapacity(20)
                    .setHeadroom(10)
                    .setDaysToExhaustion(35))
                // Template Value MEMORY_SIZE = 100, consumedFactor = 0.4, effectiveUsed = 40
                // PM MEM value : used = 40, capacity = 200
                // MEM headroom calculation :
                // headroomCapacity = capacity / effectiveUsed = 5, headroomAvailable = (capacity - used) / effectiveUsed = 4
                // daysToExhaust = (headroomAvailable / vmGrowth) * peakLookbackDays = 14
                .setMemHeadroomInfo(CommodityHeadroom.newBuilder()
                    .setCapacity(5)
                    .setHeadroom(4)
                    .setDaysToExhaustion(14))
                // Template Value DISK_SIZE = 200, consumedFactor = 1, effectiveUsed = 200
                // Storage value : used = 100, capacity = 600
                // Storage headroom calculation :
                // headroomCapacity = capacity / effectiveUsed = 3, headroomAvailable = (capacity - used) / effectiveUsed = 2
                // daysToExhaust = (headroomAvailable / vmGrowth) * peakLookbackDays = 7
                .setStorageHeadroomInfo(CommodityHeadroom.newBuilder()
                    .setCapacity(3)
                    .setHeadroom(2)
                    .setDaysToExhaustion(7))
                .build());
    }

    @Test
    public void testPlanSucceeded() throws NoSuchObjectException {
        final ClusterHeadroomPlanPostProcessor processor =
                spy(new ClusterHeadroomPlanPostProcessor(PLAN_ID, CLUSTER.getId(),
                        grpcTestServer.getChannel(), grpcTestServer.getChannel(), ADDED_CLONES,
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
                        grpcTestServer.getChannel(), grpcTestServer.getChannel(), ADDED_CLONES,
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

    private Template getTemplateForHeadroom() {
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
                                .addFields(TemplateField.newBuilder().setName(SystemLoadCalculatedProfile.DISK_SIZE).setValue("200"))
                                .addFields(TemplateField.newBuilder().setName(SystemLoadCalculatedProfile.DISK_CONSUMED_FACTOR).setValue("1"))
                                .build())))
                        .setId(1234)
                        .build();
    }

}
