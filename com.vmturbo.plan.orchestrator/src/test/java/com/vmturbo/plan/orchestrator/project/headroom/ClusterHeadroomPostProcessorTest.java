package com.vmturbo.plan.orchestrator.project.headroom;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.TemplateProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory;
import com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory.ResourcesCategoryName;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateField;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateResource;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetMultiSupplyChainsResponse;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.repository.SupplyChainProtoMoles.SupplyChainServiceMole;
import com.vmturbo.common.protobuf.setting.SettingProto.GetGlobalSettingResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingServiceMole;
import com.vmturbo.common.protobuf.stats.Stats.CommodityHeadroom;
import com.vmturbo.common.protobuf.stats.Stats.SaveClusterHeadroomRequest;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;
import com.vmturbo.common.protobuf.stats.StatsMoles.StatsHistoryServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntityBatch;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;
import com.vmturbo.plan.orchestrator.plan.PlanDao;
import com.vmturbo.plan.orchestrator.project.ProjectPlanPostProcessor;
import com.vmturbo.plan.orchestrator.templates.TemplatesDao;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

public class ClusterHeadroomPostProcessorTest {

    private static final long PLAN_ID = 7;
    private static final long CLUSTER_ID = 10;
    private static final int MORE_THAN_A_YEAR = 3650;
    private static final long ONE_HOUR = 3600000;
    // Milliseconds in a day
    private static final long DAY_MILLI_SECS = TimeUnit.DAYS.toMillis(1);

    private static final Grouping CLUSTER = Grouping.newBuilder()
            .setDefinition(GroupDefinition.newBuilder()
                    .setType(GroupType.COMPUTE_HOST_CLUSTER)
                    .setDisplayName("foo"))
            .setId(CLUSTER_ID)
            .build();

    private RepositoryServiceMole repositoryServiceMole = spy(new RepositoryServiceMole());

    private StatsHistoryServiceMole historyServiceMole = spy(new StatsHistoryServiceMole());

    private SupplyChainServiceMole supplyChainServiceMole = spy(new SupplyChainServiceMole());

    private GroupServiceMole groupRpcServiceMole = spy(new GroupServiceMole());

    private SettingServiceMole settingServiceMole = spy(new SettingServiceMole());

    private PlanDao planDao = mock(PlanDao.class);

    private TemplatesDao templatesDao = mock(TemplatesDao.class);

    @Rule
    public GrpcTestServer grpcTestServer =
            GrpcTestServer.newServer(repositoryServiceMole, historyServiceMole,
                    supplyChainServiceMole, groupRpcServiceMole, settingServiceMole);

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
    public void testProjectedTopologyWithInActiveHost() throws Exception {
        prepareForHeadroomCalculation(true);
        RemoteIterator<ProjectedTopologyEntity> topologyIt = getProjectedTopology(false);

        processor.handleProjectedTopology(100, TopologyInfo.getDefaultInstance(), topologyIt);

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
     */
    private void prepareForHeadroomCalculation(boolean setValidValues) {
        when(settingServiceMole.getGlobalSetting(any()))
            .thenReturn(GetGlobalSettingResponse.newBuilder().setSetting(Setting.newBuilder()
                .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(1))).build());

        when(settingServiceMole.getGlobalSetting(any()))
            .thenReturn(GetGlobalSettingResponse.newBuilder().setSetting(Setting.newBuilder()
                .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(1))).build());

        final ClusterHeadroomPlanPostProcessor processor =
            spy(new ClusterHeadroomPlanPostProcessor(PLAN_ID, Collections.singleton(CLUSTER.getId()),
                grpcTestServer.getChannel(), grpcTestServer.getChannel(),
                planDao, grpcTestServer.getChannel(), templatesDao));
        final long projectedTopologyId = 100;

        when(templatesDao.getClusterHeadroomTemplateForGroup(Mockito.anyLong()))
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
    }

    /**
     * Get the projected topology.
     *
     * @param setHostActive whether to set host active or not
     * @return a list of {@link PartialEntityBatch}
     */
    private RemoteIterator<ProjectedTopologyEntity> getProjectedTopology(final boolean setHostActive) {
        List<TopologyEntityDTO> entities =  ImmutableList.of(
            TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setOid(7)
                .build(),
            PartialEntity.newBuilder().setHeadroomPlanPartialEntity(
                HeadroomPlanPartialEntity.newBuilder()
                    .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                    .setOid(99).build())
                .build(),
            PartialEntity.newBuilder().setHeadroomPlanPartialEntity(
                HeadroomPlanPartialEntity.newBuilder()
                    .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                    .setEntityState(setHostActive ? EntityState.POWERED_ON : EntityState.MAINTENANCE)
                    .setOid(8)
                    .addAllCommoditySold(
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
                .build(),
            PartialEntity.newBuilder().setHeadroomPlanPartialEntity(
                HeadroomPlanPartialEntity.newBuilder()
                    .setEntityType(EntityType.STORAGE_VALUE)
                    .setEntityState(EntityState.POWERED_ON)
                    .setOid(9)
                    .addAllCommoditySold(
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

    /**
     * Multiple tests for vm growth calculation.
     */
    @Test
    public void testGetVMDailyGrowth() {
        float delta = 0.01f;
        final Map<Long, Map<Integer, List<HeadroomPlanPartialEntity>>> entityOidsByClusterAndType =
            Collections.singletonMap(1L, ImmutableMap.of(EntityType.VIRTUAL_MACHINE_VALUE,
                LongStream.range(0, 15).mapToObj(i -> HeadroomPlanPartialEntity.getDefaultInstance())
                    .collect(Collectors.toList())));

        final ClusterHeadroomPlanPostProcessor processor = spy(new ClusterHeadroomPlanPostProcessor(PLAN_ID, ImmutableSet.of(1L),
            grpcTestServer.getChannel(), grpcTestServer.getChannel(),
            planDao, grpcTestServer.getChannel(), templatesDao));

        long mostRecentHistoricalDate = System.currentTimeMillis();
        Map<Long, Long> vmsByDate = getVMsByDate(getVMCountData(10, 5), mostRecentHistoricalDate);
        when(historyServiceMole.getClusterStats(any())).thenReturn(getStatsSnapshots(vmsByDate));

        Map<Long, Float> growthPerCluster = processor.getVMDailyGrowth(entityOidsByClusterAndType);

        // We should pick record with numVm value 6 since it is oldest (5 days ago)
        // but less than peak look back days. Also, make sure we divide by this value instead of
        // peak look back days value.
        assertEquals(growthPerCluster.get(1L),
            (float) (15L - 6L)/((mostRecentHistoricalDate - vmsByDate.get(6L))/DAY_MILLI_SECS), 0.01f);

        // No records, growth should be 0.
        when(historyServiceMole.getClusterStats(any())).thenReturn(new ArrayList<>());
        growthPerCluster = processor.getVMDailyGrowth(entityOidsByClusterAndType);
        assertEquals(growthPerCluster.get(1L), 0.0f, delta);

        // Negative growth, should override to 0. all values in history are greater than current VM values.
        mostRecentHistoricalDate = System.currentTimeMillis();
        vmsByDate = getVMsByDate(getVMCountData(100, 5), mostRecentHistoricalDate);
        when(historyServiceMole.getClusterStats(any())).thenReturn(getStatsSnapshots(vmsByDate));
        growthPerCluster = processor.getVMDailyGrowth(entityOidsByClusterAndType);
        assertEquals(growthPerCluster.get(1L), 0, delta);
    }

    /**
     * Start Value is decreased per day for given number of days.
     * @param startValue value to start from
     * @param numDays number of days for which we insert values.
     * @return array with values {startValue, startValue+1.... , startValue + numDays - 1}
     */
    private long[] getVMCountData(int startValue, int numDays) {
        long[] vmCounts = new long[numDays];
        int i = 0;
        while(i < numDays) {
            vmCounts[i] = startValue;
            startValue--;
            i++;
        }
        return vmCounts;
    }

    /**
     * Inserts each value in numVMs mapped with numVMs -> (endDate - x * millis in days)
     * with x starting from 1 incrementing by 1 with each insert.
     * @param numVms set of values of number of VMs
     * @param endDate date we decrement from.
     * @return vm -> date map.
     */
    private Map<Long, Long> getVMsByDate(final long[] numVms, long endDate) {
        int days = 1;
        Map<Long, Long> vmByDate = new HashMap<>();
        for (Long numVM : numVms) {
            // Keep going a day before
            vmByDate.put(numVM, endDate - (days * DAY_MILLI_SECS));
            days++;
        };
        return vmByDate;
    }

    private List<StatSnapshot> getStatsSnapshots(Map<Long, Long> vmsByDate) {
        List<StatSnapshot> statsList = new ArrayList<>();
        vmsByDate.forEach((numVm, date) -> {
            StatSnapshot statSnapshot = StatSnapshot.newBuilder()
                .setSnapshotDate(date)
                .addStatRecords((StatRecord.newBuilder()
                        .setValues(StatValue.newBuilder()
                            .setAvg(numVm)
                            .build())
                        .setName(StringConstants.VM_NUM_VMS)
                        .build()))
                .build();
            statsList.add(statSnapshot);
        });
        return statsList;
    }

    private Template getTemplateForHeadroom(boolean setValidValues) {
        return Template.newBuilder().setTemplateInfo(
                        TemplateInfo.newBuilder()
                        .setName("AVG:Cluster")
                        .setTemplateSpecId(100)
                        .addAllResources(ImmutableList.of(
                            TemplateResource.newBuilder()
                                .setCategory(ResourcesCategory.newBuilder().setName(ResourcesCategoryName.Compute))
                                .addFields(TemplateField.newBuilder().setName(TemplateProtoUtil.VM_COMPUTE_VCPU_SPEED).setValue("10"))
                                .addFields(TemplateField.newBuilder().setName(TemplateProtoUtil.VM_COMPUTE_CPU_CONSUMED_FACTOR).setValue("0.5"))
                                .addFields(TemplateField.newBuilder().setName(TemplateProtoUtil.VM_COMPUTE_NUM_OF_VCPU).setValue("1"))
                                .addFields(TemplateField.newBuilder().setName(TemplateProtoUtil.VM_COMPUTE_MEM_SIZE).setValue("100"))
                                .addFields(TemplateField.newBuilder().setName(TemplateProtoUtil.VM_COMPUTE_MEM_CONSUMED_FACTOR).setValue("0.4"))
                                .build(),
                           TemplateResource.newBuilder()
                                .setCategory(ResourcesCategory.newBuilder().setName(ResourcesCategoryName.Storage))
                                .addFields(TemplateField.newBuilder().setName(TemplateProtoUtil.VM_STORAGE_DISK_SIZE).setValue(setValidValues ? "200" : "0"))
                                .addFields(TemplateField.newBuilder().setName(TemplateProtoUtil.VM_STORAGE_DISK_CONSUMED_FACTOR).setValue("1"))
                                .build())))
                        .setId(1234)
                        .build();
    }
}
