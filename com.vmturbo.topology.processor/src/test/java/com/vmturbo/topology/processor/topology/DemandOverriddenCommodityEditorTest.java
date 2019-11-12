package com.vmturbo.topology.processor.topology;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.PlanChanges;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.PlanChanges.UtilizationLevel;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.HistoricalValues;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PlanTopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.TopologyEntity.Builder;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.GroupResolver;

public class DemandOverriddenCommodityEditorTest {
    TopologyEntity.Builder pm1 = TopologyEntity.newBuilder(TopologyEntityDTO.newBuilder()
            .setOid(1111)
            .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                    .setCommodityType(CommodityType.newBuilder()
                            .setType(CommodityDTO.CommodityType.MEM_VALUE))
                    .setUsed(15)
                    .setHistoricalUsed(HistoricalValues.newBuilder()
                            .setHistUtilization(10))));
    TopologyEntity.Builder pm2 = TopologyEntity.newBuilder(TopologyEntityDTO.newBuilder()
            .setOid(1112)
            .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                    .setCommodityType(CommodityType.newBuilder()
                            .setType(CommodityDTO.CommodityType.MEM_VALUE))
                    .setUsed(20)));
    TopologyEntity.Builder vm1 = TopologyEntity.newBuilder(TopologyEntityDTO.newBuilder()
            .setOid(0001)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderId(pm1.getOid())
                    .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                    .addCommodityBought(CommodityBoughtDTO.newBuilder()
                            .setCommodityType(CommodityType.newBuilder()
                                    .setType(CommodityDTO.CommodityType.MEM_VALUE))
                            .setUsed(10)
                    .setHistoricalUsed(HistoricalValues.newBuilder().setHistUtilization(15)).build()).build())
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                    .setUsed(5)
                    .setCapacity(10000)
                    .setHistoricalUsed(HistoricalValues.newBuilder().setHistUtilization(8).build())
                    .setCommodityType(CommodityType.newBuilder()
                            .setType(CommodityDTO.CommodityType.VMEM_VALUE)).build()));
    TopologyEntity.Builder vm2 = TopologyEntity.newBuilder(TopologyEntityDTO.newBuilder()
        .setOid(0002)
        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
        .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
            .setProviderId(pm2.getOid())
            .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
            .addCommodityBought(CommodityBoughtDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.MEM_VALUE))
                .setUsed(5).build()))
        .addCommoditySoldList(CommoditySoldDTO.newBuilder()
            .setUsed(5)
            .setCapacity(10000)
            .setCommodityType(CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.VMEM_VALUE)).build()));
    private final Map<Long, Builder> topologyMap = ImmutableMap.of(
        vm1.getOid(), vm1,
        vm2.getOid(), vm2,
        pm1.getOid(), pm1,
        pm2.getOid(), pm2
    );

    private final TopologyGraph<TopologyEntity> topologyGraph = TopologyEntityTopologyGraphCreator.newGraph(topologyMap);

    private static final GroupResolver groupResolver = mock(GroupResolver.class);
    private static final GroupServiceMole groupServiceClient = spy(new GroupServiceMole());
    private DemandOverriddenCommodityEditor editor;

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(groupServiceClient);

    @Before
    public void setup() {
        editor = new DemandOverriddenCommodityEditor(com.vmturbo.common.protobuf.group.GroupServiceGrpc
                .newBlockingStub(grpcServer.getChannel()));
    }

    /**
     * Test the utilization level at a global level
     */
    @Test
    public void testApplyDemandUsageChangeGlobally() {
        TopologyInfo topologyInfo = TopologyInfo.newBuilder().setPlanInfo(PlanTopologyInfo.newBuilder()).build();
        List<ScenarioChange> globalChange = new ArrayList<ScenarioChange>();
        globalChange.add(ScenarioChange.newBuilder().setPlanChanges(PlanChanges.newBuilder()
            .setUtilizationLevel(UtilizationLevel.newBuilder().setPercentage(50))).build());
        editor.applyDemandUsageChange(topologyGraph, groupResolver, globalChange);
        // vm1 has the historical used, so increasing util by 50% is vm1 historicalUsed increase by 50% -> 8 * 1.5 = 12
        Assert.assertTrue(vm1.getEntityBuilder().getCommoditySoldListList().get(0).getHistoricalUsed().getHistUtilization() == 12);
        // vm1 vmem increased by 4 so the vm1 mem bought increase by 4 as well -> 15 + 4 = 19
        Assert.assertTrue(vm1.getEntityBuilder().getCommoditiesBoughtFromProvidersBuilderList().get(0)
                .getCommodityBought(0).getHistoricalUsed().getHistUtilization() == 19);
        // pm1 hosts the vm1, so pm1 mem increase by 4 -> 10 + 4 = 14
        Assert.assertTrue(pm1.getEntityBuilder().getCommoditySoldList(0).getHistoricalUsed().getHistUtilization() == 14);
        // vm2 has no historical used, so increasing util by 50% is vm1 used increase by 50% -> 5 * 1.5 = 7.5
        Assert.assertTrue(vm2.getEntityBuilder().getCommoditySoldListList().get(0).getUsed() == 7.5);
        // vm2 vmem increased by 2.5 so the vm1 mem bought increase by 2.5 as well -> 5 + 2.5 = 7.5
        Assert.assertTrue(vm2.getEntityBuilder().getCommoditiesBoughtFromProvidersBuilderList().get(0)
            .getCommodityBought(0).getUsed() == 7.5);
        // pm2 hosts the vm2, so pm2 mem increase by 2.5 -> 20 + 2.5 = 22.5
        Assert.assertTrue(pm2.getEntityBuilder().getCommoditySoldList(0).getUsed() == 22.5);
    }

    /**
     * Test the utilization level at a group level.
     */
    @Test
    public void testApplyDemandUsageChangeForGroup() {
        long groupOid = 999L;
        final List<Grouping> groups = new ArrayList<>();
        Grouping g = Grouping.newBuilder()
                .setId(groupOid)
                .addExpectedTypes(MemberType.newBuilder().setEntity(UIEntityType.VIRTUAL_MACHINE.typeNumber()))
                .setDefinition(GroupDefinition.newBuilder()
                        .setStaticGroupMembers(StaticMembers.newBuilder()
                        .addMembersByType(StaticMembersByType.newBuilder()
                                .setType(MemberType.newBuilder()
                                .setEntity(UIEntityType.VIRTUAL_MACHINE.typeNumber()))
                        .addMembers(vm1.getOid())
                    )))
            .build();
        groups.add(g);
        when(groupServiceClient.getGroups(GetGroupsRequest.newBuilder()
            .setGroupFilter(GroupFilter.newBuilder().addId(groupOid))
            .setReplaceGroupPropertyWithGroupMembershipFilter(true)
            .build())).thenReturn(groups);
        when(groupResolver.resolve(eq(g), eq(topologyGraph))).thenReturn(new HashSet<>(Arrays.asList(vm1.getOid())));
        TopologyInfo topologyInfo = TopologyInfo.newBuilder().setPlanInfo(PlanTopologyInfo.newBuilder()).build();
        List<ScenarioChange> groupChange = new ArrayList<ScenarioChange>();
        groupChange.add(ScenarioChange.newBuilder().setPlanChanges(PlanChanges.newBuilder()
            .setUtilizationLevel(UtilizationLevel.newBuilder().setPercentage(50).setGroupOid(groupOid))).build());
        editor.applyDemandUsageChange(topologyGraph, groupResolver, groupChange);
        // vm1 has the historical used, so increasing util by 50% is vm1 historicalUsed increase by 50% -> 8 * 1.5 = 12
        Assert.assertTrue(vm1.getEntityBuilder().getCommoditySoldListList().get(0).getHistoricalUsed().getHistUtilization() == 12);
        // vm1 has the historical used, so increasing util by 50% is vm1 historicalUsed increase by 50% -> 8 * 1.5 = 12
        Assert.assertTrue(vm1.getEntityBuilder().getCommoditiesBoughtFromProvidersBuilderList().get(0)
            .getCommodityBought(0).getHistoricalUsed().getHistUtilization() == 19);
        Assert.assertTrue(pm1.getEntityBuilder().getCommoditySoldList(0).getHistoricalUsed().getHistUtilization() == 14);
    }
}
