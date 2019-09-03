package com.vmturbo.topology.processor.topology;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticGroupMembers;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScope;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScopeEntry;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline.PipelineStageException;

/**
 * Unit tests for {@link PlanTopologyScopeEditor}.
 */
public class PlanTopologyScopeEditorTest {
    private static final int HYPERVISOR_TARGET = 0;
    private static final int CLOUD_TARGET = 1;
    private static final TopologyEntity.Builder DA1 = createHypervisorTopologyEntity(50001L, "DA1", EntityType.DISK_ARRAY);
    private static final TopologyEntity.Builder ST1 = createHypervisorTopologyEntity(40001L, "ST1", EntityType.STORAGE, DA1.getOid());
    private static final TopologyEntity.Builder ST2 = createHypervisorTopologyEntity(40002L, "ST2", EntityType.STORAGE);
    private static final TopologyEntity.Builder DC1 = createHypervisorTopologyEntity(10001L, "DC1", EntityType.DATACENTER);
    private static final TopologyEntity.Builder DC2 = createHypervisorTopologyEntity(10002L, "DC2", EntityType.DATACENTER);
    private static final TopologyEntity.Builder PM1_IN_DC1 = createHypervisorTopologyEntity(20001L, "PM1_IN_DC1", EntityType.PHYSICAL_MACHINE, DC1.getOid());
    private static final TopologyEntity.Builder PM2_IN_DC1 = createHypervisorTopologyEntity(20002L, "PM2_IN_DC1", EntityType.PHYSICAL_MACHINE, DC1.getOid());
    private static final TopologyEntity.Builder PM_IN_DC2 = createHypervisorTopologyEntity(20003L, "PM_IN_DC2", EntityType.PHYSICAL_MACHINE, DC2.getOid());
    private static final TopologyEntity.Builder VM1_IN_DC1 = createHypervisorTopologyEntity(30001L, "VM1_IN_DC1", EntityType.VIRTUAL_MACHINE, PM1_IN_DC1.getOid(), ST1.getOid());
    private static final TopologyEntity.Builder VM2_IN_DC1 = createHypervisorTopologyEntity(30002L, "VM2_IN_DC1", EntityType.VIRTUAL_MACHINE, PM2_IN_DC1.getOid(), ST1.getOid());
    private static final TopologyEntity.Builder VM_IN_DC2 = createHypervisorTopologyEntity(30003L, "VM_IN_DC2", EntityType.VIRTUAL_MACHINE, PM_IN_DC2.getOid(), ST2.getOid());
    private static final TopologyEntity.Builder APP1 = createHypervisorTopologyEntity(60001L, "APP1", EntityType.APPLICATION, VM1_IN_DC1.getOid());
    private static final TopologyEntity.Builder AS1 = createHypervisorTopologyEntity(70001L, "AS1", EntityType.APPLICATION_SERVER, VM1_IN_DC1.getOid());
    private static final TopologyEntity.Builder AS2 = createHypervisorTopologyEntity(70002L, "AS2", EntityType.APPLICATION_SERVER, VM_IN_DC2.getOid());
    private static final TopologyEntity.Builder BAPP1 = createHypervisorTopologyEntity(80001L, "BAPP1", EntityType.BUSINESS_APPLICATION, AS1.getOid(), AS2.getOid());
    private static final TopologyEntity.Builder VIRTUAL_VOLUME = TopologyEntityUtils.connectedTopologyEntity(90001L, HYPERVISOR_TARGET, 0, "VIRTUAL_VOLUME", EntityType.VIRTUAL_VOLUME, VM1_IN_DC1.getOid());
    private static final TopologyEntity.Builder AZ1_LONDON = createCloudTopologyAvailabilityZone(1001L, "AZ1 London");
    private static final TopologyEntity.Builder AZ2_LONDON = createCloudTopologyAvailabilityZone(1002L, "AZ2 London");
    private static final TopologyEntity.Builder AZ_OHIO = createCloudTopologyAvailabilityZone(1003L, "AZ Ohio");
    private static final TopologyEntity.Builder AZ1_HONG_KONG = createCloudTopologyAvailabilityZone(1004L, "AZ1 Hong Kong");
    private static final TopologyEntity.Builder AZ2_HONG_KONG = createCloudTopologyAvailabilityZone(1005L, "AZ2 Hong Kong");
    private static final TopologyEntity.Builder REGION_LONDON = createCloudConnectedTopologyEntity(2001L, "London", EntityType.REGION, AZ1_LONDON.getOid(), AZ2_LONDON.getOid());
    private static final TopologyEntity.Builder REGION_OHIO = createCloudConnectedTopologyEntity(2002L, "Ohio", EntityType.REGION, AZ_OHIO.getOid());
    private static final TopologyEntity.Builder REGION_HONG_KONG = createCloudConnectedTopologyEntity(2003L, "Hong Kong", EntityType.REGION, AZ1_HONG_KONG.getOid(), AZ2_HONG_KONG.getOid());
    private static final TopologyEntity.Builder VM1_IN_LONDON = createCloudConnectedTopologyEntity(4001L, "VM1 in London", EntityType.VIRTUAL_MACHINE, AZ1_LONDON.getOid());
    private static final TopologyEntity.Builder VM2_IN_LONDON = createCloudConnectedTopologyEntity(4002L, "VM2 in London", EntityType.VIRTUAL_MACHINE, AZ2_LONDON.getOid());
    private static final TopologyEntity.Builder DB_LONDON = createCloudConnectedTopologyEntity(8001L, "DB in London", EntityType.DATABASE, REGION_LONDON.getOid());
    private static final TopologyEntity.Builder DBS_LONDON = createCloudConnectedTopologyEntity(9001L, "DBS in London", EntityType.DATABASE_SERVER, AZ1_LONDON.getOid());
    private static final TopologyEntity.Builder DBS_HONG_KONG = createCloudConnectedTopologyEntity(9002L, "DBS in Hong Kong", EntityType.DATABASE_SERVER, AZ2_HONG_KONG.getOid());
    private static final TopologyEntity.Builder COMPUTE_TIER = createCloudConnectedTopologyEntity(3001L, "Compute tier", EntityType.COMPUTE_TIER, REGION_LONDON.getOid(), REGION_OHIO.getOid());
    private static final TopologyEntity.Builder STORAGE_TIER = createCloudConnectedTopologyEntity(7001L, "Storage tier", EntityType.STORAGE_TIER, REGION_LONDON.getOid(), REGION_OHIO.getOid());
    private static final TopologyEntity.Builder VIRTUAL_VOLUME_IN_OHIO = createCloudConnectedTopologyEntity(6001L, "Virtual Volume in Ohio", EntityType.VIRTUAL_VOLUME, AZ_OHIO.getOid(), STORAGE_TIER.getOid());
    private static final TopologyEntity.Builder VM_IN_OHIO = createCloudConnectedTopologyEntity(4003L, "VM in Ohio", EntityType.VIRTUAL_MACHINE, AZ_OHIO.getOid(), VIRTUAL_VOLUME_IN_OHIO.getOid());
    private static final TopologyEntity.Builder BUSINESS_ACC1 = createCloudConnectedTopologyEntity(5001L, "Business account 1", EntityType.BUSINESS_ACCOUNT, VM1_IN_LONDON.getOid());
    private static final TopologyEntity.Builder BUSINESS_ACC2 = createCloudConnectedTopologyEntity(5002L, "Business account 2", EntityType.BUSINESS_ACCOUNT, VM_IN_OHIO.getOid());
    private static final TopologyEntity.Builder BUSINESS_ACC3 = createCloudConnectedTopologyEntity(5003L, "Business account 3", EntityType.BUSINESS_ACCOUNT, REGION_HONG_KONG.getOid());

    private static final TopologyGraph<TopologyEntity> GRAPH = TopologyEntityUtils
                    .topologyGraphOf(APP1, VM1_IN_DC1, VM2_IN_DC1, VM_IN_DC2, PM1_IN_DC1,
                                     PM2_IN_DC1, PM_IN_DC2, DC1, DC2, ST1, ST2, DA1, BAPP1, AS1,
                                     AS2, VIRTUAL_VOLUME, AZ1_LONDON, AZ2_LONDON, AZ_OHIO,
                                     AZ1_HONG_KONG, AZ2_HONG_KONG, REGION_LONDON, REGION_OHIO,
                                     REGION_HONG_KONG, COMPUTE_TIER, VM1_IN_LONDON,
                                     VM2_IN_LONDON, DB_LONDON, DBS_LONDON, DBS_HONG_KONG,
                                     VM_IN_OHIO, BUSINESS_ACC1, BUSINESS_ACC2, BUSINESS_ACC3,
                                     VIRTUAL_VOLUME_IN_OHIO, STORAGE_TIER);

    private static final Set<TopologyEntity.Builder> EXPECTED_ENTITIES_FOR_REGION = Stream
                    .of(AZ1_LONDON, AZ2_LONDON, REGION_LONDON, COMPUTE_TIER,
                        VM1_IN_LONDON, VM2_IN_LONDON, DB_LONDON, DBS_LONDON, BUSINESS_ACC1,
                        BUSINESS_ACC2, STORAGE_TIER)
                    .collect(Collectors.collectingAndThen(Collectors.toSet(),
                                                          Collections::unmodifiableSet));

    private static final Set<TopologyEntity.Builder> EXPECTED_ENTITIES_FOR_REGIONS_LIST = Stream
                    .of(AZ1_LONDON, AZ2_LONDON, AZ_OHIO, REGION_LONDON, REGION_OHIO, COMPUTE_TIER,
                        VM1_IN_LONDON, VM2_IN_LONDON, VM_IN_OHIO, DB_LONDON, DBS_LONDON,
                        BUSINESS_ACC1, BUSINESS_ACC2, STORAGE_TIER, VIRTUAL_VOLUME_IN_OHIO)
                    .collect(Collectors.collectingAndThen(Collectors.toSet(),
                                                          Collections::unmodifiableSet));

    private GroupResolver groupResolver = mock(GroupResolver.class);
    private PlanTopologyScopeEditor planTopologyScopeEditor;
    private GroupServiceMole groupServiceClient = spy(new GroupServiceMole());

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(groupServiceClient);

    @Before
    public void setup() {
        planTopologyScopeEditor = new PlanTopologyScopeEditor(GroupServiceGrpc
                .newBlockingStub(grpcServer.getChannel()));
    }

    /**
     * Tests scope cloud topology for the plan scope with single region.
     * Topology graph contains entities for 2 targets: hypervisor and cloud.
     * EXPECTED_ENTITIES_FOR_REGION - set of cloud entities expected as result of applying plan scope to the topology.
     */
    @Test
    public void testScopeCloudTopologyForRegion() {
        final PlanScope planScope = PlanScope.newBuilder()
                        .addScopeEntries(PlanScopeEntry.newBuilder()
                                        .setClassName(StringConstants.REGION)
                                        .setScopeObjectOid(2001L).setDisplayName("London").build())
                        .build();
        testScopeCloudTopology(planScope, EXPECTED_ENTITIES_FOR_REGION);
    }

    /**
     * Tests scope cloud topology for the plan scope with 2 regions.
     * Topology graph contains entities for 2 targets: hypervisor and cloud.
     * EXPECTED_ENTITIES_FOR_REGIONS_LIST - set of cloud entities expected as result of applying plan scope to the topology.
     */
    @Test
    public void testScopeCloudTopologyForRegionsList() {
        final PlanScope planScope = PlanScope.newBuilder()
                        .addScopeEntries(PlanScopeEntry.newBuilder()
                                        .setClassName(StringConstants.REGION)
                                        .setScopeObjectOid(2001L).setDisplayName("London").build())
                        .addScopeEntries(PlanScopeEntry.newBuilder()
                                         .setClassName(StringConstants.REGION)
                                         .setScopeObjectOid(2002L).setDisplayName("Ohio").build())
                        .build();
        testScopeCloudTopology(planScope, EXPECTED_ENTITIES_FOR_REGIONS_LIST);
    }

    private void testScopeCloudTopology(PlanScope planScope, Set<TopologyEntity.Builder> expectedEntities) {
        final TopologyGraph<TopologyEntity> result = planTopologyScopeEditor
                        .scopeCloudTopology(GRAPH, planScope);
        Assert.assertEquals(expectedEntities.size(), result.size());
        expectedEntities.forEach(entity -> assertTrue(entity.getOid() + " is missing", result.getEntity(entity.getOid())
                        .isPresent()));
    }

    @Test
    @Ignore
    public void testScopeOnpremTopologyOnCluster() throws PipelineStageException {
        TopologyInfo topologyInfo = TopologyInfo.newBuilder().build();
        Group g = Group.newBuilder().setCluster(ClusterInfo.newBuilder()
                .setMembers(StaticGroupMembers.newBuilder().addStaticMemberOids(PM1_IN_DC1.getOid())
                        .addStaticMemberOids(PM2_IN_DC1.getOid()))).build();
        List<Group> groups = Arrays.asList(g);
        when(groupServiceClient.getGroups(GetGroupsRequest.newBuilder().addId(90001L)
                .setResolveClusterSearchFilters(true).build())).thenReturn(groups);
        when(groupResolver.resolve(eq(g), eq(GRAPH))).thenReturn(new HashSet<>(Arrays.asList(PM1_IN_DC1.getOid(), PM2_IN_DC1.getOid())));

        final PlanScope planScope = PlanScope.newBuilder()
                        .addScopeEntries(PlanScopeEntry.newBuilder().setClassName("Cluster")
                                .setScopeObjectOid(90001L).setDisplayName("PM cluster/DC1").build()).build();
        TopologyGraph<TopologyEntity> result = planTopologyScopeEditor
                .scopeOnPremTopology(topologyInfo, GRAPH, planScope, groupResolver,  new ArrayList<ScenarioChange>());
        assertTrue(result.size() == 11);
    }

    @Test
    @Ignore
    public void testScopeOnpremTopologyOnBA() throws PipelineStageException {
        TopologyInfo topologyInfo = TopologyInfo.newBuilder().build();

        final PlanScope planScope = PlanScope.newBuilder()
                        .addScopeEntries(PlanScopeEntry.newBuilder().setClassName("BusinessApplication")
                                .setScopeObjectOid(80001L).setDisplayName("BusinessApplication1").build()).build();
        TopologyGraph<TopologyEntity> result = planTopologyScopeEditor
                .scopeOnPremTopology(topologyInfo, GRAPH, planScope, groupResolver,  new ArrayList<ScenarioChange>());
        assertTrue(result.size() == 16);
    }

    @Test
    public void testScopeOnpremTopologyOnStorage() throws PipelineStageException {
        TopologyInfo topologyInfo = TopologyInfo.newBuilder().build();

        final PlanScope planScope = PlanScope.newBuilder()
                        .addScopeEntries(PlanScopeEntry.newBuilder().setClassName("Storage")
                                .setScopeObjectOid(40002L).setDisplayName("Storage2").build()).build();
        TopologyGraph<TopologyEntity> result = planTopologyScopeEditor
                .scopeOnPremTopology(topologyInfo, GRAPH, planScope, groupResolver,  new ArrayList<ScenarioChange>());
        result.entities().forEach(e -> System.out.println(e.getOid() + " "));
        assertTrue(result.size() == 6);
    }

    @Test
    @Ignore
    public void testScopeOnpremTopologyOnVM() throws PipelineStageException {
        TopologyInfo topologyInfo = TopologyInfo.newBuilder().build();

        final PlanScope planScope = PlanScope.newBuilder()
                        .addScopeEntries(PlanScopeEntry.newBuilder().setClassName("VirtualMachine")
                                .setScopeObjectOid(30002L).setDisplayName("VM2").build()).build();
        TopologyGraph<TopologyEntity> result = planTopologyScopeEditor
                .scopeOnPremTopology(topologyInfo, GRAPH, planScope, groupResolver,  new ArrayList<ScenarioChange>());
        result.entities().forEach(e -> System.out.println(e.getOid() + " "));
        assertTrue(result.size() == 11);
    }

    private static TopologyEntity.Builder createHypervisorTopologyEntity(long oid,
                                                                         String displayName,
                                                                         EntityType entityType,
                                                                         long... producers) {
        return TopologyEntityUtils.topologyEntity(oid, HYPERVISOR_TARGET, 0, displayName,
                                                  entityType, producers);
    }

    private static TopologyEntity.Builder createCloudTopologyAvailabilityZone(long oid,
                                                                              String displayName) {
        return TopologyEntityUtils.topologyEntity(oid, CLOUD_TARGET, 0, displayName, EntityType.AVAILABILITY_ZONE);
    }

    private static TopologyEntity.Builder createCloudConnectedTopologyEntity(long oid,
                                                                             String displayName,
                                                                             EntityType entityType,
                                                                             long... connectedToEntities) {
        return TopologyEntityUtils.connectedTopologyEntity(oid, CLOUD_TARGET, 0, displayName,
                                                           entityType, connectedToEntities);
    }

}
