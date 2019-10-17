package com.vmturbo.topology.processor.topology;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
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
import com.vmturbo.common.protobuf.topology.TopologyDTO.PlanTopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.components.api.test.GrpcTestServer;
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
    private static final int CLOUD_TARGET_1 = 1;
    private static final int CLOUD_TARGET_2 = 2;
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
    private static final TopologyEntity.Builder DB_LONDON = createCloudConnectedTopologyEntity(8001L, "DB in London", EntityType.DATABASE, AZ2_LONDON.getOid());
    private static final TopologyEntity.Builder DBS_LONDON = createCloudConnectedTopologyEntity(9001L, "DBS in London", EntityType.DATABASE_SERVER, AZ1_LONDON.getOid());
    private static final TopologyEntity.Builder DBS_HONG_KONG = createCloudConnectedTopologyEntity(9002L, "DBS in Hong Kong", EntityType.DATABASE_SERVER, AZ2_HONG_KONG.getOid());
    private static final TopologyEntity.Builder COMPUTE_TIER = createCloudConnectedTopologyEntity(3001L, "Compute tier", EntityType.COMPUTE_TIER, REGION_LONDON.getOid(), REGION_OHIO.getOid());
    private static final TopologyEntity.Builder STORAGE_TIER = createCloudConnectedTopologyEntity(7001L, "Storage tier", EntityType.STORAGE_TIER, REGION_LONDON.getOid(), REGION_OHIO.getOid(), REGION_HONG_KONG.getOid());
    private static final TopologyEntity.Builder VIRTUAL_VOLUME_IN_LONDON = createCloudConnectedTopologyEntity(6002L, "Virtual Volume in London", EntityType.VIRTUAL_VOLUME, AZ1_LONDON.getOid(), STORAGE_TIER.getOid());
    private static final TopologyEntity.Builder VM1_IN_LONDON = createCloudConnectedTopologyEntity(4001L, "VM1 in London", EntityType.VIRTUAL_MACHINE, AZ1_LONDON.getOid(), VIRTUAL_VOLUME_IN_LONDON.getOid());
    private static final TopologyEntity.Builder VM2_IN_LONDON = createCloudConnectedTopologyEntity(4002L, "VM2 in London", EntityType.VIRTUAL_MACHINE, AZ2_LONDON.getOid(), VIRTUAL_VOLUME_IN_LONDON.getOid());
    private static final TopologyEntity.Builder VIRTUAL_VOLUME_IN_OHIO = createCloudConnectedTopologyEntity(6001L, "Virtual Volume in Ohio", EntityType.VIRTUAL_VOLUME, AZ_OHIO.getOid(), STORAGE_TIER.getOid());
    private static final TopologyEntity.Builder VM_IN_OHIO = createCloudConnectedTopologyEntity(4003L, "VM in Ohio", EntityType.VIRTUAL_MACHINE, AZ_OHIO.getOid(), VIRTUAL_VOLUME_IN_OHIO.getOid());
    private static final TopologyEntity.Builder VIRTUAL_VOLUME_IN_HONG_KONG = createCloudConnectedTopologyEntity(6003L, "Virtual Volume in Hong Kong", EntityType.VIRTUAL_VOLUME, AZ1_HONG_KONG.getOid(), STORAGE_TIER.getOid());
    private static final TopologyEntity.Builder VM_IN_HONG_KONG = createCloudConnectedTopologyEntity(4004L, "VM in Hong Kong", EntityType.VIRTUAL_MACHINE, AZ1_HONG_KONG.getOid(), VIRTUAL_VOLUME_IN_HONG_KONG.getOid());
    private static final TopologyEntity.Builder BUSINESS_ACC2 = createCloudConnectedTopologyEntity(5002L, "Business account 2", EntityType.BUSINESS_ACCOUNT, VM_IN_OHIO.getOid());
    private static final TopologyEntity.Builder BUSINESS_ACC3 = createCloudConnectedTopologyEntity(5003L, "Business account 3", EntityType.BUSINESS_ACCOUNT, VM_IN_HONG_KONG.getOid());
    private static final TopologyEntity.Builder BUSINESS_ACC1 = createCloudConnectedTopologyEntity(5001L, "Business account 1", EntityType.BUSINESS_ACCOUNT, BUSINESS_ACC3.getOid(), VM1_IN_LONDON.getOid());
    private static final TopologyEntity.Builder REGION_CENTRAL_US = createCloud2ConnectedTopologyEntity(2004L, "Central US", EntityType.REGION);
    private static final TopologyEntity.Builder REGION_CANADA = createCloud2ConnectedTopologyEntity(2005L, "Canada", EntityType.REGION);
    private static final TopologyEntity.Builder DB_CENTRAL_US = createCloud2ConnectedTopologyEntity(8002L, "DB in Central US", EntityType.DATABASE, REGION_CENTRAL_US.getOid());
    private static final TopologyEntity.Builder DBS_CENTRAL_US = createCloud2ConnectedTopologyEntity(9003L, "DBS in Central US", EntityType.DATABASE_SERVER, REGION_CENTRAL_US.getOid());
    private static final TopologyEntity.Builder COMPUTE_TIER_2 = createCloud2ConnectedTopologyEntity(3002L, "Compute tier 2", EntityType.COMPUTE_TIER, REGION_CENTRAL_US.getOid(), REGION_CANADA.getOid());
    private static final TopologyEntity.Builder STORAGE_TIER_2 = createCloud2ConnectedTopologyEntity(7002L, "Storage tier 2", EntityType.STORAGE_TIER, REGION_CENTRAL_US.getOid(), REGION_CANADA.getOid());
    private static final TopologyEntity.Builder VIRTUAL_VOLUME_IN_CENTRAL_US = createCloud2ConnectedTopologyEntity(6004L, "Virtual Volume in Central US", EntityType.VIRTUAL_VOLUME, REGION_CENTRAL_US.getOid(), STORAGE_TIER_2.getOid());
    private static final TopologyEntity.Builder VM_IN_CENTRAL_US = createCloud2ConnectedTopologyEntity(4005L, "VM in Central US", EntityType.VIRTUAL_MACHINE, REGION_CENTRAL_US.getOid(), VIRTUAL_VOLUME_IN_CENTRAL_US.getOid());
    private static final TopologyEntity.Builder VIRTUAL_VOLUME_IN_CANADA = createCloud2ConnectedTopologyEntity(6005L, "Virtual Volume in Canada", EntityType.VIRTUAL_VOLUME, REGION_CANADA.getOid(), STORAGE_TIER_2.getOid());
    private static final TopologyEntity.Builder VM_IN_CANADA = createCloud2ConnectedTopologyEntity(4006L, "VM in Canada", EntityType.VIRTUAL_MACHINE, REGION_CANADA.getOid(), VIRTUAL_VOLUME_IN_CANADA.getOid());
    private static final TopologyEntity.Builder BUSINESS_ACC4 = createCloud2ConnectedTopologyEntity(5004L, "Business account 4", EntityType.BUSINESS_ACCOUNT, VM_IN_CANADA.getOid());

    /* Creating an on prem topology.

                       ba
                    /     \
                   /       \
                  /         \
        app1    as1         as2
           \  /              |
           vm1               vm3
           / \               / \
        pm1  vv            pm3  st2
        /     \            |
     dc1     st1-da1      dc2
      |       /
     pm2    /
       \   /
        vm2

     */
    private static final TopologyGraph<TopologyEntity> GRAPH = TopologyEntityUtils
        .topologyGraphOf(APP1, VM1_IN_DC1, VM2_IN_DC1, VM_IN_DC2, PM1_IN_DC1,
            PM2_IN_DC1, PM_IN_DC2, DC1, DC2, ST1, ST2, DA1, BAPP1, AS1,
            AS2, VIRTUAL_VOLUME, AZ1_LONDON, AZ2_LONDON, AZ_OHIO,
            AZ1_HONG_KONG, AZ2_HONG_KONG, REGION_LONDON, REGION_OHIO,
            REGION_HONG_KONG, COMPUTE_TIER, VM1_IN_LONDON,
            VM2_IN_LONDON, DB_LONDON, DBS_LONDON, DBS_HONG_KONG,
            VM_IN_OHIO, VM_IN_HONG_KONG, BUSINESS_ACC1, BUSINESS_ACC2, BUSINESS_ACC3,
            VIRTUAL_VOLUME_IN_OHIO, VIRTUAL_VOLUME_IN_LONDON, VIRTUAL_VOLUME_IN_HONG_KONG,
            STORAGE_TIER, REGION_CENTRAL_US, REGION_CANADA, DB_CENTRAL_US, DBS_CENTRAL_US,
            COMPUTE_TIER_2, STORAGE_TIER_2, VIRTUAL_VOLUME_IN_CENTRAL_US,
            VM_IN_CENTRAL_US, VIRTUAL_VOLUME_IN_CANADA, VM_IN_CANADA, BUSINESS_ACC4);

    private static final Set<TopologyEntity.Builder> EXPECTED_ENTITIES_FOR_AWS_REGION = Stream
        .of(AZ1_LONDON, AZ2_LONDON, REGION_LONDON, COMPUTE_TIER,
            VM1_IN_LONDON, VM2_IN_LONDON, DB_LONDON, DBS_LONDON, BUSINESS_ACC1,
            VIRTUAL_VOLUME_IN_LONDON, STORAGE_TIER)
        .collect(Collectors.collectingAndThen(Collectors.toSet(),
            Collections::unmodifiableSet));

    private static final Set<TopologyEntity.Builder> EXPECTED_ENTITIES_FOR_AZURE_REGION = Stream
                    .of(REGION_CENTRAL_US, DB_CENTRAL_US, DBS_CENTRAL_US, COMPUTE_TIER_2,
                        STORAGE_TIER_2, VIRTUAL_VOLUME_IN_CENTRAL_US, VM_IN_CENTRAL_US)
                    .collect(Collectors.collectingAndThen(Collectors.toSet(),
                        Collections::unmodifiableSet));

    private static final Set<TopologyEntity.Builder> EXPECTED_ENTITIES_FOR_REGIONS_LIST = Stream
        .of(AZ1_LONDON, AZ2_LONDON, AZ_OHIO, REGION_LONDON, REGION_OHIO, COMPUTE_TIER,
            VM1_IN_LONDON, VM2_IN_LONDON, VM_IN_OHIO, DB_LONDON, DBS_LONDON, BUSINESS_ACC1,
            BUSINESS_ACC2, STORAGE_TIER, VIRTUAL_VOLUME_IN_LONDON, VIRTUAL_VOLUME_IN_OHIO)
        .collect(Collectors.collectingAndThen(Collectors.toSet(),
            Collections::unmodifiableSet));

    private static final Set<TopologyEntity.Builder> EXPECTED_ENTITIES_FOR_BUSINESS_ACCOUNT = Stream
                    .of(AZ1_HONG_KONG, VM_IN_HONG_KONG, STORAGE_TIER, REGION_HONG_KONG,
                        BUSINESS_ACC1, BUSINESS_ACC3, VIRTUAL_VOLUME_IN_HONG_KONG)
                    .collect(Collectors.collectingAndThen(Collectors.toSet(),
                                                          Collections::unmodifiableSet));

    private static final Set<TopologyEntity.Builder> EXPECTED_ENTITIES_FOR_BUSINESS_ACCOUNTS_LIST = Stream
                    .of(AZ_OHIO, VM_IN_OHIO, BUSINESS_ACC1, COMPUTE_TIER, STORAGE_TIER,
                        BUSINESS_ACC2, BUSINESS_ACC3, AZ1_HONG_KONG, VM_IN_HONG_KONG,
                        REGION_OHIO, REGION_HONG_KONG, VIRTUAL_VOLUME_IN_HONG_KONG, VIRTUAL_VOLUME_IN_OHIO)
                    .collect(Collectors.collectingAndThen(Collectors.toSet(),
                                                          Collections::unmodifiableSet));

    private static final Set<TopologyEntity.Builder> EXPECTED_ENTITIES_FOR_BILLING_FAMILY = Stream
                    .of(AZ1_LONDON, VM1_IN_LONDON, COMPUTE_TIER, STORAGE_TIER,
                        BUSINESS_ACC1, BUSINESS_ACC3, AZ1_HONG_KONG, VM_IN_HONG_KONG,
                        REGION_LONDON, REGION_HONG_KONG, VIRTUAL_VOLUME_IN_HONG_KONG,
                        VIRTUAL_VOLUME_IN_LONDON)
                    .collect(Collectors.collectingAndThen(Collectors.toSet(),
                                                          Collections::unmodifiableSet));

    private static final Set<TopologyEntity.Builder> EXPECTED_ENTITIES_FOR_AWS_VM = Stream
                    .of(AZ1_LONDON, REGION_LONDON, VM1_IN_LONDON, BUSINESS_ACC1,
                        COMPUTE_TIER, VIRTUAL_VOLUME_IN_LONDON, STORAGE_TIER)
                    .collect(Collectors.collectingAndThen(Collectors.toSet(),
                                                          Collections::unmodifiableSet));

    private static final Set<TopologyEntity.Builder> EXPECTED_ENTITIES_FOR_AZURE_DB = Stream
                    .of(DB_CENTRAL_US, REGION_CENTRAL_US, COMPUTE_TIER_2, STORAGE_TIER_2)
                    .collect(Collectors.collectingAndThen(Collectors.toSet(),
                                                          Collections::unmodifiableSet));

    private static final Set<TopologyEntity.Builder> EXPECTED_ENTITIES_FOR_AWS_DBS_GROUP = Stream
                    .of(AZ1_LONDON, REGION_LONDON, DBS_LONDON, COMPUTE_TIER, STORAGE_TIER,
                        DBS_HONG_KONG, AZ2_HONG_KONG, REGION_HONG_KONG)
                    .collect(Collectors.collectingAndThen(Collectors.toSet(),
                                                          Collections::unmodifiableSet));

    private static final GroupResolver groupResolver = mock(GroupResolver.class);
    private PlanTopologyScopeEditor planTopologyScopeEditor;
    private static final GroupServiceMole groupServiceClient = spy(new GroupServiceMole());

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(groupServiceClient);

    @Before
    public void setup() {
        planTopologyScopeEditor = new PlanTopologyScopeEditor(GroupServiceGrpc
            .newBlockingStub(grpcServer.getChannel()));
    }

    /**
     * Tests scope cloud topology for the plan scope with single region.
     * Topology graph contains entities for 3 targets: hypervisor and 2 clouds.
     * AWS target has Availability Zone entities.
     * EXPECTED_ENTITIES_FOR_REGION - set of cloud entities expected as result of applying plan scope to the topology.
     */
    @Test
    public void testScopeCloudTopologyForAWSRegion() {
        // Region London
        final List<Long> oidsList = Arrays.asList(2001L);
        testScopeCloudTopology(oidsList, EXPECTED_ENTITIES_FOR_AWS_REGION);
    }

    /**
     * Tests scope cloud topology for the plan scope with single region.
     * Topology graph contains entities for 3 targets: hypervisor and 2 clouds.
     * Azure target doesn't have Availability Zone entities.
     * EXPECTED_ENTITIES_FOR_REGION - set of cloud entities expected as result of applying plan scope to the topology.
     */
    @Test
    public void testScopeCloudTopologyForAzureRegion() {
        // Region Central US
        final List<Long> oidsList = Arrays.asList(2004L);
        testScopeCloudTopology(oidsList, EXPECTED_ENTITIES_FOR_AZURE_REGION);
    }

    /**
     * Tests scope cloud topology for the plan scope with 2 regions.
     * Topology graph contains entities for 3 targets: hypervisor and 2 clouds.
     * EXPECTED_ENTITIES_FOR_REGIONS_LIST - set of cloud entities expected as result of applying plan scope to the topology.
     */
    @Test
    public void testScopeCloudTopologyForRegionsList() {
        // Regions London and Ohio
        final List<Long> oidsList = Arrays.asList(2001L, 2002L);
        testScopeCloudTopology(oidsList, EXPECTED_ENTITIES_FOR_REGIONS_LIST);
    }

    /**
     * Tests scope cloud topology for the plan scope with 2 regions.
     * Topology graph contains entities for 3 targets: hypervisor and 2 clouds.
     * EXPECTED_ENTITIES_FOR_BUSINESS_ACCOUNT - set of cloud entities expected as result of applying plan scope to the topology.
     */
    @Test
    public void testScopeCloudTopologyForBusinessAccount() {
        // Business account 3
        final List<Long> oidsList = Arrays.asList(5003L);
        testScopeCloudTopology(oidsList, EXPECTED_ENTITIES_FOR_BUSINESS_ACCOUNT);
    }

    /**
     * Tests scope cloud topology for the plan scope with 2 regions.
     * Topology graph contains entities for 3 targets: hypervisor and 2 clouds.
     * EXPECTED_ENTITIES_FOR_BUSINESS_ACCOUNT - set of cloud entities expected as result of applying plan scope to the topology.
     */
    @Test
    public void testScopeCloudTopologyForBusinessAccountsList() {
        // Business account 2 and Business account 3
        final List<Long> oidsList = Arrays.asList(5002L, 5003L);
        testScopeCloudTopology(oidsList, EXPECTED_ENTITIES_FOR_BUSINESS_ACCOUNTS_LIST);
    }

    /**
     * Tests scope cloud topology for the plan scope with 2 regions.
     * Topology graph contains entities for 3 targets: hypervisor and 2 clouds.
     * EXPECTED_ENTITIES_FOR_BUSINESS_ACCOUNT - set of cloud entities expected as result of applying plan scope to the topology.
     */
    @Test
    public void testScopeCloudTopologyForBillingFamily() {
        // Billing family
        final List<Long> oidsList = Arrays.asList(5001L);
        testScopeCloudTopology(oidsList, EXPECTED_ENTITIES_FOR_BILLING_FAMILY);
    }

    /**
     * Tests scope cloud topology for the plan scope with 1 VM.
     * Topology graph contains entities for 3 targets: hypervisor and 2 clouds.
     * EXPECTED_ENTITIES_FOR_AWS_VM - set of cloud entities expected as result of applying plan scope to the topology.
     */
    @Test
    public void testScopeCloudTopologyForVM() {
        // VM1 in London
        final List<Long> oidsList = Arrays.asList(4001L);
        testScopeCloudTopology(oidsList, EXPECTED_ENTITIES_FOR_AWS_VM);
    }

    /**
     * Tests scope cloud topology for the plan scope with 1 DB.
     * Topology graph contains entities for 3 targets: hypervisor and 2 clouds.
     * EXPECTED_ENTITIES_FOR_AWS_VM - set of cloud entities expected as result of applying plan scope to the topology.
     */
    @Test
    public void testScopeCloudTopologyForDB() {
        // DB in Central US
        final List<Long> oidsList = Arrays.asList(8002L);
        testScopeCloudTopology(oidsList, EXPECTED_ENTITIES_FOR_AZURE_DB);
    }

    /**
     * Tests scope cloud topology for the plan scope with group of 2 DBS.
     * Topology graph contains entities for 3 targets: hypervisor and 2 clouds.
     * EXPECTED_ENTITIES_FOR_AWS_DBS_GROUP - set of cloud entities expected as result of applying plan scope to the topology.
     */
    @Test
    public void testScopeCloudTopologyForDBSGroup() {
        // DBS in London and DBS in Hong Kong
        final List<Long> oidsList = Arrays.asList(9001L, 9002L);
        testScopeCloudTopology(oidsList, EXPECTED_ENTITIES_FOR_AWS_DBS_GROUP);
    }

    private void testScopeCloudTopology(List<Long> oidsList, Set<TopologyEntity.Builder> expectedEntities) {
        final TopologyInfo cloudTopologyInfo = TopologyInfo.newBuilder()
                        .setTopologyContextId(1)
                        .setTopologyId(1)
                        .setTopologyType(TopologyType.PLAN)
                        .setPlanInfo(PlanTopologyInfo.newBuilder().setPlanType("OPTIMIZE_CLOUD").build())
                        .addAllScopeSeedOids(oidsList)
                        .build();
        final TopologyGraph<TopologyEntity> result = planTopologyScopeEditor.scopeCloudTopology(cloudTopologyInfo, GRAPH);
        Assert.assertEquals(expectedEntities.size(), result.size());
        expectedEntities.forEach(entity -> assertTrue(entity.getOid() + " is missing", result.getEntity(entity.getOid())
                        .isPresent()));
    }

    /**
     * Scenario: scope on pm1 and pm2 which consumes on dc1.
     * Expected: the entities in scope should be vm1, vm2, pm1, pm2, dc1, vv, st1, da1, app1, as1, ba
     *
     * @throws PipelineStageException An exception thrown when a stage of the pipeline fails.
     */
    @Test
    public void testScopeOnpremTopologyOnCluster() throws PipelineStageException {
        TopologyInfo topologyInfo = TopologyInfo.getDefaultInstance();
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
        assertEquals(11, result.size());
    }

    /**
     * Scenario: scope on ba which consumes as1 and as2.
     * Expected: the entities in scope should be ba, as1, app1, vm1, vm2, pm1, pm2, dc1, vv, st1,
     * da1, as2, vm3, pm3, dc2, st2
     *
     * @throws PipelineStageException An exception thrown when a stage of the pipeline fails.
     */
    @Test
    public void testScopeOnpremTopologyOnBA() throws PipelineStageException {
        TopologyInfo topologyInfo = TopologyInfo.getDefaultInstance();
        final PlanScope planScope = PlanScope.newBuilder()
                        .addScopeEntries(PlanScopeEntry.newBuilder().setClassName("BusinessApplication")
                                .setScopeObjectOid(80001L).setDisplayName("BusinessApplication1").build()).build();
        TopologyGraph<TopologyEntity> result = planTopologyScopeEditor
                .scopeOnPremTopology(topologyInfo, GRAPH, planScope, groupResolver,  new ArrayList<ScenarioChange>());
        assertEquals(16, result.size());
    }

    /**
     * Scenario: scope on st2 which hosts vm on dc2.
     * Expected: the entities in scope should be ba, as2, vm3, pm3, st2, dc2
     *
     * @throws PipelineStageException An exception thrown when a stage of the pipeline fails.
     */
    @Test
    public void testScopeOnpremTopologyOnStorage() throws PipelineStageException {
        TopologyInfo topologyInfo = TopologyInfo.getDefaultInstance();
        final PlanScope planScope = PlanScope.newBuilder()
                        .addScopeEntries(PlanScopeEntry.newBuilder().setClassName("Storage")
                                .setScopeObjectOid(40002L).setDisplayName("Storage2").build()).build();
        TopologyGraph<TopologyEntity> result = planTopologyScopeEditor
                .scopeOnPremTopology(topologyInfo, GRAPH, planScope, groupResolver,  new ArrayList<ScenarioChange>());
        result.entities().forEach(e -> System.out.println(e.getOid() + " "));
        assertEquals(6, result.size());
    }

    /**
     * Scenario: scope on vm2 which consumes pm2 on dc1, st1 on da1. The vm2 hosts no application at all.
     * Expected: the entities in scope should be vm1, vm2, pm1, pm2, dc1, vv, st1, da1, app1, as1, ba
     *
     * @throws PipelineStageException An exception thrown when a stage of the pipeline fails.
     */
    @Test
    public void testScopeOnpremTopologyOnVM() throws PipelineStageException {
        TopologyInfo topologyInfo = TopologyInfo.getDefaultInstance();
        final PlanScope planScope = PlanScope.newBuilder()
                        .addScopeEntries(PlanScopeEntry.newBuilder().setClassName("VirtualMachine")
                                .setScopeObjectOid(30002L).setDisplayName("VM2").build()).build();
        TopologyGraph<TopologyEntity> result = planTopologyScopeEditor
                .scopeOnPremTopology(topologyInfo, GRAPH, planScope, groupResolver,  new ArrayList<ScenarioChange>());
        result.entities().forEach(e -> System.out.println(e.getOid() + " "));
        assertEquals(11, result.size());
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
        return TopologyEntityUtils.topologyEntity(oid, CLOUD_TARGET_1, 0, displayName, EntityType.AVAILABILITY_ZONE);
    }

    private static TopologyEntity.Builder createCloudConnectedTopologyEntity(long oid,
                                                                             String displayName,
                                                                             EntityType entityType,
                                                                             long... connectedToEntities) {
        return TopologyEntityUtils.connectedTopologyEntity(oid, CLOUD_TARGET_1, 0, displayName,
                                                           entityType, connectedToEntities);
    }

    private static TopologyEntity.Builder createCloud2ConnectedTopologyEntity(long oid,
                                                                             String displayName,
                                                                             EntityType entityType,
                                                                             long... connectedToEntities) {
        return TopologyEntityUtils.connectedTopologyEntity(oid, CLOUD_TARGET_2, 0, displayName,
                                                           entityType, connectedToEntities);
    }

}
