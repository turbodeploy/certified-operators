package com.vmturbo.market.topology.conversions;

import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.LongStream;

import com.google.common.collect.ImmutableSet;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingGroup;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingPolicyServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.market.topology.conversions.ConsistentScalingHelper.ConsistentScalingHelperFactory;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySpecificationTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class ConsistentScalingHelperTest {

    private static final long REALTIME_TOPOLOGY_CONTEXT_ID = 7777777;
    private static final int TEST_TOPOLOGY_ID = 5678;

    Set<Long> invalidOids = ImmutableSet.of(101L);
    Set<Long> poweredOffOids = ImmutableSet.of(6L, 7L);
    Set<Long> onPremOids = ImmutableSet.of(1L, 4L);

    private Map<Long, TopologyEntityDTO> topology;
    SettingPolicyServiceBlockingStub settingsPolicyService;
    ConsistentScalingHelperFactory consistentScalingHelperFactory =
            new ConsistentScalingHelperFactory(settingsPolicyService);
    private SettingPolicyServiceMole settingPolicyServiceMole = spy(new SettingPolicyServiceMole());

    /**
     * Stub server for group queries.
     */
    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(settingPolicyServiceMole);

    TopologyInfo topologyInfo = TopologyInfo.newBuilder()
        .setTopologyContextId(REALTIME_TOPOLOGY_CONTEXT_ID)
        .setTopologyId(TEST_TOPOLOGY_ID).build();

    private CloudTopology<TopologyEntityDTO> cloudTopology;

    @Before
    public void setup() throws IOException {
        topology = new HashMap<>();
        grpcTestServer.start();
        settingsPolicyService = SettingPolicyServiceGrpc.newBlockingStub(grpcTestServer.getChannel());
        consistentScalingHelperFactory = new ConsistentScalingHelperFactory(settingsPolicyService);
        cloudTopology = mock(CloudTopology.class);
    }

    @After
    public void tearDown() {
        grpcTestServer.close();
    }

    /**
     * This just wraps fetchConsistentScalingSettings, so we are actually testing that here. First,
     * define some EntitySettingGroup instances that can be returned via the call to
     * settingPolicyService.getEntitySettings(), then ensure that the appropriate ScalingGroups are
     * created.
     */
    @Test
    public void initialize() {
        // Create test groups: Group-1: VM-1, VM-2, Group-2, VM-3
        long[][] membership = {
            {1L, 1L, 2L},  // Group-1: VM oid 1, 2
            {2L, 3L, 4L},   // Group-2: VM oid 3, 4
            {10L, 100L, 101L, 102L}  // Group-10: VM oid 100, 102.  101 is invalid and is skipped
        };
        ConsistentScalingHelper csh = createConsistentScalingHelper(membership);

        // Get group members
        Assert.assertEquals(new HashSet<>(Arrays.asList(1L, 2L)), csh.getGroupMembers("Group-1"));
        Assert.assertEquals(new HashSet<>(Arrays.asList(3L, 4L)), csh.getGroupMembers("Group-2"));
        // invalid group
        Assert.assertEquals(new HashSet<Long>(), csh.getGroupMembers(null));
        Assert.assertEquals(new HashSet<Long>(), csh.getGroupMembers("Group-3"));
        // Get peers
        Assert.assertEquals(new HashSet<>(Arrays.asList(1L, 2L)), csh.getPeers(1L, true));
        Assert.assertEquals(new HashSet<>(Arrays.asList(3L)), csh.getPeers(4L, false));
        // Get peers, invalid OID
        Assert.assertEquals(new HashSet<>(), csh.getPeers(10L, false));
        // Get peers, valid key, group wih invalid OID
        Assert.assertEquals(new HashSet<>(Arrays.asList(102L)), csh.getPeers(100L, false));
    }

    @Test
    public void testGetScalingGroup() {
        // Create test groups: Group-1: VM-1, VM-2, Group-2, VM-3
        long[][] membership = {
            {1L, 1L, 2L},  // Group-1: VM oid 1, 2
            {2L, 3L, 4L},   // Group-2: VM oid 3, 4
            {4L, 6L, 7L}  // No leader candidates, so do not create a scaling group
        };
        ConsistentScalingHelper csh = createConsistentScalingHelper(membership);

        TopologyEntityDTO te1 = topology.get(1L);
        Assert.assertFalse(csh.getScalingGroup(te1, true).isPresent());
        Assert.assertTrue(csh.getScalingGroup(te1, false).isPresent());
    }

    private ConsistentScalingHelper createConsistentScalingHelper(long[][] membership) {
        createSettingPolicies(membership);
        ConsistentScalingHelper csh = consistentScalingHelperFactory
            .newConsistentScalingHelper(null, null);
        csh.initialize(topology);
        return csh;
    }

    @Test
    public void testGetGroupFactor() {
        // Create test groups: Group-1: VM-1, VM-2, Group-2, VM-3
        long[][] membership = {
            {1L, 1L, 4L},  // Group-1: on-prem
            {2L, 2L, 3L}   // Group-2: cloud
        };
        ConsistentScalingHelper csh = createConsistentScalingHelper(membership);
        TopologyEntityDTO cloudEntity = topology.get(2L);
        TopologyEntityDTO nonCloudEntity = topology.get(4L);
        Assert.assertThat(csh.getGroupFactor(cloudEntity), is(2));
        Assert.assertThat(csh.getGroupFactor(nonCloudEntity), is(1));
    }

    private EntitySettingGroup createEntitySettingGroup(final long[] group) {
        // Create the test entities for this group
        for (long oid : group) {
            createEntity(oid);
        }
        StringSettingValue sv = StringSettingValue.newBuilder()
            .setValue(String.format("Group-%d", group[0]))
            .build();
        Setting membershipSetting = Setting.newBuilder()
            .setSettingSpecName(EntitySettingSpecs.ScalingGroupMembership.getSettingName())
            .setStringSettingValue(sv)
            .build();
        EntitySettingGroup entitySettingGroup = EntitySettingGroup.newBuilder()
            .setSetting(membershipSetting)
            .addAllEntityOids(LongStream.of(group).skip(1)::iterator)
            .build();
        return entitySettingGroup;
    }

    /**
     * Create a mock SettingPolicyServiceBlockingStub that will return scaling group configuration
     * entries.
     * @param membership List of membership entries.  Each entries is a list whose first element
     *                   is a group ID and the remaining entries is a list of member VM OIDs.
     * @return
     */
    private void createSettingPolicies(final long[][] membership) {
        GetEntitySettingsResponse.Builder response = GetEntitySettingsResponse.newBuilder();
        Arrays.stream(membership).forEach(group -> {
            response.addSettingGroup(createEntitySettingGroup(group));
        });
        when(settingPolicyServiceMole.getEntitySettings(any()))
            .thenReturn(Arrays.asList(response.build()));
    }

    private TopologyEntityDTO createEntity(Long oid) {
        TopologyEntityDTO.Builder te = TopologyEntityDTO.newBuilder()
            .setDisplayName("Entity-" + oid)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setEntityState(poweredOffOids.contains(oid)
                ? EntityState.POWERED_OFF
                : EntityState.POWERED_ON)
            .setOid(oid);
        if (!onPremOids.contains(oid)) {
            te.setEnvironmentType(EnvironmentType.CLOUD);
        }
        TopologyEntityDTO entity = te.build();
        if (!invalidOids.contains(oid)) {
            topology.put(oid, entity);
        }
        return entity;
    }

    private CommoditySoldTO createCommSoldTO(final int commType,
                                            final float usedValue) {
        return CommoditySoldTO.newBuilder()
            .setSpecification(CommoditySpecificationTO.newBuilder()
                .setType(commType).setBaseType(commType).build())
            .setQuantity(usedValue)
            .build();
    }

    private TraderTO.Builder createTraderTOBuilder(long oid, CommoditySoldTO... commSoldList) {
        return TraderTO.newBuilder().setOid(oid)
            .addAllCommoditiesSold(Arrays.asList(commSoldList));
    }

}
