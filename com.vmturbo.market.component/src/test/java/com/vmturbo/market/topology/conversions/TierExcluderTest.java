package com.vmturbo.market.topology.conversions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingGroup;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingGroup.SettingPolicyId;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.ListSettingPoliciesRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.SortedSetOfOidSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingPolicyServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.market.topology.conversions.TierExcluder.TierExcluderFactory;
import com.vmturbo.market.topology.conversions.TierExcluder.TierExcluderFactory.DefaultTierExcluderFactory;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActionTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActivateTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.Compliance;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.CompoundMoveTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.DeactivateTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.MoveExplanation;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.MoveTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.Performance;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ProvisionByDemandTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ProvisionBySupplyTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ReconfigureTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ResizeTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Unit tests for Tier exclusion applicator.
 */
public class TierExcluderTest {

    private static final long REALTIME_TOPOLOGY_CONTEXT_ID = 7777777;
    private static final int TEST_TOPOLOGY_ID = 5678;

    private static final String T2_FAMILY = "t2";
    private static final String M4_FAMILY = "m4";

    private static final long VM1 = 101L;
    private static final long VM2 = 102L;
    private static final long VM1SlOid = 1000L;
    private static final long VM2SlOid = 1010L;
    private static final List<Long> consumers = Arrays.asList(VM1, VM2);

    private Map<Long, TopologyEntityDTO> topology;

    private SettingPolicyServiceMole settingPolicyServiceMole = spy(new SettingPolicyServiceMole());

    /**
     * GRPC test server which can be used to mock rpc calls.
     */
    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(settingPolicyServiceMole);

    TopologyInfo topologyInfo = TopologyInfo.newBuilder()
        .setTopologyContextId(REALTIME_TOPOLOGY_CONTEXT_ID)
        .setTopologyId(TEST_TOPOLOGY_ID).build();

    SettingPolicyServiceBlockingStub settingsPolicyService;

    TierExcluder tierExcluder;

    private TierExcluderFactory tierExcluderFactory;

    private CommodityConverter commodityConverter;

    private CloudTopology<TopologyEntityDTO> cloudTopology;

    /**
     * Setup will run before every test.
     *
     * @throws IOException If there is an error starting up the GRPC test server.
     */
    @Before
    public void setup() throws IOException {
        grpcTestServer.start();
        settingsPolicyService = SettingPolicyServiceGrpc.newBlockingStub(grpcTestServer.getChannel());
        tierExcluderFactory = new DefaultTierExcluderFactory(settingsPolicyService);
        topology = createTestTopology();
        commodityConverter = mock(CommodityConverter.class);
        when(commodityConverter.commodityIdToCommodityType(anyInt()))
            .thenAnswer(invocation -> {
                int commSpecType = invocation.getArgumentAt(0, Integer.class);
                return CommodityType.newBuilder()
                    .setKey(TierExcluder.TIER_EXCLUSION_KEY_PREFIX + commSpecType)
                    .setType(CommodityDTO.CommodityType.SEGMENTATION_VALUE)
                    .build();
            });
        ShoppingListInfo slInfo1 = new ShoppingListInfo(VM1SlOid, VM1, null, null, null, Lists.newArrayList());
        ShoppingListInfo slInfo2 = new ShoppingListInfo(VM2SlOid, VM2, null, null, null, Lists.newArrayList());
        tierExcluder = tierExcluderFactory.newExcluder(topologyInfo, commodityConverter,
            ImmutableMap.of(VM1SlOid, slInfo1, VM2SlOid, slInfo2));
        cloudTopology = mock(CloudTopology.class);
    }

    /**
     * tearDown will run after every test.
     */
    @After
    public void tearDown() {
        grpcTestServer.close();
    }

    /**
     * 2 VMs - VM1 and VM2, 5 compute tiers - 1, 2, 3, 4, 5.
     * Policy 500 excludes compute tiers 1 and 2 for VM1.
     * Assert that VM1 can only go to tiers 3, 4, 5.
     */
    @Test
    public void testSingleSetting() {
        EntitySettingGroup entitySettingGroup1 = createEntitySettingGroup(
            Arrays.asList(1L, 2L), Arrays.asList(VM1), Arrays.asList(500L));
        GetEntitySettingsResponse response = GetEntitySettingsResponse.newBuilder()
            .addSettingGroup(entitySettingGroup1).build();
        when(settingPolicyServiceMole.getEntitySettings(any())).thenReturn(Arrays.asList(response));

        tierExcluder.initialize(topology, ImmutableSet.of(VM1));

        Map<Long, Set<Long>> expectedConsumerToPossibleSuppliers =
            ImmutableMap.of(
                VM1, LongStream.rangeClosed(3L, 5L).boxed().collect(Collectors.toSet()),
                VM2, LongStream.rangeClosed(1L, 5L).boxed().collect(Collectors.toSet()));
        assertCommodities(expectedConsumerToPossibleSuppliers, consumers);
        assertEquals(1, tierExcluder.getTierExclusionCommoditiesToBuy(VM1).size());
    }

    /**
     * 2 VMs - VM1 and VM2, 5 compute tiers - 1, 2, 3, 4, 5.
     * Policy 500 excludes compute tiers 1 and 2 for VM1.
     * Policy 501 excludes compute tiers 2 and 3 for VM1 and VM2.
     * Assert that VM1 can only go to tiers 4, 5.
     * Assert that VM2 can only go to tiers 1, 4, 5.
     * Assert that tiers 1, 3 sell only one commodity each.
     * Assert that tiers 4, 5 sell two commodities each.
     * Assert that VM1 buys two commodities.
     * Assert that VM2 buys one commodity.
     */
    @Test
    public void testTwoSettingsSameVM() {
        EntitySettingGroup entitySettingGroup1 = createEntitySettingGroup(
            Arrays.asList(1L, 2L), Arrays.asList(VM1), Arrays.asList(500L));
        EntitySettingGroup entitySettingGroup2 = createEntitySettingGroup(
            Arrays.asList(2L, 3L), Arrays.asList(VM1, VM2), Arrays.asList(501L));
        GetEntitySettingsResponse response = GetEntitySettingsResponse.newBuilder()
            .addSettingGroup(entitySettingGroup1)
            .addSettingGroup(entitySettingGroup2).build();
        when(settingPolicyServiceMole.getEntitySettings(any())).thenReturn(Arrays.asList(response));

        tierExcluder.initialize(topology, ImmutableSet.of(VM1, VM2));

        Map<Long, Set<Long>> expectedConsumerToPossibleSuppliers = ImmutableMap.of(
            VM1, ImmutableSet.of(4L, 5L),
            VM2, ImmutableSet.of(1L, 4L, 5L));
        assertCommodities(expectedConsumerToPossibleSuppliers, consumers);
        assertEquals(1, tierExcluder.getTierExclusionCommoditiesToSell(1L).size());
        assertEquals(1, tierExcluder.getTierExclusionCommoditiesToSell(3L).size());
        assertEquals(2, tierExcluder.getTierExclusionCommoditiesToSell(4L).size());
        assertEquals(2, tierExcluder.getTierExclusionCommoditiesToSell(5L).size());
        assertEquals(2, tierExcluder.getTierExclusionCommoditiesToBuy(VM1).size());
        assertEquals(1, tierExcluder.getTierExclusionCommoditiesToBuy(VM2).size());
    }

    /**
     * 2 VMs - VM1 and VM2, 5 compute tiers - 1, 2, 3, 4, 5.
     * Policy 500 excludes compute tiers 1 and 2 for VM1.
     * Policy 501 excludes compute tiers 1 and 2 for VM2.
     * Assert that VM1, VM2 can only go to tiers 3, 4, 5.
     * Assert that tiers 3, 4, 5 sell only one commodity each.
     */
    @Test
    public void testTwoSettingsSameExludedTiers() {
        EntitySettingGroup entitySettingGroup1 = createEntitySettingGroup(
            Arrays.asList(1L, 2L), Arrays.asList(VM1), Arrays.asList(500L));
        EntitySettingGroup entitySettingGroup2 = createEntitySettingGroup(
            Arrays.asList(2L, 1L), Arrays.asList(VM2), Arrays.asList(501L));
        GetEntitySettingsResponse response = GetEntitySettingsResponse.newBuilder()
            .addSettingGroup(entitySettingGroup1)
            .addSettingGroup(entitySettingGroup2).build();
        when(settingPolicyServiceMole.getEntitySettings(any())).thenReturn(Arrays.asList(response));

        tierExcluder.initialize(topology, ImmutableSet.of(VM1, VM2));

        Map<Long, Set<Long>> expectedConsumerToPossibleSuppliers =
            ImmutableMap.of(
                VM1, LongStream.rangeClosed(3L, 5L).boxed().collect(Collectors.toSet()),
                VM2, LongStream.rangeClosed(3L, 5L).boxed().collect(Collectors.toSet()));
        assertCommodities(expectedConsumerToPossibleSuppliers, consumers);
        assertEquals(1, tierExcluder.getTierExclusionCommoditiesToSell(3L).size());
        assertEquals(1, tierExcluder.getTierExclusionCommoditiesToSell(4L).size());
        assertEquals(1, tierExcluder.getTierExclusionCommoditiesToSell(5L).size());
        assertEquals(1, tierExcluder.getTierExclusionCommoditiesToBuy(VM1).size());
        assertEquals(1, tierExcluder.getTierExclusionCommoditiesToBuy(VM2).size());
    }

    /**
     * 2 VMs - VM1 and VM2, 5 compute tiers - 1, 2, 3, 4, 5.
     * Policy 500 excludes compute tiers 1 and 2 for VM1.
     * Policy 501 excludes compute tiers 3 and 4 for VM2.
     * Assert that VM1 can only go to tiers 3, 4, 5.
     * Assert that VM2 can only go to tiers 1, 2, 5.
     */
    @Test
    public void testTwoSettingsDifferentExludedTiers() {
        EntitySettingGroup entitySettingGroup1 = createEntitySettingGroup(
            Arrays.asList(1L, 2L), Arrays.asList(VM1), Arrays.asList(500L));
        EntitySettingGroup entitySettingGroup2 = createEntitySettingGroup(
            Arrays.asList(3L, 4L), Arrays.asList(VM2), Arrays.asList(501L));
        GetEntitySettingsResponse response = GetEntitySettingsResponse.newBuilder()
            .addSettingGroup(entitySettingGroup1)
            .addSettingGroup(entitySettingGroup2).build();
        when(settingPolicyServiceMole.getEntitySettings(any())).thenReturn(Arrays.asList(response));

        tierExcluder.initialize(topology, ImmutableSet.of(VM1, VM2));

        Map<Long, Set<Long>> expectedConsumerToPossibleSuppliers =
            ImmutableMap.of(
                VM1, LongStream.rangeClosed(3L, 5L).boxed().collect(Collectors.toSet()),
                VM2, ImmutableSet.of(1L, 2L, 5L));
        assertCommodities(expectedConsumerToPossibleSuppliers, consumers);
        assertEquals(2, tierExcluder.getTierExclusionCommoditiesToSell(5L).size());
        assertEquals(1, tierExcluder.getTierExclusionCommoditiesToBuy(VM1).size());
        assertEquals(1, tierExcluder.getTierExclusionCommoditiesToBuy(VM2).size());
    }

    /**
     * 2 VMs - VM1 and VM2, 5 compute tiers - 1, 2, 3, 4, 5.
     * Policy 500 excludes compute tier 1 for VM1.
     * Policy 501 excludes compute tier 2, 3 for VM2.
     * Assert that VM1 can only go to tiers 2, 3, 4, 5.
     * Assert that VM2 can only go to tier 1, 4, 5.
     * Assert that t2 and m4 families sells 2 commodities.
     */
    @Test
    public void testFamilySoldCommodities() {
        EntitySettingGroup entitySettingGroup1 = createEntitySettingGroup(
            Arrays.asList(1L), Arrays.asList(VM1), Arrays.asList(500L));
        EntitySettingGroup entitySettingGroup2 = createEntitySettingGroup(
            Arrays.asList(2L, 3L), Arrays.asList(VM2), Arrays.asList(501L));
        GetEntitySettingsResponse response = GetEntitySettingsResponse.newBuilder()
            .addSettingGroup(entitySettingGroup1)
            .addSettingGroup(entitySettingGroup2).build();
        when(settingPolicyServiceMole.getEntitySettings(any())).thenReturn(Arrays.asList(response));

        tierExcluder.initialize(topology, ImmutableSet.of(VM1, VM2));

        Map<Long, Set<Long>> expectedConsumerToPossibleSuppliers =
            ImmutableMap.of(
                VM1, LongStream.rangeClosed(2L, 5L).boxed().collect(Collectors.toSet()),
                VM2, ImmutableSet.of(1L, 4L, 5L));
        assertCommodities(expectedConsumerToPossibleSuppliers, consumers);
        Set<CommodityType> t2Commodities = tierExcluder.getTierExclusionCommoditiesToSell(T2_FAMILY);
        Set<CommodityType> m4Commodities = tierExcluder.getTierExclusionCommoditiesToSell(M4_FAMILY);
        assertEquals(2, t2Commodities.size());
        assertEquals(t2Commodities, m4Commodities);
        assertEquals(1, tierExcluder.getTierExclusionCommoditiesToBuy(VM1).size());
        assertEquals(1, tierExcluder.getTierExclusionCommoditiesToBuy(VM2).size());
    }

    /**
     * 2 VMs - VM1 and VM2, 5 compute tiers - 1, 2, 3, 4, 5.
     * There are no policies.
     * Assert that VM1 and VM2 can only go anywhere.
     */
    @Test
    public void testNoPolicy() {
        GetEntitySettingsResponse response = GetEntitySettingsResponse.newBuilder()
            .addSettingGroup(EntitySettingGroup.getDefaultInstance()).build();
        when(settingPolicyServiceMole.getEntitySettings(any())).thenReturn(Arrays.asList(response));

        tierExcluder.initialize(topology, ImmutableSet.of(VM1, VM2));

        Map<Long, Set<Long>> expectedConsumerToPossibleSuppliers =
            ImmutableMap.of(
                VM1, LongStream.rangeClosed(1L, 5L).boxed().collect(Collectors.toSet()),
                VM2, LongStream.rangeClosed(1L, 5L).boxed().collect(Collectors.toSet()));
        assertCommodities(expectedConsumerToPossibleSuppliers, consumers);
        assertEquals(0, tierExcluder.getTierExclusionCommoditiesToBuy(VM1).size());
        assertEquals(0, tierExcluder.getTierExclusionCommoditiesToBuy(VM2).size());
    }

    /**
     * 2 VMs - VM1 and VM2, 5 compute tiers - 1, 2, 3, 4, 5.
     * Policy 500 excludes all compute tiers for VM1, VM2.
     * Assert that VM1, VM2 cannot go anywhere.
     */
    @Test
    public void testAllTiersExcluded() {
        EntitySettingGroup entitySettingGroup1 = createEntitySettingGroup(
            Arrays.asList(1L, 2L, 3L, 4L, 5L), Arrays.asList(VM1, VM2), Arrays.asList(500L));
        GetEntitySettingsResponse response = GetEntitySettingsResponse.newBuilder()
            .addSettingGroup(entitySettingGroup1).build();
        when(settingPolicyServiceMole.getEntitySettings(any())).thenReturn(Arrays.asList(response));

        tierExcluder.initialize(topology, ImmutableSet.of(VM1, VM2));

        Map<Long, Set<Long>> expectedConsumerToPossibleSuppliers =
            ImmutableMap.of(
                VM1, Sets.newHashSet(),
                VM2, Sets.newHashSet());
        assertCommodities(expectedConsumerToPossibleSuppliers, consumers);
        assertEquals(1, tierExcluder.getTierExclusionCommoditiesToBuy(VM1).size());
        assertEquals(1, tierExcluder.getTierExclusionCommoditiesToBuy(VM2).size());
    }

    /**
     * 2 VMs - VM1 and VM2, 5 compute tiers - 1, 2, 3, 4, 5. Two DB Tiers - 6 and 7.
     * Policy 500 excludes compute tiers 1 and 2 for VM1, VM2.
     * Assert that VM1, VM2 can go to 3, 4, 5.
     * Assert that DB tiers do not sell any commodities.
     */
    @Test
    public void testDifferentTypeOfTierDoesNotSellExclusionCommodity() {
        EntitySettingGroup entitySettingGroup1 = createEntitySettingGroup(
            Arrays.asList(1L, 2L), Arrays.asList(VM1, VM2), Arrays.asList(500L));
        GetEntitySettingsResponse response = GetEntitySettingsResponse.newBuilder()
            .addSettingGroup(entitySettingGroup1).build();
        when(settingPolicyServiceMole.getEntitySettings(any())).thenReturn(Arrays.asList(response));
        // Create 2 DB Tiers
        LongStream.rangeClosed(6L, 7L).forEach(dbOid -> topology.put(dbOid, TopologyEntityDTO.newBuilder()
            .setOid(dbOid)
            .setEntityType(EntityType.DATABASE_TIER_VALUE).build()));

        tierExcluder.initialize(topology, ImmutableSet.of(VM1, VM2));

        Map<Long, Set<Long>> expectedConsumerToPossibleSuppliers =
            ImmutableMap.of(
                VM1, LongStream.rangeClosed(3L, 5L).boxed().collect(Collectors.toSet()),
                VM2, LongStream.rangeClosed(3L, 5L).boxed().collect(Collectors.toSet()));
        assertCommodities(expectedConsumerToPossibleSuppliers, consumers);
        assertTrue(tierExcluder.getTierExclusionCommoditiesToSell(6L).isEmpty());
        assertTrue(tierExcluder.getTierExclusionCommoditiesToSell(7L).isEmpty());
        assertEquals(1, tierExcluder.getTierExclusionCommoditiesToBuy(VM1).size());
        assertEquals(1, tierExcluder.getTierExclusionCommoditiesToBuy(VM2).size());
    }

    /**
     * 2 VMs - VM1 and VM2, 5 compute tiers - 1, 2, 3, 4, 5.
     * Policy 500 excludes nothing for VM1, VM2.
     * Assert that VM1, VM2 can go anywhere.
     * Assert that we do not create any commodities.
     */
    @Test
    public void testNothingExcluded() {
        EntitySettingGroup entitySettingGroup1 = createEntitySettingGroup(
            Arrays.asList(), Arrays.asList(VM1, VM2), Arrays.asList(500L));
        GetEntitySettingsResponse response = GetEntitySettingsResponse.newBuilder()
            .addSettingGroup(entitySettingGroup1).build();
        when(settingPolicyServiceMole.getEntitySettings(any())).thenReturn(Arrays.asList(response));

        tierExcluder.initialize(topology, ImmutableSet.of(VM1, VM2));

        Map<Long, Set<Long>> expectedConsumerToPossibleSuppliers =
            ImmutableMap.of(
                VM1, LongStream.rangeClosed(1L, 5L).boxed().collect(Collectors.toSet()),
                VM2, LongStream.rangeClosed(1L, 5L).boxed().collect(Collectors.toSet()));
        assertCommodities(expectedConsumerToPossibleSuppliers, consumers);
        assertTrue(tierExcluder.getTierExclusionCommoditiesToBuy(VM1).isEmpty());
        assertTrue(tierExcluder.getTierExclusionCommoditiesToBuy(VM2).isEmpty());
    }


    /**
     * 2 VMs - VM3 and VM4, 5 compute tiers - 1, 2, 3, 4, 5.
     * Policy 500 excludes tiers 1 and 2 and for VM3, VM4.
     * But the incoming topology is scoped and does not include VM3 and VM4.
     * Assert that we do not apply the policies i.e. we do not create any commodities.
     */
    @Test
    public void testAllConsumersNotInScopedTopology() {
        Long VM3 = 103L;
        Long VM4 = 104L;
        EntitySettingGroup entitySettingGroup1 = createEntitySettingGroup(
            Arrays.asList(1L, 2L), Arrays.asList(VM3, VM4), Arrays.asList(500L));
        GetEntitySettingsResponse response = GetEntitySettingsResponse.newBuilder()
            .addSettingGroup(entitySettingGroup1).build();
        when(settingPolicyServiceMole.getEntitySettings(any())).thenReturn(Arrays.asList(response));

        tierExcluder.initialize(topology, ImmutableSet.of(VM1, VM2));

        assertTrue(tierExcluder.getTierExclusionCommoditiesToBuy(VM3).isEmpty());
        assertTrue(tierExcluder.getTierExclusionCommoditiesToBuy(VM4).isEmpty());
    }

    /**
     * 2 VMs - VM1 and VM3, 5 compute tiers - 1, 2, 3, 4, 5.
     * Policy 500 excludes tiers 1 and 2 and for VM1, VM3.
     * But the incoming topology is scoped and does not include VM3.
     * But we still apply the policy as VM1 is included in the scoped topology.
     */
    @Test
    public void testSomeConsumersNotInScopedTopology() {
        Long VM3 = 103L;
        EntitySettingGroup entitySettingGroup1 = createEntitySettingGroup(
            Arrays.asList(1L, 2L), Arrays.asList(VM3, VM1), Arrays.asList(500L));
        GetEntitySettingsResponse response = GetEntitySettingsResponse.newBuilder()
            .addSettingGroup(entitySettingGroup1).build();
        when(settingPolicyServiceMole.getEntitySettings(any())).thenReturn(Arrays.asList(response));

        tierExcluder.initialize(topology, ImmutableSet.of(VM1, VM3));

        Map<Long, Set<Long>> expectedConsumerToPossibleSuppliers =
            ImmutableMap.of(
                VM3, LongStream.rangeClosed(3L, 5L).boxed().collect(Collectors.toSet()),
                VM1, LongStream.rangeClosed(3L, 5L).boxed().collect(Collectors.toSet()));
        assertCommodities(expectedConsumerToPossibleSuppliers, Arrays.asList(VM1, VM3));
        assertEquals(1, tierExcluder.getTierExclusionCommoditiesToBuy(VM1).size());
        assertEquals(1, tierExcluder.getTierExclusionCommoditiesToBuy(VM3).size());
    }

    /**
     * 2 VMs - VM1 and VM2, 5 compute tiers - 1, 2, 3, 4, 5.
     * Policy 500 excludes tiers 6 and 7 for VM1, VM2.
     * But the incoming topology is scoped and does not include tiers 6 and 7.
     * So we do not apply the policy.
     */
    @Test
    public void testExcludedTiersNotInScopedTopology() {
        EntitySettingGroup entitySettingGroup1 = createEntitySettingGroup(
            Arrays.asList(6L, 7L), Arrays.asList(VM1, VM2), Arrays.asList(500L));
        GetEntitySettingsResponse response = GetEntitySettingsResponse.newBuilder()
            .addSettingGroup(entitySettingGroup1).build();
        when(settingPolicyServiceMole.getEntitySettings(any())).thenReturn(Arrays.asList(response));

        tierExcluder.initialize(topology, ImmutableSet.of(VM1, VM2));

        assertTrue(tierExcluder.getTierExclusionCommoditiesToBuy(VM1).isEmpty());
        assertTrue(tierExcluder.getTierExclusionCommoditiesToBuy(VM2).isEmpty());
    }

    /**
     * 2 VMs - VM1 and VM2, 5 compute tiers - 1, 2, 3, 4, 5.
     * Policy 500 excludes tiers 5, 6, 7 and 8 for VM1, VM2.
     * But the incoming topology is scoped and does not include tiers 6, 7 and 8.
     * We should still apply the policy.
     */
    @Test
    public void testSomeExcludedTiersNotInScopedTopology() {
        EntitySettingGroup entitySettingGroup1 = createEntitySettingGroup(
            Arrays.asList(6L, 7L, 8L, 5L), Arrays.asList(VM1, VM2), Arrays.asList(500L));
        GetEntitySettingsResponse response = GetEntitySettingsResponse.newBuilder()
            .addSettingGroup(entitySettingGroup1).build();
        when(settingPolicyServiceMole.getEntitySettings(any())).thenReturn(Arrays.asList(response));

        tierExcluder.initialize(topology, ImmutableSet.of(VM1, VM2));

        Map<Long, Set<Long>> expectedConsumerToPossibleSuppliers =
            ImmutableMap.of(
                VM1, LongStream.rangeClosed(1L, 4L).boxed().collect(Collectors.toSet()),
                VM2, LongStream.rangeClosed(1L, 4L).boxed().collect(Collectors.toSet()));
        assertCommodities(expectedConsumerToPossibleSuppliers, Arrays.asList(VM1, VM2));
        assertEquals(1, tierExcluder.getTierExclusionCommoditiesToBuy(VM1).size());
        assertEquals(1, tierExcluder.getTierExclusionCommoditiesToBuy(VM2).size());
    }

    /**
     * Setting policy 500 excludes Tiers 1 for VM1.
     * Setting policy 501 excludes Tiers 2 for VM1.
     * Setting policy 502 excludes Tiers 1, 4 for VM1.
     * Setting policy 503 excludes Tiers 1, 2, 3 for VM1.
     * So VM1 cannot go onto 1, 2, 3 and 4. The entity setting group coming into market component will
     * represent this.
     * VM1 is on tier 1. Market generates move for VM1 out of tier 1.
     * Check that the responsible settings are 500, 502 and 503, and not 501 even though all of them have VM1 in their scope.
     */
    @Test
    public void testComputeReasonSettings1() {
        EntitySettingGroup entitySettingGroup1 = createEntitySettingGroup(
            Arrays.asList(1L, 2L, 4L), Arrays.asList(VM1), Arrays.asList(500L, 501L, 502L));
        EntitySettingGroup entitySettingGroup2 = createEntitySettingGroup(
            Arrays.asList(1L, 2L, 3L), Arrays.asList(VM1), Arrays.asList(503L));
        GetEntitySettingsResponse response = GetEntitySettingsResponse.newBuilder()
            .addAllSettingGroup(Arrays.asList(entitySettingGroup1, entitySettingGroup2)).build();
        when(settingPolicyServiceMole.getEntitySettings(any())).thenReturn(Arrays.asList(response));

        tierExcluder.initialize(topology, ImmutableSet.of(VM1));

        SettingPolicy sp1 = createSettingPolicy(Arrays.asList(1L), 500L);
        SettingPolicy sp2 = createSettingPolicy(Arrays.asList(2L), 501L);
        SettingPolicy sp3 = createSettingPolicy(Arrays.asList(1L, 4L), 502L);
        SettingPolicy sp4 = createSettingPolicy(Arrays.asList(1L, 2L, 3L), 503L);
        when(settingPolicyServiceMole.listSettingPolicies(ListSettingPoliciesRequest.newBuilder()
            .addAllIdFilter(Arrays.asList(500L, 501L, 502L, 503L)).build())).thenReturn(Arrays.asList(sp1, sp2, sp3, sp4));
        when(cloudTopology.getPrimaryTier(VM1)).thenReturn(Optional.of(topology.get(1L)));
        ActionTO moveActionTO = createMoveActionTO(VM1SlOid, Arrays.asList(0));

        tierExcluder.computeReasonSettings(Arrays.asList(moveActionTO), cloudTopology);

        assertEquals(ImmutableSet.of(500L, 502L, 503L), tierExcluder.getReasonSettings(moveActionTO).get());
    }

    /**
     * Setting policy 500 excludes Tiers 1 for VM1.
     * Setting policy 501 excludes Tiers 1 for VM2.
     * So VM1 cannot go onto 1. The entity setting group coming into market component will
     * represent this.
     * VM1 is on tier 1. Market generates move for VM1 out of tier 1.
     * Check that the responsible settings are 500, and not 501 even though both exclude tier 1.
     */
    @Test
    public void testComputeReasonSettings2() {
        EntitySettingGroup entitySettingGroup1 = createEntitySettingGroup(
            Arrays.asList(1L), Arrays.asList(VM1), Arrays.asList(500L));
        EntitySettingGroup entitySettingGroup2 = createEntitySettingGroup(
            Arrays.asList(1L), Arrays.asList(VM2), Arrays.asList(501L));
        GetEntitySettingsResponse response = GetEntitySettingsResponse.newBuilder()
            .addAllSettingGroup(Arrays.asList(entitySettingGroup1, entitySettingGroup2)).build();
        when(settingPolicyServiceMole.getEntitySettings(any())).thenReturn(Arrays.asList(response));

        tierExcluder.initialize(topology, ImmutableSet.of(VM1, VM2));

        SettingPolicy sp1 = createSettingPolicy(Arrays.asList(1L), 500L);
        // SP2 will never be fetched. Only 500 policy will be fetched because that's the one for VM1
        SettingPolicy sp2 = createSettingPolicy(Arrays.asList(1L), 501L);
        when(settingPolicyServiceMole.listSettingPolicies(ListSettingPoliciesRequest.newBuilder()
            .addAllIdFilter(Arrays.asList(500L)).build())).thenReturn(Arrays.asList(sp1));
        when(cloudTopology.getPrimaryTier(VM1)).thenReturn(Optional.of(topology.get(1L)));
        ActionTO moveActionTO = createMoveActionTO(VM1SlOid, Arrays.asList(0));

        tierExcluder.computeReasonSettings(Arrays.asList(moveActionTO), cloudTopology);

        assertEquals(ImmutableSet.of(500L), tierExcluder.getReasonSettings(moveActionTO).get());
    }

    /**
     * Other action types  - resize, activate, deactivate, provBySupply, provByDemand don't
     * generate reason settings.
     */
    @Test
    public void testOtherActionTypesDoNotGenerateReasonSettings() {
        ActionTO resize = ActionTO.newBuilder().setResize(ResizeTO.getDefaultInstance())
            .setImportance(100).setIsNotExecutable(false).build();
        ActionTO activate = ActionTO.newBuilder().setActivate(ActivateTO.newBuilder()
            .setTraderToActivate(1L).setModelSeller(2L))
            .setImportance(100).setIsNotExecutable(false).build();
        ActionTO deativate = ActionTO.newBuilder().setDeactivate(DeactivateTO.newBuilder()
            .setTraderToDeactivate(1L))
            .setImportance(100).setIsNotExecutable(false).build();
        ActionTO provisionBySupply = ActionTO.newBuilder().setProvisionBySupply(
            ProvisionBySupplyTO.newBuilder().setModelSeller(1L))
            .setImportance(100).setIsNotExecutable(false).build();
        ActionTO provisionByDemand = ActionTO.newBuilder().setProvisionByDemand(
            ProvisionByDemandTO.newBuilder().setModelSeller(1L).setModelBuyer(2L))
            .setImportance(100).setIsNotExecutable(false).build();

        tierExcluder.computeReasonSettings(Arrays.asList(resize, activate, deativate,
            provisionBySupply, provisionByDemand), cloudTopology);

        assertEquals(Optional.empty(), tierExcluder.getReasonSettings(resize));
        assertEquals(Optional.empty(), tierExcluder.getReasonSettings(activate));
        assertEquals(Optional.empty(), tierExcluder.getReasonSettings(deativate));
        assertEquals(Optional.empty(), tierExcluder.getReasonSettings(provisionBySupply));
        assertEquals(Optional.empty(), tierExcluder.getReasonSettings(provisionByDemand));
    }

    /**
     * Setting policy 500 excludes all tiers for VM1.
     * VM1 is on tier 1. Market generates reconfigure.
     * Check that the responsible settings are 500.
     */
    @Test
    public void testReconfigureGeneratesReasonSettings() {
        EntitySettingGroup entitySettingGroup1 = createEntitySettingGroup(
            Arrays.asList(1L, 2L, 3L, 4L, 5L), Arrays.asList(VM1), Arrays.asList(500L));
        GetEntitySettingsResponse response = GetEntitySettingsResponse.newBuilder()
            .addAllSettingGroup(Arrays.asList(entitySettingGroup1)).build();
        when(settingPolicyServiceMole.getEntitySettings(any())).thenReturn(Arrays.asList(response));

        tierExcluder.initialize(topology, ImmutableSet.of(VM1, VM2));

        SettingPolicy sp1 = createSettingPolicy(Arrays.asList(1L, 2L, 3L, 4L, 5L), 500L);
        when(settingPolicyServiceMole.listSettingPolicies(ListSettingPoliciesRequest.newBuilder()
            .addAllIdFilter(Arrays.asList(500L)).build())).thenReturn(Arrays.asList(sp1));
        when(cloudTopology.getPrimaryTier(VM1)).thenReturn(Optional.of(topology.get(1L)));
        ActionTO reconfigureActionTO = createReconfigureActionTO(VM1SlOid, Arrays.asList(0));

        tierExcluder.computeReasonSettings(Arrays.asList(reconfigureActionTO), cloudTopology);

        assertEquals(ImmutableSet.of(500L), tierExcluder.getReasonSettings(reconfigureActionTO).get());
    }

    /**
     * Setting policy 500 excludes tiers 1 and 2 for VM1.
     * VM1 is on tier 1. Market generates compound move.
     * Check that the responsible settings are 500.
     */
    @Test
    public void testCompoundMoveGeneratesReasonSettings() {
        EntitySettingGroup entitySettingGroup1 = createEntitySettingGroup(
            Arrays.asList(1L, 2L), Arrays.asList(VM1), Arrays.asList(500L));
        GetEntitySettingsResponse response = GetEntitySettingsResponse.newBuilder()
            .addAllSettingGroup(Arrays.asList(entitySettingGroup1)).build();
        when(settingPolicyServiceMole.getEntitySettings(any())).thenReturn(Arrays.asList(response));

        tierExcluder.initialize(topology, ImmutableSet.of(VM1, VM2));

        SettingPolicy sp1 = createSettingPolicy(Arrays.asList(1L, 2L), 500L);
        when(settingPolicyServiceMole.listSettingPolicies(ListSettingPoliciesRequest.newBuilder()
            .addAllIdFilter(Arrays.asList(500L)).build())).thenReturn(Arrays.asList(sp1));
        when(cloudTopology.getPrimaryTier(VM1)).thenReturn(Optional.of(topology.get(1L)));
        ActionTO compoundMoveActionTO = createCompoundMoveActionTO(VM1SlOid, Arrays.asList(0));

        tierExcluder.computeReasonSettings(Arrays.asList(compoundMoveActionTO), cloudTopology);

        assertEquals(ImmutableSet.of(500L), tierExcluder.getReasonSettings(compoundMoveActionTO).get());
    }

    private SettingPolicy createSettingPolicy(List<Long> excludedTemplates, long settingPolicyId) {
        return SettingPolicy.newBuilder()
            .setId(settingPolicyId)
            .setInfo(SettingPolicyInfo.newBuilder()
                .addSettings(Setting.newBuilder()
                    .setSettingSpecName(EntitySettingSpecs.ExcludedTemplates.getSettingName())
                    .setSortedSetOfOidSettingValue(SortedSetOfOidSettingValue.newBuilder()
                        .addAllOids(excludedTemplates))))
            .build();
    }

    private ActionTO createMoveActionTO(long slToMove, List<Integer> missingComms) {
        return ActionTO.newBuilder().setMove(
                createMoveTO(slToMove, missingComms))
            .setImportance(100)
            .setIsNotExecutable(false)
            .build();
    }

    private MoveTO createMoveTO(long slToMove, List<Integer> missingComms) {
        return MoveTO.newBuilder()
            .setShoppingListToMove(slToMove)
            .setMoveExplanation(MoveExplanation.newBuilder()
                .setCompliance(Compliance.newBuilder()
                    .addAllMissingCommodities(missingComms)))
            .build();
    }

    private ActionTO createCompoundMoveActionTO(long slToMove, List<Integer> missingComms) {
        MoveTO dummyPerformanceAction = MoveTO.newBuilder()
            .setShoppingListToMove(1234L)
            .setMoveExplanation(MoveExplanation.newBuilder()
                .setPerformance(Performance.getDefaultInstance())).build();
        return ActionTO.newBuilder().setCompoundMove(
            CompoundMoveTO.newBuilder()
                .addMoves(createMoveTO(slToMove, missingComms))
                .addMoves(dummyPerformanceAction))
            .setImportance(100)
            .setIsNotExecutable(false)
            .build();
    }

    private ActionTO createReconfigureActionTO(long slToReconfigure, List<Integer> missingComms) {
        return ActionTO.newBuilder().setReconfigure(
            ReconfigureTO.newBuilder()
                .setShoppingListToReconfigure(slToReconfigure)
                .addAllCommodityToReconfigure(missingComms))
            .setImportance(100)
            .setIsNotExecutable(false)
            .build();
    }

    private void assertCommodities(Map<Long, Set<Long>> expectedConsumerToPossibleProviders,
                                   List<Long> consumers) {
        // Compute actual consumer to possible suppliers
        Map<Long, Set<Long>> actualConsumerToPossibleProviders = Maps.newHashMap();
        for (Long consumer : consumers) {
            Set<TopologyDTO.CommodityType> commodityBoughtByConsumer =
                tierExcluder.getTierExclusionCommoditiesToBuy(consumer);
            for (Long provider : topology.keySet()) {
                if (topology.get(provider).getEntityType() != EntityType.VIRTUAL_MACHINE_VALUE) {
                    Set<TopologyDTO.CommodityType> commoditySoldByTier =
                        tierExcluder.getTierExclusionCommoditiesToSell(provider);
                    Set<Long> providers = actualConsumerToPossibleProviders
                        .computeIfAbsent(consumer, k -> new HashSet<>());
                    if (commoditySoldByTier.containsAll(commodityBoughtByConsumer)) {
                        providers.add(provider);
                    }
                }
            }
        }
        assertEquals(expectedConsumerToPossibleProviders, actualConsumerToPossibleProviders);
    }

    private EntitySettingGroup createEntitySettingGroup(
        List<Long> excludedTiers, List<Long> consumers, List<Long> policyIds) {
        Setting exclusionSetting = Setting.newBuilder().setSortedSetOfOidSettingValue(
            SortedSetOfOidSettingValue.newBuilder()
                .addAllOids(excludedTiers).build()).build();
        List<SettingPolicyId> settingPolicyIds = Lists.newArrayList();
        policyIds.forEach(policyId -> {
            settingPolicyIds.add(SettingPolicyId.newBuilder().setPolicyId(policyId).build());
        });
        EntitySettingGroup entitySettingGroup = EntitySettingGroup.newBuilder()
            .setSetting(exclusionSetting).addAllPolicyId(settingPolicyIds).addAllEntityOids(consumers).build();
        return entitySettingGroup;
    }

    /**
     * Creates 5 Compute tiers (oids 1 to 5).
     * Compute tiers 1 and 2 are t2 family, and 3, 4, 5 are m4 family.
     * @return the test topology
     */
    private Map<Long, TopologyEntityDTO> createTestTopology() {
        long oid = 1;
        Map<Long, TopologyEntityDTO> topology = Maps.newHashMap();
        while (oid <= 5) {
            String family = oid <= 2 ? T2_FAMILY : M4_FAMILY;
            TypeSpecificInfo typeSpecificInfo = TypeSpecificInfo.newBuilder()
                .setComputeTier(ComputeTierInfo.newBuilder()
                .setFamily(family)).build();
            topology.put(oid, TopologyEntityDTO.newBuilder()
                .setOid(oid++)
                .setEntityType(EntityType.COMPUTE_TIER_VALUE)
                .setTypeSpecificInfo(typeSpecificInfo).build());
        }
        topology.put(VM1, TopologyEntityDTO.newBuilder()
            .setOid(VM1)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE).build());
        topology.put(VM2, TopologyEntityDTO.newBuilder()
            .setOid(VM2)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE).build());
        return topology;
    }
}
