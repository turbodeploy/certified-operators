package com.vmturbo.market.topology.conversions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SortedSetOfOidSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingPolicyServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.market.topology.conversions.TierExcluder.TierExcluderFactory;
import com.vmturbo.market.topology.conversions.TierExcluder.TierExcluderFactory.DefaultTierExcluderFactory;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Unit tests for Tier exclusion applicator.
 */
public class TierExcluderTest {

    private static final long REALTIME_TOPOLOGY_CONTEXT_ID = 7777777;
    private static final int TEST_TOPOLOGY_ID = 5678;

    private static final String T2_FAMILY = "t2";
    private static final String M4_FAMILY = "m4";

    private static final long VM1 = 100L;
    private static final long VM2 = 101L;
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

        tierExcluder = tierExcluderFactory.newExcluder(topologyInfo);
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

        tierExcluder.initialize(topology);

        Map<Long, Set<Long>> expectedConsumerToPossibleSuppliers =
            ImmutableMap.of(
                VM1, LongStream.rangeClosed(3L, 5L).boxed().collect(Collectors.toSet()),
                VM2, LongStream.rangeClosed(1L, 5L).boxed().collect(Collectors.toSet()));
        assertCommodities(expectedConsumerToPossibleSuppliers);
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

        tierExcluder.initialize(topology);

        Map<Long, Set<Long>> expectedConsumerToPossibleSuppliers =
            ImmutableMap.of(
                VM1, LongStream.rangeClosed(3L, 5L).boxed().collect(Collectors.toSet()),
                VM2, LongStream.rangeClosed(3L, 5L).boxed().collect(Collectors.toSet()));
        assertCommodities(expectedConsumerToPossibleSuppliers);
        assertEquals(1, tierExcluder.getTierExclusionCommoditiesToSell(3L).size());
        assertEquals(1, tierExcluder.getTierExclusionCommoditiesToSell(4L).size());
        assertEquals(1, tierExcluder.getTierExclusionCommoditiesToSell(5L).size());
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

        tierExcluder.initialize(topology);

        Map<Long, Set<Long>> expectedConsumerToPossibleSuppliers =
            ImmutableMap.of(
                VM1, LongStream.rangeClosed(3L, 5L).boxed().collect(Collectors.toSet()),
                VM2, ImmutableSet.of(1L, 2L, 5L));
        assertCommodities(expectedConsumerToPossibleSuppliers);
        assertEquals(2, tierExcluder.getTierExclusionCommoditiesToSell(5L).size());
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

        tierExcluder.initialize(topology);

        Map<Long, Set<Long>> expectedConsumerToPossibleSuppliers =
            ImmutableMap.of(
                VM1, LongStream.rangeClosed(2L, 5L).boxed().collect(Collectors.toSet()),
                VM2, ImmutableSet.of(1L, 4L, 5L));
        assertCommodities(expectedConsumerToPossibleSuppliers);
        Set<CommodityType> t2Commodities = tierExcluder.getTierExclusionCommoditiesToSell(T2_FAMILY);
        Set<CommodityType> m4Commodities = tierExcluder.getTierExclusionCommoditiesToSell(M4_FAMILY);
        assertEquals(2, t2Commodities.size());
        assertEquals(t2Commodities, m4Commodities);
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

        tierExcluder.initialize(topology);

        Map<Long, Set<Long>> expectedConsumerToPossibleSuppliers =
            ImmutableMap.of(
                VM1, LongStream.rangeClosed(1L, 5L).boxed().collect(Collectors.toSet()),
                VM2, LongStream.rangeClosed(1L, 5L).boxed().collect(Collectors.toSet()));
        assertCommodities(expectedConsumerToPossibleSuppliers);
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

        tierExcluder.initialize(topology);

        Map<Long, Set<Long>> expectedConsumerToPossibleSuppliers =
            ImmutableMap.of(
                VM1, Sets.newHashSet(),
                VM2, Sets.newHashSet());
        assertCommodities(expectedConsumerToPossibleSuppliers);
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

        tierExcluder.initialize(topology);

        Map<Long, Set<Long>> expectedConsumerToPossibleSuppliers =
            ImmutableMap.of(
                VM1, LongStream.rangeClosed(3L, 5L).boxed().collect(Collectors.toSet()),
                VM2, LongStream.rangeClosed(3L, 5L).boxed().collect(Collectors.toSet()));
        assertCommodities(expectedConsumerToPossibleSuppliers);
        assertTrue(tierExcluder.getTierExclusionCommoditiesToSell(6L).isEmpty());
        assertTrue(tierExcluder.getTierExclusionCommoditiesToSell(7L).isEmpty());
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

        tierExcluder.initialize(topology);

        Map<Long, Set<Long>> expectedConsumerToPossibleSuppliers =
            ImmutableMap.of(
                VM1, LongStream.rangeClosed(1L, 5L).boxed().collect(Collectors.toSet()),
                VM2, LongStream.rangeClosed(1L, 5L).boxed().collect(Collectors.toSet()));
        assertCommodities(expectedConsumerToPossibleSuppliers);
        assertTrue(tierExcluder.getTierExclusionCommoditiesToBuy(1L).isEmpty());
        assertTrue(tierExcluder.getTierExclusionCommoditiesToBuy(2L).isEmpty());
    }

    private void assertCommodities(Map<Long, Set<Long>> expectedConsumerToPossibleProviders) {
        // Compute actual consumer to possible suppliers
        Map<Long, Set<Long>> actualConsumerToPossibleProviders = Maps.newHashMap();
        for (Long consumer : consumers) {
            Set<TopologyDTO.CommodityType> commodityBoughtByConsumer =
                tierExcluder.getTierExclusionCommoditiesToBuy(consumer);
            // Consumer will at most buy only one commodity
            assertTrue(commodityBoughtByConsumer.size() <= 1);
            for (Long provider : topology.keySet()) {
                Set<TopologyDTO.CommodityType> commoditySoldByTier =
                    tierExcluder.getTierExclusionCommoditiesToSell(provider);
                Set<Long> providers = actualConsumerToPossibleProviders
                    .computeIfAbsent(consumer, k -> new HashSet<>());
                if (commoditySoldByTier.containsAll(commodityBoughtByConsumer)) {
                    providers.add(provider);
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
        SettingPolicyId settingPolicyId = SettingPolicyId.newBuilder().setPolicyId(policyIds.get(0)).build();
        EntitySettingGroup entitySettingGroup = EntitySettingGroup.newBuilder()
            .setSetting(exclusionSetting).setPolicyId(settingPolicyId).addAllEntityOids(consumers).build();
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
        return topology;
    }
}
