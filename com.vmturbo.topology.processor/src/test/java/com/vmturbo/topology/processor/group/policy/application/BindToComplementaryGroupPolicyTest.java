package com.vmturbo.topology.processor.group.policy.application;

import static com.vmturbo.topology.processor.group.policy.PolicyGroupingHelper.resolvedGroup;
import static com.vmturbo.topology.processor.group.policy.PolicyMatcher.searchParametersCollection;
import static com.vmturbo.topology.processor.topology.TopologyEntityUtils.connectedTopologyEntity;
import static com.vmturbo.topology.processor.topology.TopologyEntityUtils.topologyEntity;
import static com.vmturbo.topology.processor.topology.TopologyInvertedIndexFactory.DEFAULT_MINIMAL_SCAN_STOP_THRESHOLD;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ReservationOrigin;
import com.vmturbo.commons.analysis.InvertedIndex;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.GroupResolutionException;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.policy.PolicyGroupingHelper;
import com.vmturbo.topology.processor.group.policy.PolicyMatcher;
import com.vmturbo.topology.processor.group.policy.application.PolicyFactory.PolicyEntities;
import com.vmturbo.topology.processor.topology.TopologyEntityTopologyGraphCreator;
import com.vmturbo.topology.processor.topology.TopologyInvertedIndexFactory;

/**
 * The tests use the following topology (no links are provided below 1,2 are hosts and 3,4 are Storage
 * and 5 and 6 are VMs):
 *
 * VM5 VM7  VM6                   VM12
 *  | /     |\                   |    \
 *  |/      | \                VV10   VV11
 *  PM1   PM2 ST3  ST4          |       \
 *                      StorageTier8 StorageTier9
 */
public class BindToComplementaryGroupPolicyTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    final Grouping consumerGroup = PolicyGroupingHelper.policyGrouping(
        searchParametersCollection(), EntityType.VIRTUAL_MACHINE_VALUE, 1234L);

    final Grouping providerGroup = PolicyGroupingHelper.policyGrouping(
        searchParametersCollection(), EntityType.PHYSICAL_MACHINE_VALUE, 5678L);

    final long consumerID = 1234L;
    final long providerID = 5678L;

    final PolicyDTO.PolicyInfo.BindToComplementaryGroupPolicy bindToComplementarytGroup =
            PolicyDTO.PolicyInfo.BindToComplementaryGroupPolicy.newBuilder()
                .setConsumerGroupId(consumerID)
                .setProviderGroupId(providerID)
            .build();

    private static final long POLICY_ID = 9999L;
    final PolicyDTO.Policy policy = PolicyDTO.Policy.newBuilder()
            .setId(POLICY_ID)
            .setPolicyInfo(PolicyInfo.newBuilder()
                .setBindToComplementaryGroup(bindToComplementarytGroup))
            .build();

    TopologyGraph<TopologyEntity> topologyGraph;
    PolicyMatcher policyMatcher;

    TopologyInvertedIndexFactory invertedIndexFactory = mock(TopologyInvertedIndexFactory.class);

    final GroupResolver groupResolver = mock(GroupResolver.class);

    final TopologyEntity.Builder reservationVM = TopologyEntity.newBuilder(
            TopologyEntityDTO.newBuilder()
                    .setOid(5L)
                    .setEntityType(EntityType.VIRTUAL_MACHINE.getNumber())
                    .setDisplayName("reservationVM")
                    .setOrigin(Origin.newBuilder().setReservationOrigin(ReservationOrigin.newBuilder()
                            .setReservationId(11111L)))
                    .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                            .setProviderId(1L)
                            .build()));
    private static final CommodityType DATA_STORE_COMMODITY = CommodityType.newBuilder().setType(
        CommodityDTO.CommodityType.DATASTORE_VALUE).setKey("abcd").build();

    @Before
    public void setup() {
        final Map<Long, TopologyEntity.Builder> topologyMap = new HashMap<>();
        topologyMap.put(1L, topologyEntity(1L, 785L, 1601064471L, "PM1", EntityType.PHYSICAL_MACHINE,
            Arrays.asList(DATA_STORE_COMMODITY)));
        topologyMap.put(2L, topologyEntity(2L, 785L, 1601064471L, "PM2", EntityType.PHYSICAL_MACHINE,
            Arrays.asList(DATA_STORE_COMMODITY)));
        topologyMap.put(100L, topologyEntity(100L, 785L, 1601064471L, "PM100", EntityType.PHYSICAL_MACHINE,
            Arrays.asList(DATA_STORE_COMMODITY)));

        topologyMap.put(3L, topologyEntity(3L, EntityType.STORAGE));
        topologyMap.put(4L, topologyEntity(4L, EntityType.STORAGE));
        topologyMap.put(5L, reservationVM);
        topologyMap.put(6L, topologyEntity(6L, 785L, 1601064471L, "VM6", EntityType.VIRTUAL_MACHINE,
            ImmutableMap.of(2L, Arrays.asList(DATA_STORE_COMMODITY),
                3L, new ArrayList<>()), new ArrayList<>()));

        topologyMap.put(7L, topologyEntity(7L, EntityType.VIRTUAL_MACHINE, 1));

        // VM12 --> VV10 --> StorageTier8
        // VM12 --> VV11 --> StorageTier9
        topologyMap.put(8L, connectedTopologyEntity(8L, EntityType.STORAGE_TIER));
        topologyMap.put(9L, connectedTopologyEntity(9L, EntityType.STORAGE_TIER));
        topologyMap.put(10L, connectedTopologyEntity(10L, EntityType.VIRTUAL_VOLUME, 8L));
        topologyMap.put(11L, connectedTopologyEntity(11L, EntityType.VIRTUAL_VOLUME, 9L));
        topologyMap.put(12L, connectedTopologyEntity(12L, EntityType.VIRTUAL_MACHINE, 10L, 11L));
        // replacement from template
        topologyMap.put(13L, topologyEntity(13L, EntityType.PHYSICAL_MACHINE));
        topologyMap.get(2L).getEntityBuilder().getEditBuilder().setReplaced(
                TopologyDTO.TopologyEntityDTO.Replaced.newBuilder().setPlanId(7777L).setReplacementId(13L).build());

        topologyMap.put(14L, topologyEntity(14L, EntityType.VIRTUAL_MACHINE));
        topologyMap.put(15L, topologyEntity(15L, EntityType.CONTAINER_POD, 14L));
        topologyMap.put(16L, topologyEntity(16L, EntityType.CONTAINER_POD, 14L));

        // VM12 is also buying from the StorageTiers
        topologyMap.get(12L)
            .getEntityBuilder()
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(8L)
                .setVolumeId(10L))
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(9L)
                .setVolumeId(11L));

        topologyGraph = TopologyEntityTopologyGraphCreator.newGraph(topologyMap);
        policyMatcher = new PolicyMatcher(topologyGraph);
    }

    @Test
    public void testApplyEmpty() throws GroupResolutionException, PolicyApplicationException {
        when(groupResolver.resolve(eq(consumerGroup), eq(topologyGraph)))
                .thenReturn(resolvedGroup(consumerGroup));
        when(groupResolver.resolve(eq(providerGroup), eq(topologyGraph)))
                .thenReturn(resolvedGroup(providerGroup));

        applyPolicy(new BindToComplementaryGroupPolicy(policy,
            new PolicyEntities(consumerGroup, Collections.emptySet()),
            new PolicyEntities(providerGroup)));
        // No segments should be sold, since we are binding "nothing".
        assertThat(topologyGraph.getEntity(1L).get(), (policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(2L).get(), (policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(5L).get(), not(policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE)));
        assertThat(topologyGraph.getEntity(6L).get(), not(policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE)));
        assertThat(topologyGraph.getEntity(13L).get(), (policyMatcher.hasProviderSegment(POLICY_ID)));
    }

    @Test
    public void testApplyEmptyConsumers() throws GroupResolutionException, PolicyApplicationException {
        when(groupResolver.resolve(eq(consumerGroup), eq(topologyGraph)))
                .thenReturn(resolvedGroup(consumerGroup));
        when(groupResolver.resolve(eq(providerGroup), eq(topologyGraph)))
                .thenReturn(resolvedGroup(providerGroup, 1L));

        applyPolicy(new BindToComplementaryGroupPolicy(policy,
            new PolicyEntities(consumerGroup, Collections.emptySet()),
            new PolicyEntities(providerGroup)));

        // No segments should be sold, since we are binding "nothing."1
        assertThat(topologyGraph.getEntity(1L).get(), not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(2L).get(), (policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(13L).get(), (policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(5L).get(), not(policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE)));
        assertThat(topologyGraph.getEntity(6L).get(), not(policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE)));
    }

    private InvertedIndex<TopologyEntity, CommoditiesBoughtFromProvider> mockInvertedIndex(Set<Long> potentialProviders) {
        final InvertedIndex<TopologyEntity, CommoditiesBoughtFromProvider> index = mock(InvertedIndex.class);
        when(index.getSatisfyingSellers(any())).thenAnswer(invocation -> potentialProviders.stream()
            .map(topologyGraph::getEntity)
            .filter(Optional::isPresent)
            .map(Optional::get));
        when(invertedIndexFactory.typeInvertedIndex(any(), any(), anyInt())).thenReturn(index);
        return index;
    }

    @Test
    public void testApplyEmptyProviders() throws GroupResolutionException, PolicyApplicationException {
        when(groupResolver.resolve(eq(consumerGroup), eq(topologyGraph)))
                .thenReturn(resolvedGroup(consumerGroup, 5L));
        when(groupResolver.resolve(eq(providerGroup), eq(topologyGraph)))
                .thenReturn(resolvedGroup(providerGroup));

        mockInvertedIndex(Sets.newHashSet(1L, 100L));

        applyPolicy(new BindToComplementaryGroupPolicy(policy,
            new PolicyEntities(consumerGroup, Sets.newHashSet(7L)),
            new PolicyEntities(providerGroup)));
        // Host 1 and 100 should sell the segmentation commodity, because they are potential providers
        // for VM 5.
        assertThat(topologyGraph.getEntity(1L).get(), policyMatcher.hasProviderSegment(POLICY_ID));
        assertThat(topologyGraph.getEntity(100L).get(), policyMatcher.hasProviderSegment(POLICY_ID));
        // Host 2 doesn't need to sell a segmentation commodity, because VM 5 is constrained
        // by a cluster commodity to stay on Host 1.
        assertThat(topologyGraph.getEntity(2L).get(), (policyMatcher.hasProviderSegment(POLICY_ID)));

        // Host 13 (which replaces host 2) also doesn't need to sell a segmentation commodity,
        // because VM 5 is constrained by a cluster commodity to stay on Host 1.
        assertThat(topologyGraph.getEntity(13L).get(), (policyMatcher.hasProviderSegment(POLICY_ID)));
        // reservation vm will not buy the segmentation commodity
        assertThat(topologyGraph.getEntity(5L).get(), not(policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE)));
        assertThat(topologyGraph.getEntity(6L).get(), not(policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE)));
        assertThat(topologyGraph.getEntity(7L).get(), policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE));
    }

    @Test
    public void testApplyVmToAllHostAffinity() throws GroupResolutionException, PolicyApplicationException {
        when(groupResolver.resolve(eq(consumerGroup), eq(topologyGraph)))
                .thenReturn(resolvedGroup(consumerGroup, 5L, 6L));
        when(groupResolver.resolve(eq(providerGroup), eq(topologyGraph)))
                .thenReturn(resolvedGroup(providerGroup, 1L, 2L, 100L));

        // All PMs are available as providers.
        // None of them should get a segmentation commodity.
        mockInvertedIndex(topologyGraph.entitiesOfType(EntityType.PHYSICAL_MACHINE)
            .map(TopologyEntity::getOid)
            .collect(Collectors.toSet()));

        applyPolicy(new BindToComplementaryGroupPolicy(policy,
            new PolicyEntities(consumerGroup, Sets.newHashSet(7L)),
            new PolicyEntities(providerGroup)));
        assertThat(topologyGraph.getEntity(1L).get(), not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(2L).get(), not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(13L).get(), not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(3L).get(), not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(4L).get(), not(policyMatcher.hasProviderSegment(POLICY_ID)));
        // reservation vm will not buy the segmentation commodity
        assertThat(topologyGraph.getEntity(5L).get(), not(policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE)));
        assertThat(topologyGraph.getEntity(6L).get(), policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE));
        assertThat(topologyGraph.getEntity(7L).get(), policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE));
    }

    @Test
    public void testApplyVmToSomeHostAffinity() throws GroupResolutionException, PolicyApplicationException {
        when(groupResolver.resolve(eq(consumerGroup), eq(topologyGraph)))
                .thenReturn(resolvedGroup(consumerGroup, 5L, 6L));
        when(groupResolver.resolve(eq(providerGroup), eq(topologyGraph)))
                .thenReturn(resolvedGroup(providerGroup, 2L));

        // All PMs are available as providers.
        mockInvertedIndex(topologyGraph.entitiesOfType(EntityType.PHYSICAL_MACHINE)
                .map(TopologyEntity::getOid)
                .collect(Collectors.toSet()));

        applyPolicy(new BindToComplementaryGroupPolicy(policy,
            new PolicyEntities(consumerGroup, Sets.newHashSet(7L)),
            new PolicyEntities(providerGroup)));
        assertThat(topologyGraph.getEntity(1L).get(), policyMatcher.hasProviderSegment(POLICY_ID));
        assertThat(topologyGraph.getEntity(100L).get(), policyMatcher.hasProviderSegment(POLICY_ID));
        assertThat(topologyGraph.getEntity(2L).get(), not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(13L).get(), not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(3L).get(), not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(4L).get(), not(policyMatcher.hasProviderSegment(POLICY_ID)));
        // reservation vm will not buy the segmentation commodity
        assertThat(topologyGraph.getEntity(5L).get(), not(policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE)));
        assertThat(topologyGraph.getEntity(6L).get(), policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE));
        assertThat(topologyGraph.getEntity(7L).get(), policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE));
    }

    @Test
    public void testApplyVmToStorageAffinity() throws GroupResolutionException, PolicyApplicationException {
        final Grouping providerGroup = PolicyGroupingHelper.policyGrouping(
            searchParametersCollection(), EntityType.STORAGE_VALUE, 5678L);

        final PolicyDTO.PolicyInfo.BindToComplementaryGroupPolicy bindToComplementaryGroup =
                PolicyDTO.PolicyInfo.BindToComplementaryGroupPolicy.newBuilder()
                    .setConsumerGroupId(consumerID)
                    .setProviderGroupId(providerID)
                    .build();

        final PolicyDTO.Policy policy = PolicyDTO.Policy.newBuilder()
                .setId(POLICY_ID)
                .setPolicyInfo(PolicyInfo.newBuilder()
                    .setBindToComplementaryGroup(bindToComplementaryGroup))
                .build();

        when(groupResolver.resolve(eq(consumerGroup), eq(topologyGraph)))
                .thenReturn(resolvedGroup(consumerGroup, 6L));
        when(groupResolver.resolve(eq(providerGroup), eq(topologyGraph)))
                .thenReturn(resolvedGroup(providerGroup, 3L));

        // Both storages are available as providers.
        mockInvertedIndex(Sets.newHashSet(3L, 4L));

        applyPolicy(new BindToComplementaryGroupPolicy(policy,
            new PolicyEntities(consumerGroup, Collections.emptySet()),
            new PolicyEntities(providerGroup)));
        assertThat(topologyGraph.getEntity(1L).get(), not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(2L).get(), not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(13L).get(), not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(3L).get(), not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(4L).get(), policyMatcher.hasProviderSegment(POLICY_ID));
        assertThat(topologyGraph.getEntity(5L).get(), not(policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.STORAGE)));
        assertThat(topologyGraph.getEntity(6L).get(), policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.STORAGE));
    }

    @Test
    public void testApplyVmToStorageTierAffinity() throws GroupResolutionException, PolicyApplicationException {
        final Grouping virtualVolumeGroup = PolicyGroupingHelper.policyGrouping(
            searchParametersCollection(), EntityType.VIRTUAL_VOLUME_VALUE, 1212L);
        final Grouping storageTierGroup = PolicyGroupingHelper.policyGrouping(
            searchParametersCollection(), EntityType.STORAGE_TIER_VALUE, 1213L);

        // VM12 --> VV10 --> StorageTier8
        // VM12 --> VV11 --> StorageTier9
        when(groupResolver.resolve(eq(virtualVolumeGroup), eq(topologyGraph)))
            .thenReturn(resolvedGroup(virtualVolumeGroup, 10L));
        when(groupResolver.resolve(eq(storageTierGroup), eq(topologyGraph)))
            .thenReturn(resolvedGroup(storageTierGroup, 8L));

        mockInvertedIndex(Sets.newHashSet(9L, 8L));

        final PolicyDTO.PolicyInfo.BindToComplementaryGroupPolicy bindToComplementaryGroup =
            PolicyDTO.PolicyInfo.BindToComplementaryGroupPolicy.newBuilder()
                .setConsumerGroupId(1212L)
                .setProviderGroupId(1213L)
                .build();

        final PolicyDTO.Policy policy = PolicyDTO.Policy.newBuilder()
            .setId(POLICY_ID)
            .setPolicyInfo(PolicyInfo.newBuilder()
                .setBindToComplementaryGroup(bindToComplementaryGroup))
            .build();

        applyPolicy(new BindToComplementaryGroupPolicy(policy,
            new PolicyEntities(virtualVolumeGroup, Collections.emptySet()),
            new PolicyEntities(storageTierGroup)));

        // VM12 has two commodityBought group:
        //     one from storage tier 8, with related volumeId 10
        //     the other one from storage tier 9, with related volumeId 11
        // verify that VM is buying segment in the commodityBought group for tier 8
        assertThat(topologyGraph.getEntity(12L).get(), policyMatcher.hasConsumerSegment(POLICY_ID, 8L, 10L));
        // verify that VM is NOT buying segment in the commodityBought group for tier 9
        assertThat(topologyGraph.getEntity(12L).get(), not(policyMatcher.hasConsumerSegment(POLICY_ID, 9L, 11L)));
        // verify that tier 8 is NOT selling segment
        assertThat(topologyGraph.getEntity(8L).get(), not(policyMatcher.hasProviderSegment(POLICY_ID)));
        // verify that tier 9 is selling segment
        assertThat(topologyGraph.getEntity(9L).get(), policyMatcher.hasProviderSegment(POLICY_ID));
    }

    /**
     * Test applying pod to VM affinity policy and SegmentationCommodities deduplication when calling
     * {@link PlacementPolicyApplication#addCommoditySoldToComplementaryProviders(Set, Set, long, InvertedIndex, CommoditySoldDTO)}.
     *
     * @throws GroupResolutionException Exception when resolving group.
     */
    @Test
    public void testApplyPodToVMAffinity() throws GroupResolutionException {
        Grouping podConsumerGroup = PolicyGroupingHelper.policyGrouping(
            searchParametersCollection(), EntityType.CONTAINER_POD_VALUE, 123L);
        Grouping vmProviderGroup = PolicyGroupingHelper.policyGrouping(
            searchParametersCollection(), EntityType.VIRTUAL_MACHINE_VALUE, 456L);
        when(groupResolver.resolve(eq(podConsumerGroup), eq(topologyGraph)))
            .thenReturn(resolvedGroup(podConsumerGroup, 15L, 16L));
        // Add all VMs to vmProviderGroup except VM 14L
        when(groupResolver.resolve(eq(vmProviderGroup), eq(topologyGraph)))
            .thenReturn(resolvedGroup(vmProviderGroup, 5L, 6L, 7L));

        // All VMs except 12L are available as providers.
        mockInvertedIndex(Sets.newHashSet(5L, 6L, 7L, 14L));

        applyPolicy(new BindToComplementaryGroupPolicy(policy,
            new PolicyEntities(podConsumerGroup, Sets.newHashSet(15L, 16L)),
            new PolicyEntities(vmProviderGroup)));

        // 2 ContainerPods will buy same SegmentationCommodities with key as POLICY_ID from VM
        TopologyEntity pod1 = topologyGraph.getEntity(15L).get();
        TopologyEntity pod2 = topologyGraph.getEntity(16L).get();
        assertThat(pod1, policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.VIRTUAL_MACHINE));
        assertThat(pod2, policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.VIRTUAL_MACHINE));

        // VM will sell only one SegmentationCommodity
        TopologyEntity vm = topologyGraph.getEntity(14L).get();
        assertThat(vm, policyMatcher.hasProviderSegment(POLICY_ID));
        Assert.assertEquals(1, vm.getTopologyEntityDtoBuilder().getCommoditySoldListList().size());

        // Even though 12L is part of complementary group it will not sell the commodity because
        // it is not potential provider.
        TopologyEntity vm1 = topologyGraph.getEntity(12L).get();
        assertThat(vm1, not(policyMatcher.hasProviderSegment(POLICY_ID)));


    }

    /**
     * There is one VM - VM6, and 2 hosts - Host 1 and Host 2.
     * Policy 1 - VM6 cannot be placed on host 2
     * Policy 2 - VM6 cannot be placed on host 1
     * Ensure host 1 sells policy 1, does not sell policy 2.
     * Ensure host 2 sells policy 2, does not sell policy 1.
     * Ensure that the VM buys both policies.
     * @throws GroupResolutionException group resolution exception
     */
    @Test
    public void testApplyMultiplePoliciesWithRealInvertedIndex() throws GroupResolutionException {
        final long policy2Id = POLICY_ID - 1;
        when(groupResolver.resolve(eq(consumerGroup), eq(topologyGraph)))
            .thenReturn(resolvedGroup(consumerGroup, 6L));
        when(groupResolver.resolve(eq(providerGroup), eq(topologyGraph)))
            .thenReturn(resolvedGroup(providerGroup, 2L))
            .thenReturn(resolvedGroup(providerGroup, 1L));
        BindToComplementaryGroupPolicy policy1 = new BindToComplementaryGroupPolicy(policy,
            new PolicyEntities(consumerGroup), new PolicyEntities(providerGroup));
        BindToComplementaryGroupPolicy policy2 = new BindToComplementaryGroupPolicy(
            policy.toBuilder().setId(policy2Id).build(),
            new PolicyEntities(consumerGroup), new PolicyEntities(providerGroup));

        // We apply the policies with real inverted index
        applyPoliciesWithRealInvertedIndex(Arrays.asList(policy1, policy2));

        assertThat(topologyGraph.getEntity(100L).get(), policyMatcher.hasProviderSegment(POLICY_ID));
        assertThat(topologyGraph.getEntity(100L).get(), policyMatcher.hasProviderSegment(policy2Id));
        assertThat(topologyGraph.getEntity(1L).get(), policyMatcher.hasProviderSegment(POLICY_ID));
        assertThat(topologyGraph.getEntity(1L).get(), not(policyMatcher.hasProviderSegment(policy2Id)));
        assertThat(topologyGraph.getEntity(2L).get(), policyMatcher.hasProviderSegment(policy2Id));
        assertThat(topologyGraph.getEntity(2L).get(), not(policyMatcher.hasProviderSegment(POLICY_ID)));

        assertThat(topologyGraph.getEntity(6L).get(), policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE));
        assertThat(topologyGraph.getEntity(6L).get(), policyMatcher.hasConsumerSegment(policy2Id, EntityType.PHYSICAL_MACHINE));
    }

    private void applyPolicy(@Nonnull final BindToComplementaryGroupPolicy policy) {
        BindToComplementaryGroupPolicyApplication application =
            new BindToComplementaryGroupPolicyApplication(groupResolver, topologyGraph, invertedIndexFactory,
                DEFAULT_MINIMAL_SCAN_STOP_THRESHOLD);
        application.apply(Collections.singletonList(policy));
    }

    private void applyPoliciesWithRealInvertedIndex(@Nonnull final List<PlacementPolicy> policies) {
        // We use 1 as the minimalScanThreshold for the inverted index as we want to test the
        // inverted index as well. With the default of 32, some bugs which are caused because of not updating index
        // can get masked.
        BindToComplementaryGroupPolicyApplication application =
            new BindToComplementaryGroupPolicyApplication(groupResolver, topologyGraph, new TopologyInvertedIndexFactory(), 1);
        application.apply(policies);
    }
}
