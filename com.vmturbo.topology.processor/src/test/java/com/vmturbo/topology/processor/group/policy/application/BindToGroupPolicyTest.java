package com.vmturbo.topology.processor.group.policy.application;

import static com.vmturbo.topology.processor.group.policy.PolicyGroupingHelper.resolvedGroup;
import static com.vmturbo.topology.processor.group.policy.PolicyMatcher.searchParametersCollection;
import static com.vmturbo.topology.processor.topology.TopologyEntityUtils.connectedTopologyEntity;
import static com.vmturbo.topology.processor.topology.TopologyEntityUtils.topologyEntity;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.GroupResolutionException;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.policy.PolicyGroupingHelper;
import com.vmturbo.topology.processor.group.policy.PolicyMatcher;
import com.vmturbo.topology.processor.group.policy.application.PlacementPolicyApplication.PolicyApplicationResults;
import com.vmturbo.topology.processor.group.policy.application.PolicyFactory.PolicyEntities;
import com.vmturbo.topology.processor.topology.TopologyEntityTopologyGraphCreator;

/**
 * The tests use the following topology (no links are provided below 1,2 are hosts and 3,4 are VMs):
 *
 *  VM4 VM6  VM5               VM11
 *   | /     | \               |   \
 *   |/      |  \         VV9 VV10  \
 *  PM1      PM2 ST3       |   |     \
 *                     StorageTier7 ComputeTier8
 */
public class BindToGroupPolicyTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final Grouping consumerGroup = PolicyGroupingHelper.policyGrouping(
        searchParametersCollection(), EntityType.VIRTUAL_MACHINE_VALUE, 1234L);

    private final Grouping providerGroup = PolicyGroupingHelper.policyGrouping(
        searchParametersCollection(), EntityType.PHYSICAL_MACHINE_VALUE, 5678L);

    private final long consumerID = 1234L;
    private final long providerID = 5678L;

    private final PolicyInfo.BindToGroupPolicy bindToGroup = PolicyInfo.BindToGroupPolicy.newBuilder()
        .setConsumerGroupId(consumerID)
        .setProviderGroupId(providerID)
        .build();

    private final PolicyInfo.BindToGroupAndLicencePolicy bindToGroupAndLicense =
            PolicyInfo.BindToGroupAndLicencePolicy.newBuilder()
                .setConsumerGroupId(consumerID)
                .setProviderGroupId(providerID)
                .build();

    private static final long POLICY_ID = 9999L;
    final PolicyDTO.Policy btgPolicy = PolicyDTO.Policy.newBuilder()
        .setId(POLICY_ID)
        .setPolicyInfo(PolicyInfo.newBuilder()
            .setBindToGroup(bindToGroup))
        .build();

    private static final long LICENSE_POLICY_ID = 9998L;
    final PolicyDTO.Policy btglPolicy = PolicyDTO.Policy.newBuilder()
            .setId(LICENSE_POLICY_ID)
            .setPolicyInfo(PolicyInfo.newBuilder()
                    .setBindToGroupAndLicense(bindToGroupAndLicense))
            .build();

    private TopologyGraph<TopologyEntity> topologyGraph;
    private PolicyMatcher policyMatcher;

    private final GroupResolver groupResolver = mock(GroupResolver.class);

    @Before
    public void setup() {
        final Map<Long, TopologyEntity.Builder> topologyMap = new HashMap<>();
        topologyMap.put(1L, topologyEntity(1L, EntityType.PHYSICAL_MACHINE));
        topologyMap.put(2L, topologyEntity(2L, EntityType.PHYSICAL_MACHINE));
        topologyMap.put(3L, topologyEntity(3L, EntityType.STORAGE));
        topologyMap.put(4L, topologyEntity(4L, EntityType.VIRTUAL_MACHINE, 1));
        topologyMap.put(5L, topologyEntity(5L, EntityType.VIRTUAL_MACHINE, 2));
        topologyMap.put(6L, topologyEntity(6L, EntityType.VIRTUAL_MACHINE, 1));
        topologyMap.put(7L, connectedTopologyEntity(7L, EntityType.STORAGE_TIER));
        topologyMap.put(8L, connectedTopologyEntity(8L, EntityType.COMPUTE_TIER));
        topologyMap.put(9L, connectedTopologyEntity(9L, EntityType.VIRTUAL_VOLUME, 7L));
        topologyMap.put(10L, connectedTopologyEntity(10L, EntityType.VIRTUAL_VOLUME, 7L));
        topologyMap.put(11L, connectedTopologyEntity(11L, EntityType.VIRTUAL_MACHINE, 10L));
        // replacement from template
        topologyMap.put(12L, topologyEntity(12L, EntityType.PHYSICAL_MACHINE));
        topologyMap.get(2L).getEntityBuilder().getEditBuilder().setReplaced(
                TopologyDTO.TopologyEntityDTO.Replaced.newBuilder().setPlanId(7777L).setReplacementId(12L).build());

        // VM5 is also buying from the storage.
        topologyMap.get(5L)
            .getEntityBuilder()
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(3L));

        // VM11 is also buying from the StorageTier and ComputeTier
        topologyMap.get(11L)
            .getEntityBuilder()
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(7L)
                .setVolumeId(10L))
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(8L));

        topologyGraph = TopologyEntityTopologyGraphCreator.newGraph(topologyMap);
        policyMatcher = new PolicyMatcher(topologyGraph);
    }

    @Test
    public void testApplyEmpty() throws GroupResolutionException, PolicyApplicationException {
        when(groupResolver.resolve(eq(consumerGroup), eq(topologyGraph)))
            .thenReturn(resolvedGroup(consumerGroup));
        when(groupResolver.resolve(eq(providerGroup), eq(topologyGraph)))
            .thenReturn(resolvedGroup(providerGroup));

        applyPolicy(new BindToGroupPolicy(btgPolicy,
            new PolicyEntities(consumerGroup, Collections.emptySet()),
            new PolicyEntities(providerGroup)));
        assertThat(topologyGraph.getEntity(1L).get(), not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(2L).get(), not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(12L).get(), not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(4L).get(),
            not(policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE)));
        assertThat(topologyGraph.getEntity(5L).get(),
            not(policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE)));
    }

    @Test
    public void testApplyEmptyConsumers() throws GroupResolutionException, PolicyApplicationException {
        when(groupResolver.resolve(eq(consumerGroup), eq(topologyGraph)))
            .thenReturn(resolvedGroup(consumerGroup));
        when(groupResolver.resolve(eq(providerGroup), eq(topologyGraph)))
            .thenReturn(resolvedGroup(providerGroup, 1L));

        applyPolicy(new BindToGroupPolicy(btgPolicy,
            new PolicyEntities(consumerGroup, Collections.emptySet()),
            new PolicyEntities(providerGroup)));
        assertThat(topologyGraph.getEntity(1L).get(), policyMatcher.hasProviderSegment(POLICY_ID));
        assertThat(topologyGraph.getEntity(2L).get(),
            not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(12L).get(),
                not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(4L).get(),
            not(policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE)));
        assertThat(topologyGraph.getEntity(5L).get(),
            not(policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE)));
    }

    @Test
    public void testApplyEmptyProviders() throws GroupResolutionException, PolicyApplicationException {
        when(groupResolver.resolve(eq(consumerGroup), eq(topologyGraph)))
            .thenReturn(resolvedGroup(consumerGroup, 4L));
        when(groupResolver.resolve(eq(providerGroup), eq(topologyGraph)))
            .thenReturn(resolvedGroup(providerGroup));

        applyPolicy(new BindToGroupPolicy(btgPolicy,
            new PolicyEntities(consumerGroup, Sets.newHashSet(6L)),
            new PolicyEntities(providerGroup)));
        assertThat(topologyGraph.getEntity(1L).get(), not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(2L).get(), not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(12L).get(), not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(4L).get(),
            policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE));
        assertThat(topologyGraph.getEntity(5L).get(),
            not(policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE)));
        assertThat(topologyGraph.getEntity(6L).get(),
                policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE));
    }

    @Test
    public void testApplyVmToHostAffinity() throws GroupResolutionException, PolicyApplicationException {
        when(groupResolver.resolve(eq(consumerGroup), eq(topologyGraph)))
            .thenReturn(resolvedGroup(consumerGroup, 4L, 5L));
        when(groupResolver.resolve(eq(providerGroup), eq(topologyGraph)))
            .thenReturn(resolvedGroup(providerGroup, 1L, 2L));

        final PolicyApplicationResults results = applyPolicy(new BindToGroupPolicy(btgPolicy,
            new PolicyEntities(consumerGroup, Sets.newHashSet(6L)),
            new PolicyEntities(providerGroup)));

        // verify BindToGroupAndLicensePolicy creation
        final PolicyApplicationResults resultsForLicense = applyPolicy(new BindToGroupPolicy(btglPolicy,
                new PolicyEntities(consumerGroup, Sets.newHashSet(6L)),
                new PolicyEntities(providerGroup)));

        assertThat(topologyGraph.getEntity(1L).get(), policyMatcher.hasProviderSegment(POLICY_ID));
        assertThat(topologyGraph.getEntity(2L).get(), policyMatcher.hasProviderSegment(POLICY_ID));
        assertThat(topologyGraph.getEntity(12L).get(), policyMatcher.hasProviderSegment(POLICY_ID));
        assertThat(topologyGraph.getEntity(3L).get(), not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(4L).get(),
            policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE));
        assertThat(topologyGraph.getEntity(5L).get(),
            policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE));
        assertThat(topologyGraph.getEntity(6L).get(),
                policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE));
        assertThat(results.addedCommodities().get(CommodityType.SEGMENTATION), is(6));
        assertThat(resultsForLicense.addedCommodities().get(CommodityType.SEGMENTATION), is(6));
    }

    /**
     * This test should eventually assert that a PolicyApplication Exception will be thrown for VM4
     * (since it is not currently buying from storage).
     */
    @Test
    public void testApplyVmToStorageAffinity() throws GroupResolutionException, PolicyApplicationException {
        final Grouping providerGroup = PolicyGroupingHelper.policyGrouping(
            searchParametersCollection(), EntityType.STORAGE_VALUE, 5678L);

        final PolicyInfo.BindToGroupPolicy bindToGroup = PolicyInfo.BindToGroupPolicy.newBuilder()
            .setConsumerGroupId(consumerID)
            .setProviderGroupId(providerID)
            .build();

        final PolicyDTO.Policy policy = PolicyDTO.Policy.newBuilder()
            .setId(POLICY_ID)
            .setPolicyInfo(PolicyInfo.newBuilder()
                .setBindToGroup(bindToGroup))
            .build();

        when(groupResolver.resolve(eq(consumerGroup), eq(topologyGraph)))
            .thenReturn(resolvedGroup(consumerGroup, 4L, 5L));
        when(groupResolver.resolve(eq(providerGroup), eq(topologyGraph)))
            .thenReturn(resolvedGroup(providerGroup, 3L));

        final BindToGroupPolicy placementPolicy = new BindToGroupPolicy(policy,
                new PolicyEntities(consumerGroup, Collections.emptySet()),
                new PolicyEntities(providerGroup));
        assertTrue(applyPolicy(placementPolicy).errors().containsKey(placementPolicy));
    }

    @Test
    public void testApplyVolumeToStorageTierAffinity() throws GroupResolutionException, PolicyApplicationException {
        final Grouping virtualVolumeGroup = PolicyGroupingHelper.policyGrouping(
            searchParametersCollection(), EntityType.VIRTUAL_VOLUME_VALUE, 1212L);
        final Grouping storageTierGroup = PolicyGroupingHelper.policyGrouping(
            searchParametersCollection(), EntityType.STORAGE_TIER_VALUE, 1213L);

        when(groupResolver.resolve(eq(virtualVolumeGroup), eq(topologyGraph)))
            .thenReturn(resolvedGroup(virtualVolumeGroup, 9L, 10L));
        when(groupResolver.resolve(eq(storageTierGroup), eq(topologyGraph)))
            .thenReturn(resolvedGroup(storageTierGroup, 7L));

        applyPolicy(new BindToGroupPolicy(btgPolicy,
            new PolicyEntities(virtualVolumeGroup, Collections.emptySet()),
            new PolicyEntities(storageTierGroup)));

        // verify that vm is buying segment from storage tier
        assertThat(topologyGraph.getEntity(11L).get(), policyMatcher.hasConsumerSegment(POLICY_ID,
            EntityType.STORAGE_TIER));
        // verify that vm is not buying segment from compute tier
        assertThat(topologyGraph.getEntity(11L).get(), not(policyMatcher.hasConsumerSegment(POLICY_ID,
            EntityType.COMPUTE_TIER)));
        // verify that storage tier is selling segment
        assertThat(topologyGraph.getEntity(7L).get(), policyMatcher.hasProviderSegment(POLICY_ID));
        // verify that compute tier is not selling segment
        assertThat(topologyGraph.getEntity(8L).get(), not(policyMatcher.hasProviderSegment(POLICY_ID)));
        // verify that volume is not buying segment from storage tier
        assertThat(topologyGraph.getEntity(9L).get(),
            not(policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.STORAGE_TIER)));
        assertThat(topologyGraph.getEntity(10L).get(),
            not(policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.STORAGE_TIER)));
    }

    private PolicyApplicationResults applyPolicy(@Nonnull final BindToGroupPolicy policy) {
        BindToGroupPolicyApplication application =
            new BindToGroupPolicyApplication(groupResolver, topologyGraph);
        return application.apply(Collections.singletonList(policy));
    }
}