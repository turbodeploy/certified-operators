package com.vmturbo.topology.processor.group.policy.application;

import static com.vmturbo.topology.processor.group.policy.PolicyGroupingHelper.resolvedGroup;
import static com.vmturbo.topology.processor.group.policy.PolicyMatcher.searchParametersCollection;
import static com.vmturbo.topology.processor.topology.TopologyEntityUtils.topologyEntity;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

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
import com.vmturbo.commons.analysis.InvertedIndex;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.common.builders.SDKConstants;
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
import com.vmturbo.topology.processor.topology.TopologyInvertedIndexFactory;

public class AtMostNPolicyTest {
    /**
     * The tests use the following topology
     *
     *  VM8 VM4 VM5  VM6  VM7
     *   \  |  /     |  \/ |
     *    \ | /      | / \ |
     *     PM1      PM2   ST3
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final Grouping consumerGroup = PolicyGroupingHelper.policyGrouping(
        Arrays.asList(4L, 5L), EntityType.VIRTUAL_MACHINE_VALUE, 1234L);

    private final Grouping providerGroup = PolicyGroupingHelper.policyGrouping(
        Arrays.asList(1L), EntityType.PHYSICAL_MACHINE_VALUE, 5678L);

    private final long consumerID = 1234L;
    private final long providerID = 5678L;

    private final PolicyDTO.PolicyInfo.AtMostNPolicy atMostN = PolicyDTO.PolicyInfo.AtMostNPolicy.newBuilder()
        .setConsumerGroupId(consumerID)
        .setProviderGroupId(providerID)
        .setCapacity(1.0f)
        .build();

    private static final long POLICY_ID = 9999L;
    final PolicyDTO.Policy policy = PolicyDTO.Policy.newBuilder()
        .setId(POLICY_ID)
        .setPolicyInfo(PolicyInfo.newBuilder()
            .setAtMostN(atMostN))
        .build();

    private TopologyGraph<TopologyEntity> topologyGraph;
    private PolicyMatcher policyMatcher;

    private final GroupResolver groupResolver = mock(GroupResolver.class);

    TopologyInvertedIndexFactory invertedIndexFactory = mock(TopologyInvertedIndexFactory.class);

    @Before
    public void setup() {
        IdentityGenerator.initPrefix(0);

        final Map<Long, TopologyEntity.Builder> topologyMap = new HashMap<>();
        topologyMap.put(1L, topologyEntity(1L, EntityType.PHYSICAL_MACHINE));
        topologyMap.put(2L, topologyEntity(2L, EntityType.PHYSICAL_MACHINE));
        topologyMap.put(3L, topologyEntity(3L, EntityType.STORAGE));
        topologyMap.put(4L, topologyEntity(4L, EntityType.VIRTUAL_MACHINE, 1));
        topologyMap.put(5L, topologyEntity(5L, EntityType.VIRTUAL_MACHINE, 1));
        topologyMap.put(6L, topologyEntity(6L, EntityType.VIRTUAL_MACHINE, 2, 3));
        topologyMap.put(7L, topologyEntity(7L, EntityType.VIRTUAL_MACHINE, 2, 3));
        topologyMap.put(8L, topologyEntity(8L, EntityType.VIRTUAL_MACHINE, 1));
        // replacement from template
        topologyMap.put(9L, topologyEntity(9L, EntityType.PHYSICAL_MACHINE));
        topologyMap.get(2L).getEntityBuilder().getEditBuilder().setReplaced(
                TopologyDTO.TopologyEntityDTO.Replaced.newBuilder().setPlanId(7777L).setReplacementId(9L).build());

        topologyGraph = TopologyEntityTopologyGraphCreator.newGraph(topologyMap);
        policyMatcher = new PolicyMatcher(topologyGraph);
    }

    private InvertedIndex<TopologyEntity, CommoditiesBoughtFromProvider> mockInvertedIndex(
            Set<Long> potentialProviders) {
        final InvertedIndex<TopologyEntity, CommoditiesBoughtFromProvider> index = mock(InvertedIndex.class);
        when(index.getSatisfyingSellers(any())).thenAnswer(invocation -> potentialProviders.stream()
                .map(topologyGraph::getEntity)
                .filter(Optional::isPresent)
                .map(Optional::get));
        when(invertedIndexFactory.typeInvertedIndex(any(), any())).thenReturn(index);
        return index;
    }

    @Test
    public void testApplyEmpty() throws GroupResolutionException, PolicyApplicationException {
        when(groupResolver.resolve(eq(consumerGroup), eq(topologyGraph)))
            .thenReturn(resolvedGroup(consumerGroup));
        when(groupResolver.resolve(eq(providerGroup), eq(topologyGraph)))
            .thenReturn(resolvedGroup(providerGroup));

        applyPolicy(new AtMostNPolicy(policy, new PolicyEntities(consumerGroup, Collections.emptySet()),
                new PolicyEntities(providerGroup)));
        assertThat(topologyGraph.getEntity(1L).get(),
            not(policyMatcher.hasProviderSegmentWithCapacity(POLICY_ID, 1.0f)));
        assertThat(topologyGraph.getEntity(2L).get(),
            not(policyMatcher.hasProviderSegmentWithCapacity(POLICY_ID, 1.0f)));
        // No commodities being sold, because the consumer group is empty, so no one wants these
        // commodities anyway.
        assertThat(topologyGraph.getEntity(9L).get(),
                not(policyMatcher.hasProviderSegmentWithCapacity(POLICY_ID, 1.0f)));
        assertThat(topologyGraph.getEntity(1L).get(),
            not(policyMatcher.hasProviderSegmentWithCapacity(POLICY_ID, SDKConstants.ACCESS_COMMODITY_CAPACITY)));
        assertThat(topologyGraph.getEntity(2L).get(),
            not(policyMatcher.hasProviderSegmentWithCapacity(POLICY_ID, SDKConstants.ACCESS_COMMODITY_CAPACITY)));
    }

    @Test
    public void testApplyEmptyProviders() throws GroupResolutionException, PolicyApplicationException {
        when(groupResolver.resolve(eq(consumerGroup), eq(topologyGraph)))
            .thenReturn(resolvedGroup(consumerGroup, 4L));
        when(groupResolver.resolve(eq(providerGroup), eq(topologyGraph)))
            .thenReturn(resolvedGroup(providerGroup));

        // All PMs are valid providers.
        mockInvertedIndex(topologyGraph.entitiesOfType(EntityType.PHYSICAL_MACHINE)
                .map(TopologyEntity::getOid)
                .collect(Collectors.toSet()));

        applyPolicy(new AtMostNPolicy(policy, new PolicyEntities(consumerGroup, Collections.emptySet()),
                new PolicyEntities(providerGroup)));
        assertThat(topologyGraph.getEntity(1L).get(),
            policyMatcher.hasProviderSegmentWithCapacity(POLICY_ID, SDKConstants.ACCESS_COMMODITY_CAPACITY));
        assertThat(topologyGraph.getEntity(2L).get(),
            policyMatcher.hasProviderSegmentWithCapacity(POLICY_ID, SDKConstants.ACCESS_COMMODITY_CAPACITY));
        assertThat(topologyGraph.getEntity(9L).get(),
                policyMatcher.hasProviderSegmentWithCapacity(POLICY_ID, SDKConstants.ACCESS_COMMODITY_CAPACITY));
        assertThat(topologyGraph.getEntity(4L).get(),
            policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE));
        assertThat(topologyGraph.getEntity(5L).get(),
            not(policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE)));
    }

    /**
     * Providers that are not accessible from the consumers shouldn't get the segmentation policies.
     *
     * @throws GroupResolutionException To satisfy compiler.
     */
    @Test
    public void testApplyIgnoreUnaccessibleProviders() throws GroupResolutionException {
        when(groupResolver.resolve(eq(consumerGroup), eq(topologyGraph)))
                .thenReturn(resolvedGroup(consumerGroup, 4L));
        when(groupResolver.resolve(eq(providerGroup), eq(topologyGraph)))
                .thenReturn(resolvedGroup(providerGroup));

        // PM 1 is a valid provider.
        // PM 2 is not.
        mockInvertedIndex(Collections.singleton(1L));

        applyPolicy(new AtMostNPolicy(policy, new PolicyEntities(consumerGroup, Collections.emptySet()),
                new PolicyEntities(providerGroup)));
        assertThat(topologyGraph.getEntity(1L).get(),
                policyMatcher.hasProviderSegmentWithCapacity(POLICY_ID, SDKConstants.ACCESS_COMMODITY_CAPACITY));
        // PM 2 and 9 (which replaces 2) should not sell the segmentation commodity because they
        // are not valid destinations for VM 4 (according to the mock inverted index).
        assertThat(topologyGraph.getEntity(2L).get(),
                not(policyMatcher.hasProviderSegmentWithCapacity(POLICY_ID, SDKConstants.ACCESS_COMMODITY_CAPACITY)));
        assertThat(topologyGraph.getEntity(9L).get(),
                not(policyMatcher.hasProviderSegmentWithCapacity(POLICY_ID, SDKConstants.ACCESS_COMMODITY_CAPACITY)));
        assertThat(topologyGraph.getEntity(4L).get(),
                policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE));
        assertThat(topologyGraph.getEntity(5L).get(),
                not(policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE)));

    }

    /**
     * Test that hosts get segmentation commodities applied to them properly.
     *
     * @throws GroupResolutionException To satisfy compiler.
     */
    @Test
    public void testApplyVmToHostAntiAffinity() throws GroupResolutionException {
        when(groupResolver.resolve(eq(consumerGroup), eq(topologyGraph)))
            .thenReturn(resolvedGroup(consumerGroup, 4L, 5L));
        when(groupResolver.resolve(eq(providerGroup), eq(topologyGraph)))
            .thenReturn(resolvedGroup(providerGroup, 1L, 2L));

        // All PMs are valid providers for the VMs.
        mockInvertedIndex(topologyGraph.entitiesOfType(EntityType.PHYSICAL_MACHINE)
                .map(TopologyEntity::getOid)
                .collect(Collectors.toSet()));

        applyPolicy(new AtMostNPolicy(policy, new PolicyEntities(consumerGroup, Sets.newHashSet(8L)),
                new PolicyEntities(providerGroup)));
        assertThat(topologyGraph.getEntity(1L).get(),
            policyMatcher.hasProviderSegmentWithCapacityAndUsed(POLICY_ID, 1.0f, 3.0f));
        assertThat(topologyGraph.getEntity(2L).get(),
            policyMatcher.hasProviderSegmentWithCapacityAndUsed(POLICY_ID, 1.0f, 0.0f));
        assertThat(topologyGraph.getEntity(9L).get(),
            policyMatcher.hasProviderSegmentWithCapacityAndUsed(POLICY_ID, 1.0f, 0.0f));
        assertThat(topologyGraph.getEntity(3L).get(), not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(4L).get(),
            policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE));
        assertThat(topologyGraph.getEntity(5L).get(),
            policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE));
        assertThat(topologyGraph.getEntity(8L).get(),
                policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE));
    }

    /**
     * This test should eventually assert that a PolicyApplication Exception will be thrown for VM4
     * (since it is not currently buying from storage).
     */
    @Test
    public void testApplyVmToStorageAffinity() throws GroupResolutionException, PolicyApplicationException {
        final Grouping providerGroup = PolicyGroupingHelper.policyGrouping(
            searchParametersCollection(), EntityType.STORAGE_VALUE, 5678L);

        final PolicyDTO.PolicyInfo.AtMostNPolicy atMostNPolicy =
            PolicyDTO.PolicyInfo.AtMostNPolicy.newBuilder()
                .setConsumerGroupId(consumerID)
                .setProviderGroupId(providerID)
                .setCapacity(1.0f)
                .build();

        final PolicyDTO.Policy policy = PolicyDTO.Policy.newBuilder()
                .setId(POLICY_ID)
                .setPolicyInfo(PolicyInfo.newBuilder()
                    .setAtMostN(atMostNPolicy))
                .build();

        // All Storages are valid providers for the VMs.
        mockInvertedIndex(topologyGraph.entitiesOfType(EntityType.STORAGE)
                .map(TopologyEntity::getOid)
                .collect(Collectors.toSet()));

        when(groupResolver.resolve(eq(consumerGroup), eq(topologyGraph)))
            .thenReturn(resolvedGroup(consumerGroup, 4L, 5L));
        when(groupResolver.resolve(eq(providerGroup), eq(topologyGraph)))
            .thenReturn(resolvedGroup(providerGroup, 3L));

        final AtMostNPolicy atMostN = new AtMostNPolicy(policy,
            new PolicyEntities(consumerGroup,  Collections.emptySet()),
            new PolicyEntities(providerGroup));
        assertTrue(applyPolicy(atMostN).errors().containsKey(atMostN));
    }

    private PolicyApplicationResults applyPolicy(@Nonnull final AtMostNPolicy policy) {
        AtMostNPolicyApplication application = new AtMostNPolicyApplication(groupResolver, topologyGraph, invertedIndexFactory);
        return application.apply(Collections.singletonList(policy));
    }
}