package com.vmturbo.topology.processor.group.policy;

import static com.vmturbo.topology.processor.group.filter.FilterUtils.topologyEntity;
import static com.vmturbo.topology.processor.group.policy.PolicyMatcher.searchParametersCollection;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.group.GroupResolutionException;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.topology.TopologyEntity;
import com.vmturbo.topology.processor.topology.TopologyGraph;

/**
 * The tests use the following topology (no links are provided below 1,2 are hosts and 3,4 are VMs):
 *
 *  VM4  VM5
 *   |    |\
 *   |    | \
 *  PM1 PM2 ST3
 */
public class BindToGroupPolicyTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final Group consumerGroup = PolicyGroupingHelper.policyGrouping(
        searchParametersCollection(), EntityType.VIRTUAL_MACHINE_VALUE, 1234L);

    private final Group providerGroup = PolicyGroupingHelper.policyGrouping(
        searchParametersCollection(), EntityType.PHYSICAL_MACHINE_VALUE, 5678L);

    private final long consumerID = 1234L;
    private final long providerID = 5678L;

    private final PolicyDTO.Policy.BindToGroupPolicy bindToGroup = PolicyDTO.Policy.BindToGroupPolicy.newBuilder()
        .setConsumerGroupId(consumerID)
        .setProviderGroupId(providerID)
        .build();

    private static final long POLICY_ID = 9999L;
    final PolicyDTO.Policy policy = PolicyDTO.Policy.newBuilder()
        .setId(POLICY_ID)
        .setBindToGroup(bindToGroup)
        .build();

    private TopologyGraph topologyGraph;
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

        // VM5 is also buying from the storage.
        topologyMap.get(5L)
            .getEntityBuilder()
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(3L));

        topologyGraph = TopologyGraph.newGraph(topologyMap);
        policyMatcher = new PolicyMatcher(topologyGraph);
    }

    @Test
    public void testApplyEmpty() throws GroupResolutionException, PolicyApplicationException {
        when(groupResolver.resolve(eq(consumerGroup), eq(topologyGraph)))
            .thenReturn(Collections.<Long>emptySet());
        when(groupResolver.resolve(eq(providerGroup), eq(topologyGraph)))
            .thenReturn(Collections.<Long>emptySet());

        new BindToGroupPolicy(policy, consumerGroup, providerGroup)
                .apply(groupResolver, topologyGraph);
        assertThat(topologyGraph.getEntity(1L).get(), not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(2L).get(), not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(4L).get(),
            not(policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE)));
        assertThat(topologyGraph.getEntity(5L).get(),
            not(policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE)));
    }

    @Test
    public void testApplyEmptyConsumers() throws GroupResolutionException, PolicyApplicationException {
        when(groupResolver.resolve(eq(consumerGroup), eq(topologyGraph)))
            .thenReturn(Collections.<Long>emptySet());
        when(groupResolver.resolve(eq(providerGroup), eq(topologyGraph)))
            .thenReturn(Collections.singleton(1L));

        new BindToGroupPolicy(policy, consumerGroup, providerGroup)
                .apply(groupResolver, topologyGraph);
        assertThat(topologyGraph.getEntity(1L).get(), policyMatcher.hasProviderSegment(POLICY_ID));
        assertThat(topologyGraph.getEntity(2L).get(),
            not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(4L).get(),
            not(policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE)));
        assertThat(topologyGraph.getEntity(5L).get(),
            not(policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE)));
    }

    @Test
    public void testApplyEmptyProviders() throws GroupResolutionException, PolicyApplicationException {
        when(groupResolver.resolve(eq(consumerGroup), eq(topologyGraph)))
            .thenReturn(Collections.singleton(4L));
        when(groupResolver.resolve(eq(providerGroup), eq(topologyGraph)))
            .thenReturn(Collections.<Long>emptySet());

        new BindToGroupPolicy(policy, consumerGroup, providerGroup)
                .apply(groupResolver, topologyGraph);
        assertThat(topologyGraph.getEntity(1L).get(), not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(2L).get(), not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(4L).get(),
            policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE));
        assertThat(topologyGraph.getEntity(5L).get(),
            not(policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE)));
    }

    @Test
    public void testApplyVmToHostAffinity() throws GroupResolutionException, PolicyApplicationException {
        when(groupResolver.resolve(eq(consumerGroup), eq(topologyGraph)))
            .thenReturn(Sets.newHashSet(4L, 5L));
        when(groupResolver.resolve(eq(providerGroup), eq(topologyGraph)))
            .thenReturn(Sets.newHashSet(1L, 2L));

        new BindToGroupPolicy(policy, consumerGroup, providerGroup)
                .apply(groupResolver, topologyGraph);
        assertThat(topologyGraph.getEntity(1L).get(), policyMatcher.hasProviderSegment(POLICY_ID));
        assertThat(topologyGraph.getEntity(2L).get(), policyMatcher.hasProviderSegment(POLICY_ID));
        assertThat(topologyGraph.getEntity(3L).get(), not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(4L).get(),
            policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE));
        assertThat(topologyGraph.getEntity(5L).get(),
            policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE));
    }

    /**
     * This test should eventually assert that a PolicyApplication Exception will be thrown for VM4
     * (since it is not currently buying from storage).
     */
    @Test
    public void testApplyVmToStorageAffinity() throws GroupResolutionException, PolicyApplicationException {
        final Group providerGroup = PolicyGroupingHelper.policyGrouping(
            searchParametersCollection(), EntityType.STORAGE_VALUE, 5678L);

        final PolicyDTO.Policy.BindToGroupPolicy bindToGroup = PolicyDTO.Policy.BindToGroupPolicy.newBuilder()
            .setConsumerGroupId(consumerID)
            .setProviderGroupId(providerID)
            .build();

        final PolicyDTO.Policy policy = PolicyDTO.Policy.newBuilder()
            .setId(POLICY_ID)
            .setBindToGroup(bindToGroup)
            .build();

        when(groupResolver.resolve(eq(consumerGroup), eq(topologyGraph)))
            .thenReturn(Sets.newHashSet(4L, 5L));
        when(groupResolver.resolve(eq(providerGroup), eq(topologyGraph)))
            .thenReturn(Collections.singleton(3L));

        expectedException.expect(PolicyApplicationException.class);
        new BindToGroupPolicy(policy, consumerGroup, providerGroup)
                .apply(groupResolver, topologyGraph);
    }
}