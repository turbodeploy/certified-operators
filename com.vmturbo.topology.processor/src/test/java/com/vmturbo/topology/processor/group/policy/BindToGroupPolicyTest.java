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

import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommodityBoughtList;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.group.GroupResolutionException;
import com.vmturbo.topology.processor.group.GroupResolver;
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

    final PolicyDTO.PolicyGrouping consumerGroup = PolicyGroupingHelper.policyGrouping(
        searchParametersCollection(), EntityType.VIRTUAL_MACHINE_VALUE, 1234L);

    final PolicyDTO.PolicyGrouping providerGroup = PolicyGroupingHelper.policyGrouping(
        searchParametersCollection(), EntityType.PHYSICAL_MACHINE_VALUE, 5678L);

    final PolicyDTO.Policy.BindToGroupPolicy bindToGroup = PolicyDTO.Policy.BindToGroupPolicy.newBuilder()
        .setConsumerGroup(consumerGroup)
        .setProviderGroup(providerGroup)
        .build();

    private static final long POLICY_ID = 9999L;
    final PolicyDTO.Policy policy = PolicyDTO.Policy.newBuilder()
        .setId(POLICY_ID)
        .setBindToGroup(bindToGroup)
        .build();

    TopologyGraph topologyGraph;
    PolicyMatcher policyMatcher;

    final GroupResolver groupResolver = mock(GroupResolver.class);

    @Before
    public void setup() {
        final Map<Long, Builder> topologyMap = new HashMap<>();
        topologyMap.put(1L, topologyEntity(1L, EntityType.PHYSICAL_MACHINE));
        topologyMap.put(2L, topologyEntity(2L, EntityType.PHYSICAL_MACHINE));
        topologyMap.put(3L, topologyEntity(3L, EntityType.STORAGE));
        topologyMap.put(4L, topologyEntity(4L, EntityType.VIRTUAL_MACHINE, 1));
        topologyMap.put(5L, topologyEntity(5L, EntityType.VIRTUAL_MACHINE, 2));

        // VM5 is also buying from the storage.
        topologyMap.get(5L).putCommodityBoughtMap(3L, CommodityBoughtList.getDefaultInstance());

        topologyGraph = new TopologyGraph(topologyMap);
        policyMatcher = new PolicyMatcher(topologyGraph);
    }

    @Test
    public void testApplyEmpty() throws GroupResolutionException, PolicyApplicationException {
        when(groupResolver.resolve(eq(consumerGroup), eq(topologyGraph)))
            .thenReturn(Collections.<Long>emptySet());
        when(groupResolver.resolve(eq(providerGroup), eq(topologyGraph)))
            .thenReturn(Collections.<Long>emptySet());

        new BindToGroupPolicy(policy).apply(groupResolver, topologyGraph);
        assertThat(topologyGraph.getVertex(1L).get(), not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getVertex(2L).get(), not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getVertex(4L).get(),
            not(policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE)));
        assertThat(topologyGraph.getVertex(5L).get(),
            not(policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE)));
    }

    @Test
    public void testApplyEmptyConsumers() throws GroupResolutionException, PolicyApplicationException {
        when(groupResolver.resolve(eq(consumerGroup), eq(topologyGraph)))
            .thenReturn(Collections.<Long>emptySet());
        when(groupResolver.resolve(eq(providerGroup), eq(topologyGraph)))
            .thenReturn(Collections.singleton(1L));

        new BindToGroupPolicy(policy).apply(groupResolver, topologyGraph);
        assertThat(topologyGraph.getVertex(1L).get(), policyMatcher.hasProviderSegment(POLICY_ID));
        assertThat(topologyGraph.getVertex(2L).get(),
            not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getVertex(4L).get(),
            not(policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE)));
        assertThat(topologyGraph.getVertex(5L).get(),
            not(policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE)));
    }

    @Test
    public void testApplyEmptyProviders() throws GroupResolutionException, PolicyApplicationException {
        when(groupResolver.resolve(eq(consumerGroup), eq(topologyGraph)))
            .thenReturn(Collections.singleton(4L));
        when(groupResolver.resolve(eq(providerGroup), eq(topologyGraph)))
            .thenReturn(Collections.<Long>emptySet());

        new BindToGroupPolicy(policy).apply(groupResolver, topologyGraph);
        assertThat(topologyGraph.getVertex(1L).get(), not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getVertex(2L).get(), not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getVertex(4L).get(),
            policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE));
        assertThat(topologyGraph.getVertex(5L).get(),
            not(policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE)));
    }

    @Test
    public void testApplyVmToHostAffinity() throws GroupResolutionException, PolicyApplicationException {
        when(groupResolver.resolve(eq(consumerGroup), eq(topologyGraph)))
            .thenReturn(Sets.newHashSet(4L, 5L));
        when(groupResolver.resolve(eq(providerGroup), eq(topologyGraph)))
            .thenReturn(Sets.newHashSet(1L, 2L));

        new BindToGroupPolicy(policy).apply(groupResolver, topologyGraph);
        assertThat(topologyGraph.getVertex(1L).get(), policyMatcher.hasProviderSegment(POLICY_ID));
        assertThat(topologyGraph.getVertex(2L).get(), policyMatcher.hasProviderSegment(POLICY_ID));
        assertThat(topologyGraph.getVertex(3L).get(), not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getVertex(4L).get(),
            policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE));
        assertThat(topologyGraph.getVertex(5L).get(),
            policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE));
    }

    /**
     * This test should eventually assert that a new bundle of commodities bought is created for VM4
     * (since it is not currently buying from storage) and the existing bundle of commodities bought
     * is reused on VM5 (since it is already buying from storage). For now, we expect an exception
     * because buying a commodity from no provider is currently unsupported (see OM-21673).
     */
    @Test
    public void testApplyVmToStorageAffinity() throws GroupResolutionException, PolicyApplicationException {
        final PolicyDTO.PolicyGrouping providerGroup = PolicyGroupingHelper.policyGrouping(
            searchParametersCollection(), EntityType.STORAGE_VALUE, 5678L);

        final PolicyDTO.Policy.BindToGroupPolicy bindToGroup = PolicyDTO.Policy.BindToGroupPolicy.newBuilder()
            .setConsumerGroup(consumerGroup)
            .setProviderGroup(providerGroup)
            .build();

        final PolicyDTO.Policy policy = PolicyDTO.Policy.newBuilder()
            .setId(POLICY_ID)
            .setBindToGroup(bindToGroup)
            .build();

        when(groupResolver.resolve(eq(consumerGroup), eq(topologyGraph)))
            .thenReturn(Sets.newHashSet(4L, 5L));
        when(groupResolver.resolve(eq(providerGroup), eq(topologyGraph)))
            .thenReturn(Collections.singleton(3L));

        // TODO: This should not generate an exception when OM-21673 is implemented. Instead we should assert
        // the segmentation commodity was created with no provider.
        expectedException.expect(PolicyApplicationException.class);
        new BindToGroupPolicy(policy).apply(groupResolver, topologyGraph);
    }
}