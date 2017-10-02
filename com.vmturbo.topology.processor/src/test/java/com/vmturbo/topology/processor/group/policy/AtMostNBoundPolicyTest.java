package com.vmturbo.topology.processor.group.policy;

import static com.vmturbo.topology.processor.group.filter.FilterUtils.topologyEntity;
import static com.vmturbo.topology.processor.group.policy.PolicyMatcher.searchParametersCollection;
import static com.vmturbo.topology.processor.group.policy.PolicyMatcher.staticGroupMembers;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
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
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyGrouping;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Builder;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.group.GroupResolutionException;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.topology.TopologyGraph;

public class AtMostNBoundPolicyTest {

    /**
     * The tests use the following topology
     *
     *  VM4  VM5  VM6  VM7
     *   |  /     |  \/ |
     *   | /      | / \ |
     *  PM1      PM2   ST3
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    final PolicyGrouping consumerGroup = PolicyGroupingHelper.policyGrouping(
        staticGroupMembers(4L, 5L), EntityType.VIRTUAL_MACHINE_VALUE, 1234L);

    final PolicyGrouping providerGroup = PolicyGroupingHelper.policyGrouping(
        staticGroupMembers(1L), EntityType.PHYSICAL_MACHINE_VALUE, 5678L);

    final PolicyDTO.Policy.AtMostNBoundPolicy atMostNBound = PolicyDTO.Policy.AtMostNBoundPolicy.newBuilder()
        .setConsumerGroup(consumerGroup)
        .setProviderGroup(providerGroup)
        .setCapacity(1.0f)
        .build();

    private static final long POLICY_ID = 9999L;
    final PolicyDTO.Policy policy = PolicyDTO.Policy.newBuilder()
        .setId(POLICY_ID)
        .setAtMostNBound(atMostNBound)
        .build();

    TopologyGraph topologyGraph;
    PolicyMatcher policyMatcher;

    final GroupResolver groupResolver = mock(GroupResolver.class);

    @Before
    public void setup() {
        IdentityGenerator.initPrefix(0);

        final Map<Long, Builder> topologyMap = new HashMap<>();
        topologyMap.put(1L, topologyEntity(1L, EntityType.PHYSICAL_MACHINE));
        topologyMap.put(2L, topologyEntity(2L, EntityType.PHYSICAL_MACHINE));
        topologyMap.put(3L, topologyEntity(3L, EntityType.STORAGE));
        topologyMap.put(4L, topologyEntity(4L, EntityType.VIRTUAL_MACHINE, 1));
        topologyMap.put(5L, topologyEntity(5L, EntityType.VIRTUAL_MACHINE, 1));
        topologyMap.put(6L, topologyEntity(6L, EntityType.VIRTUAL_MACHINE, 2, 3));
        topologyMap.put(7L, topologyEntity(7L, EntityType.VIRTUAL_MACHINE, 2, 3));

        topologyGraph = new TopologyGraph(topologyMap);
        policyMatcher = new PolicyMatcher(topologyGraph);
    }

    @Test
    public void testApplyEmpty() throws GroupResolutionException, PolicyApplicationException {
        when(groupResolver.resolve(eq(consumerGroup), eq(topologyGraph)))
            .thenReturn(Collections.<Long>emptySet());
        when(groupResolver.resolve(eq(providerGroup), eq(topologyGraph)))
            .thenReturn(Collections.<Long>emptySet());

        new AtMostNBoundPolicy(policy).apply(groupResolver, topologyGraph);
        assertThat(topologyGraph.getVertex(1L).get(),
            not(policyMatcher.hasProviderSegmentWithCapacity(POLICY_ID, 1.0f)));
        assertThat(topologyGraph.getVertex(2L).get(),
            not(policyMatcher.hasProviderSegmentWithCapacity(POLICY_ID, 1.0f)));
        assertThat(topologyGraph.getVertex(1L).get(),
            not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getVertex(2L).get(),
            not(policyMatcher.hasProviderSegment(POLICY_ID)));
    }

    @Test
    public void testApplyEmptyProviders() throws GroupResolutionException, PolicyApplicationException {
        when(groupResolver.resolve(eq(consumerGroup), eq(topologyGraph)))
            .thenReturn(Collections.singleton(4L));
        when(groupResolver.resolve(eq(providerGroup), eq(topologyGraph)))
            .thenReturn(Collections.<Long>emptySet());

        new AtMostNBoundPolicy(policy).apply(groupResolver, topologyGraph);
        assertThat(topologyGraph.getVertex(1L).get(),
            not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getVertex(2L).get(),
            not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getVertex(4L).get(),
            policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE));
        assertThat(topologyGraph.getVertex(5L).get(),
            not(policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE)));
    }

    @Test
    public void testApplyVmToHostAntiAffinity() throws GroupResolutionException, PolicyApplicationException {
        when(groupResolver.resolve(eq(consumerGroup), eq(topologyGraph)))
            .thenReturn(Sets.newHashSet(4L, 5L));
        when(groupResolver.resolve(eq(providerGroup), eq(topologyGraph)))
            .thenReturn(Sets.newHashSet(1L));

        new AtMostNBoundPolicy(policy).apply(groupResolver, topologyGraph);
        assertThat(topologyGraph.getVertex(1L).get(),
            policyMatcher.hasProviderSegmentWithCapacityAndUsed(POLICY_ID, 1.0f, 2.0f));
        assertThat(topologyGraph.getVertex(2L).get(),
            not(policyMatcher.hasProviderSegment(POLICY_ID)));
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
        final PolicyGrouping providerGroup = PolicyGroupingHelper.policyGrouping(
            searchParametersCollection(), EntityType.STORAGE_VALUE, 5678L);

        final PolicyDTO.Policy.AtMostNBoundPolicy atMostNBoundPolicy = PolicyDTO.Policy.AtMostNBoundPolicy.newBuilder()
            .setConsumerGroup(consumerGroup)
            .setProviderGroup(providerGroup)
            .setCapacity(1.0f)
            .build();

        final PolicyDTO.Policy policy = PolicyDTO.Policy.newBuilder()
            .setId(POLICY_ID)
            .setAtMostNBound(atMostNBoundPolicy)
            .build();

        when(groupResolver.resolve(eq(consumerGroup), eq(topologyGraph)))
            .thenReturn(Sets.newHashSet(4L, 5L));
        when(groupResolver.resolve(eq(providerGroup), eq(topologyGraph)))
            .thenReturn(Collections.singleton(3L));

        // TODO: This should not generate an exception when OM-21673 is implemented. Instead we should assert
        // the segmentation commodity was created with no provider.
        expectedException.expect(PolicyApplicationException.class);
        new AtMostNBoundPolicy(policy).apply(groupResolver, topologyGraph);
    }
}