package com.vmturbo.topology.processor.group.policy;

import static com.vmturbo.topology.processor.group.filter.FilterUtils.topologyEntity;
import static com.vmturbo.topology.processor.group.policy.PolicyMatcher.searchParametersCollection;
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

import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.group.GroupResolutionException;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.policy.PolicyFactory.PolicyEntities;
import com.vmturbo.topology.processor.topology.TopologyGraph;

/**
 * The tests use the following topology (no links are provided below 1,2 are hosts and 3,4 are Storage
 * and 5 and 6 are VMs):
 *
 * VM5 VM7  VM6
 *  | /     |\
 *  |/      | \
 *  PM1   PM2 ST3  ST4
 */
public class BindToComplementaryGroupPolicyTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    final Group consumerGroup = PolicyGroupingHelper.policyGrouping(
        searchParametersCollection(), EntityType.VIRTUAL_MACHINE_VALUE, 1234L);

    final Group providerGroup = PolicyGroupingHelper.policyGrouping(
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

    TopologyGraph topologyGraph;
    PolicyMatcher policyMatcher;

    final GroupResolver groupResolver = mock(GroupResolver.class);

    @Before
    public void setup() {
        final Map<Long, TopologyEntity.Builder> topologyMap = new HashMap<>();
        topologyMap.put(1L, topologyEntity(1L, EntityType.PHYSICAL_MACHINE));
        topologyMap.put(2L, topologyEntity(2L, EntityType.PHYSICAL_MACHINE));
        topologyMap.put(3L, topologyEntity(3L, EntityType.STORAGE));
        topologyMap.put(4L, topologyEntity(4L, EntityType.STORAGE));
        topologyMap.put(5L, topologyEntity(5L, EntityType.VIRTUAL_MACHINE, 1));
        topologyMap.put(6L, topologyEntity(6L, EntityType.VIRTUAL_MACHINE, 2, 3));
        topologyMap.put(7L, topologyEntity(7L, EntityType.VIRTUAL_MACHINE, 1));

        topologyGraph = TopologyGraph.newGraph(topologyMap);
        policyMatcher = new PolicyMatcher(topologyGraph);
    }

    @Test
    public void testApplyEmpty() throws GroupResolutionException, PolicyApplicationException {
        when(groupResolver.resolve(eq(consumerGroup), eq(topologyGraph)))
                .thenReturn(Collections.emptySet());
        when(groupResolver.resolve(eq(providerGroup), eq(topologyGraph)))
                .thenReturn(Collections.emptySet());

        new BindToComplementaryGroupPolicy(policy, new PolicyEntities(consumerGroup, Collections.emptySet()),
                new PolicyEntities(providerGroup))
                .apply(groupResolver, topologyGraph);
        assertThat(topologyGraph.getEntity(1L).get(), policyMatcher.hasProviderSegment(POLICY_ID));
        assertThat(topologyGraph.getEntity(2L).get(), policyMatcher.hasProviderSegment(POLICY_ID));
        assertThat(topologyGraph.getEntity(5L).get(), not(policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE)));
        assertThat(topologyGraph.getEntity(6L).get(), not(policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE)));
    }

    @Test
    public void testApplyEmptyConsumers() throws GroupResolutionException, PolicyApplicationException {
        when(groupResolver.resolve(eq(consumerGroup), eq(topologyGraph)))
                .thenReturn(Collections.emptySet());
        when(groupResolver.resolve(eq(providerGroup), eq(topologyGraph)))
                .thenReturn(Collections.singleton(1L));

        new BindToComplementaryGroupPolicy(policy, new PolicyEntities(consumerGroup, Collections.emptySet()),
                new PolicyEntities(providerGroup))
                .apply(groupResolver, topologyGraph);
        assertThat(topologyGraph.getEntity(1L).get(), not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(2L).get(), policyMatcher.hasProviderSegment(POLICY_ID));
        assertThat(topologyGraph.getEntity(5L).get(), not(policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE)));
        assertThat(topologyGraph.getEntity(6L).get(), not(policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE)));
    }

    @Test
    public void testApplyEmptyProviders() throws GroupResolutionException, PolicyApplicationException {
        when(groupResolver.resolve(eq(consumerGroup), eq(topologyGraph)))
                .thenReturn(Collections.singleton(5L));
        when(groupResolver.resolve(eq(providerGroup), eq(topologyGraph)))
                .thenReturn(Collections.<Long>emptySet());

        new BindToComplementaryGroupPolicy(policy, new PolicyEntities(consumerGroup, Sets.newHashSet(7L)),
                new PolicyEntities(providerGroup))
                .apply(groupResolver, topologyGraph);
        assertThat(topologyGraph.getEntity(1L).get(), policyMatcher.hasProviderSegment(POLICY_ID));
        assertThat(topologyGraph.getEntity(2L).get(), policyMatcher.hasProviderSegment(POLICY_ID));
        assertThat(topologyGraph.getEntity(5L).get(), policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE));
        assertThat(topologyGraph.getEntity(6L).get(), not(policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE)));
        assertThat(topologyGraph.getEntity(7L).get(), policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE));
    }

    @Test
    public void testApplyVmToAllHostAffinity() throws GroupResolutionException, PolicyApplicationException {
        when(groupResolver.resolve(eq(consumerGroup), eq(topologyGraph)))
                .thenReturn(Sets.newHashSet(5L, 6L));
        when(groupResolver.resolve(eq(providerGroup), eq(topologyGraph)))
                .thenReturn(Sets.newHashSet(1L, 2L));

        new BindToComplementaryGroupPolicy(policy, new PolicyEntities(consumerGroup, Sets.newHashSet(7L)),
                new PolicyEntities(providerGroup))
                .apply(groupResolver, topologyGraph);
        assertThat(topologyGraph.getEntity(1L).get(), not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(2L).get(), not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(3L).get(), not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(4L).get(), not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(5L).get(), policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE));
        assertThat(topologyGraph.getEntity(6L).get(), policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE));
        assertThat(topologyGraph.getEntity(7L).get(), policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE));
    }

    @Test
    public void testApplyVmToSomeHostAffinity() throws GroupResolutionException, PolicyApplicationException {
        when(groupResolver.resolve(eq(consumerGroup), eq(topologyGraph)))
                .thenReturn(Sets.newHashSet(5L, 6L));
        when(groupResolver.resolve(eq(providerGroup), eq(topologyGraph)))
                .thenReturn(Sets.newHashSet(2L));

        new BindToComplementaryGroupPolicy(policy, new PolicyEntities(consumerGroup, Sets.newHashSet(7L)),
                new PolicyEntities(providerGroup))
                .apply(groupResolver, topologyGraph);
        assertThat(topologyGraph.getEntity(1L).get(), policyMatcher.hasProviderSegment(POLICY_ID));
        assertThat(topologyGraph.getEntity(2L).get(), not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(3L).get(), not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(4L).get(), not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(5L).get(), policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE));
        assertThat(topologyGraph.getEntity(6L).get(), policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE));
        assertThat(topologyGraph.getEntity(7L).get(), policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE));
    }

    @Test
    public void testApplyVmToStorageAffinity() throws GroupResolutionException, PolicyApplicationException {
        final Group providerGroup = PolicyGroupingHelper.policyGrouping(
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
                .thenReturn(Sets.newHashSet(6L));
        when(groupResolver.resolve(eq(providerGroup), eq(topologyGraph)))
                .thenReturn(Collections.singleton(3L));

        new BindToComplementaryGroupPolicy(policy, new PolicyEntities(consumerGroup, Collections.emptySet()),
                new PolicyEntities(providerGroup))
                .apply(groupResolver, topologyGraph);
        assertThat(topologyGraph.getEntity(1L).get(), not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(2L).get(), not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(3L).get(), not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(4L).get(), policyMatcher.hasProviderSegment(POLICY_ID));
        assertThat(topologyGraph.getEntity(5L).get(), not(policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.STORAGE)));
        assertThat(topologyGraph.getEntity(6L).get(), policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.STORAGE));
    }
}
