package com.vmturbo.topology.processor.group.policy.application;

import static com.vmturbo.topology.processor.group.policy.PolicyGroupingHelper.resolvedGroup;
import static com.vmturbo.topology.processor.group.policy.PolicyMatcher.searchParametersCollection;
import static com.vmturbo.topology.processor.topology.TopologyEntityUtils.topologyEntity;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
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
 * The tests use the following topology:
 *  VM5  VM8 VM6   VM7
 *   |     \  |  \/ |
 *   |      \ | / \ |
 *  PM1      PM2   ST3 ST4
 */
public class MustRunTogetherPolicyTest {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    // vm group (consumers)
    private final Grouping group = PolicyGroupingHelper.policyGrouping(
        searchParametersCollection(), EntityType.VIRTUAL_MACHINE_VALUE, 1234L);
    private final long groupID = group.getId();

    // must run together on host policy
    private final PolicyInfo.MustRunTogetherPolicy mustRunTogetherPolicy =
        PolicyInfo.MustRunTogetherPolicy.newBuilder()
            .setGroupId(groupID)
            .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
            .build();

    private static final long POLICY_ID = 9999L;
    final PolicyDTO.Policy policy = PolicyDTO.Policy.newBuilder()
        .setId(POLICY_ID)
        .setPolicyInfo(PolicyInfo.newBuilder()
            .setMustRunTogether(mustRunTogetherPolicy))
        .build();

    // must run together on storage policy
    private final PolicyInfo.MustRunTogetherPolicy mustRunTogetherOnStoragePolicy =
            PolicyInfo.MustRunTogetherPolicy.newBuilder()
                .setGroupId(groupID)
                .setProviderEntityType(EntityType.STORAGE_VALUE)
                .build();

    private static final long POLICY_ST_ID = 9998L;
    final PolicyDTO.Policy policyStorage = PolicyDTO.Policy.newBuilder()
            .setId(POLICY_ST_ID)
            .setPolicyInfo(PolicyInfo.newBuilder()
                .setMustRunTogether(mustRunTogetherOnStoragePolicy))
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
        topologyMap.put(4L, topologyEntity(4L, EntityType.STORAGE));
        topologyMap.put(5L, topologyEntity(5L, EntityType.VIRTUAL_MACHINE, 1));
        topologyMap.put(6L, topologyEntity(6L, EntityType.VIRTUAL_MACHINE, 2, 3));
        topologyMap.put(7L, topologyEntity(7L, EntityType.VIRTUAL_MACHINE, 2, 3));
        topologyMap.put(8L, topologyEntity(8L, EntityType.VIRTUAL_MACHINE, 2));
        // replacement from template
        topologyMap.put(9L, topologyEntity(9L, EntityType.PHYSICAL_MACHINE));
        topologyMap.get(2L).getEntityBuilder().getEditBuilder().setReplaced(
                TopologyDTO.TopologyEntityDTO.Replaced.newBuilder().setPlanId(7777L).setReplacementId(9L).build());

        topologyGraph = TopologyEntityTopologyGraphCreator.newGraph(topologyMap);
        policyMatcher = new PolicyMatcher(topologyGraph);
    }

    @Test
    public void testApplyEmpty() throws GroupResolutionException, PolicyApplicationException {
        when(groupResolver.resolve(eq(group), eq(topologyGraph)))
            .thenReturn(resolvedGroup(group));

        applyPolicy(new MustRunTogetherPolicy(policy,
            new PolicyEntities(group, Collections.emptySet())));
        assertThat(topologyGraph.getEntity(1L).get(),
                not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(2L).get(),
                not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(9L).get(),
                not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(5L).get(),
                not(policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE)));
        assertThat(topologyGraph.getEntity(6L).get(),
                not(policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE)));
        assertThat(topologyGraph.getEntity(7L).get(),
                not(policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE)));
        assertThat(topologyGraph.getEntity(8L).get(),
                not(policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE)));
    }

    @Test
    public void testApplyVmTogetherOnHost() throws GroupResolutionException, PolicyApplicationException {
        when(groupResolver.resolve(eq(group), eq(topologyGraph)))
                .thenReturn(resolvedGroup(group, 5L, 6L, 7L));

        applyPolicy(new MustRunTogetherPolicy(policy,
            new PolicyEntities(group, Collections.emptySet())));

        // check consumers
        assertThat(topologyGraph.getEntity(5L).get(),
                policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE));
        assertThat(topologyGraph.getEntity(6L).get(),
                policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE));
        assertThat(topologyGraph.getEntity(7L).get(),
                policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE));
        assertThat(topologyGraph.getEntity(8L).get(),
                not(policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE)));

        // check providers
        // the only one to have the segmentation commodity should be host2
        // because it has already the most number of vms (in the policy) running on it
        assertThat(topologyGraph.getEntity(1L).get(),
                not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(2L).get(),
                policyMatcher.hasProviderSegment(POLICY_ID));
        assertThat(topologyGraph.getEntity(9L).get(),
                policyMatcher.hasProviderSegment(POLICY_ID));
        assertThat(topologyGraph.getEntity(3L).get(),
                not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(4L).get(),
                not(policyMatcher.hasProviderSegment(POLICY_ID)));
    }

    @Test
    public void testApplyVmTogetherOnStorage() throws GroupResolutionException, PolicyApplicationException {
        when(groupResolver.resolve(eq(group), eq(topologyGraph)))
                .thenReturn(resolvedGroup(group, 6L, 7L));

        applyPolicy(new MustRunTogetherPolicy(policyStorage,
            new PolicyEntities(group, Collections.emptySet())));

        // check consumers
        assertThat(topologyGraph.getEntity(5L).get(),
                not(policyMatcher.hasConsumerSegment(POLICY_ST_ID, EntityType.STORAGE)));
        assertThat(topologyGraph.getEntity(6L).get(),
                policyMatcher.hasConsumerSegment(POLICY_ST_ID, EntityType.STORAGE));
        assertThat(topologyGraph.getEntity(7L).get(),
                policyMatcher.hasConsumerSegment(POLICY_ST_ID, EntityType.STORAGE));
        assertThat(topologyGraph.getEntity(8L).get(),
                not(policyMatcher.hasConsumerSegment(POLICY_ST_ID, EntityType.STORAGE)));

        // check providers
        // the only one to have the segmentation commodity should be storage3
        // because it has already the most number of vms (in the policy) running on it
        assertThat(topologyGraph.getEntity(1L).get(),
                not(policyMatcher.hasProviderSegment(POLICY_ST_ID)));
        assertThat(topologyGraph.getEntity(2L).get(),
                not(policyMatcher.hasProviderSegment(POLICY_ST_ID)));
        assertThat(topologyGraph.getEntity(3L).get(),
                policyMatcher.hasProviderSegment(POLICY_ST_ID));
        assertThat(topologyGraph.getEntity(4L).get(),
                not(policyMatcher.hasProviderSegment(POLICY_ST_ID)));
    }

    private void applyPolicy(@Nonnull final MustRunTogetherPolicy policy) {
        MustRunTogetherPolicyApplication application =
            new MustRunTogetherPolicyApplication(groupResolver, topologyGraph, new TopologyInvertedIndexFactory());
        application.apply(Collections.singletonList(policy));
    }

}
