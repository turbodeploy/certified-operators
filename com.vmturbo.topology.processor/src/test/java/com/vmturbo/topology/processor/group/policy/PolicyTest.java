package com.vmturbo.topology.processor.group.policy;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import javax.annotation.Nonnull;

import org.junit.Test;

import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.topology.processor.group.GroupResolutionException;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.topology.TopologyGraph;

public class PolicyTest {
    final GroupResolver groupResolver = mock(GroupResolver.class);
    final TopologyGraph topologyGraph = mock(TopologyGraph.class);

    @Test
    public void testAppliesEnabled() throws Exception {
        final PolicyDTO.Policy policyDefinition = PolicyDTO.Policy.newBuilder()
            .setId(1234)
            .setPolicyInfo(PolicyInfo.newBuilder()
                .setEnabled(true))
            .build();

        final PlacementPolicy placementPolicy = spy(new PlacementPolicy(policyDefinition) {
            @Override
            protected void applyInternal(@Nonnull GroupResolver groupResolver, @Nonnull TopologyGraph topologyGraph)
                throws GroupResolutionException, PolicyApplicationException {
                // Do nothing.
            }
        });

        placementPolicy.apply(groupResolver, topologyGraph);
        verify(placementPolicy).applyInternal(eq(groupResolver), eq(topologyGraph));
    }

    @Test
    public void testDoesNotApplyDisabled() throws Exception {
        final PolicyDTO.Policy policyDefinition = PolicyDTO.Policy.newBuilder()
            .setId(1234)
            .setPolicyInfo(PolicyInfo.newBuilder()
                .setEnabled(false))
            .build();

        final PlacementPolicy placementPolicy = spy(new PlacementPolicy(policyDefinition) {
            @Override
            protected void applyInternal(@Nonnull GroupResolver groupResolver, @Nonnull TopologyGraph topologyGraph)
                throws GroupResolutionException, PolicyApplicationException {
                // Do nothing.
            }
        });

        placementPolicy.apply(groupResolver, topologyGraph);
        verify(placementPolicy, never()).applyInternal(eq(groupResolver), eq(topologyGraph));
    }
}