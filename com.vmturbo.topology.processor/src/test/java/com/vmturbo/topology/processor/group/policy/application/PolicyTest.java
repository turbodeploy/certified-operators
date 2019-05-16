package com.vmturbo.topology.processor.group.policy.application;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
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

        MutableBoolean applied = new MutableBoolean(false);
        PlacementPolicyApplication application = new PlacementPolicyApplication(groupResolver, topologyGraph) {
            @Override
            protected Map<PlacementPolicy, PolicyApplicationException> applyInternal(@Nonnull final List<PlacementPolicy> policies) {
                applied.setTrue();
                return Collections.emptyMap();
            }
        };

        application.apply(Collections.singletonList(new PlacementPolicy(policyDefinition) {}));

        assertTrue(applied.booleanValue());
    }

    @Test
    public void testDoesNotApplyDisabled() throws Exception {
        final PolicyDTO.Policy policyDefinition = PolicyDTO.Policy.newBuilder()
            .setId(1234)
            .setPolicyInfo(PolicyInfo.newBuilder()
                .setEnabled(false))
            .build();

        PlacementPolicyApplication application = new PlacementPolicyApplication(groupResolver, topologyGraph) {
            @Override
            protected Map<PlacementPolicy, PolicyApplicationException> applyInternal(@Nonnull final List<PlacementPolicy> policies) {
                Assert.assertTrue("Shouldn't apply any disabled policies.", policies.isEmpty());
                return Collections.emptyMap();
            }
        };

        application.apply(Collections.singletonList(new PlacementPolicy(policyDefinition) {}));
    }
}