package com.vmturbo.topology.processor.group.policy.application;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.PolicyDetailCase;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.policy.application.PolicyApplicator.Results;

public class PolicyApplicatorTest {

    private GroupResolver groupResolver = mock(GroupResolver.class);

    private TopologyGraph<TopologyEntity> topologyGraph = mock(TopologyGraph.class);

    private PolicyFactory policyFactory = mock(PolicyFactory.class);

    private final PolicyInfo.MergePolicy mergePolicy =
        PolicyInfo.MergePolicy.newBuilder()
        .addMergeGroupIds(2)
        .setMergeType(PolicyInfo.MergePolicy.MergeType.DATACENTER)
        .build();

    @Test
    public void testRunInOrder() throws PolicyApplicationException {
        final PolicyApplicator policyApplicator = new PolicyApplicator(policyFactory);

        final MergePolicyApplication mergeApplication = mock(MergePolicyApplication.class);
        when(mergeApplication.apply(any())).thenReturn(ImmutablePolicyApplicationResults.builder()
            .build());
        final MustNotRunTogetherPolicyApplication mustNotRunTogetherApplication =
                mock(MustNotRunTogetherPolicyApplication.class);
        when(mustNotRunTogetherApplication.apply(any())).thenReturn(ImmutablePolicyApplicationResults.builder()
            .build());
        final BindToGroupPolicyApplication bindToGroupApplication =
            mock(BindToGroupPolicyApplication.class);
        when(bindToGroupApplication.apply(any())).thenReturn(ImmutablePolicyApplicationResults.builder()
            .build());

        when(policyFactory.newPolicyApplication(PolicyDetailCase.MERGE, groupResolver, topologyGraph))
            .thenReturn(mergeApplication);
        when(policyFactory.newPolicyApplication(PolicyDetailCase.MUST_NOT_RUN_TOGETHER, groupResolver, topologyGraph))
            .thenReturn(mustNotRunTogetherApplication);
        when(policyFactory.newPolicyApplication(PolicyDetailCase.BIND_TO_GROUP, groupResolver, topologyGraph))
            .thenReturn(bindToGroupApplication);

        final MergePolicy mergePolicy = mergePolicy();
        final MustNotRunTogetherPolicy mustNotRunTogetherPolicy = mustNotRunTogetherPolicy();
        final BindToGroupPolicy bindToGroupPolicy = bindToGroupPolicy();

        final List<PlacementPolicy> policies =
            Arrays.asList(mustNotRunTogetherPolicy, mergePolicy, bindToGroupPolicy);

        final Results results = policyApplicator.applyPolicies(policies, groupResolver, topologyGraph);

        final InOrder order = Mockito.inOrder(mergeApplication, mustNotRunTogetherApplication, bindToGroupApplication);
        order.verify(mergeApplication).apply(Collections.singletonList(mergePolicy));
        order.verify(mustNotRunTogetherApplication).apply(Collections.singletonList(mustNotRunTogetherPolicy));
        order.verify(bindToGroupApplication).apply(Collections.singletonList(bindToGroupPolicy));

        assertThat(results.getAppliedCounts().get(PolicyDetailCase.MERGE), is(1));
        assertThat(results.getAppliedCounts().get(PolicyDetailCase.MUST_NOT_RUN_TOGETHER), is(1));
        assertThat(results.getAppliedCounts().get(PolicyDetailCase.BIND_TO_GROUP), is(1));
    }

    @Test
    public void testCombineErrors() {
        final PolicyApplicator policyApplicator = new PolicyApplicator(policyFactory);

        final MergePolicy mergePolicy = mergePolicy();
        final PolicyApplicationException mergeException = new PolicyApplicationException("boo");
        final MustNotRunTogetherPolicy mustNotRunTogetherPolicy = mustNotRunTogetherPolicy();
        final PolicyApplicationException mustNotRunTogetherException = new PolicyApplicationException("foo");

        final MergePolicyApplication mergeApplication = mock(MergePolicyApplication.class);
        when(mergeApplication.apply(any())).thenReturn(ImmutablePolicyApplicationResults.builder()
            .putErrors(mergePolicy, mergeException)
            .build());
        final MustNotRunTogetherPolicyApplication mustNotRunTogetherApplication =
            mock(MustNotRunTogetherPolicyApplication.class);
        when(mustNotRunTogetherApplication.apply(any())).thenReturn(ImmutablePolicyApplicationResults.builder()
            .putErrors(mustNotRunTogetherPolicy, mustNotRunTogetherException)
            .build());

        when(policyFactory.newPolicyApplication(PolicyDetailCase.MERGE, groupResolver, topologyGraph))
            .thenReturn(mergeApplication);
        when(policyFactory.newPolicyApplication(PolicyDetailCase.MUST_NOT_RUN_TOGETHER, groupResolver, topologyGraph))
            .thenReturn(mustNotRunTogetherApplication);

        final List<PlacementPolicy> policies =
            Arrays.asList(mustNotRunTogetherPolicy, mergePolicy);

        final Results results = policyApplicator.applyPolicies(policies, groupResolver, topologyGraph);
        assertThat(results.getErrors().get(mergePolicy), is(mergeException));
        assertThat(results.getErrors().get(mustNotRunTogetherPolicy), is(mustNotRunTogetherException));
    }

    @Test
    public void testCombineCommodityCounts() {
        final PolicyApplicator policyApplicator = new PolicyApplicator(policyFactory);

        final MergePolicy mergePolicy = mergePolicy();
        final MustNotRunTogetherPolicy mustNotRunTogetherPolicy = mustNotRunTogetherPolicy();

        final MergePolicyApplication mergeApplication = mock(MergePolicyApplication.class);
        when(mergeApplication.apply(any())).thenReturn(ImmutablePolicyApplicationResults.builder()
            .putAddedCommodities(CommodityType.SEGMENTATION, 5)
            // Why not?
            .putAddedCommodities(CommodityType.VSTORAGE, 1)
            .build());
        final MustNotRunTogetherPolicyApplication mustNotRunTogetherApplication =
            mock(MustNotRunTogetherPolicyApplication.class);
        when(mustNotRunTogetherApplication.apply(any())).thenReturn(ImmutablePolicyApplicationResults.builder()
            .putAddedCommodities(CommodityType.SEGMENTATION, 5)
            .build());

        when(policyFactory.newPolicyApplication(PolicyDetailCase.MERGE, groupResolver, topologyGraph))
            .thenReturn(mergeApplication);
        when(policyFactory.newPolicyApplication(PolicyDetailCase.MUST_NOT_RUN_TOGETHER, groupResolver, topologyGraph))
            .thenReturn(mustNotRunTogetherApplication);

        final List<PlacementPolicy> policies =
            Arrays.asList(mustNotRunTogetherPolicy, mergePolicy);

        final Results results = policyApplicator.applyPolicies(policies, groupResolver, topologyGraph);
        assertThat(results.getAddedCommodityCounts().get(CommodityType.SEGMENTATION), is(10));
        assertThat(results.getAddedCommodityCounts().get(CommodityType.VSTORAGE), is(1));
    }

    private MergePolicy mergePolicy() {
        final MergePolicy mergePolicy = mock(MergePolicy.class);
        final Policy policyDef = Policy.newBuilder()
            .setId(7)
            .setPolicyInfo(PolicyInfo.newBuilder()
                .setMerge(PolicyInfo.MergePolicy.newBuilder()
                    .addMergeGroupIds(2)
                    .setMergeType(PolicyInfo.MergePolicy.MergeType.DATACENTER)))
            .build();
        when(mergePolicy.getPolicyDefinition()).thenReturn(policyDef);
        return mergePolicy;
    }

    private BindToGroupPolicy bindToGroupPolicy() {
        final BindToGroupPolicy bindToGroupPolicy = mock(BindToGroupPolicy.class);
        final Policy policyDef = Policy.newBuilder()
            .setId(7)
            .setPolicyInfo(PolicyInfo.newBuilder()
                .setBindToGroup(PolicyInfo.BindToGroupPolicy.newBuilder()
                    .setConsumerGroupId(1)
                    .setConsumerGroupId(2)))
            .build();
        when(bindToGroupPolicy.getPolicyDefinition()).thenReturn(policyDef);
        return bindToGroupPolicy;
    }

    private MustNotRunTogetherPolicy mustNotRunTogetherPolicy() {
        final MustNotRunTogetherPolicy mustNotRunTogetherPolicy = mock(MustNotRunTogetherPolicy.class);
        final Policy policyDef = Policy.newBuilder()
            .setId(7)
            .setPolicyInfo(PolicyInfo.newBuilder()
                .setMustNotRunTogether(PolicyInfo.MustNotRunTogetherPolicy.newBuilder()
                    .setGroupId(1)
                    .setProviderEntityType(EntityType.STORAGE_VALUE)))
            .build();
        when(mustNotRunTogetherPolicy.getPolicyDefinition()).thenReturn(policyDef);
        return mustNotRunTogetherPolicy;
    }

}