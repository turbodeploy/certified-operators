package com.vmturbo.topology.processor.group.policy.application;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.SearchParametersCollection;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.group.policy.PolicyGroupingHelper;

public class PolicyFactoryTest {

    private static final long POLICY_ID = 9999L;

    private final Grouping groupA = PolicyGroupingHelper.policyGrouping(
        SearchParametersCollection.getDefaultInstance(), EntityType.VIRTUAL_MACHINE_VALUE, 1234L);

    private final Grouping groupB = PolicyGroupingHelper.policyGrouping(
        SearchParametersCollection.getDefaultInstance(), EntityType.PHYSICAL_MACHINE_VALUE, 5678L);

    private final long groupIdA = 1234L;

    private final long groupIdB = 5678L;

    private final Map<Long, Grouping> groupingMap =
            new HashMap<Long, Grouping>() {{
                put(groupIdA, groupA);
                put(groupIdB, groupB);
            }};

    private final PolicyFactory policyFactory = new PolicyFactory();

    @Test
    public void testBindToGroupPolicy() {
        final PolicyInfo.BindToGroupPolicy bindToGroup = PolicyInfo.BindToGroupPolicy.newBuilder()
            .setConsumerGroupId(groupIdA)
            .setProviderGroupId(groupIdB)
            .build();

        final PolicyDTO.Policy policy = PolicyDTO.Policy.newBuilder()
            .setId(POLICY_ID)
            .setPolicyInfo(PolicyInfo.newBuilder()
                .setBindToGroup(bindToGroup))
            .build();

        assertThat(policyFactory.newPolicy(policy, groupingMap, Collections.emptySet(),
                Collections.emptySet()), instanceOf(BindToGroupPolicy.class));
    }

    @Test
    public void testBindToComplementaryGroupPolicy() {
        final PolicyInfo.BindToComplementaryGroupPolicy bindToComplementaryGroup =
            PolicyInfo.BindToComplementaryGroupPolicy.newBuilder()
                .setConsumerGroupId(groupIdA)
                .setProviderGroupId(groupIdB)
                .build();

        final PolicyDTO.Policy policy = PolicyDTO.Policy.newBuilder()
            .setId(POLICY_ID)
            .setPolicyInfo(PolicyInfo.newBuilder()
                .setBindToComplementaryGroup(bindToComplementaryGroup))
            .build();

        assertThat(policyFactory.newPolicy(policy, groupingMap, Collections.emptySet(),
                Collections.emptySet()), instanceOf(BindToComplementaryGroupPolicy.class));
    }

    @Test
    public void testAtMostNPolicy() {
        final PolicyInfo.AtMostNPolicy atMostNPolicy = PolicyInfo.AtMostNPolicy.newBuilder()
            .setConsumerGroupId(groupIdA)
            .setProviderGroupId(groupIdB)
            .setCapacity(3.0f)
            .build();

        final PolicyDTO.Policy policy = PolicyDTO.Policy.newBuilder()
            .setId(POLICY_ID)
            .setPolicyInfo(PolicyInfo.newBuilder()
                .setAtMostN(atMostNPolicy))
            .build();

        assertThat(policyFactory.newPolicy(policy, groupingMap, Collections.emptySet(),
                Collections.emptySet()), instanceOf(AtMostNPolicy.class));
    }

    @Test
    public void testAtMostNBoundPolicy() {
        final PolicyInfo.AtMostNBoundPolicy atMostNBoundPolicy =
                PolicyInfo.AtMostNBoundPolicy.newBuilder()
                        .setConsumerGroupId(groupIdA)
                        .setProviderGroupId(groupIdB)
                        .setCapacity(3.0f)
                        .build();

        final PolicyDTO.Policy policy = PolicyDTO.Policy.newBuilder()
            .setId(POLICY_ID)
            .setPolicyInfo(PolicyInfo.newBuilder()
                .setAtMostNbound(atMostNBoundPolicy))
            .build();

        assertThat(policyFactory.newPolicy(policy, groupingMap, Collections.emptySet(),
                Collections.emptySet()), instanceOf(AtMostNBoundPolicy.class));
    }

    @Test
    public void testMustRunTogetherPolicy() {
        final PolicyInfo.MustRunTogetherPolicy mustRunTogetherPolicy =
            PolicyInfo.MustRunTogetherPolicy.newBuilder()
                .setGroupId(groupIdA)
                .build();

        final PolicyDTO.Policy policy = PolicyDTO.Policy.newBuilder()
            .setId(POLICY_ID)
            .setPolicyInfo(PolicyInfo.newBuilder()
                .setMustRunTogether(mustRunTogetherPolicy))
            .build();

        assertThat(policyFactory.newPolicy(policy, groupingMap, Collections.emptySet(),
                Collections.emptySet()), instanceOf(MustRunTogetherPolicy.class));
    }

    @Test
    public void testMustNotRunTogetherPolicy() {
        final PolicyInfo.MustNotRunTogetherPolicy mustNotRunTogetherPolicy =
                PolicyInfo.MustNotRunTogetherPolicy.newBuilder()
                        .setGroupId(groupIdA)
                        .build();

        final PolicyDTO.Policy policy = PolicyDTO.Policy.newBuilder()
                .setId(POLICY_ID)
                .setPolicyInfo(PolicyInfo.newBuilder()
                    .setMustNotRunTogether(mustNotRunTogetherPolicy))
                .build();

        assertThat(policyFactory.newPolicy(policy, groupingMap, Collections.emptySet(),
                Collections.emptySet()), instanceOf(MustNotRunTogetherPolicy.class));
    }
}
