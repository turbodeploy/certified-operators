package com.vmturbo.topology.processor.group.policy;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.vmturbo.common.protobuf.group.GroupDTO.SearchParametersCollection;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyGrouping;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyGroupingID;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class PolicyFactoryTest {

    private static final long POLICY_ID = 9999L;

    private final PolicyDTO.PolicyGrouping groupA = PolicyGroupingHelper.policyGrouping(
        SearchParametersCollection.getDefaultInstance(), EntityType.VIRTUAL_MACHINE_VALUE, 1234L);

    private final PolicyDTO.PolicyGrouping groupB = PolicyGroupingHelper.policyGrouping(
        SearchParametersCollection.getDefaultInstance(), EntityType.PHYSICAL_MACHINE_VALUE, 5678L);

    private final PolicyDTO.PolicyGroupingID groupIdA = PolicyGroupingHelper.policyGroupingID(1234L);

    private final PolicyDTO.PolicyGroupingID groupIdB = PolicyGroupingHelper.policyGroupingID(5678L);

    private final Map<PolicyGroupingID, PolicyDTO.PolicyGrouping> groupingMap =
            new HashMap<PolicyGroupingID, PolicyGrouping>(){{
                put(groupIdA, groupA);
                put(groupIdB, groupB);
            }};

    private final PolicyFactory policyFactory = new PolicyFactory();

    @Test
    public void testBindToGroupPolicy() {
        final PolicyDTO.Policy.BindToGroupPolicy bindToGroup = PolicyDTO.Policy.BindToGroupPolicy.newBuilder()
            .setConsumerGroupId(groupIdA)
            .setProviderGroupId(groupIdB)
            .build();

        final PolicyDTO.Policy policy = PolicyDTO.Policy.newBuilder()
            .setId(POLICY_ID)
            .setBindToGroup(bindToGroup)
            .build();

        assertThat(policyFactory.newPolicy(policy, groupingMap), instanceOf(BindToGroupPolicy.class));
    }

    @Test
    public void testBindToComplementaryGroupPolicy() {
        final PolicyDTO.Policy.BindToComplementaryGroupPolicy bindToComplementaryGroup =
            PolicyDTO.Policy.BindToComplementaryGroupPolicy.newBuilder()
                .setConsumerGroupId(groupIdA)
                .setProviderGroupId(groupIdB)
                .build();

        final PolicyDTO.Policy policy = PolicyDTO.Policy.newBuilder()
            .setId(POLICY_ID)
            .setBindToComplementaryGroup(bindToComplementaryGroup)
            .build();

        assertThat(policyFactory.newPolicy(policy, groupingMap),
                instanceOf(BindToComplementaryGroupPolicy.class));
    }

    @Test
    public void testAtMostNPolicy() {
        final PolicyDTO.Policy.AtMostNPolicy atMostNPolicy = PolicyDTO.Policy.AtMostNPolicy.newBuilder()
            .setConsumerGroupId(groupIdA)
            .setProviderGroupId(groupIdB)
            .setCapacity(3.0f)
            .build();

        final PolicyDTO.Policy policy = PolicyDTO.Policy.newBuilder()
            .setId(POLICY_ID)
            .setAtMostN(atMostNPolicy)
            .build();

        assertThat(policyFactory.newPolicy(policy, groupingMap), instanceOf(AtMostNPolicy.class));
    }

    @Test
    public void testAtMostNBoundPolicy() {
        final PolicyDTO.Policy.AtMostNBoundPolicy atMostNBoundPolicy =
                PolicyDTO.Policy.AtMostNBoundPolicy.newBuilder()
                        .setConsumerGroupId(groupIdA)
                        .setProviderGroupId(groupIdB)
                        .setCapacity(3.0f)
                        .build();

        final PolicyDTO.Policy policy = PolicyDTO.Policy.newBuilder()
            .setId(POLICY_ID)
            .setAtMostNBound(atMostNBoundPolicy)
            .build();

        assertThat(policyFactory.newPolicy(policy, groupingMap),
                instanceOf(AtMostNBoundPolicy.class));
    }

    @Test
    public void testMustRunTogetherPolicy() {
        final PolicyDTO.Policy.MustRunTogetherPolicy mustRunTogetherPolicy =
            PolicyDTO.Policy.MustRunTogetherPolicy.newBuilder()
                .setConsumerGroupId(groupIdA)
                .setProviderGroupId(groupIdB)
                .build();

        final PolicyDTO.Policy policy = PolicyDTO.Policy.newBuilder()
            .setId(POLICY_ID)
            .setMustRunTogether(mustRunTogetherPolicy)
            .build();

        assertThat(policyFactory.newPolicy(policy, groupingMap),
                instanceOf(MustRunTogetherPolicy.class));
    }
}