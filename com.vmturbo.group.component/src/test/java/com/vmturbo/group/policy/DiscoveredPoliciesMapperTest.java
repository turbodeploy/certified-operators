package com.vmturbo.group.policy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredPolicyInfo;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.BindToComplementaryGroupPolicy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.BindToGroupPolicy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.MustNotRunTogetherPolicy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.MustRunTogetherPolicy;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.ConstraintType;

/**
 * Test the mapping of discovered policies to policies.
 */
public class DiscoveredPoliciesMapperTest {

    private static final String BUYERS_GROUP_NAME = "B-group";
    private static final String SELLERS_GROUP_NAME = "S-group";
    private static final long BUYERS_GROUP_OID = 1111L;
    private static final long SELLERS_GROUP_OID = 2222L;

    private static final ImmutableMap<String, Long> groupOids = ImmutableMap.of(
        BUYERS_GROUP_NAME, BUYERS_GROUP_OID,
        SELLERS_GROUP_NAME, SELLERS_GROUP_OID);

    private DiscoveredPoliciesMapper mapper;

    @Before
    public void setup() {
        mapper = new DiscoveredPoliciesMapper(groupOids);
    }

    @Test
    public void testVmPmAffinity() {
        DiscoveredPolicyInfo info = DiscoveredPolicyInfo.newBuilder()
                        .setPolicyName("VM-PM-AFFINITY")
                        .setBuyersGroupStringId(BUYERS_GROUP_NAME)
                        .setSellersGroupStringId(SELLERS_GROUP_NAME)
                        .setConstraintType(ConstraintType.BUYER_SELLER_AFFINITY_VALUE)
                        .build();
        PolicyInfo policy = mapper.inputPolicy(info).get();
        assertTrue(policy.hasBindToGroup());
        assertEquals(info.getPolicyName(), policy.getName());
        BindToGroupPolicy bind = policy.getBindToGroup();
        assertEquals(BUYERS_GROUP_OID, bind.getConsumerGroupId());
        assertEquals(SELLERS_GROUP_OID, bind.getProviderGroupId());
    }

    @Test
    public void testVmPmAntiAffinity() {
        DiscoveredPolicyInfo info = DiscoveredPolicyInfo.newBuilder()
                        .setPolicyName("VM-PM-ANTI-AFFINITY")
                        .setBuyersGroupStringId(BUYERS_GROUP_NAME)
                        .setSellersGroupStringId(SELLERS_GROUP_NAME)
                        .setConstraintType(ConstraintType.BUYER_SELLER_ANTI_AFFINITY_VALUE)
                        .build();
        PolicyInfo policy = mapper.inputPolicy(info).get();
        assertTrue(policy.hasBindToComplementaryGroup());
        assertEquals(info.getPolicyName(), policy.getName());
        BindToComplementaryGroupPolicy bind = policy.getBindToComplementaryGroup();
        assertEquals(BUYERS_GROUP_OID, bind.getConsumerGroupId());
        assertEquals(SELLERS_GROUP_OID, bind.getProviderGroupId());
    }

    @Test
    public void testVmVmAffinity() {
        DiscoveredPolicyInfo info = DiscoveredPolicyInfo.newBuilder()
                        .setPolicyName("VM-VM-ANTI-AFFINITY")
                        .setBuyersGroupStringId(BUYERS_GROUP_NAME)
                        .setConstraintType(ConstraintType.BUYER_BUYER_AFFINITY_VALUE)
                        .build();
        PolicyInfo policy = mapper.inputPolicy(info).get();
        assertTrue(policy.hasMustRunTogether());
        assertEquals(info.getPolicyName(), policy.getName());
        MustRunTogetherPolicy together = policy.getMustRunTogether();
        assertEquals(BUYERS_GROUP_OID, together.getGroupId());
        assertEquals(EntityType.PHYSICAL_MACHINE_VALUE, together.getProviderEntityType());
    }

    @Test
    public void testVmVmAntiAffinity() {
        DiscoveredPolicyInfo info = DiscoveredPolicyInfo.newBuilder()
                        .setPolicyName("VM-VM-ANTI-AFFINITY")
                        .setBuyersGroupStringId(BUYERS_GROUP_NAME)
                        .setConstraintType(ConstraintType.BUYER_BUYER_ANTI_AFFINITY_VALUE)
                        .build();
        PolicyInfo policy = mapper.inputPolicy(info).get();
        assertTrue(policy.hasMustNotRunTogether());
        assertEquals(info.getPolicyName(), policy.getName());
        MustNotRunTogetherPolicy separate = policy.getMustNotRunTogether();
        assertEquals(BUYERS_GROUP_OID, separate.getGroupId());
        assertEquals(EntityType.PHYSICAL_MACHINE_VALUE, separate.getProviderEntityType());
    }

    @Test
    public void testUnhandledType() {
        DiscoveredPolicyInfo info = DiscoveredPolicyInfo.newBuilder()
                        .setPolicyName("MERGE")
                        .setBuyersGroupStringId(BUYERS_GROUP_NAME)
                        .setSellersGroupStringId(SELLERS_GROUP_NAME)
                        .setConstraintType(ConstraintType.MERGE_VALUE)
                        .build();
        assertTrue(!mapper.inputPolicy(info).isPresent());
    }

    @Test
    public void testSameGroup() {
        DiscoveredPolicyInfo info = DiscoveredPolicyInfo.newBuilder()
                        .setPolicyName("SAME-GROUP")
                        .setBuyersGroupStringId(BUYERS_GROUP_NAME)
                        .setSellersGroupStringId(BUYERS_GROUP_NAME)
                        .setConstraintType(ConstraintType.BUYER_BUYER_ANTI_AFFINITY_VALUE)
                        .build();
        assertTrue(!mapper.inputPolicy(info).isPresent());
    }
}
