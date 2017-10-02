package com.vmturbo.group.persistent;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredPolicyInfo;
import com.vmturbo.common.protobuf.group.PolicyDTO.InputPolicy;
import com.vmturbo.common.protobuf.group.PolicyDTO.InputPolicy.AtMostNPolicy;
import com.vmturbo.common.protobuf.group.PolicyDTO.InputPolicy.BindToComplementaryGroupPolicy;
import com.vmturbo.common.protobuf.group.PolicyDTO.InputPolicy.BindToGroupPolicy;
import com.vmturbo.common.protobuf.group.PolicyDTO.InputPolicy.MustRunTogetherPolicy;
import com.vmturbo.group.policy.DiscoveredPoliciesMapper;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.ConstraintType;

/**
 * Test the mapping of discovered policies to policies.
 */
public class DiscoveredPoliciesMapperTest {

    String BUYERS_GROUP_NAME = "B-group";
    String SELLERS_GROUP_NAME = "S-group";
    long BUYERS_GROUP_OID = 1111L;
    long SELLERS_GROUP_OID = 2222L;

    ImmutableMap<String, Long> groupOids = ImmutableMap.of(
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
        InputPolicy policy = mapper.inputPolicy(info).get();
        assertTrue(policy.hasBindToGroup());
        assertEquals(info.getPolicyName(), policy.getName());
        BindToGroupPolicy bind = policy.getBindToGroup();
        assertEquals(BUYERS_GROUP_OID, bind.getConsumerGroup().getGroupId());
        assertEquals(SELLERS_GROUP_OID, bind.getProviderGroup().getGroupId());
    }

    @Test
    public void testVmPmAntiAffinity() {
        DiscoveredPolicyInfo info = DiscoveredPolicyInfo.newBuilder()
                        .setPolicyName("VM-PM-ANTI-AFFINITY")
                        .setBuyersGroupStringId(BUYERS_GROUP_NAME)
                        .setSellersGroupStringId(SELLERS_GROUP_NAME)
                        .setConstraintType(ConstraintType.BUYER_SELLER_ANTI_AFFINITY_VALUE)
                        .build();
        InputPolicy policy = mapper.inputPolicy(info).get();
        assertTrue(policy.hasBindToComplementaryGroup());
        assertEquals(info.getPolicyName(), policy.getName());
        BindToComplementaryGroupPolicy bind = policy.getBindToComplementaryGroup();
        assertEquals(BUYERS_GROUP_OID, bind.getConsumerGroup().getGroupId());
        assertEquals(SELLERS_GROUP_OID, bind.getProviderGroup().getGroupId());
    }

    @Test
    public void testVmVmAffinity() {
        DiscoveredPolicyInfo info = DiscoveredPolicyInfo.newBuilder()
                        .setPolicyName("VM-VM-ANTI-AFFINITY")
                        .setBuyersGroupStringId(BUYERS_GROUP_NAME)
                        .setSellersGroupStringId(SELLERS_GROUP_NAME)
                        .setConstraintType(ConstraintType.BUYER_BUYER_AFFINITY_VALUE)
                        .build();
        InputPolicy policy = mapper.inputPolicy(info).get();
        assertTrue(policy.hasMustRunTogether());
        assertEquals(info.getPolicyName(), policy.getName());
        MustRunTogetherPolicy together = policy.getMustRunTogether();
        assertEquals(BUYERS_GROUP_OID, together.getConsumerGroup().getGroupId());
        assertEquals(SELLERS_GROUP_OID, together.getProviderGroup().getGroupId());
    }

    @Test
    public void testVmVmAntiAffinity() {
        DiscoveredPolicyInfo info = DiscoveredPolicyInfo.newBuilder()
                        .setPolicyName("VM-VM-ANTI-AFFINITY")
                        .setBuyersGroupStringId(BUYERS_GROUP_NAME)
                        .setSellersGroupStringId(SELLERS_GROUP_NAME)
                        .setConstraintType(ConstraintType.BUYER_BUYER_ANTI_AFFINITY_VALUE)
                        .build();
        InputPolicy policy = mapper.inputPolicy(info).get();
        assertTrue(policy.hasAtMostN());
        assertEquals(info.getPolicyName(), policy.getName());
        AtMostNPolicy separate = policy.getAtMostN();
        assertEquals(BUYERS_GROUP_OID, separate.getConsumerGroup().getGroupId());
        assertEquals(SELLERS_GROUP_OID, separate.getProviderGroup().getGroupId());
        assertEquals(1.0, separate.getCapacity(), 0.001);
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
