package com.vmturbo.group.policy;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.BindToGroupPolicy;
import com.vmturbo.group.group.GroupDAO;

/**
 * Unit tests for {@link PolicyStore}.
 */
public class PolicyValidatorTest {

    private static final long CONSUMER_GROUP_ID = 1L;

    private static final long PRODUCER_GROUP_ID = 2L;

    private static final Policy POLICY = Policy.newBuilder()
        .setId(7L)
        .setPolicyInfo(PolicyInfo.newBuilder()
            .setName("no watch tv")
            .setDisplayName("No watch tv")
            .setEnabled(true)
            .setBindToGroup(BindToGroupPolicy.newBuilder()
                .setConsumerGroupId(CONSUMER_GROUP_ID)
                .setProviderGroupId(PRODUCER_GROUP_ID)))
        .build();

    private GroupDAO groupDAO = mock(GroupDAO.class);

    private PolicyValidator validator = new PolicyValidator(groupDAO);

    private DSLContext dslContext = mock(DSLContext.class);

    /**
     * Sets up the test.
     */
    @Before
    public void setup() {
        Table<Long, MemberType, Boolean> retTable = HashBasedTable.create();
        // One entity type for the consumer group.
        retTable.put(CONSUMER_GROUP_ID, MemberType.newBuilder()
            .setEntity(10)
            .build(), true);

        // One entity types for the producer group.
        retTable.put(PRODUCER_GROUP_ID, MemberType.newBuilder()
            .setEntity(11)
            .build(), true);
        when(groupDAO.getExpectedMemberTypes(any(), any())).thenReturn(retTable);
    }

    /**
     * Test that a policy with a group with multiple entity types doesn't pass validation.
     *
     * @throws InvalidPolicyException To satisfy compiler.
     */
    @Test(expected = InvalidPolicyException.class)
    public void testMultiEntityType() throws InvalidPolicyException {
        Table<Long, MemberType, Boolean> retTable = HashBasedTable.create();
        // One entity type for the consumer group.
        retTable.put(CONSUMER_GROUP_ID, MemberType.newBuilder()
            .setEntity(10)
            .build(), true);

        // Two entity types for the producer group.
        retTable.put(PRODUCER_GROUP_ID, MemberType.newBuilder()
            .setEntity(10)
            .build(), true);
        retTable.put(PRODUCER_GROUP_ID, MemberType.newBuilder()
            .setEntity(11)
            .build(), true);
        when(groupDAO.getExpectedMemberTypes(any(), any())).thenReturn(retTable);
        validator.validatePolicy(dslContext, POLICY);
    }

    /**
     * Test that a policy with a non-existing group doesn't pass validation.
     *
     * @throws InvalidPolicyException To satisfy compiler.
     */
    @Test(expected = InvalidPolicyException.class)
    public void testGroupNotFound() throws InvalidPolicyException {
        when(groupDAO.getExpectedMemberTypes(any(), any())).thenReturn(HashBasedTable.create());
        validator.validatePolicy(dslContext, POLICY);
    }

    /**
     * Test that a valid policy doesn't throw exceptions.
     *
     * @throws InvalidPolicyException To satisfy compiler.
     */
    @Test
    public void testValidPolicy() throws InvalidPolicyException {
        validator.validatePolicy(dslContext, POLICY);
    }

    /**
     * Test the case that the policy info is missing.
     */
    @Test
    public void testPolicyWithNoPolicyInfo() {
        Policy policy = POLICY.toBuilder()
            .clearPolicyInfo()
            .build();
        try {
            validator.validatePolicy(dslContext, policy);
        } catch (InvalidPolicyException e) {
            assertThat(e.getMessage(), startsWith("Policy has 1 errors:"));
            assertThat(e.getMessage(), containsString("The policy should have information"));
            return;
        }
        fail("The validator should have thrown an exception");
    }

    /**
     * Test the case that name is missing from policy.
     */
    @Test
    public void testPolicyWithNoName() {
        Policy policy = POLICY.toBuilder()
            .setPolicyInfo(POLICY.getPolicyInfo().toBuilder()
                .clearName()
            )
            .build();
        try {
            validator.validatePolicy(dslContext, policy);
        } catch (InvalidPolicyException e) {
            assertThat(e.getMessage(), startsWith("Policy has 1 errors:"));
            assertThat(e.getMessage(), containsString("The policy should have a name"));
            return;
        }
        fail("The validator should have thrown an exception");
    }

    /**
     * Test the case that display name is missing from policy.
     */
    @Test
    public void testPolicyWithNoDisplayName() {
        Policy policy = POLICY.toBuilder()
            .setPolicyInfo(POLICY.getPolicyInfo().toBuilder()
                .clearDisplayName()
            )
            .build();
        try {
            validator.validatePolicy(dslContext, policy);
        } catch (InvalidPolicyException e) {
            assertThat(e.getMessage(), startsWith("Policy has 1 errors:"));
            assertThat(e.getMessage(), containsString("The policy should have a display name."));
            return;
        }
        fail("The validator should have thrown an exception");
    }

    /**
     * Test the case that policy does not have any type.
     */
    @Test
    public void testPolicyWithNoType() {
        Policy policy = POLICY.toBuilder()
            .setPolicyInfo(POLICY.getPolicyInfo().toBuilder()
                .clearPolicyDetail()
            )
            .build();
        try {
            validator.validatePolicy(dslContext, policy);
        } catch (InvalidPolicyException e) {
            assertThat(e.getMessage(), startsWith("Policy has 1 errors:"));
            assertThat(e.getMessage(), containsString("is not supported."));
            return;
        }
        fail("The validator should have thrown an exception");
    }

    /**
     * Test the case that with different types do not have proper fields set.
     */
    @Test
    public void testPolicyTypes() {
        PolicyInfo policyInfo = POLICY.getPolicyInfo();
        testPolicyType(policyInfo.toBuilder().setAtMostN(PolicyInfo.AtMostNPolicy.getDefaultInstance()), 3);
        testPolicyType(policyInfo.toBuilder().setAtMostNbound(PolicyInfo.AtMostNBoundPolicy.getDefaultInstance()),
            3);
        testPolicyType(policyInfo.toBuilder().setBindToComplementaryGroup(PolicyInfo.BindToComplementaryGroupPolicy.getDefaultInstance()),
            2);
        testPolicyType(policyInfo.toBuilder().setBindToGroup(PolicyInfo.BindToGroupPolicy.getDefaultInstance()), 2);
        testPolicyType(policyInfo.toBuilder().setBindToGroupAndLicense(PolicyInfo.BindToGroupAndLicencePolicy.getDefaultInstance()), 2);
        testPolicyType(policyInfo.toBuilder().setBindToGroupAndGeoRedundancy(PolicyInfo.BindToGroupAndGeoRedundancyPolicy.getDefaultInstance()), 2);
        testPolicyType(policyInfo.toBuilder().setMerge(PolicyInfo.MergePolicy.getDefaultInstance()), 1);
        testPolicyType(policyInfo.toBuilder().setMustRunTogether(PolicyInfo.MustRunTogetherPolicy.getDefaultInstance()), 2);
        testPolicyType(policyInfo.toBuilder().setMustNotRunTogether(PolicyInfo.MustNotRunTogetherPolicy.getDefaultInstance()), 2);
    }

    private void testPolicyType(PolicyInfo.Builder updatedInfo, int numberOfErrors) {
        Policy policy = POLICY.toBuilder()
            .setPolicyInfo(updatedInfo)
            .build();
        try {
            validator.validatePolicy(dslContext, policy);
        } catch (InvalidPolicyException e) {
            assertThat(e.getMessage(), startsWith("Policy has " + numberOfErrors + " errors:"));
            assertThat(e.getMessage(), containsString("should have the"));
            return;
        }
        fail("The validator should have thrown an exception");
    }

}