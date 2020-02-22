package com.vmturbo.group.policy;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import org.jooq.DSLContext;
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
            .setEnabled(true)
            .setBindToGroup(BindToGroupPolicy.newBuilder()
                .setConsumerGroupId(CONSUMER_GROUP_ID)
                .setProviderGroupId(PRODUCER_GROUP_ID)))
        .build();

    private GroupDAO groupDAO = mock(GroupDAO.class);

    private PolicyValidator validator = new PolicyValidator(groupDAO);

    private DSLContext dslContext = mock(DSLContext.class);

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
        validator.validatePolicy(dslContext, POLICY);
    }


}