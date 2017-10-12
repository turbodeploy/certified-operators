package com.vmturbo.common.protobuf;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;

import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import junit.framework.TestCase;

import com.vmturbo.common.protobuf.GroupDTOUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.NameFilter;
import com.vmturbo.common.protobuf.group.GroupTestUtil;
import com.vmturbo.common.protobuf.group.PolicyDTO.MergeType;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy.AtMostNPolicy;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy.MergePolicy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyGroupingID;

public class GroupDTOUtilTest {

    private static final String REGEX = ".*1";

    private static final String MATCHING_NAME = "test 1";

    private static final String NOT_MATCHING_NAME = "test 2";

    @Test
    public void testNameMatches() {
        TestCase.assertTrue(GroupDTOUtil.nameFilterMatches(MATCHING_NAME,
                NameFilter.newBuilder().setNameRegex(REGEX).build()));
    }

    @Test
    public void testNameNotMatches() {
        assertFalse(GroupDTOUtil.nameFilterMatches(NOT_MATCHING_NAME,
                NameFilter.newBuilder().setNameRegex(REGEX).build()));
    }

    @Test
    public void testNegateMatch() {
        assertFalse(GroupDTOUtil.nameFilterMatches(MATCHING_NAME,
                NameFilter.newBuilder()
                    .setNameRegex(REGEX)
                    .setNegateMatch(true)
                    .build()));
    }

    @Test
    public void testNegateNoMatch() {
        assertTrue(GroupDTOUtil.nameFilterMatches(NOT_MATCHING_NAME,
                NameFilter.newBuilder()
                        .setNameRegex(REGEX)
                        .setNegateMatch(true)
                        .build()));
    }

    @Test
    public void testGetGroupIdsFromMergePolicy() {
        Policy policy = Policy.newBuilder()
                .setMerge(
                        MergePolicy.newBuilder().setMergeType(MergeType.DATACENTER)
                                .addMergeGroupIds(GroupTestUtil.groupId1)
                                .addMergeGroupIds(GroupTestUtil.groupId2)
                                .addMergeGroupIds(GroupTestUtil.clusterId2)
                                .addMergeGroupIds(GroupTestUtil.clusterId1).build())
                .setName("policy")
                .setId(GroupTestUtil.id1)
                .setEnabled(false)
                .setCommodityType("commodityType")
                .build();
        List<PolicyGroupingID> expected =
                Arrays.asList(GroupTestUtil.groupId1, GroupTestUtil.groupId2,
                        GroupTestUtil.clusterId2, GroupTestUtil.clusterId1);

        List<PolicyGroupingID> result = GroupDTOUtil.retrieveIdsFromPolicy(policy);
        Assert.assertTrue(result.containsAll(expected));
        Assert.assertTrue(expected.containsAll(result));
    }

    @Test
    public void testGetGroupIdsFromNonMergePolicy() {

        Policy policy = Policy.newBuilder().setEnabled(false)
                .setCommodityType("commodityType")
                .setId(GroupTestUtil.id1)
                .setName("policy")
                .setAtMostN(AtMostNPolicy.newBuilder()
                        .setCapacity(35)
                        .setConsumerGroupId(GroupTestUtil.clusterId2)
                        .setProviderGroupId(GroupTestUtil.groupId1)
                        .build())
                .build();

        List<PolicyGroupingID> expected =
                Arrays.asList(GroupTestUtil.clusterId2, GroupTestUtil.groupId1);
        List<PolicyGroupingID> result = GroupDTOUtil.retrieveIdsFromPolicy(policy);

        Assert.assertTrue(result.containsAll(expected));
        Assert.assertTrue(expected.containsAll(result));

    }
}
