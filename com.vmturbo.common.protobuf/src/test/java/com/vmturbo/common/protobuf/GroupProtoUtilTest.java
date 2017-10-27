package com.vmturbo.common.protobuf;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import junit.framework.TestCase;

import com.vmturbo.common.protobuf.group.GroupDTO.NameFilter;
import com.vmturbo.common.protobuf.group.PolicyDTO.MergeType;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy.AtMostNPolicy;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy.MergePolicy;

public class GroupProtoUtilTest {

    private static final String REGEX = ".*1";

    private static final String MATCHING_NAME = "test 1";

    private static final String NOT_MATCHING_NAME = "test 2";

    @Test
    public void testNameMatches() {
        TestCase.assertTrue(GroupProtoUtil.nameFilterMatches(MATCHING_NAME,
                NameFilter.newBuilder().setNameRegex(REGEX).build()));
    }

    @Test
    public void testNameNotMatches() {
        assertFalse(GroupProtoUtil.nameFilterMatches(NOT_MATCHING_NAME,
                NameFilter.newBuilder().setNameRegex(REGEX).build()));
    }

    @Test
    public void testNegateMatch() {
        assertFalse(GroupProtoUtil.nameFilterMatches(MATCHING_NAME,
                NameFilter.newBuilder()
                    .setNameRegex(REGEX)
                    .setNegateMatch(true)
                    .build()));
    }

    @Test
    public void testNegateNoMatch() {
        assertTrue(GroupProtoUtil.nameFilterMatches(NOT_MATCHING_NAME,
                NameFilter.newBuilder()
                        .setNameRegex(REGEX)
                        .setNegateMatch(true)
                        .build()));
    }

    @Test
    public void testGetGroupIdsFromMergePolicy() {
        List<Long> expected =
                Arrays.asList(1L, 2L,
                        3L, 4L);
        Policy policy = Policy.newBuilder()
                .setMerge(
                        MergePolicy.newBuilder().setMergeType(MergeType.DATACENTER)
                                .addAllMergeGroupIds(expected)
                                .build())
                .setName("policy")
                .setId(7L)
                .setEnabled(false)
                .setCommodityType("commodityType")
                .build();

        Set<Long> result = GroupProtoUtil.getPolicyGroupIds(policy);
        Assert.assertTrue(result.containsAll(expected));
        Assert.assertTrue(expected.containsAll(result));
    }

    @Test
    public void testGetGroupIdsFromNonMergePolicy() {

        Policy policy = Policy.newBuilder().setEnabled(false)
                .setCommodityType("commodityType")
                .setId(1L)
                .setName("policy")
                .setAtMostN(AtMostNPolicy.newBuilder()
                        .setCapacity(35)
                        .setConsumerGroupId(7L)
                        .setProviderGroupId(8L)
                        .build())
                .build();

        final List<Long> expected = Arrays.asList(7L, 8L);
        Set<Long> result = GroupProtoUtil.getPolicyGroupIds(policy);

        Assert.assertTrue(result.containsAll(expected));
        Assert.assertTrue(expected.containsAll(result));

    }
}
