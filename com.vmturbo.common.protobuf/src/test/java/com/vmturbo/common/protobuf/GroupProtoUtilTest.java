package com.vmturbo.common.protobuf;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import junit.framework.TestCase;

import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.AtMostNPolicy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.MergePolicy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.MergePolicy.MergeType;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class GroupProtoUtilTest {

    private static final String REGEX = ".*1";

    private static final String MATCHING_NAME = "test 1";

    private static final String NOT_MATCHING_NAME = "test 2";

    @Test
    public void testNameMatches() {
        TestCase.assertTrue(GroupProtoUtil.nameFilterMatches(MATCHING_NAME,
                StringFilter.newBuilder().setStringPropertyRegex(REGEX).build()));
    }

    @Test
    public void testNameNotMatches() {
        assertFalse(GroupProtoUtil.nameFilterMatches(NOT_MATCHING_NAME,
                StringFilter.newBuilder().setStringPropertyRegex(REGEX).build()));
    }

    @Test
    public void testNegateMatch() {
        assertFalse(GroupProtoUtil.nameFilterMatches(MATCHING_NAME,
                StringFilter.newBuilder()
                    .setStringPropertyRegex(REGEX)
                    .setPositiveMatch(false)
                    .build()));
    }

    @Test
    public void testNegateNoMatch() {
        assertTrue(GroupProtoUtil.nameFilterMatches(NOT_MATCHING_NAME,
                StringFilter.newBuilder()
                        .setStringPropertyRegex(REGEX)
                        .setPositiveMatch(false)
                        .build()));
    }

    @Test
    public void testGetGroupIdsFromMergePolicy() {
        List<Long> expected =
                Arrays.asList(1L, 2L,
                        3L, 4L);
        Policy policy = Policy.newBuilder()
                .setId(7L)
                .setPolicyInfo(PolicyInfo.newBuilder()
                    .setEnabled(false)
                    .setMerge(MergePolicy.newBuilder().setMergeType(MergeType.DATACENTER)
                        .addAllMergeGroupIds(expected)
                        .build())
                    .setName("policy"))
                .build();

        Set<Long> result = GroupProtoUtil.getPolicyGroupIds(policy);
        Assert.assertTrue(result.containsAll(expected));
        Assert.assertTrue(expected.containsAll(result));
    }

    @Test
    public void testGetGroupIdsFromNonMergePolicy() {

        Policy policy = Policy.newBuilder()
                .setId(1L)
                .setPolicyInfo(PolicyInfo.newBuilder()
                    .setEnabled(false)
                    .setName("policy")
                    .setAtMostN(AtMostNPolicy.newBuilder()
                            .setCapacity(35)
                            .setConsumerGroupId(7L)
                            .setProviderGroupId(8L)
                            .build()))
                .build();

        final List<Long> expected = Arrays.asList(7L, 8L);
        Set<Long> result = GroupProtoUtil.getPolicyGroupIds(policy);

        Assert.assertTrue(result.containsAll(expected));
        Assert.assertTrue(expected.containsAll(result));
    }

    @Test
    public void testGroupDTODiscoveredId() {
        final CommonDTO.GroupDTO group = CommonDTO.GroupDTO.newBuilder()
            .setGroupName("foo")
            .setEntityType(EntityType.PHYSICAL_MACHINE)
            .build();

        long targetId = 111L;
        Assert.assertEquals("foo-PHYSICAL_MACHINE" + "-" + String.valueOf(targetId),
                GroupProtoUtil.discoveredIdFromName(group, targetId));
    }

    @Test
    public void testGroupInfoDiscoveredId() {
        final GroupInfo group = GroupInfo.newBuilder()
            .setName("foo")
            .setEntityType(EntityType.STORAGE_VALUE)
            .build();

        long targetId = 111L;
        Assert.assertEquals("foo-STORAGE" + "-" + String.valueOf(targetId),
                GroupProtoUtil.discoveredIdFromName(group, targetId));
    }

    @Test
    public void testClusterInfoDiscoveredId() {
        final ClusterInfo group = ClusterInfo.newBuilder()
            .setName("foo")
            .setClusterType(Type.COMPUTE)
            .build();

        long targetId = 111L;
        Assert.assertEquals("foo-PHYSICAL_MACHINE" + "-" + String.valueOf(targetId),
                GroupProtoUtil.discoveredIdFromName(group, targetId));
    }
}
