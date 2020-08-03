package com.vmturbo.common.protobuf;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import junit.framework.TestCase;

import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredGroupsPoliciesSettings.UploadedGroup;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.AtMostNPolicy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.MergePolicy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.MergePolicy.MergeType;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

public class GroupProtoUtilTest {

    private static final String REGEX = ".*1";

    private static final String MATCHING_NAME = "test 1";

    private static final String NOT_MATCHING_NAME = "test 2";

    private static final String REGEX_CASE_INSENSITIVE = ".*test.*";

    private static final String MATCHING_NAME_WITH_CAPITAL_LETTER = "Test 2";

    @Test
    public void testNameMatches() {
        TestCase.assertTrue(GroupProtoUtil.nameFilterMatches(MATCHING_NAME,
                StringFilter.newBuilder().setStringPropertyRegex(REGEX).build()));
    }

    // If the filter set case sensitive to true, marching should be case sensitive.
    @Test
    public void testNameMatchesWithCaseSensitiveTrue() {
        TestCase.assertFalse(GroupProtoUtil.nameFilterMatches(MATCHING_NAME_WITH_CAPITAL_LETTER,
            StringFilter.newBuilder()
                .setCaseSensitive(true)
                .setStringPropertyRegex(REGEX_CASE_INSENSITIVE)
                .build()));
    }

    // If the filter set case sensitive to false, marching should be case insensitive.
    @Test
    public void testNameMatchesWithCaseSensitiveFalse() {
        TestCase.assertTrue(GroupProtoUtil.nameFilterMatches(MATCHING_NAME_WITH_CAPITAL_LETTER,
            StringFilter.newBuilder()
                .setCaseSensitive(false)
                .setStringPropertyRegex(REGEX_CASE_INSENSITIVE)
                .build()));
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

    /**
     * Test that identifying key is created correctly for uploaded group.
     */
    @Test
    public void testCreateIdentifyingKeyForUploadedGroup() {
        UploadedGroup group = UploadedGroup.newBuilder()
                .setSourceIdentifier("foo")
                .setDefinition(GroupDefinition.newBuilder().setType(GroupType.REGULAR))
                .build();
        Assert.assertEquals(GroupType.REGULAR_VALUE + "-foo",
                GroupProtoUtil.createIdentifyingKey(group));
    }

    /**
     * Test that identifying key is created correctly for sdk group.
     */
    @Test
    public void testCreateIdentifyingKeyForSdkGroup() {
        GroupDTO group = GroupDTO.newBuilder()
                .setGroupName("foo")
                .setGroupType(GroupType.RESOURCE)
                .build();
        Assert.assertEquals(GroupType.RESOURCE_VALUE + "-foo",
                GroupProtoUtil.createIdentifyingKey(group));
    }

    /**
     * Test that when isGroupOfClusters is called with a dto that doesn't have a definition, an
     * IllegalArgumentException is being thrown.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testIsGroupOfClustersWithoutDefinition() {
        // GIVEN
        final Grouping grouping = Grouping.newBuilder().build();

        // WHEN
        GroupProtoUtil.isGroupOfClusters(grouping);

        // THEN
        //exception is being thrown
    }

    /**
     * Test that when isGroupOfClusters is called with a dto that contains a group of clusters, it
     * returns true.
     */
    @Test
    public void testIsGroupOfClustersForGroupOfClusters() {
        // GIVEN
        final Grouping grouping = Grouping.newBuilder()
                .setDefinition(GroupDefinition.newBuilder()
                        .setStaticGroupMembers(StaticMembers.newBuilder()
                                .addMembersByType(StaticMembersByType.newBuilder()
                                        .setType(MemberType.newBuilder()
                                                .setGroup(GroupType.COMPUTE_HOST_CLUSTER)
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();

        // THEN
        assertTrue(GroupProtoUtil.isGroupOfClusters(grouping));
    }

    /**
     * Test that when isGroupOfClusters is called with a dto that contains a cluster, it
     * returns false.
     */
    @Test
    public void testIsGroupOfClustersForCluster() {
        // GIVEN
        final Grouping grouping = Grouping.newBuilder()
                .setDefinition(GroupDefinition.newBuilder()
                        .setStaticGroupMembers(StaticMembers.newBuilder()
                                .addMembersByType(StaticMembersByType.newBuilder()
                                        .setType(MemberType.newBuilder()
                                                .setEntity(14)
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();

        // THEN
        assertFalse(GroupProtoUtil.isGroupOfClusters(grouping));
    }
}
