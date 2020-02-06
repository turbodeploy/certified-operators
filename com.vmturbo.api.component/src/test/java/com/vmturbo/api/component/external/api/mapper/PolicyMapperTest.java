package com.vmturbo.api.component.external.api.mapper;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.dto.policy.PolicyApiDTO;
import com.vmturbo.api.dto.policy.PolicyApiInputDTO;
import com.vmturbo.api.enums.MergePolicyType;
import com.vmturbo.api.enums.PolicyType;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;

/**
 * Tests for {@link PolicyMapper}.
 *
 */
public class PolicyMapperTest {

    private Policy.Builder rawPolicyBuilder;
    private PolicyInfo.MergePolicy.Builder rawMergeBuilder;

    private Map<Long, Grouping> policyGroupingMap = new HashMap<>();

    private GroupApiDTO consumerDTO;
    private GroupApiDTO providerDTO;

    private PolicyMapper policyMapper;

    private static final long testPolicyID = 4815162342L;
    private static final String testPolicyCommodityType = "typeFoo";
    private static final String testPolicyName = "nameFoo";
    private static final long testConsumerId = 3141592L;
    private static final long testProviderId = 1234567890L;

    @Before
    public void setup() {
        final Grouping consumerGroup = Grouping.newBuilder().setId(testConsumerId).build();
        final Grouping providerGroup = Grouping.newBuilder().setId(testProviderId).build();

        policyGroupingMap.put(testProviderId, providerGroup);
        policyGroupingMap.put(testConsumerId, consumerGroup);

        consumerDTO = new GroupApiDTO();
        consumerDTO.setUuid(String.valueOf(testConsumerId));
        consumerDTO.setDisplayName("abcdef");
        providerDTO = new GroupApiDTO();
        providerDTO.setUuid(String.valueOf(testProviderId));
        providerDTO.setDisplayName("ghijkl");

        rawPolicyBuilder = Policy.newBuilder()
                .setId(testPolicyID)
                .setPolicyInfo(PolicyInfo.newBuilder()
                    .setCommodityType(testPolicyCommodityType)
                    .setEnabled(false)
                    .setName(testPolicyName));

        rawMergeBuilder = PolicyInfo.MergePolicy.newBuilder()
                .addMergeGroupIds(testProviderId)
                .addMergeGroupIds(testConsumerId);

        GroupMapper mockGroupMapper = Mockito.mock(GroupMapper.class);
        Mockito.when(mockGroupMapper.groupsToGroupApiDto(Collections.singletonList(consumerGroup),
                false)).thenReturn(Collections.singletonMap(consumerGroup.getId(), consumerDTO));
        Mockito.when(mockGroupMapper.groupsToGroupApiDto(Collections.singletonList(providerGroup),
                false)).thenReturn(Collections.singletonMap(providerGroup.getId(), providerDTO));
        policyMapper = new PolicyMapper(mockGroupMapper);

    }

    @Test
    public void testPolicyToApiDtoDiscoveredPolicy() {
        rawPolicyBuilder.getPolicyInfoBuilder().clearCommodityType();
        final Policy policy = rawPolicyBuilder
                .setTargetId(1L)
                .build();

        final PolicyApiDTO result = policyMapper.policyToApiDto(policy, policyGroupingMap);
        // This only tests things unique to discovered policies.
        assertThat(result.getCommodityType(), is("DrsSegmentationCommodity"));
    }

    @Test
    public void testPolicyToApiDtoDiscoveredPolicyExplicitCommodityType() {
        final Policy policy = rawPolicyBuilder
                .setTargetId(1L)
                .build();

        final PolicyApiDTO result = policyMapper.policyToApiDto(policy, policyGroupingMap);
        // This only tests things unique to discovered policies.
        assertThat(result.getCommodityType(), is(testPolicyCommodityType));
    }

    @Test
    public void testPolicyToApiDtoAtMostN() {
        // given
        rawPolicyBuilder.getPolicyInfoBuilder()
                .setAtMostN(PolicyInfo.AtMostNPolicy.newBuilder()
                        .setCapacity(10)
                        .setConsumerGroupId(testConsumerId)
                        .setProviderGroupId(testProviderId)
                        .build());
        final Policy policy = rawPolicyBuilder.build();

        // when
        PolicyApiDTO result = policyMapper.policyToApiDto(policy, policyGroupingMap);

        // then - general to any type of policy
        assertEquals(result.getName(), result.getDisplayName());
        assertEquals(result.getName(), testPolicyName);
        assertEquals(result.getUuid(), Long.toString(testPolicyID));
        assertFalse(result.isEnabled());
        assertEquals(result.getCommodityType(), testPolicyCommodityType);
        assertEquals(result.getType(), PolicyType.valueOf(policy.getPolicyInfo().getPolicyDetailCase().name()));

        // then - specifically this type of policy
        assertEquals(Integer.valueOf(10), result.getCapacity());
        assertEquals(result.getType(), PolicyType.AT_MOST_N);
        assertEquals(result.getConsumerGroup(), consumerDTO);
        assertEquals(result.getProviderGroup(), providerDTO);

        // the reverse conversion
        Policy reverse = policyMapper.policyApiDtoToProto(result);
        assertEquals(policy, reverse);
    }

    @Test
    public void testPolicyToApiDtoAtMostNBound() {
        // given
        rawPolicyBuilder.getPolicyInfoBuilder()
                .setAtMostNbound(PolicyInfo.AtMostNBoundPolicy.newBuilder()
                        .setCapacity(10)
                        .setConsumerGroupId(testConsumerId)
                        .setProviderGroupId(testProviderId)
                        .build());
        final Policy policy = rawPolicyBuilder.build();

        // when
        PolicyApiDTO result = policyMapper.policyToApiDto(policy, policyGroupingMap);

        // then - general
        assertEquals(result.getName(), result.getDisplayName());
        assertEquals(result.getName(), testPolicyName);
        assertEquals(result.getUuid(), Long.toString(testPolicyID));
        assertFalse(result.isEnabled());
        assertEquals(result.getCommodityType(), testPolicyCommodityType);

        // then - specific
        assertEquals(Integer.valueOf(10), result.getCapacity());
        assertEquals(result.getType(), PolicyType.AT_MOST_N_BOUND);
        assertEquals(result.getConsumerGroup(), consumerDTO);
        assertEquals(result.getProviderGroup(), providerDTO);

        // the reverse conversion
        Policy reverse = policyMapper.policyApiDtoToProto(result);
        assertEquals(policy, reverse);
    }

    @Test
    public void testPolicyToApiDtoBindToCompGroup() {
        // given
        rawPolicyBuilder.getPolicyInfoBuilder()
                .setBindToComplementaryGroup(PolicyInfo.BindToComplementaryGroupPolicy.newBuilder()
                        .setConsumerGroupId(testConsumerId)
                        .setProviderGroupId(testProviderId)
                        .build());
        final Policy policy = rawPolicyBuilder.build();

        // when
        PolicyApiDTO result = policyMapper.policyToApiDto(policy, policyGroupingMap);

        // then - general
        assertEquals(result.getName(), result.getDisplayName());
        assertEquals(result.getName(), testPolicyName);
        assertEquals(result.getUuid(), Long.toString(testPolicyID));
        assertFalse(result.isEnabled());
        assertEquals(result.getCommodityType(), testPolicyCommodityType);

        // then - specific
        assertEquals(result.getType(), PolicyType.BIND_TO_COMPLEMENTARY_GROUP);
        assertEquals(result.getConsumerGroup(), consumerDTO);
        assertEquals(result.getProviderGroup(), providerDTO);

        // the reverse conversion
        Policy reverse = policyMapper.policyApiDtoToProto(result);
        assertEquals(policy, reverse);
    }

    @Test
    public void testPolicyToApiDtoBindToGroup() {
         rawPolicyBuilder.getPolicyInfoBuilder()
                .setBindToGroup(PolicyInfo.BindToGroupPolicy.newBuilder()
                        .setConsumerGroupId(testConsumerId)
                        .setProviderGroupId(testProviderId)
                        .build())
                .build();
         final Policy policy = rawPolicyBuilder.build();

        PolicyApiDTO result = policyMapper.policyToApiDto(policy, policyGroupingMap);

        assertEquals(result.getName(), result.getDisplayName());
        assertEquals(result.getName(), testPolicyName);
        assertEquals(result.getUuid(), Long.toString(testPolicyID));
        assertFalse(result.isEnabled());
        assertEquals(result.getCommodityType(), testPolicyCommodityType);
        assertEquals(result.getType(), PolicyType.BIND_TO_GROUP);
        assertEquals(result.getConsumerGroup(), consumerDTO);
        assertEquals(result.getProviderGroup(), providerDTO);

        // the reverse conversion
        Policy reverse = policyMapper.policyApiDtoToProto(result);
        assertEquals(policy, reverse);
    }

    @Test
    public void testPolicyToApiDtoBindToGroupLicense() {
        rawPolicyBuilder.getPolicyInfoBuilder()
                .setBindToGroupAndLicense(PolicyInfo.BindToGroupAndLicencePolicy.newBuilder()
                        .setConsumerGroupId(testConsumerId)
                        .setProviderGroupId(testProviderId)
                        .build())
                .build();
        final Policy policy = rawPolicyBuilder.build();

        PolicyApiDTO result = policyMapper.policyToApiDto(policy, policyGroupingMap);

        assertEquals(result.getName(), result.getDisplayName());
        assertEquals(result.getName(), testPolicyName);
        assertEquals(result.getUuid(), Long.toString(testPolicyID));
        assertFalse(result.isEnabled());
        assertEquals(result.getCommodityType(), testPolicyCommodityType);
        assertEquals(result.getType(), PolicyType.BIND_TO_GROUP_AND_LICENSE);

        assertEquals(result.getType(), PolicyType.BIND_TO_GROUP_AND_LICENSE);
        assertEquals(result.getConsumerGroup(), consumerDTO);
        assertEquals(result.getProviderGroup(), providerDTO);

        // the reverse conversion
        Policy reverse = policyMapper.policyApiDtoToProto(result);
        assertEquals(policy, reverse);
    }

    @Test
    public void testPolicyToApiDtoBindToGroupGeoRedundancy() {
        rawPolicyBuilder.getPolicyInfoBuilder()
            .setBindToGroupAndGeoRedundancy(PolicyInfo.BindToGroupAndGeoRedundancyPolicy.newBuilder()
                        .setConsumerGroupId(testConsumerId)
                        .setProviderGroupId(testProviderId)
                        .build())
            .build();
        final Policy policy = rawPolicyBuilder.build();

        PolicyApiDTO result = policyMapper.policyToApiDto(policy, policyGroupingMap);

        assertEquals(result.getName(), result.getDisplayName());
        assertEquals(result.getName(), testPolicyName);
        assertEquals(result.getUuid(), Long.toString(testPolicyID));
        assertFalse(result.isEnabled());
        assertEquals(result.getCommodityType(), testPolicyCommodityType);

        assertEquals(result.getType(), PolicyType.BIND_TO_GROUP_AND_GEO_REDUNDANCY);
        assertEquals(result.getConsumerGroup(), consumerDTO);
        assertEquals(result.getProviderGroup(), providerDTO);

        // the reverse conversion
        Policy reverse = policyMapper.policyApiDtoToProto(result);
        assertEquals(policy, reverse);
    }

    @Test
    public void testPolicyToApiDtoMergeCluster() {

        rawPolicyBuilder.getPolicyInfoBuilder()
                .setMerge(rawMergeBuilder
                        .setMergeType(PolicyInfo.MergePolicy.MergeType.CLUSTER)
                        .build())
                .build();
        final Policy policy = rawPolicyBuilder.build();

        PolicyApiDTO result = policyMapper.policyToApiDto(policy, policyGroupingMap);

        assertEquals(result.getName(), result.getDisplayName());
        assertEquals(result.getName(), testPolicyName);
        assertEquals(result.getUuid(), Long.toString(testPolicyID));
        assertFalse(result.isEnabled());
        assertEquals(result.getCommodityType(), testPolicyCommodityType);

        assertEquals(result.getType(), PolicyType.MERGE);
        assertTrue(result.getMergeGroups().containsAll(Arrays.asList(consumerDTO, providerDTO)));
        assertTrue(Arrays.asList(consumerDTO, providerDTO).containsAll(result.getMergeGroups()));
        assertEquals(result.getMergeType(), MergePolicyType.Cluster);

        // the reverse conversion
        Policy reverse = policyMapper.policyApiDtoToProto(result);
        assertEquals(policy, reverse);
    }

    @Test
    public void testPolicyToApiDtoMergeStorageCluster() {

        rawPolicyBuilder.getPolicyInfoBuilder()
                .setMerge(rawMergeBuilder
                        .setMergeType(PolicyInfo.MergePolicy.MergeType.STORAGE_CLUSTER)
                        .build())
                .build();
        final Policy policy = rawPolicyBuilder.build();

        PolicyApiDTO result = policyMapper.policyToApiDto(policy, policyGroupingMap);

        assertEquals(result.getName(), result.getDisplayName());
        assertEquals(result.getName(), testPolicyName);
        assertEquals(result.getUuid(), Long.toString(testPolicyID));
        assertFalse(result.isEnabled());
        assertEquals(result.getCommodityType(), testPolicyCommodityType);

        assertEquals(result.getType(), PolicyType.MERGE);
        assertTrue(result.getMergeGroups().containsAll(Arrays.asList(consumerDTO, providerDTO)));
        assertTrue(Arrays.asList(consumerDTO, providerDTO).containsAll(result.getMergeGroups()));
        assertEquals(result.getMergeType(), MergePolicyType.StorageCluster);

        // the reverse conversion
        Policy reverse = policyMapper.policyApiDtoToProto(result);
        assertEquals(policy, reverse);
    }

    @Test
    public void testPolicyToApiDtoMergeDataCenter() {

        rawPolicyBuilder.getPolicyInfoBuilder()
                .setMerge(rawMergeBuilder
                        .setMergeType(PolicyInfo.MergePolicy.MergeType.DATACENTER)
                        .build())
                .build();
        final Policy policy = rawPolicyBuilder.build();

        PolicyApiDTO result = policyMapper.policyToApiDto(policy, policyGroupingMap);

        assertEquals(result.getName(), result.getDisplayName());
        assertEquals(result.getName(), testPolicyName);
        assertEquals(result.getUuid(), Long.toString(testPolicyID));
        assertFalse(result.isEnabled());
        assertEquals(result.getCommodityType(), testPolicyCommodityType);

        assertEquals(result.getType(), PolicyType.MERGE);
        assertTrue(result.getMergeGroups().containsAll(Arrays.asList(consumerDTO, providerDTO)));
        assertTrue(Arrays.asList(consumerDTO, providerDTO).containsAll(result.getMergeGroups()));
        assertEquals(result.getMergeType(), MergePolicyType.DataCenter);

        // the reverse conversion
        Policy reverse = policyMapper.policyApiDtoToProto(result);
        assertEquals(policy, reverse);
    }

    @Test
    public void testPolicyToApiDtoMustRunTogether() {
        rawPolicyBuilder.getPolicyInfoBuilder()
                .setMustRunTogether(PolicyInfo.MustRunTogetherPolicy.newBuilder()
                        .setGroupId(testConsumerId)
                        .build())
                .build();
        final Policy policy = rawPolicyBuilder.build();

        PolicyApiDTO result = policyMapper.policyToApiDto(policy, policyGroupingMap);

        assertEquals(result.getName(), result.getDisplayName());
        assertEquals(result.getName(), testPolicyName);
        assertEquals(result.getUuid(), Long.toString(testPolicyID));
        assertFalse(result.isEnabled());
        assertEquals(result.getCommodityType(), testPolicyCommodityType);

        assertEquals(result.getType(), PolicyType.MUST_RUN_TOGETHER);
        assertEquals(result.getConsumerGroup(), consumerDTO);
        // the provider group should not be populated in this case
        assertEquals(null, result.getProviderGroup());

        // the reverse conversion
        Policy reverse = policyMapper.policyApiDtoToProto(result);
        assertEquals(policy, reverse);
    }

    @Test
    public void testPolicyToApiDtoMustNotRunTogether() {
        rawPolicyBuilder.getPolicyInfoBuilder()
                .setMustNotRunTogether(PolicyInfo.MustNotRunTogetherPolicy.newBuilder()
                        .setGroupId(testConsumerId)
                        .build())
                .build();
        final Policy policy = rawPolicyBuilder.build();

        final PolicyApiDTO result = policyMapper.policyToApiDto(policy, policyGroupingMap);

        assertEquals(result.getName(), result.getDisplayName());
        assertEquals(result.getName(), testPolicyName);
        assertEquals(result.getUuid(), Long.toString(testPolicyID));
        assertFalse(result.isEnabled());
        assertEquals(result.getCommodityType(), testPolicyCommodityType);
        assertEquals(result.getType(), PolicyType.MUST_NOT_RUN_TOGETHER);
        assertEquals(result.getConsumerGroup(), consumerDTO);
        // the provider group should not be populated in this case
        assertEquals(null, result.getProviderGroup());
    }


    @Test
    public void testPolicyApiInputDtoToProtoBindGroup() {
        final PolicyApiInputDTO inputDTO = makeTestPolicyApiInputDTO();
        inputDTO.setType(PolicyType.BIND_TO_GROUP);

        final PolicyInfo result = policyMapper.policyApiInputDtoToProto(inputDTO);

        assertEquals(result.getName(), testPolicyName);
        assertFalse(result.getEnabled());

        assertTrue(result.hasBindToGroup());

        PolicyInfo.BindToGroupPolicy policy = result.getBindToGroup();

        assertEquals(policy.getConsumerGroupId(), testConsumerId);
        assertEquals(policy.getProviderGroupId(), testProviderId);
    }

    @Test
    public void testPolicyApiInputDtoToProtoBindCompGroup() {
        PolicyApiInputDTO inputDTO = makeTestPolicyApiInputDTO();
        inputDTO.setType(PolicyType.BIND_TO_COMPLEMENTARY_GROUP);

        final PolicyInfo result = policyMapper.policyApiInputDtoToProto(inputDTO);

        assertEquals(result.getName(), testPolicyName);
        assertFalse(result.getEnabled());

        assertTrue(result.hasBindToComplementaryGroup());
        final PolicyInfo.BindToComplementaryGroupPolicy policy = result.getBindToComplementaryGroup();

        assertEquals(policy.getConsumerGroupId(), testConsumerId);
        assertEquals(policy.getProviderGroupId(), testProviderId);
    }

    @Test
    public void testPolicyApiInputDtoToProtoBindGroupLicense() {
        PolicyApiInputDTO inputDTO = makeTestPolicyApiInputDTO();
        inputDTO.setType(PolicyType.BIND_TO_GROUP_AND_LICENSE);

        final PolicyInfo result = policyMapper.policyApiInputDtoToProto(inputDTO);

        assertEquals(result.getName(), testPolicyName);
        assertFalse(result.getEnabled());

        assertTrue(result.hasBindToGroupAndLicense());
        final PolicyInfo.BindToGroupAndLicencePolicy policy = result.getBindToGroupAndLicense();

        assertEquals(policy.getConsumerGroupId(), testConsumerId);
        assertEquals(policy.getProviderGroupId(), testProviderId);
    }

    @Test
    public void testPolicyApiInputDtoToProtoMergeCluster() {
        PolicyApiInputDTO inputDTO = makeTestPolicyApiInputDTO();
        inputDTO.setType(PolicyType.MERGE);
        inputDTO.setSellerUuid(null);
        inputDTO.setBuyerUuid(null);
        inputDTO.setMergeUuids(
                Arrays.asList(Long.toString(testConsumerId),
                        Long.toString(testProviderId))
        );
        inputDTO.setMergeType(MergePolicyType.Cluster);

        final PolicyInfo result = policyMapper.policyApiInputDtoToProto(inputDTO);

        assertEquals(result.getName(), testPolicyName);
        assertFalse(result.getEnabled());

        assertTrue(result.hasMerge());
        final PolicyInfo.MergePolicy policy = result.getMerge();

        assertEquals(policy.getMergeGroupIdsList(), Arrays.asList(testConsumerId, testProviderId));
        assertEquals(policy.getMergeType(), PolicyInfo.MergePolicy.MergeType.CLUSTER);
    }

    @Test
    public void testPolicyApiInputDtoToProtoMergeStorageCluster() {
        PolicyApiInputDTO inputDTO = makeTestPolicyApiInputDTO();
        inputDTO.setType(PolicyType.MERGE);
        inputDTO.setSellerUuid(null);
        inputDTO.setBuyerUuid(null);
        inputDTO.setMergeUuids(
                Arrays.asList(Long.toString(testConsumerId),
                        Long.toString(testProviderId))
        );
        inputDTO.setMergeType(MergePolicyType.StorageCluster);

        final PolicyInfo result = policyMapper.policyApiInputDtoToProto(inputDTO);

        assertEquals(result.getName(), testPolicyName);
        assertFalse(result.getEnabled());
        assertTrue(result.hasMerge());
        final PolicyInfo.MergePolicy policy = result.getMerge();

        assertEquals(policy.getMergeGroupIdsList(), Arrays.asList(testConsumerId, testProviderId));
        assertEquals(policy.getMergeType(), PolicyInfo.MergePolicy.MergeType.STORAGE_CLUSTER);
    }

    @Test
    public void testPolicyApiInputDtoToProtoMergeDataCenter() {
        PolicyApiInputDTO inputDTO = makeTestPolicyApiInputDTO();
        inputDTO.setType(PolicyType.MERGE);
        inputDTO.setSellerUuid(null);
        inputDTO.setBuyerUuid(null);
        inputDTO.setMergeUuids(
                Arrays.asList(Long.toString(testConsumerId),
                        Long.toString(testProviderId))
        );
        inputDTO.setMergeType(MergePolicyType.DataCenter);

        final PolicyInfo result = policyMapper.policyApiInputDtoToProto(inputDTO);

        assertEquals(result.getName(), testPolicyName);
        assertFalse(result.getEnabled());
        assertTrue(result.hasMerge());
        final PolicyInfo.MergePolicy policy = result.getMerge();

        assertEquals(policy.getMergeGroupIdsList(), Arrays.asList(testConsumerId, testProviderId));
        assertEquals(policy.getMergeType(), PolicyInfo.MergePolicy.MergeType.DATACENTER);
    }

    @Test
    public void testPolicyApiInputDtoToProtoAtMostN() {

        PolicyApiInputDTO inputDTO = makeTestPolicyApiInputDTO();
        inputDTO.setType(PolicyType.AT_MOST_N);
        inputDTO.setCapacity(10);

        final PolicyInfo result = policyMapper.policyApiInputDtoToProto(inputDTO);

        assertEquals(result.getName(), testPolicyName);
        assertFalse(result.getEnabled());

        assertTrue(result.hasAtMostN());
        final PolicyInfo.AtMostNPolicy policy = result.getAtMostN();

        assertEquals(policy.getConsumerGroupId(), testConsumerId);
        assertEquals(policy.getProviderGroupId(), testProviderId);
        assertTrue(policy.getCapacity() == 10);
    }

    @Test
    public void testPolicyApiInputDtoToProtoAtMostNBound() {
        PolicyApiInputDTO inputDTO = makeTestPolicyApiInputDTO();
        inputDTO.setType(PolicyType.AT_MOST_N_BOUND);
        inputDTO.setCapacity(10);

        final PolicyInfo result = policyMapper.policyApiInputDtoToProto(inputDTO);

        assertEquals(result.getName(), testPolicyName);
        assertFalse(result.getEnabled());

        assertTrue(result.hasAtMostNbound());
        final PolicyInfo.AtMostNBoundPolicy policy = result.getAtMostNbound();
        assertEquals(policy.getConsumerGroupId(), testConsumerId);
        assertEquals(policy.getProviderGroupId(), testProviderId);
        assertTrue(policy.getCapacity() == 10);
    }

    @Test
    public void testPolicyApiInputDtoToProtoMustRunTogether() {
        PolicyApiInputDTO inputDTO = makeTestPolicyApiInputDTO();
        inputDTO.setType(PolicyType.MUST_RUN_TOGETHER);

        final PolicyInfo result = policyMapper.policyApiInputDtoToProto(inputDTO);

        assertEquals(result.getName(), testPolicyName);
        assertFalse(result.getEnabled());

        assertTrue(result.hasMustRunTogether());
        final PolicyInfo.MustRunTogetherPolicy policy = result.getMustRunTogether();
        assertEquals(policy.getGroupId(), testConsumerId);
    }

    @Test
    public void testPolicyApiInputDtoToProtoMustNotRunTogether() {
        PolicyApiInputDTO inputDTO = makeTestPolicyApiInputDTO();
        inputDTO.setType(PolicyType.MUST_NOT_RUN_TOGETHER);

        final PolicyInfo result = policyMapper.policyApiInputDtoToProto(inputDTO);

        assertEquals(result.getName(), testPolicyName);
        assertFalse(result.getEnabled());

        assertTrue(result.hasMustNotRunTogether());
        final PolicyInfo.MustNotRunTogetherPolicy policy = result.getMustNotRunTogether();
        assertEquals(policy.getGroupId(), testConsumerId);
    }

    @Test
    public void testPolicyApiInputDtoToProtoBindGroupGeoRedundancy() {
        PolicyApiInputDTO inputDTO = makeTestPolicyApiInputDTO();
        inputDTO.setType(PolicyType.BIND_TO_GROUP_AND_GEO_REDUNDANCY);

        final PolicyInfo result = policyMapper.policyApiInputDtoToProto(inputDTO);

        assertEquals(result.getName(), testPolicyName);
        assertFalse(result.getEnabled());

        assertTrue(result.hasBindToGroupAndGeoRedundancy());
        final PolicyInfo.BindToGroupAndGeoRedundancyPolicy policy =
                result.getBindToGroupAndGeoRedundancy();
        assertEquals(policy.getConsumerGroupId(), testConsumerId);
        assertEquals(policy.getProviderGroupId(), testProviderId);
    }

    /**
     * Test that if group can not be found, basic info for PolicyApiDTO is still populated, and
     * GroupApiDTO is returned with only one uuid field.
     */
    @Test
    public void testPolicyToApiDtoGroupNotFound() {
        rawPolicyBuilder.getPolicyInfoBuilder()
                .setBindToGroup(PolicyInfo.BindToGroupPolicy.newBuilder()
                        .setConsumerGroupId(testConsumerId)
                        .setProviderGroupId(testProviderId)
                        .build())
                .build();
        final Policy policy = rawPolicyBuilder.build();

        // clear the groups map so they are not found
        policyGroupingMap.clear();
        PolicyApiDTO result = policyMapper.policyToApiDto(policy, policyGroupingMap);

        // check that policy info is still populated in PolicyApiDTO
        assertEquals(result.getName(), result.getDisplayName());
        assertEquals(result.getName(), testPolicyName);
        assertEquals(result.getUuid(), Long.toString(testPolicyID));
        assertFalse(result.isEnabled());
        assertEquals(result.getCommodityType(), testPolicyCommodityType);
        assertEquals(result.getType(), PolicyType.BIND_TO_GROUP);

        // check that default GroupApiDTO with only one field uuid is set
        final GroupApiDTO consumerGroup = (GroupApiDTO)result.getConsumerGroup();
        assertEquals(String.valueOf(testConsumerId), consumerGroup.getUuid());
        assertNull(consumerGroup.getIsStatic());
        assertNull(consumerGroup.getLogicalOperator());
        final GroupApiDTO providerGroup = (GroupApiDTO)result.getProviderGroup();
        assertEquals(String.valueOf(testProviderId), providerGroup.getUuid());
        assertNull(providerGroup.getIsStatic());
        assertNull(providerGroup.getLogicalOperator());
    }

    private PolicyApiInputDTO makeTestPolicyApiInputDTO() {
        PolicyApiInputDTO inputDTO = new PolicyApiInputDTO();
        inputDTO.setPolicyName(testPolicyName);
        inputDTO.setEnabled(false);
        inputDTO.setBuyerUuid(Long.toString(testConsumerId));
        inputDTO.setSellerUuid(Long.toString(testProviderId));
        return inputDTO;
    }
}
