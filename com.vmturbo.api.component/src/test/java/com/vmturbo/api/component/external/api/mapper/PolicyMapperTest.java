package com.vmturbo.api.component.external.api.mapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.Arrays;
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
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.PolicyDTO.InputPolicy;
import com.vmturbo.common.protobuf.group.PolicyDTO.MergeType;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;

public class PolicyMapperTest {

    private Policy.Builder rawPolicyBuilder;
    private Policy.MergePolicy.Builder rawMergeBuilder;

    private Map<Long, Group> policyGroupingMap = new HashMap<>();

    private GroupApiDTO consumerDTO;
    private GroupApiDTO providerDTO;

    private PolicyMapper policyMapper;

    private static final long testPolicyID = 4815162342L;
    private static final String testPolicyCommodityType = "typeFoo";
    private static final String testPolicyName = "nameFoo";
    private static final long testConsumerId = 3141592L;
    private static final long testProviderId = 1234567890L;

    @Before
    public void setup(){
        final Group consumerGroup = Group.newBuilder().setId(testConsumerId).build();
        final Group providerGroup = Group.newBuilder().setId(testProviderId).build();

        policyGroupingMap.put(testProviderId, providerGroup);
        policyGroupingMap.put(testConsumerId, consumerGroup);

        consumerDTO = new GroupApiDTO();
        consumerDTO.setDisplayName("abcdef");
        providerDTO = new GroupApiDTO();
        providerDTO.setDisplayName("ghijkl");

        rawPolicyBuilder = Policy.newBuilder()
                .setCommodityType(testPolicyCommodityType)
                .setEnabled(false)
                .setId(testPolicyID)
                .setName(testPolicyName);

        rawMergeBuilder = Policy.MergePolicy.newBuilder()
                .addMergeGroupIds(testProviderId)
                .addMergeGroupIds(testConsumerId);

        GroupMapper mockGroupMapper = Mockito.mock(GroupMapper.class);
        when(mockGroupMapper.toGroupApiDto(consumerGroup)).thenReturn(consumerDTO);
        when(mockGroupMapper.toGroupApiDto(providerGroup)).thenReturn(providerDTO);
        policyMapper = new PolicyMapper(mockGroupMapper);

    }

    @Test
    public void testPolicyToApiDtoAtMostN() {
        //given
        Policy policy = rawPolicyBuilder
                .setAtMostN(Policy.AtMostNPolicy.newBuilder()
                        .setCapacity(10)
                        .setConsumerGroupId(testConsumerId)
                        .setProviderGroupId(testProviderId)
                        .build())
                .build();

        //when
        PolicyApiDTO result = policyMapper.policyToApiDto(policy, policyGroupingMap);

        //then - general to any type of policy
        assertEquals(result.getName(), result.getDisplayName());
        assertEquals(result.getName(), testPolicyName);
        assertEquals(result.getUuid(), Long.toString(testPolicyID));
        assertFalse(result.isEnabled());
        assertEquals(result.getCommodityType(), testPolicyCommodityType);
        assertEquals(result.getType(), PolicyType.valueOf(policy.getPolicyDetailCase().name()));

        //then - specifically this type of policy
        assertEquals(Integer.valueOf(10), result.getCapacity());
        assertEquals(result.getType(), PolicyType.AT_MOST_N);
        assertEquals(result.getConsumerGroup(), consumerDTO);
        assertEquals(result.getProviderGroup(), providerDTO);
    }

    @Test
    public void testPolicyToApiDtoAtMostNBound() {
        //given
        Policy policy = rawPolicyBuilder
                .setAtMostNbound(Policy.AtMostNBoundPolicy.newBuilder()
                        .setCapacity(10)
                        .setConsumerGroupId(testConsumerId)
                        .setProviderGroupId(testProviderId)
                        .build())
                .build();

        //when
        PolicyApiDTO result = policyMapper.policyToApiDto(policy, policyGroupingMap);

        //then - general
        assertEquals(result.getName(), result.getDisplayName());
        assertEquals(result.getName(), testPolicyName);
        assertEquals(result.getUuid(), Long.toString(testPolicyID));
        assertFalse(result.isEnabled());
        assertEquals(result.getCommodityType(), testPolicyCommodityType);
        assertEquals(result.getType(), PolicyType.valueOf(policy.getPolicyDetailCase().name()));

        //then - specific
        assertEquals(Integer.valueOf(10), result.getCapacity());
        assertEquals(result.getType(), PolicyType.AT_MOST_NBOUND);
        assertEquals(result.getConsumerGroup(), consumerDTO);
        assertEquals(result.getProviderGroup(), providerDTO);
    }

    @Test
    public void testPolicyToApiDtoBindToCompGroup(){
        //given
        Policy policy = rawPolicyBuilder
                .setBindToComplementaryGroup(Policy.BindToComplementaryGroupPolicy.newBuilder()
                        .setConsumerGroupId(testConsumerId)
                        .setProviderGroupId(testProviderId)
                        .build())
                .build();

        //when
        PolicyApiDTO result = policyMapper.policyToApiDto(policy, policyGroupingMap);

        //then - general
        assertEquals(result.getName(), result.getDisplayName());
        assertEquals(result.getName(), testPolicyName);
        assertEquals(result.getUuid(), Long.toString(testPolicyID));
        assertFalse(result.isEnabled());
        assertEquals(result.getCommodityType(), testPolicyCommodityType);
        assertEquals(result.getType(), PolicyType.valueOf(policy.getPolicyDetailCase().name()));

        //then - specific
        assertEquals(result.getType(), PolicyType.BIND_TO_COMPLEMENTARY_GROUP);
        assertEquals(result.getConsumerGroup(), consumerDTO);
        assertEquals(result.getProviderGroup(), providerDTO);
    }

    @Test
    public void testPolicyToApiDtoBindToGroup(){
        Policy policy = rawPolicyBuilder
                .setBindToGroup(Policy.BindToGroupPolicy.newBuilder()
                        .setConsumerGroupId(testConsumerId)
                        .setProviderGroupId(testProviderId)
                        .build())
                .build();

        PolicyApiDTO result = policyMapper.policyToApiDto(policy, policyGroupingMap);

        assertEquals(result.getName(), result.getDisplayName());
        assertEquals(result.getName(), testPolicyName);
        assertEquals(result.getUuid(), Long.toString(testPolicyID));
        assertFalse(result.isEnabled());
        assertEquals(result.getCommodityType(), testPolicyCommodityType);
        assertEquals(result.getType(), PolicyType.valueOf(policy.getPolicyDetailCase().name()));

        assertEquals(result.getType(), PolicyType.BIND_TO_GROUP);
        assertEquals(result.getConsumerGroup(), consumerDTO);
        assertEquals(result.getProviderGroup(), providerDTO);
    }

    @Test
    public void testPolicyToApiDtoBindToGroupLicense(){
        Policy policy = rawPolicyBuilder
                .setBindToGroupAndLicense(Policy.BindToGroupAndLicencePolicy.newBuilder()
                        .setConsumerGroupId(testConsumerId)
                        .setProviderGroupId(testProviderId)
                        .build())
                .build();

        PolicyApiDTO result = policyMapper.policyToApiDto(policy,policyGroupingMap);

        assertEquals(result.getName(), result.getDisplayName());
        assertEquals(result.getName(), testPolicyName);
        assertEquals(result.getUuid(), Long.toString(testPolicyID));
        assertFalse(result.isEnabled());
        assertEquals(result.getCommodityType(), testPolicyCommodityType);
        assertEquals(result.getType(), PolicyType.valueOf(policy.getPolicyDetailCase().name()));

        assertEquals(result.getType(), PolicyType.BIND_TO_GROUP_AND_LICENSE);
        assertEquals(result.getConsumerGroup(), consumerDTO);
        assertEquals(result.getProviderGroup(), providerDTO);



    }

    @Test
    public void testPolicyToApiDtoBindToGroupGeoRedundancy(){
        Policy policy = rawPolicyBuilder
            .setBindToGroupAndGeoRedundancy(Policy.BindToGroupAndGeoRedundancyPolicy.newBuilder()
                        .setConsumerGroupId(testConsumerId)
                        .setProviderGroupId(testProviderId)
                        .build())
            .build();

        PolicyApiDTO result = policyMapper.policyToApiDto(policy, policyGroupingMap);

        assertEquals(result.getName(), result.getDisplayName());
        assertEquals(result.getName(), testPolicyName);
        assertEquals(result.getUuid(), Long.toString(testPolicyID));
        assertFalse(result.isEnabled());
        assertEquals(result.getCommodityType(), testPolicyCommodityType);
        assertEquals(result.getType(), PolicyType.valueOf(policy.getPolicyDetailCase().name()));

        assertEquals(result.getType(), PolicyType.BIND_TO_GROUP_AND_GEO_REDUNDANCY);
        assertEquals(result.getConsumerGroup(), consumerDTO);
        assertEquals(result.getProviderGroup(), providerDTO);
    }

    @Test
    public void testPolicyToApiDtoMergeCluster(){

        Policy policy = rawPolicyBuilder
                .setMerge(rawMergeBuilder
                        .setMergeType(MergeType.CLUSTER)
                        .build())
                .build();

        PolicyApiDTO result = policyMapper.policyToApiDto(policy, policyGroupingMap);

        assertEquals(result.getName(), result.getDisplayName());
        assertEquals(result.getName(), testPolicyName);
        assertEquals(result.getUuid(), Long.toString(testPolicyID));
        assertFalse(result.isEnabled());
        assertEquals(result.getCommodityType(), testPolicyCommodityType);
        assertEquals(result.getType(), PolicyType.valueOf(policy.getPolicyDetailCase().name()));

        assertEquals(result.getType(), PolicyType.MERGE);
        assertTrue(result.getMergeGroups().containsAll(Arrays.asList(consumerDTO, providerDTO)));
        assertTrue(Arrays.asList(consumerDTO, providerDTO).containsAll(result.getMergeGroups()));
        assertEquals(result.getMergeType(), MergePolicyType.Cluster);

    }

    @Test
    public void testPolicyToApiDtoMergeStorageCluster(){

        Policy policy = rawPolicyBuilder
                .setMerge(rawMergeBuilder
                        .setMergeType(MergeType.STORAGE_CLUSTER)
                        .build())
                .build();

        PolicyApiDTO result = policyMapper.policyToApiDto(policy, policyGroupingMap);

        assertEquals(result.getName(), result.getDisplayName());
        assertEquals(result.getName(), testPolicyName);
        assertEquals(result.getUuid(), Long.toString(testPolicyID));
        assertFalse(result.isEnabled());
        assertEquals(result.getCommodityType(), testPolicyCommodityType);
        assertEquals(result.getType(), PolicyType.valueOf(policy.getPolicyDetailCase().name()));

        assertEquals(result.getType(), PolicyType.MERGE);
        assertTrue(result.getMergeGroups().containsAll(Arrays.asList(consumerDTO, providerDTO)));
        assertTrue(Arrays.asList(consumerDTO, providerDTO).containsAll(result.getMergeGroups()));
        assertEquals(result.getMergeType(), MergePolicyType.StorageCluster);
    }

    @Test
    public void testPolicyToApiDtoMergeDataCenter(){

        Policy policy = rawPolicyBuilder
                .setMerge(rawMergeBuilder
                        .setMergeType(MergeType.DATACENTER)
                        .build())
                .build();

        PolicyApiDTO result = policyMapper.policyToApiDto(policy, policyGroupingMap);

        assertEquals(result.getName(), result.getDisplayName());
        assertEquals(result.getName(), testPolicyName);
        assertEquals(result.getUuid(), Long.toString(testPolicyID));
        assertFalse(result.isEnabled());
        assertEquals(result.getCommodityType(), testPolicyCommodityType);
        assertEquals(result.getType(), PolicyType.valueOf(policy.getPolicyDetailCase().name()));

        assertEquals(result.getType(), PolicyType.MERGE);
        assertTrue(result.getMergeGroups().containsAll(Arrays.asList(consumerDTO, providerDTO)));
        assertTrue(Arrays.asList(consumerDTO, providerDTO).containsAll(result.getMergeGroups()));
        assertEquals(result.getMergeType(), MergePolicyType.DataCenter);

    }

    @Test
    public void testPolicyToApiDtoMustRunTogether(){

        Policy policy = rawPolicyBuilder
                .setMustRunTogether(Policy.MustRunTogetherPolicy.newBuilder()
                        .setConsumerGroupId(testConsumerId)
                        .setProviderGroupId(testProviderId)
                        .build())
                .build();

        PolicyApiDTO result = policyMapper.policyToApiDto(policy, policyGroupingMap);

        assertEquals(result.getName(), result.getDisplayName());
        assertEquals(result.getName(), testPolicyName);
        assertEquals(result.getUuid(), Long.toString(testPolicyID));
        assertFalse(result.isEnabled());
        assertEquals(result.getCommodityType(), testPolicyCommodityType);
        assertEquals(result.getType(), PolicyType.valueOf(policy.getPolicyDetailCase().name()));

        assertEquals(result.getType(), PolicyType.MUST_RUN_TOGETHER);
        assertEquals(result.getConsumerGroup(), consumerDTO);
        assertEquals(result.getProviderGroup(), providerDTO);
    }


    @Test
    public void testPolicyApiInputDtoToProtoBindGroup(){
        PolicyApiInputDTO inputDTO = makeTestPolicyApiInputDTO();
        inputDTO.setType(PolicyType.BIND_TO_GROUP);

        InputPolicy result = policyMapper.policyApiInputDtoToProto(inputDTO);

        assertEquals(result.getName(),testPolicyName);
        assertFalse(result.getEnabled());

        assertTrue(result.hasBindToGroup());

        InputPolicy.BindToGroupPolicy policy = result.getBindToGroup();

        assertEquals(policy.getConsumerGroup(), testConsumerId);
        assertEquals(policy.getProviderGroup(), testProviderId);

    }

    @Test
    public void testPolicyApiInputDtoToProtoBindCompGroup(){
        PolicyApiInputDTO inputDTO = makeTestPolicyApiInputDTO();
        inputDTO.setType(PolicyType.BIND_TO_COMPLEMENTARY_GROUP);

        InputPolicy result = policyMapper.policyApiInputDtoToProto(inputDTO);

        assertEquals(result.getName(),testPolicyName);
        assertFalse(result.getEnabled());

        assertTrue(result.hasBindToComplementaryGroup());
        InputPolicy.BindToComplementaryGroupPolicy policy = result.getBindToComplementaryGroup();

        assertEquals(policy.getConsumerGroup(), testConsumerId);
        assertEquals(policy.getProviderGroup(), testProviderId);
    }

    @Test
    public void testPolicyApiInputDtoToProtoBindGroupLicense(){
        PolicyApiInputDTO inputDTO = makeTestPolicyApiInputDTO();
        inputDTO.setType(PolicyType.BIND_TO_GROUP_AND_LICENSE);

        InputPolicy result = policyMapper.policyApiInputDtoToProto(inputDTO);

        assertEquals(result.getName(),testPolicyName);
        assertFalse(result.getEnabled());

        assertTrue(result.hasBindToGroupAndLicense());
        InputPolicy.BindToGroupAndLicencePolicy policy = result.getBindToGroupAndLicense();

        assertEquals(policy.getConsumerGroup(), testConsumerId);
        assertEquals(policy.getProviderGroup(), testProviderId);
    }

    @Test
    public void testPolicyApiInputDtoToProtoMergeCluster(){
        PolicyApiInputDTO inputDTO = makeTestPolicyApiInputDTO();
        inputDTO.setType(PolicyType.MERGE);
        inputDTO.setSellerUuid(null);
        inputDTO.setBuyerUuid(null);
        inputDTO.setMergeUuids(
                Arrays.asList(Long.toString(testConsumerId),
                        Long.toString(testProviderId))
        );
        inputDTO.setMergeType(MergePolicyType.Cluster);

        InputPolicy result = policyMapper.policyApiInputDtoToProto(inputDTO);

        assertEquals(result.getName(),testPolicyName);
        assertFalse(result.getEnabled());

        assertTrue(result.hasMerge());
        InputPolicy.MergePolicy policy = result.getMerge();

        assertEquals(policy.getMergeGroupsList(), Arrays.asList(testConsumerId,testProviderId));
        assertEquals(policy.getMergeType(), MergeType.CLUSTER);
    }
    @Test
    public void testPolicyApiInputDtoToProtoMergeStorageCluster(){
        PolicyApiInputDTO inputDTO = makeTestPolicyApiInputDTO();
        inputDTO.setType(PolicyType.MERGE);
        inputDTO.setSellerUuid(null);
        inputDTO.setBuyerUuid(null);
        inputDTO.setMergeUuids(
                Arrays.asList(Long.toString(testConsumerId),
                        Long.toString(testProviderId))
        );
        inputDTO.setMergeType(MergePolicyType.StorageCluster);

        InputPolicy result = policyMapper.policyApiInputDtoToProto(inputDTO);

        assertEquals(result.getName(),testPolicyName);
        assertFalse(result.getEnabled());
        assertTrue(result.hasMerge());
        InputPolicy.MergePolicy policy = result.getMerge();

        assertEquals(policy.getMergeGroupsList(), Arrays.asList(testConsumerId,testProviderId));
        assertEquals(policy.getMergeType(), MergeType.STORAGE_CLUSTER);
    }
    @Test
    public void testPolicyApiInputDtoToProtoMergeDataCenter(){
        PolicyApiInputDTO inputDTO = makeTestPolicyApiInputDTO();
        inputDTO.setType(PolicyType.MERGE);
        inputDTO.setSellerUuid(null);
        inputDTO.setBuyerUuid(null);
        inputDTO.setMergeUuids(
                Arrays.asList(Long.toString(testConsumerId),
                        Long.toString(testProviderId))
        );
        inputDTO.setMergeType(MergePolicyType.DataCenter);

        InputPolicy result = policyMapper.policyApiInputDtoToProto(inputDTO);

        assertEquals(result.getName(),testPolicyName);
        assertFalse(result.getEnabled());
        assertTrue(result.hasMerge());
        InputPolicy.MergePolicy policy = result.getMerge();

        assertEquals(policy.getMergeGroupsList(), Arrays.asList(testConsumerId,testProviderId));
        assertEquals(policy.getMergeType(), MergeType.DATACENTER);
    }

    @Test
    public void testPolicyApiInputDtoToProtoAtMostN(){

        PolicyApiInputDTO inputDTO = makeTestPolicyApiInputDTO();
        inputDTO.setType(PolicyType.AT_MOST_N);
        inputDTO.setCapacity(10);

        InputPolicy result = policyMapper.policyApiInputDtoToProto(inputDTO);

        assertEquals(result.getName(),testPolicyName);
        assertFalse(result.getEnabled());

        assertTrue(result.hasAtMostN());
        InputPolicy.AtMostNPolicy policy = result.getAtMostN();

        assertEquals(policy.getConsumerGroup(), testConsumerId);
        assertEquals(policy.getProviderGroup(), testProviderId);
        assertTrue(policy.getCapacity() == 10);
    }

    @Test
    public void testPolicyApiInputDtoToProtoAtMostNBound(){
        PolicyApiInputDTO inputDTO = makeTestPolicyApiInputDTO();
        inputDTO.setType(PolicyType.AT_MOST_NBOUND);
        inputDTO.setCapacity(10);

        InputPolicy result = policyMapper.policyApiInputDtoToProto(inputDTO);

        assertEquals(result.getName(),testPolicyName);
        assertFalse(result.getEnabled());

        assertTrue(result.hasAtMostNbound());
        InputPolicy.AtMostNBoundPolicy policy = result.getAtMostNbound();
        assertEquals(policy.getConsumerGroup(), testConsumerId);
        assertEquals(policy.getProviderGroup(), testProviderId);
        assertTrue(policy.getCapacity() == 10);
    }

    @Test
    public void testPolicyApiInputDtoToProtoMustRunTogether(){
        PolicyApiInputDTO inputDTO = makeTestPolicyApiInputDTO();
        inputDTO.setType(PolicyType.MUST_RUN_TOGETHER);

        InputPolicy result = policyMapper.policyApiInputDtoToProto(inputDTO);

        assertEquals(result.getName(),testPolicyName);
        assertFalse(result.getEnabled());

        assertTrue(result.hasMustRunTogether());
        InputPolicy.MustRunTogetherPolicy policy = result.getMustRunTogether();
        assertEquals(policy.getConsumerGroup(), testConsumerId);
        assertEquals(policy.getProviderGroup(), testProviderId);
    }

    @Test
    public void testPolicyApiInputDtoToProtoBindGroupGeoRedundancy(){
        PolicyApiInputDTO inputDTO = makeTestPolicyApiInputDTO();
        inputDTO.setType(PolicyType.BIND_TO_GROUP_AND_GEO_REDUNDANCY);

        InputPolicy result = policyMapper.policyApiInputDtoToProto(inputDTO);

        assertEquals(result.getName(),testPolicyName);
        assertFalse(result.getEnabled());

        assertTrue(result.hasBindToGroupAndGeoRedundancy());
        InputPolicy.BindToGroupAndGeoRedundancyPolicy policy =
                result.getBindToGroupAndGeoRedundancy();
        assertEquals(policy.getConsumerGroup(), testConsumerId);
        assertEquals(policy.getProviderGroup(), testProviderId);
    }

    private PolicyApiInputDTO makeTestPolicyApiInputDTO(){
        PolicyApiInputDTO inputDTO = new PolicyApiInputDTO();
        inputDTO.setPolicyName(testPolicyName);
        inputDTO.setEnabled(false);
        inputDTO.setBuyerUuid(Long.toString(testConsumerId));
        inputDTO.setSellerUuid(Long.toString(testProviderId));
        return inputDTO;
    }
}
