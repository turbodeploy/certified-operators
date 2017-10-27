package com.vmturbo.api.component.external.api.mapper;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.dto.policy.PolicyApiDTO;
import com.vmturbo.api.dto.policy.PolicyApiInputDTO;
import com.vmturbo.api.enums.MergePolicyType;
import com.vmturbo.api.enums.PolicyType;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyGrouping;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyGroupingID;

/**
 * Map Policy returned from the Group component into PolicyApiDTO to be returned from the API.
 */
public class PolicyMapper {
    private static final Logger LOG = LogManager.getLogger();

    /** * Map a {@link com.vmturbo.common.protobuf.group.PolicyDTO.Policy} used by the group component
     * to a {@link PolicyApiDTO} used by the API component.
     *
     * @param policyProto The Policy protobuf to map from.
     * @return A {@link PolicyApiDTO}.
     */

    private GroupMapper groupMapper;

    public PolicyMapper(GroupMapper groupMapper) {
        this.groupMapper = groupMapper;
    }

    public PolicyApiDTO policyToApiDto(final PolicyDTO.Policy policyProto,
                                       final Map<PolicyGroupingID, PolicyGrouping> groupsByID) {
        final PolicyApiDTO policyApiDTO = new PolicyApiDTO();

        policyApiDTO.setName(policyProto.getName());
        policyApiDTO.setDisplayName(policyProto.getName());
        policyApiDTO.setUuid(Long.toString(policyProto.getId()));
        policyApiDTO.setEnabled(policyProto.getEnabled());
        policyApiDTO.setCommodityType(policyProto.getCommodityType());

        PolicyDTO.PolicyGrouping consumerGrouping;
        PolicyDTO.PolicyGrouping providerGrouping;
        switch (policyProto.getPolicyDetailCase()) {
            case AT_MOST_N:
                final PolicyDTO.Policy.AtMostNPolicy atMostN = policyProto.getAtMostN();
                consumerGrouping = groupsByID.get(atMostN.getConsumerGroupId());
                providerGrouping = groupsByID.get(atMostN.getProviderGroupId());

                policyApiDTO.setCapacity((int)atMostN.getCapacity());
                policyApiDTO.setType(PolicyType.AT_MOST_N);
                policyApiDTO.setConsumerGroup(groupMapper.toGroupApiDto(consumerGrouping));
                policyApiDTO.setProviderGroup(groupMapper.toGroupApiDto(providerGrouping));
                break;
            case AT_MOST_NBOUND:
                final PolicyDTO.Policy.AtMostNBoundPolicy atMostNBound = policyProto.getAtMostNbound();
                consumerGrouping = groupsByID.get(atMostNBound.getConsumerGroupId());
                providerGrouping = groupsByID.get(atMostNBound.getProviderGroupId());

                policyApiDTO.setCapacity((int) atMostNBound.getCapacity());
                policyApiDTO.setType(PolicyType.AT_MOST_NBOUND);
                policyApiDTO.setConsumerGroup(groupMapper.toGroupApiDto(consumerGrouping));
                policyApiDTO.setProviderGroup(groupMapper.toGroupApiDto(providerGrouping));
                break;
            case BIND_TO_COMPLEMENTARY_GROUP:
                final PolicyDTO.Policy.BindToComplementaryGroupPolicy bindToComplementaryGroup =
                        policyProto.getBindToComplementaryGroup();
                consumerGrouping = groupsByID.get(bindToComplementaryGroup.getConsumerGroupId());
                providerGrouping = groupsByID.get(bindToComplementaryGroup.getProviderGroupId());

                policyApiDTO.setType(PolicyType.BIND_TO_COMPLEMENTARY_GROUP);
                policyApiDTO.setConsumerGroup(groupMapper.toGroupApiDto(consumerGrouping));
                policyApiDTO.setProviderGroup(groupMapper.toGroupApiDto(providerGrouping));
                break;
            case BIND_TO_GROUP:
                final PolicyDTO.Policy.BindToGroupPolicy bindToGroup = policyProto.getBindToGroup();
                consumerGrouping = groupsByID.get(bindToGroup.getConsumerGroupId());
                providerGrouping = groupsByID.get(bindToGroup.getProviderGroupId());

                policyApiDTO.setType(PolicyType.BIND_TO_GROUP);
                policyApiDTO.setConsumerGroup(groupMapper.toGroupApiDto(consumerGrouping));
                policyApiDTO.setProviderGroup(groupMapper.toGroupApiDto(providerGrouping));
                break;
            case BIND_TO_GROUP_AND_LICENSE:
                final PolicyDTO.Policy.BindToGroupAndLicencePolicy bindToGroupAndLicense =
                        policyProto.getBindToGroupAndLicense();
                consumerGrouping = groupsByID.get(bindToGroupAndLicense.getConsumerGroupId());
                providerGrouping = groupsByID.get(bindToGroupAndLicense.getProviderGroupId());

                policyApiDTO.setType(PolicyType.BIND_TO_GROUP_AND_LICENSE);
                policyApiDTO.setConsumerGroup(groupMapper.toGroupApiDto(consumerGrouping));
                policyApiDTO.setProviderGroup(groupMapper.toGroupApiDto(providerGrouping));
                break;
            case BIND_TO_GROUP_AND_GEO_REDUNDANCY:
                final PolicyDTO.Policy.BindToGroupAndGeoRedundancyPolicy bindToGroupAndGeoRedundancy =
                        policyProto.getBindToGroupAndGeoRedundancy();
                consumerGrouping = groupsByID.get(bindToGroupAndGeoRedundancy.getConsumerGroupId());
                providerGrouping = groupsByID.get(bindToGroupAndGeoRedundancy.getProviderGroupId());

                policyApiDTO.setType(PolicyType.BIND_TO_GROUP_AND_GEO_REDUNDANCY);
                policyApiDTO.setConsumerGroup(groupMapper.toGroupApiDto(consumerGrouping));
                policyApiDTO.setProviderGroup(groupMapper.toGroupApiDto(providerGrouping));
                break;
            case MERGE:
                final PolicyDTO.Policy.MergePolicy merge = policyProto.getMerge();
                final List<PolicyGrouping> mergeGroupings = merge.getMergeGroupIdsList().stream()
                        .map(groupsByID::get).collect(Collectors.toList());

                policyApiDTO.setType(PolicyType.MERGE);
                switch (merge.getMergeType()) {
                    case CLUSTER:
                        policyApiDTO.setMergeType(MergePolicyType.Cluster);
                        break;
                    case STORAGE_CLUSTER:
                        policyApiDTO.setMergeType(MergePolicyType.StorageCluster);
                        break;
                    case DATACENTER:
                        policyApiDTO.setMergeType(MergePolicyType.DataCenter);
                        break;
                }
                policyApiDTO.setMergeGroups(mergeGroupings.stream()
                        .map(groupMapper::toGroupApiDto)
                        .collect(Collectors.toList()));
                break;
            case MUST_RUN_TOGETHER:
                final PolicyDTO.Policy.MustRunTogetherPolicy mustRunTogether = policyProto.getMustRunTogether();
                consumerGrouping = groupsByID.get(mustRunTogether.getConsumerGroupId());
                providerGrouping = groupsByID.get(mustRunTogether.getProviderGroupId());

                policyApiDTO.setType(PolicyType.MUST_RUN_TOGETHER);
                policyApiDTO.setConsumerGroup(groupMapper.toGroupApiDto(consumerGrouping));
                policyApiDTO.setProviderGroup(groupMapper.toGroupApiDto(providerGrouping));
                break;
        }

        return policyApiDTO;
    }

    /**
     * Map a {@link PolicyApiInputDTO} used by the API component
     * to a {@link com.vmturbo.common.protobuf.group.PolicyDTO.InputPolicy} used by the group component.
     *
     * @param policyApiInputDTO The Policy input API DTO to map from.
     * @return A {@link com.vmturbo.common.protobuf.group.PolicyDTO.InputPolicy}.
     */
    public PolicyDTO.InputPolicy policyApiInputDtoToProto(final PolicyApiInputDTO policyApiInputDTO) {
        final PolicyDTO.InputPolicy.Builder inputPolicyBuilder = PolicyDTO.InputPolicy.newBuilder();

        if (policyApiInputDTO.getPolicyName() != null) {
            inputPolicyBuilder.setName(policyApiInputDTO.getPolicyName());
        } else {
            LOG.warn("The 'name' field in PolicyApiInputDTO is null");
        }
        inputPolicyBuilder.setEnabled(policyApiInputDTO.isEnabled());

        if (policyApiInputDTO.getType() != null) {
            long providerId;
            long consumerId;
            float capacity;

            // So far UI only sends the following four types of policies.
            switch (policyApiInputDTO.getType()) {
                case BIND_TO_GROUP:
                    providerId = providerId(policyApiInputDTO);
                    consumerId = consumerId(policyApiInputDTO);

                    final PolicyDTO.InputPolicy.BindToGroupPolicy bindToGroupPolicy =
                            PolicyDTO.InputPolicy.BindToGroupPolicy.newBuilder()
                                    .setProviderGroup(PolicyDTO.InputGroup.newBuilder()
                                            .setGroupId(providerId)
                                            .build())
                                    .setConsumerGroup(PolicyDTO.InputGroup.newBuilder()
                                            .setGroupId(consumerId)
                                            .build())
                                    .build();
                    inputPolicyBuilder.setBindToGroup(bindToGroupPolicy);
                    break;
                case BIND_TO_COMPLEMENTARY_GROUP:
                    providerId = providerId(policyApiInputDTO);
                    consumerId = consumerId(policyApiInputDTO);

                    final PolicyDTO.InputPolicy.BindToComplementaryGroupPolicy bindToComplementaryGroup =
                            PolicyDTO.InputPolicy.BindToComplementaryGroupPolicy.newBuilder()
                                    .setProviderGroup(PolicyDTO.InputGroup.newBuilder()
                                            .setGroupId(providerId)
                                            .build())
                                    .setConsumerGroup(PolicyDTO.InputGroup.newBuilder()
                                            .setGroupId(consumerId)
                                            .build())
                                    .build();
                    inputPolicyBuilder.setBindToComplementaryGroup(bindToComplementaryGroup);
                    break;
                case BIND_TO_GROUP_AND_LICENSE:
                    providerId = providerId(policyApiInputDTO);
                    consumerId = consumerId(policyApiInputDTO);

                    final PolicyDTO.InputPolicy.BindToGroupAndLicencePolicy bindToGroupAndLicencePolicy =
                            PolicyDTO.InputPolicy.BindToGroupAndLicencePolicy.newBuilder()
                                    .setProviderGroup(PolicyDTO.InputGroup.newBuilder()
                                            .setGroupId(providerId)
                                            .build())
                                    .setConsumerGroup(PolicyDTO.InputGroup.newBuilder()
                                            .setGroupId(consumerId)
                                            .build())
                                    .build();
                    inputPolicyBuilder.setBindToGroupAndLicense(bindToGroupAndLicencePolicy);
                    break;
                case MERGE:
                    final PolicyDTO.InputPolicy.MergePolicy.Builder mergePolicyBuilder =
                            PolicyDTO.InputPolicy.MergePolicy.newBuilder();

                    switch (policyApiInputDTO.getMergeType()) {
                        case Cluster:
                            mergePolicyBuilder.setMergeType(PolicyDTO.MergeType.CLUSTER);
                            break;
                        case StorageCluster:
                            mergePolicyBuilder.setMergeType(PolicyDTO.MergeType.STORAGE_CLUSTER);
                            break;
                        case DataCenter:
                            mergePolicyBuilder.setMergeType(PolicyDTO.MergeType.DATACENTER);
                            break;
                    }

                    mergeGroups(policyApiInputDTO).forEach(id -> mergePolicyBuilder
                        .addMergeGroups(PolicyDTO.InputGroup.newBuilder().setGroupId(id).build()));
                    inputPolicyBuilder.setMerge(mergePolicyBuilder.build());
                    break;
                case AT_MOST_N:
                    providerId = providerId(policyApiInputDTO);
                    consumerId = consumerId(policyApiInputDTO);
                    capacity = getPolicyCapacity(policyApiInputDTO);

                    final PolicyDTO.InputPolicy.AtMostNPolicy atMostNPolicy =
                            PolicyDTO.InputPolicy.AtMostNPolicy.newBuilder()
                                    .setCapacity(capacity)
                                    .setProviderGroup(PolicyDTO.InputGroup.newBuilder()
                                            .setGroupId(providerId)
                                            .build())
                                    .setConsumerGroup(PolicyDTO.InputGroup.newBuilder()
                                            .setGroupId(consumerId)
                                            .build())
                                    .build();
                    inputPolicyBuilder.setAtMostN(atMostNPolicy);
                    break;
                case AT_MOST_NBOUND:
                    providerId = providerId(policyApiInputDTO);
                    consumerId = consumerId(policyApiInputDTO);
                    capacity = getPolicyCapacity(policyApiInputDTO);

                    final PolicyDTO.InputPolicy.AtMostNBoundPolicy atMostNBoundPolicy =
                            PolicyDTO.InputPolicy.AtMostNBoundPolicy.newBuilder()
                                    .setCapacity(capacity)
                                    .setProviderGroup(PolicyDTO.InputGroup.newBuilder()
                                            .setGroupId(providerId)
                                            .build())
                                    .setConsumerGroup(PolicyDTO.InputGroup.newBuilder()
                                            .setGroupId(consumerId)
                                            .build())
                                    .build();
                    inputPolicyBuilder.setAtMostNbound(atMostNBoundPolicy);
                    break;
                case MUST_RUN_TOGETHER:
                    providerId = providerId(policyApiInputDTO);
                    consumerId = consumerId(policyApiInputDTO);

                    final PolicyDTO.InputPolicy.MustRunTogetherPolicy mustRunTogetherPolicy =
                            PolicyDTO.InputPolicy.MustRunTogetherPolicy.newBuilder()
                                    .setProviderGroup(PolicyDTO.InputGroup.newBuilder()
                                            .setGroupId(providerId)
                                            .build())
                                    .setConsumerGroup(PolicyDTO.InputGroup.newBuilder()
                                            .setGroupId(consumerId)
                                            .build())
                                    .build();
                    inputPolicyBuilder.setMustRunTogether(mustRunTogetherPolicy);
                    break;
                case BIND_TO_GROUP_AND_GEO_REDUNDANCY:
                    providerId = providerId(policyApiInputDTO);
                    consumerId = consumerId(policyApiInputDTO);

                    final PolicyDTO.InputPolicy.BindToGroupAndGeoRedundancyPolicy bindToGroupAndGeoRedundancyPolicy =
                            PolicyDTO.InputPolicy.BindToGroupAndGeoRedundancyPolicy.newBuilder()
                                    .setProviderGroup(PolicyDTO.InputGroup.newBuilder()
                                            .setGroupId(providerId)
                                            .build())
                                    .setConsumerGroup(PolicyDTO.InputGroup.newBuilder()
                                            .setGroupId(consumerId)
                                            .build())
                                    .build();
                    inputPolicyBuilder.setBindToGroupAndGeoRedundancy(bindToGroupAndGeoRedundancyPolicy);
                    break;
            }
        } else {
            LOG.warn("The 'type' field in PolicyApiInputDTO is null");
        }

        return inputPolicyBuilder.build();
    }

    /**
     * Extract the provider ID from a {@link PolicyApiInputDTO}.
     *
     * @param policyApiInputDTO The input dto from which to extract the value.
     * @return The provider id.
     */
    private long providerId(final PolicyApiInputDTO policyApiInputDTO) {
        if (policyApiInputDTO.getSellerUuid() != null) {
            return Long.valueOf(policyApiInputDTO.getSellerUuid());
        } else {
            throw new IllegalArgumentException("PolicyApiInputDTO does not contain a seller UUID");
        }
    }

    /**
     * Extract the consumer ID from a {@link PolicyApiInputDTO}.
     *
     * @param policyApiInputDTO The input dto from which to extract the value.
     * @return The consumer id
     */
    private long consumerId(final PolicyApiInputDTO policyApiInputDTO) {
        if (policyApiInputDTO.getBuyerUuid() != null) {
            return Long.valueOf(policyApiInputDTO.getBuyerUuid());
        } else {
            throw new IllegalArgumentException("PolicyApiInputDTO does not contain a buyer UUID");
        }
    }


    /**
     * Extract the value of <code>mergeUuids</code> field from a {@link PolicyApiInputDTO}.
     *
     * @param policyApiInputDTO The input dto from which to extract the value.
     * @return The list of group UUIDs.
     */
    private Collection<Long> mergeGroups(final PolicyApiInputDTO policyApiInputDTO) {
        if (policyApiInputDTO.getMergeUuids() != null) {
            return policyApiInputDTO.getMergeUuids().stream().map(Long::valueOf).collect(Collectors.toList());
        } else {
            throw new IllegalArgumentException("PolicyApiInputDTO does not contain a value for mergeUuids");
        }
    }

    /**
     * When the value of capacity is 0 in the UI, the getCapacity() call appears to return null.
     *
     * @param dto The DTO whose capacity should be retrieved.
     * @return The capacity field on the DTO if present. If not, 0.
     */
    private static float getPolicyCapacity(@Nonnull final PolicyApiInputDTO dto) {
        return dto.getCapacity() == null ? 0 : dto.getCapacity();
    }
}
