package com.vmturbo.api.component.external.api.mapper;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.policy.PolicyApiDTO;
import com.vmturbo.api.dto.policy.PolicyApiInputDTO;
import com.vmturbo.api.enums.MergePolicyType;
import com.vmturbo.api.enums.PolicyType;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Conversions between different representations of policies.
 */
public class PolicyMapper {
    private static final Logger LOG = LogManager.getLogger();

    private GroupMapper groupMapper;

    public PolicyMapper(GroupMapper groupMapper) {
        this.groupMapper = groupMapper;
    }

    /**
     * Convert a {@link com.vmturbo.common.protobuf.group.PolicyDTO.Policy} used by the group component
     * to a {@link PolicyApiDTO} used by the API component.
     *
     * @param policyProto The Policy protobuf to convert.
     * @param groupsByID a map from group oid to the group with that oid.
     *     May only contain only the relevant groups.
     * @return The converted policy.
     */
    public PolicyApiDTO policyToApiDto(final PolicyDTO.Policy policyProto,
                                       final Map<Long, Group> groupsByID) {
        final PolicyApiDTO policyApiDTO = new PolicyApiDTO();

        policyApiDTO.setName(policyProto.getName());
        policyApiDTO.setDisplayName(policyProto.getName());
        policyApiDTO.setUuid(Long.toString(policyProto.getId()));
        policyApiDTO.setEnabled(policyProto.getEnabled());
        policyApiDTO.setCommodityType(policyProto.getCommodityType());

        Group consumerGrouping;
        Group providerGrouping;
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
                policyApiDTO.setCapacity((int)atMostNBound.getCapacity());
                policyApiDTO.setType(PolicyType.AT_MOST_N_BOUND);
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
                final List<Group> mergeGroupings = merge.getMergeGroupIdsList().stream()
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
                        .filter(Objects::nonNull)
                        .map(groupMapper::toGroupApiDto)
                        .collect(Collectors.toList()));
                break;
            case MUST_RUN_TOGETHER:
                final PolicyDTO.Policy.MustRunTogetherPolicy mustRunTogether = policyProto.getMustRunTogether();
                consumerGrouping = groupsByID.get(mustRunTogether.getGroupId());
                policyApiDTO.setType(PolicyType.MUST_RUN_TOGETHER);
                policyApiDTO.setConsumerGroup(groupMapper.toGroupApiDto(consumerGrouping));
                break;
            case MUST_NOT_RUN_TOGETHER:
                final PolicyDTO.Policy.MustNotRunTogetherPolicy mustNotRunTogether =
                        policyProto.getMustNotRunTogether();
                consumerGrouping = groupsByID.get(mustNotRunTogether.getGroupId());
                // FIXME
                // right now we are converting an internal MustNotRunTogether policy into a AtMostN
                // because the api is not supporting it yet.
                policyApiDTO.setType(PolicyType.AT_MOST_N);
                // and this is also why we cannot get the capacity from the original policy
                // so we are setting it here directly
                policyApiDTO.setCapacity(1);
                policyApiDTO.setConsumerGroup(groupMapper.toGroupApiDto(consumerGrouping));
                break;
            default:
                // Not supposed to happen
                LOG.warn("Unknown policy case " + policyProto.getPolicyDetailCase());
        }

        return policyApiDTO;
    }

    /**
     * Convert a {@link @link PolicyApiDTO} used by the API component to a
     * {@link com.vmturbo.common.protobuf.group.PolicyDTO.Policy} used by the group component.
     *
     * @param policyApiDTO The policy API DTO to convert.
     * @return The converted policy.
     */
    @Nonnull
    public PolicyDTO.Policy policyApiDtoToProto(@Nonnull final PolicyApiDTO policyApiDTO) {
        final PolicyDTO.Policy.Builder policyBuilder = PolicyDTO.Policy.newBuilder();

        if (policyApiDTO.getName() != null) {
            policyBuilder.setName(policyApiDTO.getName());
        } else {
            LOG.warn("The 'name' field in PolicyApiDTO is null : " + policyApiDTO);
        }
        String uuid = policyApiDTO.getUuid();
        if (uuid != null) {
            policyBuilder.setId(Long.valueOf(uuid));
        }
        policyBuilder.setEnabled(policyApiDTO.isEnabled());
        String commodityType = policyApiDTO.getCommodityType();
        if (commodityType != null) {
            policyBuilder.setCommodityType(commodityType);
        }

        if (policyApiDTO.getType() != null) {
            switch (policyApiDTO.getType()) {
                case BIND_TO_GROUP:
                    policyBuilder.setBindToGroup(bindToGroupPolicy(policyApiDTO));
                    break;
                case BIND_TO_COMPLEMENTARY_GROUP:
                    policyBuilder.setBindToComplementaryGroup(bindToComplementaryGroup(policyApiDTO));
                    break;
                case BIND_TO_GROUP_AND_LICENSE:
                    policyBuilder.setBindToGroupAndLicense(bindToGroupAndLicencePolicy(policyApiDTO));
                    break;
                case MERGE:
                    policyBuilder.setMerge(mergePolicy(policyApiDTO));
                    break;
                case AT_MOST_N:
                    policyBuilder.setAtMostN(atMostNPolicy(policyApiDTO));
                    break;
                case AT_MOST_N_BOUND:
                    policyBuilder.setAtMostNbound(atMostNBoundPolicy(policyApiDTO));
                    break;
                case MUST_RUN_TOGETHER:
                    policyBuilder.setMustRunTogether(mustRunTogetherPolicy(policyApiDTO));
                    break;
                case BIND_TO_GROUP_AND_GEO_REDUNDANCY:
                    policyBuilder.setBindToGroupAndGeoRedundancy(
                        bindToGroupAndGeoRedundancyPolicy(policyApiDTO));
                    break;
            }
        } else {
            LOG.warn("The 'type' field in PolicyApiDTO is null : " + policyApiDTO);
        }

        return policyBuilder.build();
    }

    @Nonnull
    private PolicyDTO.Policy.BindToGroupPolicy bindToGroupPolicy(PolicyApiDTO policyApiDTO) {
        return PolicyDTO.Policy.BindToGroupPolicy.newBuilder()
                        .setProviderGroupId(providersId(policyApiDTO))
                        .setConsumerGroupId(consumersId(policyApiDTO))
                        .build();
    }

    @Nonnull
    private PolicyDTO.Policy.BindToComplementaryGroupPolicy bindToComplementaryGroup(
                    @Nonnull PolicyApiDTO policyApiDTO) {
        return PolicyDTO.Policy.BindToComplementaryGroupPolicy.newBuilder()
                        .setProviderGroupId(providersId(policyApiDTO))
                        .setConsumerGroupId(consumersId(policyApiDTO))
                        .build();
    }

    @Nonnull
    private PolicyDTO.Policy.BindToGroupAndLicencePolicy bindToGroupAndLicencePolicy(
                    @Nonnull PolicyApiDTO policyApiDTO) {
        return PolicyDTO.Policy.BindToGroupAndLicencePolicy.newBuilder()
                        .setProviderGroupId(providersId(policyApiDTO))
                        .setConsumerGroupId(consumersId(policyApiDTO))
                        .build();
    }

    @Nonnull
    private PolicyDTO.Policy.MergePolicy mergePolicy(@Nonnull PolicyApiDTO policyApiDTO) {
        final PolicyDTO.Policy.MergePolicy.Builder mergePolicyBuilder =
                        PolicyDTO.Policy.MergePolicy.newBuilder();
        switch (policyApiDTO.getMergeType()) {
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
        mergePolicyBuilder.addAllMergeGroupIds(policyApiDTO.getMergeGroups().stream()
            .map(BaseApiDTO::getUuid)
            .map(Long::valueOf)
            .collect(Collectors.toList()));
        return mergePolicyBuilder.build();
    }

    @Nonnull
    private PolicyDTO.Policy.AtMostNPolicy atMostNPolicy(@Nonnull PolicyApiDTO policyApiDTO) {
        return PolicyDTO.Policy.AtMostNPolicy.newBuilder()
                        .setCapacity(getPolicyCapacity(policyApiDTO))
                        .setProviderGroupId(providersId(policyApiDTO))
                        .setConsumerGroupId(consumersId(policyApiDTO))
                        .build();
    }

    @Nonnull
    private PolicyDTO.Policy.AtMostNBoundPolicy atMostNBoundPolicy(
                    @Nonnull PolicyApiDTO policyApiDTO) {
        return PolicyDTO.Policy.AtMostNBoundPolicy.newBuilder()
                        .setCapacity(getPolicyCapacity(policyApiDTO))
                        .setProviderGroupId(providersId(policyApiDTO))
                        .setConsumerGroupId(consumersId(policyApiDTO))
                        .build();
    }

    @Nonnull
    private PolicyDTO.Policy.MustRunTogetherPolicy mustRunTogetherPolicy(
                    @Nonnull PolicyApiDTO policyApiDTO) {
        return PolicyDTO.Policy.MustRunTogetherPolicy.newBuilder()
                        .setGroupId(consumersId(policyApiDTO))
                        .build();
    }

    @Nonnull
    private PolicyDTO.Policy.BindToGroupAndGeoRedundancyPolicy bindToGroupAndGeoRedundancyPolicy(
                    @Nonnull PolicyApiDTO policyApiDTO) {
        return PolicyDTO.Policy.BindToGroupAndGeoRedundancyPolicy.newBuilder()
                        .setProviderGroupId(providersId(policyApiDTO))
                        .setConsumerGroupId(consumersId(policyApiDTO))
                        .build();

    }

    /**
     * Get the capacity value in a policy.
     *
     * @param dto The policy from which to retrieve the capacity.
     * @return The capacity field in the DTO if present, zero otherwise.
     */
    private float getPolicyCapacity(PolicyApiDTO dto) {
        Integer capacity = dto.getCapacity();
        return  capacity == null ? 0 : capacity;
    }

    /**
     * Extract the consumers group ID from a {@link PolicyApiDTO}.
     *
     * @param policyApiDTO The input dto from which to extract the value.
     * @return The consumers group id.
     */
    private long consumersId(PolicyApiDTO policyApiDTO) {
        return Long.valueOf(policyApiDTO.getConsumerGroup().getUuid());
    }

    /**
     * Extract the providers group ID from a {@link PolicyApiDTO}.
     *
     * @param policyApiDTO The input dto from which to extract the value.
     * @return The consumers group id.
     */
    private long providersId(PolicyApiDTO policyApiDTO) {
        return Long.valueOf(policyApiDTO.getProviderGroup().getUuid());
    }

    /**
     * Convert a {@link PolicyApiInputDTO} used by the API component
     * to a {@link com.vmturbo.common.protobuf.group.PolicyDTO.InputPolicy} used by the group component.
     *
     * @param policyApiInputDTO The Policy input API DTO to convert.
     * @return the converted policy.
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
                    providerId = providersGroupId(policyApiInputDTO);
                    consumerId = consumersGroupId(policyApiInputDTO);
                    final PolicyDTO.InputPolicy.BindToGroupPolicy bindToGroupPolicy =
                            PolicyDTO.InputPolicy.BindToGroupPolicy.newBuilder()
                                    .setProviderGroup(providerId)
                                    .setConsumerGroup(consumerId)
                                    .build();
                    inputPolicyBuilder.setBindToGroup(bindToGroupPolicy);
                    break;
                case BIND_TO_COMPLEMENTARY_GROUP:
                    providerId = providersGroupId(policyApiInputDTO);
                    consumerId = consumersGroupId(policyApiInputDTO);
                    final PolicyDTO.InputPolicy.BindToComplementaryGroupPolicy bindToComplementaryGroup =
                            PolicyDTO.InputPolicy.BindToComplementaryGroupPolicy.newBuilder()
                                    .setProviderGroup(providerId)
                                    .setConsumerGroup(consumerId)
                                    .build();
                    inputPolicyBuilder.setBindToComplementaryGroup(bindToComplementaryGroup);
                    break;
                case BIND_TO_GROUP_AND_LICENSE:
                    providerId = providersGroupId(policyApiInputDTO);
                    consumerId = consumersGroupId(policyApiInputDTO);
                    final PolicyDTO.InputPolicy.BindToGroupAndLicencePolicy bindToGroupAndLicencePolicy =
                            PolicyDTO.InputPolicy.BindToGroupAndLicencePolicy.newBuilder()
                                    .setProviderGroup(providerId)
                                    .setConsumerGroup(consumerId)
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
                    mergeGroups(policyApiInputDTO).forEach(mergePolicyBuilder::addMergeGroups);
                    inputPolicyBuilder.setMerge(mergePolicyBuilder.build());
                    break;
                case AT_MOST_N:
                    providerId = providersGroupId(policyApiInputDTO);
                    consumerId = consumersGroupId(policyApiInputDTO);
                    capacity = getInputPolicyCapacity(policyApiInputDTO);
                    final PolicyDTO.InputPolicy.AtMostNPolicy atMostNPolicy =
                            PolicyDTO.InputPolicy.AtMostNPolicy.newBuilder()
                                    .setCapacity(capacity)
                                    .setProviderGroup(providerId)
                                    .setConsumerGroup(consumerId)
                                    .build();
                    inputPolicyBuilder.setAtMostN(atMostNPolicy);
                    break;
                case AT_MOST_N_BOUND:
                    providerId = providersGroupId(policyApiInputDTO);
                    consumerId = consumersGroupId(policyApiInputDTO);
                    capacity = getInputPolicyCapacity(policyApiInputDTO);
                    final PolicyDTO.InputPolicy.AtMostNBoundPolicy atMostNBoundPolicy =
                            PolicyDTO.InputPolicy.AtMostNBoundPolicy.newBuilder()
                                    .setCapacity(capacity)
                                    .setProviderGroup(providerId)
                                    .setConsumerGroup(consumerId)
                                    .build();
                    inputPolicyBuilder.setAtMostNbound(atMostNBoundPolicy);
                    break;
                case MUST_RUN_TOGETHER:
                    providerId = providersGroupId(policyApiInputDTO);
                    consumerId = consumersGroupId(policyApiInputDTO);
                    final PolicyDTO.InputPolicy.MustRunTogetherPolicy mustRunTogetherPolicy =
                            PolicyDTO.InputPolicy.MustRunTogetherPolicy.newBuilder()
                                    .setGroup(consumerId)
                                    .build();
                    inputPolicyBuilder.setMustRunTogether(mustRunTogetherPolicy);
                    break;
                case BIND_TO_GROUP_AND_GEO_REDUNDANCY:
                    providerId = providersGroupId(policyApiInputDTO);
                    consumerId = consumersGroupId(policyApiInputDTO);
                    final PolicyDTO.InputPolicy.BindToGroupAndGeoRedundancyPolicy bindToGroupAndGeoRedundancyPolicy =
                            PolicyDTO.InputPolicy.BindToGroupAndGeoRedundancyPolicy.newBuilder()
                                    .setProviderGroup(providerId)
                                    .setConsumerGroup(consumerId)
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
     * Extract the providers group ID from a {@link PolicyApiInputDTO}.
     *
     * @param policyApiInputDTO The input dto from which to extract the value.
     * @return The providers group id.
     */
    private long providersGroupId(final PolicyApiInputDTO policyApiInputDTO) {
        if (policyApiInputDTO.getSellerUuid() != null) {
            return Long.valueOf(policyApiInputDTO.getSellerUuid());
        } else {
            throw new IllegalArgumentException("PolicyApiInputDTO does not contain a seller UUID : "
                            + policyApiInputDTO);
        }
    }

    /**
     * Extract the consumers group ID from a {@link PolicyApiInputDTO}.
     *
     * @param policyApiInputDTO The input dto from which to extract the value.
     * @return The consumers group id.
     */
    private long consumersGroupId(final PolicyApiInputDTO policyApiInputDTO) {
        if (policyApiInputDTO.getBuyerUuid() != null) {
            return Long.valueOf(policyApiInputDTO.getBuyerUuid());
        } else {
            throw new IllegalArgumentException("PolicyApiInputDTO does not contain a buyer UUID : "
                            + policyApiInputDTO);
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
            return policyApiInputDTO.getMergeUuids().stream()
                            .map(Long::valueOf).collect(Collectors.toList());
        } else {
            throw new IllegalArgumentException("PolicyApiInputDTO does not contain a value for mergeUuids : "
                            + policyApiInputDTO);
        }
    }

    /**
     * When the value of capacity is 0 in the UI, the getCapacity() call appears to return null.
     *
     * @param dto The DTO whose capacity should be retrieved.
     * @return The capacity field on the DTO if present, zero otherwise.
     */
    private static float getInputPolicyCapacity(@Nonnull final PolicyApiInputDTO dto) {
        return dto.getCapacity() == null ? 0 : dto.getCapacity();
    }
}
