package com.vmturbo.api.component.external.api.mapper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.dto.policy.PolicyApiDTO;
import com.vmturbo.api.dto.policy.PolicyApiInputDTO;
import com.vmturbo.api.enums.MergePolicyType;
import com.vmturbo.api.enums.PolicyType;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.MergePolicy.MergeType;

/**
 * Conversions between different representations of policies.
 */
public class PolicyMapper {

    /**
     * Mapping {@link MergePolicyType} -> {@link MergeType}.
     */
    public static final BiMap<MergePolicyType, MergeType> MERGE_TYPE_API_TO_PROTO = ImmutableBiMap.of(
            MergePolicyType.Cluster, MergeType.CLUSTER,
            MergePolicyType.DataCenter, MergeType.DATACENTER,
            MergePolicyType.DesktopPool, MergeType.DESKTOP_POOL,
            MergePolicyType.StorageCluster, MergeType.STORAGE_CLUSTER);

    /**
     * The commodity type the UI expects (based on OpsMgr) for discovered policies.
     */
    private static final String DRS_SEGMENTATION_COMMODITY = "DrsSegmentationCommodity";

    private final Logger logger = LogManager.getLogger();

    private GroupMapper groupMapper;

    public PolicyMapper(GroupMapper groupMapper) {
        this.groupMapper = Objects.requireNonNull(groupMapper);
    }

    /**
     * Convert a list of {@link Policy} used by the group component
     * to a {@link PolicyApiDTO} used by the API component.
     *
     * @param policyProto list of placement policies protobuf representations to convert.
     * @param groups collection of groups for conversion
     *         Should contain groups used in the input policies. Others will be ignored
     * @return The converted policies list. Order or policies in the list is guaranteed to match
     *         order of policies in input list
     * @throws ConversionException if error faced converting objects to API DTOs
     * @throws InterruptedException if current thread has been interrupted
     */
    @Nonnull
    public List<PolicyApiDTO> policyToApiDto(@Nonnull final List<PolicyDTO.Policy> policyProto,
            @Nonnull final Collection<Grouping> groups) throws ConversionException, InterruptedException {
        final Map<Long, GroupApiDTO> groupsMap = groupMapper.groupsToGroupApiDto(groups, false);
        final List<PolicyApiDTO> result = new ArrayList<>(policyProto.size());
        for (Policy policy: policyProto) {
            result.add(policyToApiDto(policy, groupsMap));
        }
        return Collections.unmodifiableList(result);
    }

    /**
     * Convert a {@link com.vmturbo.common.protobuf.group.PolicyDTO.Policy} used by the group component
     * to a {@link PolicyApiDTO} used by the API component.
     *
     * @param policyProto The Policy protobuf to convert.
     * @param groupsByID a map from group oid to the group with that oid.
     * @return The converted policy
     */
    @Nonnull
    private PolicyApiDTO policyToApiDto(final PolicyDTO.Policy policyProto,
            final Map<Long, GroupApiDTO> groupsByID) {
        final PolicyApiDTO policyApiDTO = new PolicyApiDTO();

        final PolicyInfo policyInfo = policyProto.getPolicyInfo();
        policyApiDTO.setName(policyInfo.getName());
        policyApiDTO.setDisplayName(policyInfo.getDisplayName());
        policyApiDTO.setUuid(Long.toString(policyProto.getId()));
        policyApiDTO.setEnabled(policyInfo.getEnabled());
        if (policyInfo.hasCommodityType()) {
            policyApiDTO.setCommodityType(policyInfo.getCommodityType());
            if (policyProto.hasTargetId() &&
                    policyInfo.getCommodityType().equals(DRS_SEGMENTATION_COMMODITY)) {
                // This shouldn't really happen, but if it does happen it could cause the UI
                // to fail to detect that the policy is discovered/imported.
                logger.warn("Discovered policy {} has explicit commodity type {}.",
                        policyInfo.getName(), policyInfo.getCommodityType());
            }
        } else if (policyProto.hasTargetId()) {
            // Use a special commodity type (this is also hard-coded
            // in the UI) to indicate that the policy is discovered.
            policyApiDTO.setCommodityType(DRS_SEGMENTATION_COMMODITY);
        }

        switch (policyInfo.getPolicyDetailCase()) {
            case AT_MOST_N:
                final PolicyDTO.PolicyInfo.AtMostNPolicy atMostN = policyInfo.getAtMostN();
                policyApiDTO.setCapacity((int)atMostN.getCapacity());
                policyApiDTO.setType(PolicyType.AT_MOST_N);
                policyApiDTO.setConsumerGroup(getPolicyGroupApiDTO(atMostN.getConsumerGroupId(),
                        policyInfo.getName(), groupsByID));
                policyApiDTO.setProviderGroup(getPolicyGroupApiDTO(atMostN.getProviderGroupId(),
                        policyInfo.getName(), groupsByID));
                break;
            case AT_MOST_NBOUND:
                final PolicyDTO.PolicyInfo.AtMostNBoundPolicy atMostNBound = policyInfo.getAtMostNbound();
                policyApiDTO.setCapacity((int)atMostNBound.getCapacity());
                policyApiDTO.setType(PolicyType.AT_MOST_N_BOUND);
                policyApiDTO.setConsumerGroup(
                        getPolicyGroupApiDTO(atMostNBound.getConsumerGroupId(),
                                policyInfo.getName(), groupsByID));
                policyApiDTO.setProviderGroup(
                        getPolicyGroupApiDTO(atMostNBound.getProviderGroupId(),
                                policyInfo.getName(), groupsByID));
                break;
            case BIND_TO_COMPLEMENTARY_GROUP:
                final PolicyDTO.PolicyInfo.BindToComplementaryGroupPolicy bindToComplementaryGroup =
                        policyInfo.getBindToComplementaryGroup();
                policyApiDTO.setType(PolicyType.BIND_TO_COMPLEMENTARY_GROUP);
                policyApiDTO.setConsumerGroup(
                        getPolicyGroupApiDTO(bindToComplementaryGroup.getConsumerGroupId(),
                                policyInfo.getName(), groupsByID));
                policyApiDTO.setProviderGroup(
                        getPolicyGroupApiDTO(bindToComplementaryGroup.getProviderGroupId(),
                                policyInfo.getName(), groupsByID));
                break;
            case BIND_TO_GROUP:
                final PolicyDTO.PolicyInfo.BindToGroupPolicy bindToGroup = policyInfo.getBindToGroup();
                policyApiDTO.setType(PolicyType.BIND_TO_GROUP);
                policyApiDTO.setConsumerGroup(getPolicyGroupApiDTO(bindToGroup.getConsumerGroupId(),
                        policyInfo.getName(), groupsByID));
                policyApiDTO.setProviderGroup(getPolicyGroupApiDTO(bindToGroup.getProviderGroupId(),
                        policyInfo.getName(), groupsByID));
                break;
            case BIND_TO_GROUP_AND_LICENSE:
                final PolicyDTO.PolicyInfo.BindToGroupAndLicencePolicy bindToGroupAndLicense =
                        policyInfo.getBindToGroupAndLicense();
                policyApiDTO.setType(PolicyType.BIND_TO_GROUP_AND_LICENSE);
                policyApiDTO.setConsumerGroup(
                        getPolicyGroupApiDTO(bindToGroupAndLicense.getConsumerGroupId(),
                                policyInfo.getName(), groupsByID));
                policyApiDTO.setProviderGroup(
                        getPolicyGroupApiDTO(bindToGroupAndLicense.getProviderGroupId(),
                                policyInfo.getName(), groupsByID));
                break;
            case MERGE:
                final PolicyDTO.PolicyInfo.MergePolicy merge = policyInfo.getMerge();
                policyApiDTO.setType(PolicyType.MERGE);
                final MergePolicyType mergeType =
                        MERGE_TYPE_API_TO_PROTO.inverse().get(merge.getMergeType());
                if (mergeType != null) {
                    policyApiDTO.setMergeType(mergeType);
                }
                final List<BaseApiDTO> policyGroups =
                        new ArrayList<>(merge.getMergeGroupIdsCount());
                for (Long groupId: merge.getMergeGroupIdsList()) {
                    final GroupApiDTO group =
                            getPolicyGroupApiDTO(groupId, policyInfo.getName(), groupsByID);
                    policyGroups.add(group);
                }
                policyApiDTO.setMergeGroups(policyGroups);
                break;
            case MUST_RUN_TOGETHER:
                final PolicyDTO.PolicyInfo.MustRunTogetherPolicy mustRunTogether = policyInfo.getMustRunTogether();
                policyApiDTO.setType(PolicyType.MUST_RUN_TOGETHER);
                policyApiDTO.setConsumerGroup(getPolicyGroupApiDTO(mustRunTogether.getGroupId(),
                        policyInfo.getName(), groupsByID));
                break;
            case MUST_NOT_RUN_TOGETHER:
                final PolicyDTO.PolicyInfo.MustNotRunTogetherPolicy mustNotRunTogether =
                        policyInfo.getMustNotRunTogether();
                policyApiDTO.setType(PolicyType.MUST_NOT_RUN_TOGETHER);
                policyApiDTO.setConsumerGroup(getPolicyGroupApiDTO(mustNotRunTogether.getGroupId(),
                        policyInfo.getName(), groupsByID));
                break;
            default:
                // Not supposed to happen
                logger.warn("Unknown policy case " + policyInfo.getPolicyDetailCase());
        }

        return policyApiDTO;
    }

    /**
     * Get group for the given group id and convert to GroupApiDTO. If the group does not exist,
     * return a default GroupApiDTO with only uuid populated.
     *
     * @param groupId id of the group to get
     * @param policyName name of the policy
     * @param groupsByID map of groups by id
     * @return {@link GroupApiDTO}
     */
    private GroupApiDTO getPolicyGroupApiDTO(long groupId, @Nonnull String policyName,
            @Nonnull final Map<Long, GroupApiDTO> groupsByID) {
        final GroupApiDTO groupApiDTO;
        final GroupApiDTO group = groupsByID.get(groupId);
        if (group != null) {
            groupApiDTO = group;
        } else {
            // group may have been deleted, so just create a default group with only uuid
            logger.warn("Group {} for policy \"{}\" not found", groupId, policyName);
            groupApiDTO = new GroupApiDTO();
            groupApiDTO.setUuid(String.valueOf(groupId));
            // clear these fields, otherwise it would be misleading to users
            groupApiDTO.setIsStatic(null);
            groupApiDTO.setLogicalOperator(null);
        }
        return groupApiDTO;
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
        final PolicyInfo.Builder policyInfoBuilder = PolicyInfo.newBuilder();

        if (policyApiDTO.getName() != null) {
            policyInfoBuilder.setName(policyApiDTO.getName());
        } else {
            logger.warn("The 'name' field in PolicyApiDTO is null : " + policyApiDTO);
        }
        policyInfoBuilder.setEnabled(policyApiDTO.isEnabled());
        String commodityType = policyApiDTO.getCommodityType();
        if (commodityType != null) {
            policyInfoBuilder.setCommodityType(commodityType);
        }

        if (policyApiDTO.getType() != null) {
            switch (policyApiDTO.getType()) {
                case BIND_TO_GROUP:
                    policyInfoBuilder.setBindToGroup(bindToGroupPolicy(policyApiDTO));
                    break;
                case BIND_TO_COMPLEMENTARY_GROUP:
                    policyInfoBuilder.setBindToComplementaryGroup(bindToComplementaryGroup(policyApiDTO));
                    break;
                case BIND_TO_GROUP_AND_LICENSE:
                    policyInfoBuilder.setBindToGroupAndLicense(bindToGroupAndLicencePolicy(policyApiDTO));
                    break;
                case MERGE:
                    policyInfoBuilder.setMerge(mergePolicy(policyApiDTO));
                    break;
                case AT_MOST_N:
                    policyInfoBuilder.setAtMostN(atMostNPolicy(policyApiDTO));
                    break;
                case AT_MOST_N_BOUND:
                    policyInfoBuilder.setAtMostNbound(atMostNBoundPolicy(policyApiDTO));
                    break;
                case MUST_RUN_TOGETHER:
                    policyInfoBuilder.setMustRunTogether(mustRunTogetherPolicy(policyApiDTO));
                    break;
                default:
                    logger.error("Unhandled policy type: {}", policyApiDTO.getType());
            }
        } else {
            logger.warn("The 'type' field in PolicyApiDTO is null : " + policyApiDTO);
        }


        final Policy.Builder policyBuilder = Policy.newBuilder()
                .setPolicyInfo(policyInfoBuilder);
        final String uuid = policyApiDTO.getUuid();
        if (uuid != null) {
            policyBuilder.setId(Long.valueOf(uuid));
        }
        return policyBuilder.build();
    }

    @Nonnull
    private PolicyInfo.BindToGroupPolicy bindToGroupPolicy(PolicyApiDTO policyApiDTO) {
        return PolicyInfo.BindToGroupPolicy.newBuilder()
                        .setProviderGroupId(providersId(policyApiDTO))
                        .setConsumerGroupId(consumersId(policyApiDTO))
                        .build();
    }

    @Nonnull
    private PolicyInfo.BindToComplementaryGroupPolicy bindToComplementaryGroup(
                    @Nonnull PolicyApiDTO policyApiDTO) {
        return PolicyInfo.BindToComplementaryGroupPolicy.newBuilder()
                        .setProviderGroupId(providersId(policyApiDTO))
                        .setConsumerGroupId(consumersId(policyApiDTO))
                        .build();
    }

    @Nonnull
    private PolicyInfo.BindToGroupAndLicencePolicy bindToGroupAndLicencePolicy(
                    @Nonnull PolicyApiDTO policyApiDTO) {
        return PolicyInfo.BindToGroupAndLicencePolicy.newBuilder()
                        .setProviderGroupId(providersId(policyApiDTO))
                        .setConsumerGroupId(consumersId(policyApiDTO))
                        .build();
    }

    @Nonnull
    private PolicyInfo.MergePolicy mergePolicy(@Nonnull PolicyApiDTO policyApiDTO) {
        final PolicyInfo.MergePolicy.Builder mergePolicyBuilder =
                        PolicyInfo.MergePolicy.newBuilder();
        final MergeType mergeType = MERGE_TYPE_API_TO_PROTO.get(policyApiDTO.getMergeType());
        if (mergeType != null) {
            mergePolicyBuilder.setMergeType(mergeType);
        }
        mergePolicyBuilder.addAllMergeGroupIds(policyApiDTO.getMergeGroups().stream()
            .map(BaseApiDTO::getUuid)
            .map(Long::valueOf)
            .collect(Collectors.toList()));
        return mergePolicyBuilder.build();
    }

    @Nonnull
    private PolicyInfo.AtMostNPolicy atMostNPolicy(@Nonnull PolicyApiDTO policyApiDTO) {
        return PolicyInfo.AtMostNPolicy.newBuilder()
                        .setCapacity(getPolicyCapacity(policyApiDTO))
                        .setProviderGroupId(providersId(policyApiDTO))
                        .setConsumerGroupId(consumersId(policyApiDTO))
                        .build();
    }

    @Nonnull
    private PolicyInfo.AtMostNBoundPolicy atMostNBoundPolicy(
                    @Nonnull PolicyApiDTO policyApiDTO) {
        return PolicyInfo.AtMostNBoundPolicy.newBuilder()
                        .setCapacity(getPolicyCapacity(policyApiDTO))
                        .setProviderGroupId(providersId(policyApiDTO))
                        .setConsumerGroupId(consumersId(policyApiDTO))
                        .build();
    }

    @Nonnull
    private PolicyInfo.MustRunTogetherPolicy mustRunTogetherPolicy(
                    @Nonnull PolicyApiDTO policyApiDTO) {
        return PolicyInfo.MustRunTogetherPolicy.newBuilder()
                        .setGroupId(consumersId(policyApiDTO))
                        .build();
    }

    @Nonnull
    private PolicyInfo.MustNotRunTogetherPolicy mustNotRunTogetherPolicy(
            @Nonnull PolicyApiDTO policyApiDTO) {
        return PolicyInfo.MustNotRunTogetherPolicy.newBuilder()
                        .setGroupId(consumersId(policyApiDTO))
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
     * to a {@link com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo} used by the group component.
     *
     * @param policyApiInputDTO The Policy input API DTO to convert.
     * @return the converted policy.
     */
    public PolicyDTO.PolicyInfo policyApiInputDtoToProto(final PolicyApiInputDTO policyApiInputDTO) {
        final PolicyDTO.PolicyInfo.Builder inputPolicyBuilder = PolicyDTO.PolicyInfo.newBuilder();

        if (policyApiInputDTO.getPolicyName() != null) {
            inputPolicyBuilder.setName(policyApiInputDTO.getPolicyName());
        } else {
            logger.warn("The 'name' field in PolicyApiInputDTO is null");
        }
        inputPolicyBuilder.setEnabled(policyApiInputDTO.isEnabled());

        if (policyApiInputDTO.getType() != null) {
            long providerId;
            long consumerId;
            float capacity;

            switch (policyApiInputDTO.getType()) {
                case BIND_TO_GROUP:
                    providerId = providersGroupId(policyApiInputDTO);
                    consumerId = consumersGroupId(policyApiInputDTO);
                    final PolicyDTO.PolicyInfo.BindToGroupPolicy bindToGroupPolicy =
                            PolicyDTO.PolicyInfo.BindToGroupPolicy.newBuilder()
                                    .setProviderGroupId(providerId)
                                    .setConsumerGroupId(consumerId)
                                    .build();
                    inputPolicyBuilder.setBindToGroup(bindToGroupPolicy);
                    break;
                case BIND_TO_COMPLEMENTARY_GROUP:
                    providerId = providersGroupId(policyApiInputDTO);
                    consumerId = consumersGroupId(policyApiInputDTO);
                    final PolicyDTO.PolicyInfo.BindToComplementaryGroupPolicy bindToComplementaryGroup =
                            PolicyDTO.PolicyInfo.BindToComplementaryGroupPolicy.newBuilder()
                                    .setProviderGroupId(providerId)
                                    .setConsumerGroupId(consumerId)
                                    .build();
                    inputPolicyBuilder.setBindToComplementaryGroup(bindToComplementaryGroup);
                    break;
                case BIND_TO_GROUP_AND_LICENSE:
                    providerId = providersGroupId(policyApiInputDTO);
                    consumerId = consumersGroupId(policyApiInputDTO);
                    final PolicyDTO.PolicyInfo.BindToGroupAndLicencePolicy bindToGroupAndLicencePolicy =
                            PolicyDTO.PolicyInfo.BindToGroupAndLicencePolicy.newBuilder()
                                    .setProviderGroupId(providerId)
                                    .setConsumerGroupId(consumerId)
                                    .build();
                    inputPolicyBuilder.setBindToGroupAndLicense(bindToGroupAndLicencePolicy);
                    break;
                case MERGE:
                    final PolicyDTO.PolicyInfo.MergePolicy.Builder mergePolicyBuilder =
                            PolicyDTO.PolicyInfo.MergePolicy.newBuilder();
                    final MergeType mergeType =
                            MERGE_TYPE_API_TO_PROTO.get(policyApiInputDTO.getMergeType());
                    if (mergeType != null) {
                        mergePolicyBuilder.setMergeType(mergeType);
                    }
                    mergeGroups(policyApiInputDTO).forEach(mergePolicyBuilder::addMergeGroupIds);
                    inputPolicyBuilder.setMerge(mergePolicyBuilder.build());
                    break;
                case AT_MOST_N:
                    providerId = providersGroupId(policyApiInputDTO);
                    consumerId = consumersGroupId(policyApiInputDTO);
                    capacity = getInputPolicyCapacity(policyApiInputDTO);
                    final PolicyDTO.PolicyInfo.AtMostNPolicy atMostNPolicy =
                            PolicyDTO.PolicyInfo.AtMostNPolicy.newBuilder()
                                    .setCapacity(capacity)
                                    .setProviderGroupId(providerId)
                                    .setConsumerGroupId(consumerId)
                                    .build();
                    inputPolicyBuilder.setAtMostN(atMostNPolicy);
                    break;
                case AT_MOST_N_BOUND:
                    providerId = providersGroupId(policyApiInputDTO);
                    consumerId = consumersGroupId(policyApiInputDTO);
                    capacity = getInputPolicyCapacity(policyApiInputDTO);
                    final PolicyDTO.PolicyInfo.AtMostNBoundPolicy atMostNBoundPolicy =
                            PolicyDTO.PolicyInfo.AtMostNBoundPolicy.newBuilder()
                                    .setCapacity(capacity)
                                    .setProviderGroupId(providerId)
                                    .setConsumerGroupId(consumerId)
                                    .build();
                    inputPolicyBuilder.setAtMostNbound(atMostNBoundPolicy);
                    break;
                case MUST_RUN_TOGETHER:
                    consumerId = consumersGroupId(policyApiInputDTO);
                    final PolicyDTO.PolicyInfo.MustRunTogetherPolicy mustRunTogetherPolicy =
                            PolicyDTO.PolicyInfo.MustRunTogetherPolicy.newBuilder()
                                    .setGroupId(consumerId)
                                    .build();
                    inputPolicyBuilder.setMustRunTogether(mustRunTogetherPolicy);
                    break;
                case MUST_NOT_RUN_TOGETHER:
                    consumerId = consumersGroupId(policyApiInputDTO);
                    final PolicyDTO.PolicyInfo.MustNotRunTogetherPolicy mustNotRunTogetherPolicy =
                            PolicyDTO.PolicyInfo.MustNotRunTogetherPolicy.newBuilder()
                                    .setGroupId(consumerId)
                                    .build();
                    inputPolicyBuilder.setMustNotRunTogether(mustNotRunTogetherPolicy);
                    break;
            }
        } else {
            logger.warn("The 'type' field in PolicyApiInputDTO is null");
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
