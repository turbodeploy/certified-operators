package com.vmturbo.group.policy;

import java.util.Map;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredPolicyInfo;
import com.vmturbo.common.protobuf.group.PolicyDTO.InputPolicy;
import com.vmturbo.common.protobuf.group.PolicyDTO.InputPolicy.AtMostNPolicy;
import com.vmturbo.common.protobuf.group.PolicyDTO.InputPolicy.BindToComplementaryGroupPolicy;
import com.vmturbo.common.protobuf.group.PolicyDTO.InputPolicy.BindToGroupPolicy;
import com.vmturbo.common.protobuf.group.PolicyDTO.InputPolicy.MustRunTogetherPolicy;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.ConstraintType;

/**
 * Map discovered policies (like DRS rules) to instances of {@link InputPolicy}.
 *
 */
public class DiscoveredPoliciesMapper {
    private final Logger logger = LogManager.getLogger();
    private static final String DRS_SEGMENTATION_COMMODITY = "DrsSegmentationCommodity";

    /**
     * Discovered policies reference groups by their IDs (usually the name). We need to map
     * from this ID to the group OID. The groupOids map holds this mapping. It is populated
     * when constructing {@code this} instance.
     */
    private final Map<String, Long> groupOids;

    public DiscoveredPoliciesMapper(Map<String, Long> groupOids) {
        this.groupOids = groupOids;
    }

    private static final String SAME_GROUPS =
                    "Can't create a policy where buyers group is the same as sellers group";
    private static final String NOT_FOUND = "Buyers or Sellers group ID not found";
    private static final String MESSAGE = "{}. Buyers : \"{}\" ({}). Sellers : \"{}\" ({}).";

    /**
     * Convert a discovered policy spec (representing e.g. a DRS rules)
     * to an {@link InputPolicy}.
     *
     * @param spec the discovered policy
     * @return a representation of the policy that can be saved in the DB
     */
    public Optional<InputPolicy> inputPolicy(DiscoveredPolicyInfo spec) {
        Long buyersId = groupOids.get(spec.getBuyersGroupStringId());
        Long sellersId = groupOids.get(spec.getSellersGroupStringId());
        if (sellersId == null || buyersId == null) {
            logger.warn(MESSAGE, NOT_FOUND,
                spec.getBuyersGroupStringId(), buyersId,
                spec.getSellersGroupStringId(), sellersId);
            return Optional.empty();
        }
        if (sellersId ==  buyersId) {
            logger.warn(MESSAGE, SAME_GROUPS,
                spec.getBuyersGroupStringId(), buyersId,
                spec.getSellersGroupStringId(), sellersId);
            return Optional.empty();
        }
        InputPolicy.Builder builder = InputPolicy.newBuilder()
                        .setName(spec.getPolicyName())
                        .setCommodityType(DRS_SEGMENTATION_COMMODITY);
        switch (spec.getConstraintType()) {
            case ConstraintType.BUYER_SELLER_AFFINITY_VALUE:
                return Optional.of(builder.setBindToGroup(BindToGroupPolicy.newBuilder()
                    .setConsumerGroup(buyersId)
                    .setProviderGroup(sellersId)
                    .build()).build());
            case ConstraintType.BUYER_SELLER_ANTI_AFFINITY_VALUE:
                return Optional.of(builder.setBindToComplementaryGroup(
                    BindToComplementaryGroupPolicy.newBuilder()
                        .setConsumerGroup(buyersId)
                        .setProviderGroup(sellersId)
                        .build()).build());
            case ConstraintType.BUYER_BUYER_AFFINITY_VALUE:
                return Optional.of(builder.setMustRunTogether(MustRunTogetherPolicy.newBuilder()
                    .setConsumerGroup(buyersId)
                    .setProviderGroup(sellersId)
                    .build()).build());
            case ConstraintType.BUYER_BUYER_ANTI_AFFINITY_VALUE:
                return Optional.of(builder.setAtMostN(AtMostNPolicy.newBuilder()
                    .setConsumerGroup(buyersId)
                    .setProviderGroup(sellersId)
                    .setCapacity(1)
                    .build()).build());
            default: {
                logger.warn("Constraint type " + spec.getConstraintType() + " is not supported");
                return Optional.empty();
            }
        }
    }
}
