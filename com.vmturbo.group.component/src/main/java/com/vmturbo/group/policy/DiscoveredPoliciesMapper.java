package com.vmturbo.group.policy;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredPolicyInfo;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.BindToComplementaryGroupPolicy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.BindToGroupPolicy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.MustNotRunTogetherPolicy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.MustRunTogetherPolicy;
import com.vmturbo.group.common.Truncator;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.ConstraintType;

/**
 * Map discovered policies (like DRS rules) to instances of {@link PolicyInfo}.
 */
public class DiscoveredPoliciesMapper {

    private final Logger logger = LogManager.getLogger();

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
    private static final String BUYER_BUYER_NOT_FOUND = "Buyers group ID not found";
    private static final String BUYER_BUYER_MESSAGE = "{}. Buyers : \"{}\" ({}).";

    /**
     * Convert a discovered policy spec (representing e.g. a DRS rules)
     * to an {@link PolicyInfo}.
     *
     * @param spec the discovered policy
     * @return a representation of the policy that can be saved in the DB
     */
    public Optional<PolicyInfo> inputPolicy(DiscoveredPolicyInfo spec) {

        final int constraintType = spec.getConstraintType();
        boolean isBuyerBuyerPolicy =
                constraintType == ConstraintType.BUYER_BUYER_AFFINITY_VALUE ||
                constraintType == ConstraintType.BUYER_BUYER_ANTI_AFFINITY_VALUE;

        final Long buyersId = groupOids.get(spec.getBuyersGroupStringId());

        // the seller might not be present in the spec
        Long sellersId = null;
        if (spec.hasSellersGroupStringId()) {
            sellersId = groupOids.get(spec.getSellersGroupStringId());
        }

        // TODO - This error handling is very suspicious - static analysis says
        // a null sellersId may make it through. But not sure what the right solution is.

        // check all expected groups are found
        if ((sellersId == null || buyersId == null) && !isBuyerBuyerPolicy) {
            logger.warn(MESSAGE, NOT_FOUND,
                spec.getBuyersGroupStringId(), buyersId,
                spec.getSellersGroupStringId(), sellersId);
            return Optional.empty();
        }
        if (buyersId == null && isBuyerBuyerPolicy) {
            logger.warn(BUYER_BUYER_MESSAGE, BUYER_BUYER_NOT_FOUND,
                    spec.getBuyersGroupStringId(), buyersId);
            return Optional.empty();
        }
        if (Objects.equals(sellersId, buyersId)) {
            logger.warn(MESSAGE, SAME_GROUPS,
                spec.getBuyersGroupStringId(), buyersId,
                spec.getSellersGroupStringId(), sellersId);
            return Optional.empty();
        }

        // create the policy
        final String policyDisplayName =
            Truncator.truncatePolicyDisplayName(spec.getPolicyDisplayName(), true);
        final String policyName =
            Truncator.truncatePolicyName(spec.getPolicyName(), true);
        final PolicyInfo.Builder builder = PolicyInfo.newBuilder()
                        .setDisplayName(policyDisplayName)
                        .setName(policyName);
        switch (spec.getConstraintType()) {
            case ConstraintType.BUYER_SELLER_AFFINITY_VALUE:
                return Optional.of(builder.setBindToGroup(BindToGroupPolicy.newBuilder()
                    .setConsumerGroupId(buyersId)
                    .setProviderGroupId(sellersId)
                    .build()).build());
            case ConstraintType.BUYER_SELLER_ANTI_AFFINITY_VALUE:
                return Optional.of(builder.setBindToComplementaryGroup(
                    BindToComplementaryGroupPolicy.newBuilder()
                        .setConsumerGroupId(buyersId)
                        .setProviderGroupId(sellersId)
                        .build()).build());
            case ConstraintType.BUYER_BUYER_AFFINITY_VALUE:
                return Optional.of(builder.setMustRunTogether(MustRunTogetherPolicy.newBuilder()
                    .setGroupId(buyersId)
                    // for now we are assuming that every buyer_buyer affinity is on hosts
                    .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                    .build()).build());
            case ConstraintType.BUYER_BUYER_ANTI_AFFINITY_VALUE:
                return Optional.of(builder.setMustNotRunTogether(MustNotRunTogetherPolicy.newBuilder()
                    .setGroupId(buyersId)
                    // for now we are assuming that every buyer_buyer anti-affinity is on hosts
                    .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                    .build()).build());
            default: {
                logger.warn("Constraint type " + spec.getConstraintType() + " is not supported");
                return Optional.empty();
            }
        }
    }
}
