package com.vmturbo.group.policy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.Table;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.jooq.DSLContext;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.components.api.FormattedString;
import com.vmturbo.group.group.GroupDAO;

/**
 * Utility class containing the logic to validate a {@link PolicyDTO.Policy} before saving it in
 * the database.
 */
public class PolicyValidator {
    private static final String CONSUMER_GROUP = "consumer group id";
    private static final String PROVIDER_GROUP = "provider group id";
    private static final String CAPACITY = "capacity";
    private static final String GROUP_ID = "Group Id";
    private static final String PROVIDER_ENTITY_TYPE = "Provider entity type";
    private static final String MERGE_TYPE = "Merge Type";

    private final GroupDAO groupDAO;

    /**
     * Constructs a policy validator.
     *
     * @param groupDAO group store to use for querying expected members of a group
     */
    public PolicyValidator(final GroupDAO groupDAO) {
        this.groupDAO = groupDAO;
    }

    /**
     * Validate the input policy.
     *
     * @param context The transaction context to use. Helpful to ensure validation happens in the
     *                same transaction as the creation/update of the policy.
     * @param policy  The policy to validate.
     * @throws InvalidPolicyException If the policy is not valid.
     */
    void validatePolicy(@Nonnull final DSLContext context,
                        @Nonnull final PolicyDTO.Policy policy)
        throws InvalidPolicyException {
        List<String> errors = new ArrayList<>();
        if (!policy.hasPolicyInfo()) {
            throw new InvalidPolicyException(Collections.singletonList("The policy should have information "
                + "part."));
        }
        final PolicyDTO.PolicyInfo policyInfo = policy.getPolicyInfo();

        if (!policyInfo.hasName() || StringUtils.isBlank(policyInfo.getName())) {
            errors.add("The policy should have a name.");
        }

        if (!policyInfo.hasDisplayName() || StringUtils.isBlank(policyInfo.getDisplayName())) {
            errors.add("The policy should have a display name.");
        }

        // Checks if the policy type specific has proper values
        Collection<String> policyTypeErrors = checkPolicyTypeSpecificFields(policyInfo);
        errors.addAll(policyTypeErrors);

        if (policyTypeErrors.isEmpty()) {
            errors.addAll(checkPolicyGroupInfo(context, policy));
        }

        if (!CollectionUtils.isEmpty(errors)) {
            throw new InvalidPolicyException(errors);
        }
    }

    private Collection<String> checkPolicyGroupInfo(@Nonnull final DSLContext context,
                                                    PolicyDTO.Policy policy) {
        final Set<Long> referencedGroups = GroupProtoUtil.getPolicyGroupIds(policy);
        final Table<Long, MemberType, Boolean> expectedMemberTypes =
            groupDAO.getExpectedMemberTypes(context, referencedGroups);
        List<String> errors = new ArrayList<>();

        for (Long referencedGroup : referencedGroups) {
            Map<MemberType, Boolean> memberTypesForGroup = expectedMemberTypes.row(referencedGroup);
            String error = null;
            if (memberTypesForGroup.isEmpty()) {
                // The contract of the table is that an empty map is returned for a non-existing
                // row, so we can't distinguish between a missing group vs. a group that has no
                // expected member types (which may or may not happen).
                error = "Referenced group " + referencedGroup + " doesn't exist, or has no expected member types.";
            } else if (memberTypesForGroup.size() > 1) {
                // We don't currently support creating policies on groups with more than one
                // entity type.
                error = "Referenced group " + referencedGroup + " has multiple member types.";
            }

            if (error != null) {
                errors.add(error);
            }
        }
        return errors;
    }

    private Collection<String> checkPolicyTypeSpecificFields(@Nonnull PolicyDTO.PolicyInfo policyInfo) {
        List<String> errors = new ArrayList<>();
        final PolicyDTO.PolicyInfo.PolicyDetailCase policyCase = policyInfo.getPolicyDetailCase();
        switch (policyCase) {
            case AT_MOST_N:
                createErrorIf(policyInfo.getAtMostN().hasConsumerGroupId(), policyCase.name(),
                    CONSUMER_GROUP).ifPresent(errors::add);
                createErrorIf(policyInfo.getAtMostN().hasProviderGroupId(), policyCase.name(),
                    PROVIDER_GROUP).ifPresent(errors::add);
                createErrorIf(policyInfo.getAtMostN().hasCapacity(), policyCase.name(),
                    CAPACITY).ifPresent(errors::add);
                break;
            case AT_MOST_NBOUND:
                createErrorIf(policyInfo.getAtMostNbound().hasConsumerGroupId(), policyCase.name(),
                    CONSUMER_GROUP).ifPresent(errors::add);
                createErrorIf(policyInfo.getAtMostNbound().hasProviderGroupId(), policyCase.name(),
                    PROVIDER_GROUP).ifPresent(errors::add);
                createErrorIf(policyInfo.getAtMostNbound().hasCapacity(), policyCase.name(),
                    CAPACITY).ifPresent(errors::add);
                break;
            case BIND_TO_COMPLEMENTARY_GROUP:
                createErrorIf(policyInfo.getBindToComplementaryGroup().hasConsumerGroupId(), policyCase.name(),
                    CONSUMER_GROUP).ifPresent(errors::add);
                createErrorIf(policyInfo.getBindToComplementaryGroup().hasProviderGroupId(), policyCase.name(),
                    PROVIDER_GROUP).ifPresent(errors::add);
                break;
            case BIND_TO_GROUP:
                createErrorIf(policyInfo.getBindToGroup().hasConsumerGroupId(), policyCase.name(),
                    CONSUMER_GROUP).ifPresent(errors::add);
                createErrorIf(policyInfo.getBindToGroup().hasProviderGroupId(), policyCase.name(),
                    PROVIDER_GROUP).ifPresent(errors::add);
                break;
            case BIND_TO_GROUP_AND_LICENSE:
                createErrorIf(policyInfo.getBindToGroupAndLicense().hasConsumerGroupId(), policyCase.name(),
                    CONSUMER_GROUP).ifPresent(errors::add);
                createErrorIf(policyInfo.getBindToGroupAndLicense().hasProviderGroupId(), policyCase.name(),
                    PROVIDER_GROUP).ifPresent(errors::add);
                break;
            case BIND_TO_GROUP_AND_GEO_REDUNDANCY:
                createErrorIf(policyInfo.getBindToGroupAndGeoRedundancy().hasConsumerGroupId(), policyCase.name(),
                    CONSUMER_GROUP).ifPresent(errors::add);
                createErrorIf(policyInfo.getBindToGroupAndGeoRedundancy().hasProviderGroupId(), policyCase.name(),
                    PROVIDER_GROUP).ifPresent(errors::add);
                break;
            case MERGE:
                createErrorIf(policyInfo.getMerge().hasMergeType(), policyCase.name(),
                    MERGE_TYPE).ifPresent(errors::add);
                break;
            case MUST_RUN_TOGETHER:
                createErrorIf(policyInfo.getMustRunTogether().hasGroupId(), policyCase.name(),
                    GROUP_ID).ifPresent(errors::add);
                createErrorIf(policyInfo.getMustRunTogether().hasProviderEntityType(), policyCase.name(),
                    PROVIDER_ENTITY_TYPE).ifPresent(errors::add);
                break;
            case MUST_NOT_RUN_TOGETHER:
                createErrorIf(policyInfo.getMustNotRunTogether().hasGroupId(), policyCase.name(),
                    GROUP_ID).ifPresent(errors::add);
                createErrorIf(policyInfo.getMustNotRunTogether().hasProviderEntityType(), policyCase.name(),
                    PROVIDER_ENTITY_TYPE).ifPresent(errors::add);
                break;
            default:
                errors.add("The policy case \'" + policyCase + "\' is not supported.");
        }
        return errors;
    }

    private Optional<String> createErrorIf(boolean notAdd, String policyType, String fieldName) {
        if (!notAdd) {
            return Optional.of(FormattedString.format("\'{}\' should have the \'{}\' field set.",
                policyType, fieldName));
        }
        return Optional.empty();
    }
}
