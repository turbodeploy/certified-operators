package com.vmturbo.group.policy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.Table;

import org.apache.commons.collections4.CollectionUtils;
import org.jooq.DSLContext;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.group.group.GroupDAO;

/**
 * Utility class containing the logic to validate a {@link PolicyDTO.Policy} before saving it in
 * the database.
 */
public class PolicyValidator {

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
     * @param policy The policy to validate.
     * @throws InvalidPolicyException If the policy is not valid.
     */
    void validatePolicy(@Nonnull final DSLContext context,
                        @Nonnull final PolicyDTO.Policy policy)
            throws InvalidPolicyException {

        final Set<Long> referencedGroups = GroupProtoUtil.getPolicyGroupIds(policy);
        final Table<Long, MemberType, Boolean> expectedMemberTypes =
            groupDAO.getExpectedMemberTypes(context, referencedGroups);
        List<String> errors = null;
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
                if (errors == null) {
                    errors = new ArrayList<>();
                }
                errors.add(error);
            }
        }

        if (!CollectionUtils.isEmpty(errors)) {
            throw new InvalidPolicyException(errors);
        }
    }
}
