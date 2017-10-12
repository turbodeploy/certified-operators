package com.vmturbo.common.protobuf;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.group.GroupDTO.NameFilter;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyGroupingID;

/**
 * Utilities for dealing with messages defined in group/GroupDTO.proto.
 */
public class GroupDTOUtil {

    /**
     * @param name The name to compare with the filter.
     * @param filter The name filter.
     * @return True if the name matches the filter.
     */
    public static boolean nameFilterMatches(@Nonnull final String name,
                                            @Nonnull final NameFilter filter) {
        final boolean patternMatches = Pattern.matches(filter.getNameRegex(), name);
        return patternMatches ^ filter.getNegateMatch();
    }

    /**
     *
     * @param policy The {@link Policy} object to extract {@link PolicyGroupingID}s from
     * @return List of {@link PolicyGroupingID}s associated with {@link Policy}
     */
    public static List<PolicyGroupingID> retrieveIdsFromPolicy(Policy policy) {
        List<PolicyGroupingID> result = new ArrayList<>();
        switch (policy.getPolicyDetailCase()) {
            case MERGE:
                result.addAll(policy.getMerge().getMergeGroupIdsList());
                break;
            case AT_MOST_N:
                Policy.AtMostNPolicy atMostN = policy.getAtMostN();
                result.add(atMostN.getConsumerGroupId());
                result.add(atMostN.getProviderGroupId());
                break;
            case BIND_TO_GROUP:
                Policy.BindToGroupPolicy bindToGroup = policy.getBindToGroup();
                result.add(bindToGroup.getConsumerGroupId());
                result.add(bindToGroup.getProviderGroupId());
                break;
            case AT_MOST_N_BOUND:
                Policy.AtMostNBoundPolicy atMostNBound = policy.getAtMostNBound();
                result.add(atMostNBound.getConsumerGroupId());
                result.add(atMostNBound.getProviderGroupId());
                break;
            case BIND_TO_GROUP_AND_LICENSE:
                Policy.BindToGroupAndLicencePolicy bindToGroupAndLicense = policy.getBindToGroupAndLicense();
                result.add(bindToGroupAndLicense.getConsumerGroupId());
                result.add(bindToGroupAndLicense.getProviderGroupId());
                break;
            case BIND_TO_GROUP_AND_GEO_REDUNDANCY:
                Policy.BindToGroupAndGeoRedundancyPolicy bindToGroupAndGeoRedundancy =
                        policy.getBindToGroupAndGeoRedundancy();
                result.add(bindToGroupAndGeoRedundancy.getConsumerGroupId());
                result.add(bindToGroupAndGeoRedundancy.getProviderGroupId());
                break;
            case BIND_TO_COMPLEMENTARY_GROUP:
                Policy.BindToComplementaryGroupPolicy bindToComplementaryGroup =
                        policy.getBindToComplementaryGroup();
                result.add(bindToComplementaryGroup.getConsumerGroupId());
                result.add(bindToComplementaryGroup.getProviderGroupId());
                break;
            case MUST_RUN_TOGETHER:
                Policy.MustRunTogetherPolicy mustRunTogether = policy.getMustRunTogether();
                result.add(mustRunTogether.getConsumerGroupId());
                result.add(mustRunTogether.getProviderGroupId());
                break;
        }
        return result;
    }
}
