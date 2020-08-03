package com.vmturbo.cloud.commitment.analysis.topology;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang3.tuple.Pair;

import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.components.api.SetOnce;
import com.vmturbo.group.api.GroupAndMembers;
import com.vmturbo.group.api.GroupMemberRetriever;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * An implementation of {@link BillingFamilyRetriever}, which will cache the billing family groups
 * on first query for subsequent queries.
 */
public class BillingFamilyRetrieverImpl implements BillingFamilyRetriever {


    private final SetOnce<Map<Long, GroupAndMembers>> billingFamiliesByAccountId
            = new SetOnce<>();

    private final GroupMemberRetriever groupMemberRetriever;


    /**
     * Construct a new instance.
     * @param groupMemberRetriever The {@link GroupMemberRetriever}, used for querying the group
     *                             component for billing family groups.
     */
    public BillingFamilyRetrieverImpl(@Nonnull GroupMemberRetriever groupMemberRetriever) {

        this.groupMemberRetriever = Objects.requireNonNull(groupMemberRetriever);
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Optional<GroupAndMembers> getBillingFamilyForAccount(final long accountOid) {

        final Map<Long, GroupAndMembers> billingFamilyMap =
                billingFamiliesByAccountId.ensureSet(this::createBillingFamilyAccountMap);

        return Optional.ofNullable(billingFamilyMap.get(accountOid));

    }

    private Map<Long, GroupAndMembers> createBillingFamilyAccountMap() {

        final List<GroupAndMembers> billingFamilies = groupMemberRetriever
                .getGroupsWithMembers(GetGroupsRequest.newBuilder()
                        .setGroupFilter(GroupFilter.newBuilder()
                                .setGroupType(GroupType.BILLING_FAMILY)
                                .build())
                        .build());

        return billingFamilies.stream()
                .flatMap(bf -> bf.members()
                        .stream()
                        .map(account -> Pair.of(account, bf)))
                .collect(ImmutableMap.toImmutableMap(
                        Pair::getKey,
                        Pair::getValue));

    }
}
