package com.vmturbo.cloud.common.topology;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang3.tuple.Pair;
import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
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

    private final SetOnce<BillingFamilyData> billingFamilyData = new SetOnce<>();

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
                billingFamilyData.ensureSet(this::createBillingFamilyData)
                        .billingFamilyByAccountIdMap();

        return Optional.ofNullable(billingFamilyMap.get(accountOid));

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<GroupAndMembers> getBillingFamilyById(long billingFamilyId) {
        final Map<Long, GroupAndMembers> billingFamilyMap =
                billingFamilyData.ensureSet(this::createBillingFamilyData)
                        .billingFamilyByIdMap();

        return Optional.ofNullable(billingFamilyMap.get(billingFamilyId));
    }

    @Nonnull
    private BillingFamilyData createBillingFamilyData() {

        final List<GroupAndMembers> billingFamilies = groupMemberRetriever
                .getGroupsWithMembers(GetGroupsRequest.newBuilder()
                        .setGroupFilter(GroupFilter.newBuilder()
                                .setGroupType(GroupType.BILLING_FAMILY)
                                .build())
                        .build());

        final Map<Long, GroupAndMembers> billingFamilyByIdMap = billingFamilies.stream()
                .collect(ImmutableMap.toImmutableMap(
                        bf -> bf.group().getId(),
                        Function.identity()));

        final Map<Long, GroupAndMembers> billingFamilyByAccountIdMap = billingFamilies.stream()
                .flatMap(bf -> bf.members()
                        .stream()
                        .map(account -> Pair.of(account, bf)))
                .collect(ImmutableMap.toImmutableMap(
                        Pair::getKey,
                        Pair::getValue));

        return BillingFamilyData.builder()
                .billingFamilyByIdMap(billingFamilyByIdMap)
                .billingFamilyByAccountIdMap(billingFamilyByAccountIdMap)
                .build();
    }

    /**
     * A data class containing information mapping billing families to queryable data.
     */
    @HiddenImmutableImplementation
    @Immutable
    interface BillingFamilyData {

        Map<Long, GroupAndMembers> billingFamilyByIdMap();

        Map<Long, GroupAndMembers> billingFamilyByAccountIdMap();

        static Builder builder() {
            return new Builder();
        }

        /**
         * A builder class for constructing {@link BillingFamilyData} instances.
         */
        class Builder extends ImmutableBillingFamilyData.Builder {}
    }
}
