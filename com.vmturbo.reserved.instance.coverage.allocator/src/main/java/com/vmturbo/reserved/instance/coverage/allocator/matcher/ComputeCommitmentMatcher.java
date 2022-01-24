package com.vmturbo.reserved.instance.coverage.allocator.matcher;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.common.commitment.aggregator.AggregationInfo;
import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregate;
import com.vmturbo.cloud.common.commitment.aggregator.ReservedInstanceAggregate;
import com.vmturbo.cloud.common.commitment.aggregator.ReservedInstanceAggregationInfo;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentEntityScope;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentLocation;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentLocationType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CloudCommitmentScope;

/**
 * A {@link CommitmentMatcher} implementation, responsible for generating {@link CoverageKey} instances
 * for cloud commitments covering compute entities.
 */
public class ComputeCommitmentMatcher implements CommitmentMatcher {

    private final Logger logger = LogManager.getLogger();

    private final CommitmentMatcherConfig matcherConfig;

    private ComputeCommitmentMatcher(@Nonnull CommitmentMatcherConfig matcherConfig) {
        this.matcherConfig = Objects.requireNonNull(matcherConfig);
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public Set<CoverageKey> createKeysForCommitment(@Nonnull CloudCommitmentAggregate commitmentAggregate) {

        Preconditions.checkNotNull(commitmentAggregate);

        final Set<CoverageKey> keySet = new HashSet<>();

        final ComputeCoverageKey.Builder keyBuilder = ComputeCoverageKey.builder();

        final CloudCommitmentLocation commitmentLocation = commitmentAggregate.aggregationInfo().location();
        if (commitmentLocation.getLocationType() == CloudCommitmentLocationType.REGION) {
            keyBuilder.regionOid(commitmentLocation.getLocationOid());
        } else if (commitmentLocation.getLocationType() == CloudCommitmentLocationType.AVAILABILITY_ZONE) {
            keyBuilder.zoneOid(commitmentLocation.getLocationOid());
        }

        if (commitmentAggregate.isReservedInstance()) {
            final ReservedInstanceAggregate riAggregate = commitmentAggregate.asReservedInstanceAggregate();
            final ReservedInstanceAggregationInfo riAggregateInfo = riAggregate.aggregationInfo();

            // set the tenancy
            keyBuilder.tenancy(riAggregateInfo.tenancy());

            // add tier info
            riAggregateInfo.tierInfo().tierFamily().ifPresent(keyBuilder::tierFamily);
            if (!riAggregateInfo.tierInfo().isSizeFlexible()) {
                keyBuilder.tierOid(riAggregateInfo.tierInfo().tierOid());
            }

            // add platform info
            if (!riAggregateInfo.platformInfo().isPlatformFlexible()) {
                keyBuilder.platform(riAggregateInfo.platformInfo().platform());
            }
        }

        final AggregationInfo aggregateInfo = commitmentAggregate.aggregationInfo();
        // Add account scope, if required
        final CloudCommitmentEntityScope commitmentScope = aggregateInfo.entityScope();
        if (matcherConfig.scope() == CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_BILLING_FAMILY_GROUP
                && commitmentScope.getScopeType() != CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_BILLING_FAMILY_GROUP ) {
            logger.error("Mismatch in scope configuration. Scope is configured to match against "
                            + "billing family, but commitment is not shared (Commitment Aggregate ID={})",
                    commitmentAggregate.aggregateId());
        } else {
            if (commitmentScope.getScopeType() == CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_BILLING_FAMILY_GROUP) {

                if (matcherConfig.scope() == CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_BILLING_FAMILY_GROUP) {
                    // Add the billing family ID
                    keyBuilder.billingFamilyId(commitmentScope.getGroupScope().getGroupId(0));
                } else {
                    keyBuilder.accountOid(aggregateInfo.purchasingAccountOid());
                }

                keySet.add(keyBuilder.build());
            } else { // must be account scoped RI for account matching
                commitmentScope.getEntityScope().getEntityOidList().forEach(scopedAccountOid -> {
                    keyBuilder.accountOid(scopedAccountOid);
                    keySet.add(keyBuilder.build());
                });
            }
        }

        return keySet;
    }

    /**
     * A factory class for creating {@link ComputeCommitmentMatcher} instances.
     */
    public static class ComputeCommitmentMatcherFactory {

        /**
         * Constructs and returns a new {@link ComputeCommitmentMatcher} instance, based on the provided
         * {@code matcherConfig}.
         * @param matcherConfig The matcher configuration.
         * @return The newly constructed {@link ComputeCommitmentMatcher} instance.
         */
        @Nonnull
        public ComputeCommitmentMatcher newMatcher(@Nonnull CommitmentMatcherConfig matcherConfig) {
            return new ComputeCommitmentMatcher(matcherConfig);
        }
    }
}
