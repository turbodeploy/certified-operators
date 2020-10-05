package com.vmturbo.reserved.instance.coverage.allocator.matcher;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregate;
import com.vmturbo.cloud.common.commitment.aggregator.ReservedInstanceAggregate;
import com.vmturbo.cloud.common.commitment.aggregator.ReservedInstanceAggregateInfo;
import com.vmturbo.common.protobuf.cloud.CloudCommitment.CloudCommitmentScope;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceScopeInfo;

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
        if (commitmentAggregate.isReservedInstance()) {
            final ReservedInstanceAggregate riAggregate = commitmentAggregate.asReservedInstanceAggregate();
            final ReservedInstanceAggregateInfo aggregateInfo = riAggregate.aggregateInfo();

            final ComputeCoverageKey.Builder keyBuilder = ComputeCoverageKey.builder()
                    .billingFamilyId(aggregateInfo.billingFamilyId())
                    .tenancy(aggregateInfo.tenancy())
                    .regionOid(aggregateInfo.regionOid())
                    .zoneOid(aggregateInfo.zoneOid());

            // add tier info
            aggregateInfo.tierInfo().tierFamily().ifPresent(keyBuilder::tierFamily);
            if (!aggregateInfo.tierInfo().isSizeFlexible()) {
                keyBuilder.tierOid(aggregateInfo.tierInfo().tierOid());
            }

            // add platform info
            if (!aggregateInfo.platformInfo().isPlatformFlexible()) {
                keyBuilder.platform(aggregateInfo.platformInfo().platform());
            }

            // Add scope info
            final ReservedInstanceScopeInfo scopeInfo = aggregateInfo.scopeInfo();

            // Add account scope, if required
            if (matcherConfig.scope() == CloudCommitmentScope.BILLING_FAMILY
                    && !scopeInfo.getShared()) {
                logger.error("Mismatch in scope configuration. Scope is configured to match against "
                        + "billing family, but commitment is not shared (Commitment Aggregate ID={})",
                        commitmentAggregate.aggregateId());
            } else {
                if (scopeInfo.getShared()) {
                    if (matcherConfig.scope() == CloudCommitmentScope.ACCOUNT) {
                        keyBuilder.accountOid(aggregateInfo.purchasingAccountOid());
                    }
                    keySet.add(keyBuilder.build());
                } else { // must be account scoped RI for account matching
                    scopeInfo.getApplicableBusinessAccountIdList().forEach(scopedAccountOid -> {
                        keyBuilder.accountOid(scopedAccountOid);
                        keySet.add(keyBuilder.build());
                    });
                }
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
