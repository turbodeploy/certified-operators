package com.vmturbo.reserved.instance.coverage.allocator.matcher;

import java.util.List;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.common.commitment.CloudCommitmentResourceScope.ComputeTierResourceScope;
import com.vmturbo.cloud.common.commitment.aggregator.AggregationInfo;
import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregate;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentEntityScope;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentLocation;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentLocationType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CloudCommitmentScope;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

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

        final MutableComputeCoverageKey templateKey = MutableComputeCoverageKey.create();
        final AggregationInfo aggregationInfo = commitmentAggregate.aggregationInfo();

        final CloudCommitmentLocation commitmentLocation = aggregationInfo.location();
        if (commitmentLocation.getLocationType() == CloudCommitmentLocationType.REGION) {
            templateKey.setRegionOid(commitmentLocation.getLocationOid());
        } else if (commitmentLocation.getLocationType() == CloudCommitmentLocationType.AVAILABILITY_ZONE) {
            templateKey.setZoneOid(commitmentLocation.getLocationOid());
        }

        // assume compute resource scope for now
        final List<MutableComputeCoverageKey> resourceScopedKeyList =
                addComputeResourceScope((ComputeTierResourceScope)aggregationInfo.resourceScope(), templateKey);

        // Add account scope, if required
        final ImmutableSet.Builder<CoverageKey> completedKeys = ImmutableSet.builder();
        final CloudCommitmentEntityScope entityScope = aggregationInfo.entityScope();
        if (matcherConfig.scope() == CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_BILLING_FAMILY_GROUP
                && entityScope.getScopeType() != CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_BILLING_FAMILY_GROUP ) {
            logger.error("Mismatch in scope configuration. Scope is configured to match against "
                            + "billing family, but commitment is not shared (Commitment Aggregate ID={})",
                    commitmentAggregate.aggregateId());
        } else {
            if (entityScope.getScopeType() == CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_BILLING_FAMILY_GROUP) {

                if (matcherConfig.scope() == CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_BILLING_FAMILY_GROUP) {
                    // Add the billing family ID
                    resourceScopedKeyList.forEach(resourceScopedKey -> {
                        resourceScopedKey.setBillingFamilyId(entityScope.getGroupScope().getGroupId(0));
                        completedKeys.add(resourceScopedKey.toImmutable());
                    });
                } else {
                    resourceScopedKeyList.forEach(resourceScopedKey -> {
                        resourceScopedKey.setAccountOid(aggregationInfo.purchasingAccountOid());
                        completedKeys.add(resourceScopedKey.toImmutable());
                    });
                }
            } else { // must be account scoped RI for account matching
                entityScope.getEntityScope().getEntityOidList().forEach(scopedAccountOid ->
                    resourceScopedKeyList.forEach(resourceScopedKey -> {
                        // intentionally re-use each resource scoped key, instead of creating
                        // new MutableComputeCoverageKey instances
                        resourceScopedKey.setAccountOid(scopedAccountOid);
                        completedKeys.add(resourceScopedKey.toImmutable());
                    }));
            }
        }

        return completedKeys.build();
    }

    private List<MutableComputeCoverageKey> addComputeResourceScope(@Nonnull ComputeTierResourceScope resourceScope,
                                                                    @Nonnull MutableComputeCoverageKey coverageKeyTemplate) {


        // add tier info
        if (resourceScope.isSizeFlexible()) {
            coverageKeyTemplate.setTierFamily(resourceScope.computeTierFamily());
        } else {
            coverageKeyTemplate.setTierOid(resourceScope.computeTier());
        }

        if (!resourceScope.platformInfo().isPlatformFlexible()) {
            coverageKeyTemplate.setPlatform(resourceScope.platformInfo().platform());
        }

        final Set<Tenancy> supportedTenancies = resourceScope.tenancies().isEmpty()
                ? ImmutableSet.copyOf(Tenancy.values())
                : resourceScope.tenancies();

        return supportedTenancies.stream()
                .map(tenancy -> MutableComputeCoverageKey.create()
                        .from(coverageKeyTemplate)
                        .setTenancy(tenancy))
                .collect(ImmutableList.toImmutableList());

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
